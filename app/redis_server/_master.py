from typing import Dict
import asyncio

from ..helper import get_random_replication_id
from ..redis_commands import RedisCommand
from ..redis_values import RedisValueReader, RedisValue
from ._base import (
    RedisServer,
    Address,
    ConnectionSession,
    ReplicaRecord,
)


class MasterServer(RedisServer):
    def __init__(self, server_addr: Address) -> None:
        super().__init__(server_addr)
        self.master_replid = get_random_replication_id()
        self.master_repl_offset = 0
        self.registrated_replicas: Dict[str, ReplicaRecord] = {}

    @property
    def is_master(self) -> bool:
        return True

    def registrate_replica(self, replica_record: ReplicaRecord) -> None:
        self.registrated_replicas[replica_record.replication_id] = replica_record

    async def boot(self) -> None:
        server = await asyncio.start_server(
            client_connected_cb=self._request_handler,
            host=self.server_addr[0],
            port=self.server_addr[1],
            reuse_port=True,
        )

        self.server = server
        async with server:
            await server.serve_forever()

    async def _request_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        session = ConnectionSession(reader, writer)
        try:
            async for redis_value in RedisValueReader(reader):
                command = RedisCommand.from_redis_value(redis_value)
                if command.is_write_command() and self.registrated_replicas:
                    # use the first replica for now
                    replica = list(self.registrated_replicas.values())[0]

                    replica.writer.write(command.deserialize())
                    writer.write(RedisValue.from_value("OK").deserialize())
                    await replica.writer.drain()
                    await writer.drain()

                for response_value in command.execute(self, session):
                    # print(f"sending response {response_value}")
                    writer.write(response_value.deserialize())
                    await writer.drain()

        finally:
            print("close connection")
            writer.close()
            await writer.wait_closed()
