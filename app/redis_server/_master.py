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
        self.replica_id = get_random_replication_id()
        self.replica_offset = 0
        self.registrated_replicas: Dict[str, ReplicaRecord] = {}

    @property
    def is_master(self) -> bool:
        return True

    async def registrate_replica(self, replica_record: ReplicaRecord) -> None:
        assert replica_record.replication_id is not None
        assert replica_record.replication_offset is not None
        self.registrated_replicas[replica_record.replication_id] = replica_record

        await replica_record.heart_beat()

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
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        value_reader = RedisValueReader(reader)
        session = ConnectionSession(value_reader, writer)
        try:
            async for redis_value in value_reader:
                command = RedisCommand.from_redis_value(redis_value)
                if command.is_write_command():
                    for replica in self.registrated_replicas.values():
                        replica.writer.write(command.deserialize())

                async for response_value in command.execute(self, session):
                    # print(f"sending response {response_value}")
                    writer.write(response_value.deserialize())
                    await writer.drain()

                if command.is_write_command():
                    await asyncio.gather(
                        *[
                            replica.writer.drain()
                            for replica in self.registrated_replicas.values()
                        ]
                    )
        except:
            print("close connection")
            writer.close()
            await writer.wait_closed()
