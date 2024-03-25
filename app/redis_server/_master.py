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
    ServerConfig,
)
from ._db_parser import DatabaseParser


class MasterServer(RedisServer):
    def __init__(self, server_addr: Address, config: ServerConfig) -> None:
        super().__init__(server_addr, config)
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
                # not sure how master_replica_offset used ???
                # self.replica_offset += redis_value.bytes_size
                command = RedisCommand.from_redis_value(redis_value)

                if command.is_write_command():
                    write_tasks = [
                        replica.write(command.deserialize())
                        for replica in self.registrated_replicas.values()
                    ]

                async for response_value in command.execute(self, session):
                    # print(f"sending response {response_value}")
                    writer.write(response_value.deserialize())
                    await writer.drain()

                if command.is_write_command():
                    await asyncio.gather(*write_tasks)
        except Exception as e:
            print(f"close connection {e}")
            writer.close()
            await writer.wait_closed()
