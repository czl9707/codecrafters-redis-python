import asyncio

from ..redis_values import (
    RedisValue,
    RedisValueReader,
)
from ..redis_commands import (
    RedisCommand,
    PingCommand,
    PsyncCommand,
    ReplConfCommand,
)
from ._base import (
    RedisServer,
    Address,
    ConnectionSession,
)


class ReplicaServer(RedisServer):
    def __init__(self, server_addr: Address, master_addr: Address) -> None:
        super().__init__(server_addr)
        self.master_addr = master_addr
        self.replica_id = None
        self.replica_offset = None

    @property
    def is_master(self) -> bool:
        return False

    async def boot(self) -> None:
        master_reader, master_writer = await asyncio.open_connection(
            self.master_addr[0], self.master_addr[1]
        )

        await self._handshake(master_reader, master_writer)
        asyncio.create_task(self._master_request_handler(master_reader, master_writer))

        server = await asyncio.start_server(
            client_connected_cb=self._server_request_handler,
            host=self.server_addr[0],
            port=self.server_addr[1],
            reuse_port=True,
        )
        self.server = server
        async with server:
            await server.serve_forever()

    async def _handshake(
        self, master_reader: asyncio.StreamReader, master_writer: asyncio.StreamWriter
    ) -> None:
        # PING
        master_redis_value_reader = RedisValueReader(master_reader)

        master_writer.write(PingCommand().deserialize())
        await master_writer.drain()
        response_value = await master_redis_value_reader.read()
        if not response_value:
            raise Exception("master not respond to PING request")

        # REPLCONF
        master_writer.write(
            ReplConfCommand(listening_port=self.server_addr[1]).deserialize()
        )
        await master_writer.drain()
        response_value = await master_redis_value_reader.read()
        if response_value and response_value.serialize() != "OK":
            raise Exception("master not respond to REPLCONF request with OK")

        master_writer.write(ReplConfCommand(capabilities=["psync2"]).deserialize())
        await master_writer.drain()
        response_value = await master_redis_value_reader.read()
        if response_value and response_value.serialize() != "OK":
            raise Exception("master not respond to REPLCONF request with OK")

        # PSYNC
        master_writer.write(PsyncCommand("?", -1).deserialize())
        await master_writer.drain()
        response_value = await master_redis_value_reader.read()
        if not response_value:
            raise Exception("master not respond to PSYNC request")
        else:
            replica_info = response_value.serialize()
            if not isinstance(replica_info, str):
                raise Exception("master respond to PSYNC request in a wrong format")
            _, replica_id, offset = replica_info.split(" ")
            self.replica_id = replica_id
            self.replica_offset = int(offset)

        response_value = await master_redis_value_reader.read()
        print("I guess I received RDB File!")

    async def _server_request_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        session = ConnectionSession(reader, writer)
        try:
            async for redis_value in RedisValueReader(reader):
                command = RedisCommand.from_redis_value(redis_value)
                async for response_value in command.execute(self, session):
                    writer.write(response_value.deserialize())
                    await writer.drain()
        except:
            writer.close()
            await writer.wait_closed()

    async def _master_request_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            async for redis_value in RedisValueReader(reader):
                command = RedisCommand.from_redis_value(redis_value)
                if command.is_replica_reply_command():
                    async for response in command.execute(self, None):
                        writer.write(response.deserialize())
                    await writer.drain()
                else:
                    for _ in command.execute(self, None):
                        continue

        except Exception as e:
            print(f"lost connection to master: {e}")
            writer.close()
            await writer.wait_closed()
        finally:
            self.server.close()
            await self.server.wait_closed()
