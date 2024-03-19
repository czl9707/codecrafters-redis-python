from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple
from datetime import datetime
import asyncio
import socket

from .expiration_policy import ExpirationPolicy
from .redis_command import PingCommand, PsyncCommand, RedisCommand, ReplConfCommand
from .redis_value import RedisBulkStrings, RedisValue

Address = Tuple[str, int]


class RedisServer(ABC):
    CACHE: Dict[RedisBulkStrings, "RedisEntry"]

    def __init__(self, server_addr: Address) -> None:
        self.server_addr = server_addr
        self.CACHE = {}

    @property
    @abstractmethod
    def is_master(self) -> bool: ...

    async def boot(self) -> None:
        server = await asyncio.start_server(
            client_connected_cb=self._request_handler,
            host=self.server_addr[0],
            port=self.server_addr[1],
            reuse_port=True,
        )

        async with server:
            await server.serve_forever()

    async def _request_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_bytes = await reader.read(1024)
            if not request_bytes:
                return
            print(f"request: {request_bytes}")

            command = RedisCommand.from_bytes(request_bytes)
            for response_value in command.execute(self):
                # print(f"response: {response_value}")
                writer.write(response_value.deserialize())
                await writer.drain()
        finally:
            writer.close()

    # cache operation
    def get(self, key: RedisBulkStrings) -> RedisBulkStrings:
        self.validate_entry(key)
        return self.CACHE[key].value

    def set(
        self,
        key: RedisBulkStrings,
        value: RedisValue,
        expiration: Optional[datetime] = None,
    ) -> None:
        self.CACHE[key] = RedisEntry(
            value=value,
            expiration=expiration,
        )

    def validate_entry(self, key: RedisBulkStrings) -> None:
        entry = self.CACHE[key]
        if ExpirationPolicy.is_expired(entry):
            self.CACHE.pop(key)

    def validate_all_entries(self):
        expired = []
        for key, entry in self.CACHE.items():
            if ExpirationPolicy.is_expired(entry):
                expired.append(key)

        for key in expired:
            self.CACHE.pop(key)


class MasterServer(RedisServer):
    def __init__(self, server_addr: Address) -> None:
        super().__init__(server_addr)
        self.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.master_repl_offset = 0

    @property
    def is_master(self) -> bool:
        return True


class ReplicaServer(RedisServer):
    def __init__(self, server_addr: Address, master_addr: Address) -> None:
        super().__init__(server_addr)
        self.master_addr = master_addr

    @property
    def is_master(self) -> bool:
        return False

    async def boot(self) -> None:
        master_socket = socket.create_connection(self.master_addr)

        # PING
        master_socket.send(PingCommand().deserialize())
        response = master_socket.recv(1024)
        if not response:
            raise Exception("master not respond to PING request")

        # REPLCONF
        master_socket.send(
            ReplConfCommand(listening_port=self.server_addr[1]).deserialize()
        )
        response = master_socket.recv(1024)
        if response and RedisValue.from_bytes(response).serialize() != "OK":
            raise Exception("master not respond to REPLCONF request with OK")
        master_socket.send(ReplConfCommand(capabilities=["psync2"]).deserialize())
        if response and RedisValue.from_bytes(response).serialize() != "OK":
            raise Exception("master not respond to REPLCONF request with OK")

        # PSYNC
        master_socket.send(PsyncCommand("?", -1).deserialize())
        response = master_socket.recv(1024)
        if not response:
            raise Exception("master not respond to PSYNC request")

        await super().boot()


class RedisEntry:
    def __init__(
        self,
        value: RedisValue,
        expiration: Optional[datetime],
    ) -> None:
        self.value = value
        self.expiration = expiration
