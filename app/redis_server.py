from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, final
from datetime import datetime
import threading
import socket

from .redis_command import RedisCommand
from .redis_value import RedisBulkStrings, RedisValue

Address = Tuple[str, int]


class RedisServer:
    CACHE: Dict[RedisBulkStrings, "RedisEntry"]

    def __init__(self, server_addr: Address) -> None:
        self.server_addr = server_addr
        self.CACHE = {}

    @abstractmethod
    def boot(self) -> None: ...

    @property
    @abstractmethod
    def is_master(self) -> bool: ...

    def _request_handler(self, sock: socket.socket) -> None:
        while True:
            request_bytes = sock.recv(1024)
            if not request_bytes:
                continue

            try:
                request_value = RedisValue.from_bytes(request_bytes)
                # print(f"request: {request_value}")
                command = RedisCommand.from_redis_value(request_value)
            except:
                continue

            for response_value in command.execute(self):
                # print(f"response: {response_value}")
                sock.send(response_value.deserialize())

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

    @property
    def is_master(self) -> bool:
        return True

    def boot(self) -> None:
        self.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.master_repl_offset = 0

        server_socket = socket.create_server(self.server_addr, reuse_port=True)
        while True:
            sock, ret_addr = server_socket.accept()
            t = threading.Thread(target=lambda: self._request_handler(sock))
            t.start()


class ReplicaServer(RedisServer):
    def __init__(self, server_addr: Address, master_addr: Address) -> None:
        super().__init__(server_addr)
        self.master_addr = master_addr

    @property
    def is_master(self) -> bool:
        return False

    def boot(self) -> None:
        master_socket = socket.create_connection(self.master_addr)

        # PING
        master_socket.send(
            RedisValue.from_value(
                [
                    RedisValue.from_value("ping"),
                ]
            ).deserialize()
        )
        response = master_socket.recv(1024)
        if not response:
            raise Exception("master not respond to PING request")

        # REPLCONF
        master_socket.send(
            RedisValue.from_value(
                [
                    RedisValue.from_value("replconf"),
                    RedisValue.from_value("listening-port"),
                    RedisValue.from_value(str(self.server_addr[1])),
                ]
            ).deserialize()
        )
        response = master_socket.recv(1024)
        if response and RedisValue.from_bytes(response).serialize() != "OK":
            raise Exception("master not respond to REPLCONF request with OK")
        master_socket.send(
            RedisValue.from_value(
                [
                    RedisValue.from_value("replconf"),
                    RedisValue.from_value("capa"),
                    RedisValue.from_value("psync2"),
                ]
            ).deserialize()
        )
        if response and RedisValue.from_bytes(response).serialize() != "OK":
            raise Exception("master not respond to REPLCONF request with OK")

        # PSYNC
        master_socket.send(
            RedisValue.from_value(
                [
                    RedisValue.from_value("psync"),
                    RedisValue.from_value("?"),
                    RedisValue.from_value("-1"),
                ]
            ).deserialize()
        )
        response = master_socket.recv(1024)
        if not response:
            raise Exception("master not respond to PSYNC request")

        server_socket = socket.create_server(self.server_addr, reuse_port=True)
        while True:
            sock, ret_addr = server_socket.accept()
            t = threading.Thread(target=lambda: self._request_handler(sock))
            t.start()


class RedisEntry:
    def __init__(
        self,
        value: RedisValue,
        expiration: Optional[datetime],
    ) -> None:
        self.value = value
        self.expiration = expiration


class ExpirationPolicy(ABC):
    ALL: List["ExpirationPolicy"] = []

    @staticmethod
    def is_expired(entry: RedisEntry) -> bool:
        return any(policy.is_expired(entry) for policy in ExpirationPolicy.ALL)

    def __init_subclass__(cls) -> None:
        ExpirationPolicy.ALL.append(cls)


class EndOfLifePolicy(ExpirationPolicy):
    @staticmethod
    def is_expired(entry: RedisEntry) -> bool:
        if entry.expiration is not None and entry.expiration < datetime.now():
            return True
        return False
