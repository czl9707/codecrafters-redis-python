from abc import ABC
from typing import Dict, List, Optional, Tuple, final
from datetime import datetime
import threading
import socket

from .redis_command import RedisCommand
from .redis_value import RedisBulkStrings, RedisValue

Address = Tuple[str, int]


@final
class RedisCache:
    CACHE: Dict[RedisBulkStrings, "RedisEntry"]
    is_master: bool
    master_addr: Optional[Address]
    master_replid: Optional[str]
    master_repl_offset: Optional[int]

    def __init__(self) -> None:
        self.is_master = True
        self.master_addr = None
        self.master_replid = None
        self.master_repl_offset = None

    def config(
        self,
        is_master: bool,
        master_addr: Optional[Address] = None,
    ):
        self.is_master = is_master
        self.master_addr = master_addr

    def boot(self, server_address: Address) -> None:
        if self.is_master:
            self.boot_master(server_address)
        else:
            self.boot_replica(server_address)

    def boot_master(self, server_address: Address) -> None:
        self.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.master_repl_offset = 0

        self.CACHE = {}
        server_socket = socket.create_server(server_address, reuse_port=True)

        while True:
            sock, ret_addr = server_socket.accept()
            t = threading.Thread(target=lambda: self._request_handler(sock))
            t.start()

    def boot_replica(self, server_address: Address) -> None:
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
                    RedisValue.from_value(str(server_address[1])),
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

    def _request_handler(self, sock: socket.socket) -> None:
        while True:
            request_bytes = sock.recv(1024)
            if not request_bytes:
                continue

            try:
                request_value = RedisValue.from_bytes(request_bytes)
                print(request_value)
                command = RedisCommand.from_redis_value(request_value)
            except:
                continue

            response_value = command.execute(self)
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
