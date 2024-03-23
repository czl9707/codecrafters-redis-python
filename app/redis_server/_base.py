from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple
from datetime import datetime
import asyncio

from ._expiration_policy import ExpirationPolicy
from ..redis_values import RedisBulkStrings, RedisValue, RedisValueReader
from ..redis_commands import ReplConfCommand, RedisCommand


Address = Tuple[str, int]

BUFFER_SIZE = 1024


class RedisServer(ABC):
    CACHE: Dict[RedisBulkStrings, "RedisEntry"]

    def __init__(self, server_addr: Address) -> None:
        self.server_addr = server_addr
        self.CACHE = {}

    @property
    @abstractmethod
    def is_master(self) -> bool: ...

    @abstractmethod
    async def boot(self) -> None: ...

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


class ConnectionSession:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self._replica_record = None

    @property
    def replica_record(self) -> "ReplicaRecord":
        if self._replica_record is None:
            self._replica_record = ReplicaRecord(self.reader, self.writer)
        return self._replica_record


class ReplicaRecord:
    __slot__ = [
        "reader",
        "writer",
        "replication_id",
        "replication_offset",
        "listening_port",
        "capabilities",
    ]

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.replication_id = None
        self.replication_offset = None
        self.listening_port = None
        self.capabilities = set()

    async def heart_beat(self) -> None:
        redis_value_reader = RedisValueReader(self.reader)
        try:
            while True:
                await asyncio.sleep(1)

                self.writer.write(ReplConfCommand(get_ack=True).deserialize())
                await self.writer.drain()

                ack_response_command = RedisCommand.from_redis_value(
                    await redis_value_reader.read()
                )

                assert isinstance(ack_response_command, ReplConfCommand)
                assert ack_response_command.ack_offset >= self.replication_offset
                self.replication_offset = ack_response_command.ack_offset
        except Exception as e:
            print(f"replica closed: {e}")
            self.writer.close()
            await self.writer.wait_closed()
