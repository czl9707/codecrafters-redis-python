from abc import ABC
from typing import Dict, List, Optional, Tuple, final
from datetime import datetime

from .redis_value import RedisBulkStrings, RedisValue


class RedisEntry:
    def __init__(
        self,
        value: RedisValue,
        expiration: Optional[datetime],
    ) -> None:
        self.value = value
        self.expiration = expiration


@final
class RedisCache:
    _instance: Optional["RedisCache"] = None

    CACHE: Dict[RedisBulkStrings, RedisEntry]
    is_master: bool
    booted: bool
    master_url: Optional[str]
    master_port: Optional[int]
    master_replid: Optional[str]
    master_repl_offset: Optional[int]

    def __new__(cls):
        if cls._instance is None:
            instance = super(RedisCache, cls).__new__(cls)
            instance.is_master = True
            instance.booted = False
            instance.master_url = None
            instance.master_port = None
            instance.master_replid = None
            instance.master_repl_offset = None

            cls._instance = instance
        return cls._instance

    def config(
        self,
        is_master: bool,
        master_url: Optional[str] = None,
        master_port: Optional[int] = None,
    ):
        if self.booted:
            raise Exception("RedisCache can only be configed before boot.")

        self.is_master = is_master
        self.master_url = master_url
        self.master_port = master_port

    def boot(self) -> None:
        if self.booted:
            raise Exception("RedisCache can only be booted once.")
        self.booted = True

        if self.is_master:
            self.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
            self.master_repl_offset = 0

        self.CACHE = {}

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
