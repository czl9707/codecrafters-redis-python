from abc import ABC, abstractmethod
from typing import Dict, List, Optional, final
from datetime import datetime
from time import sleep

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
    CACHE: Dict[RedisBulkStrings, RedisEntry] = {}

    @staticmethod
    def get(key: RedisBulkStrings) -> RedisBulkStrings:
        RedisCache.validate_entry(key)
        return RedisCache.CACHE[key].value

    @staticmethod
    def set(
        key: RedisBulkStrings, value: RedisValue, expiration: Optional[datetime] = None
    ) -> None:
        RedisCache.CACHE[key] = RedisEntry(
            value=value,
            expiration=expiration,
        )

    @staticmethod
    def validate_entry(key: RedisBulkStrings) -> None:
        entry = RedisCache.CACHE[key]
        if ExpirationPolicy.is_expired(entry):
            RedisCache.CACHE.pop(key)

    @staticmethod
    def validate_all_entries():
        expired = []
        for key, entry in RedisCache.CACHE.items():
            if ExpirationPolicy.is_expired(entry):
                expired.append(key)

        for key in expired:
            RedisCache.CACHE.pop(key)


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
