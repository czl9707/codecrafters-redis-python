from typing import TYPE_CHECKING, Dict, Iterator, Self, Type
from abc import ABC, abstractmethod

from ..redis_values import (
    RedisValue,
    RedisArray,
    RedisBulkStrings,
)

if TYPE_CHECKING:
    from ..redis_server import RedisServer, ConnectionSession


class RedisCommand(ABC):
    _name2command: Dict[str, Type["RedisCommand"]] = {}

    name: str

    def deserialize(self) -> bytes:
        return self.as_redis_value().deserialize()

    @staticmethod
    @abstractmethod
    def from_redis_value(redis_value: RedisArray) -> Self:
        assert isinstance(redis_value, RedisArray)
        for v in redis_value.serialize():
            assert isinstance(v, RedisBulkStrings)

        it = iter(redis_value.value)
        name: str = next(it).serialize().lower()
        CommandType = RedisCommand._name2command[name]

        return CommandType.from_redis_value(it)

    @abstractmethod
    def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> Iterator[RedisValue]: ...

    @abstractmethod
    def as_redis_value(self) -> RedisValue: ...

    def __init_subclass__(cls) -> None:
        RedisCommand._name2command[cls.name] = cls

    @classmethod
    def is_write_command(cls) -> bool:
        if hasattr(cls, "is_write"):
            return getattr(cls, "is_write")
        return False


def write(cls: Type[RedisCommand]) -> Type[RedisCommand]:
    cls.is_write = True
    return cls
