from typing import Dict, List, Type
from abc import ABC, abstractmethod

from .redis_value import (
    RedisSimpleString,
    RedisValue,
    RedisArray,
    RedisBulkStrings,
)


class RedisCommand(ABC):
    _name2command: Dict[str, Type["RedisCommand"]] = {}

    @property
    @abstractmethod
    def name(self) -> str: ...

    @staticmethod
    def from_redis_value(redis_value: RedisValue) -> "RedisCommand":
        assert isinstance(redis_value, RedisArray)
        for v in redis_value.value:
            assert isinstance(v, RedisBulkStrings)

        it = (v.value for v in redis_value.value)

        name: str = next(it)
        args: List[str] = list(it)

        return RedisCommand._name2command[name](*args)

    @abstractmethod
    def execute(self) -> RedisValue: ...

    def __init_subclass__(cls) -> None:
        RedisCommand._name2command[cls.name] = cls


class PingCommand(RedisCommand):
    name = "ping"

    def __init__(self) -> None:
        return

    def execute(self) -> RedisValue:
        return RedisSimpleString("PONG")


class EchoCommand(RedisCommand):
    name = "echo"

    def __init__(self, content: str) -> None:
        self.content = content

    def execute(self) -> RedisValue:
        return RedisBulkStrings(self.content)
