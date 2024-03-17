from typing import Dict, List, Type
from abc import ABC, abstractmethod

from .redis_value import (
    RedisValue,
    RedisArray,
    RedisBulkStrings,
)

CACHE = {}


class RedisCommand(ABC):
    _name2command: Dict[str, Type["RedisCommand"]] = {}

    name: str

    @staticmethod
    def from_redis_value(redis_value: RedisValue) -> "RedisCommand":
        assert isinstance(redis_value, RedisArray)
        for v in redis_value.serialize():
            assert isinstance(v, RedisBulkStrings)

        it = (v for v in redis_value.serialize())

        name: str = next(it).serialize()
        args: List[str] = list(it)
        print(args)
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
        return RedisValue.from_value("PONG")


class EchoCommand(RedisCommand):
    name = "echo"

    def __init__(self, content: RedisBulkStrings) -> None:
        self.content = content

    def execute(self) -> RedisValue:
        return self.content


class SetCommand(RedisCommand):
    name = "set"

    def __init__(self, key: RedisBulkStrings, value: RedisBulkStrings) -> None:
        self.key = key
        self.value = value

    def execute(self) -> RedisValue:
        global CACHE
        CACHE[self.key] = self.value
        return RedisValue.from_value("OK")


class GetCommand(RedisCommand):
    name = "get"

    def __init__(self, key: RedisBulkStrings) -> None:
        self.key = key

    def execute(self) -> RedisValue:
        global CACHE
        if self.key not in CACHE:
            return RedisBulkStrings.NULL()
        return CACHE[self.key]
