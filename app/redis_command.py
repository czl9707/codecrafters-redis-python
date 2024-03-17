from typing import Dict, List, Tuple, Type, Any
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from .redis_value import (
    RedisValue,
    RedisArray,
    RedisBulkStrings,
)
from .redis_cache import RedisCache

#     CACHE,
#     EXPIRE,
# )


class RedisCommand(ABC):
    _name2command: Dict[str, Type["RedisCommand"]] = {}

    name: str

    @staticmethod
    def from_redis_value(redis_value: RedisValue) -> "RedisCommand":
        assert isinstance(redis_value, RedisArray)
        for v in redis_value.serialize():
            assert isinstance(v, RedisBulkStrings)

        it = (v for v in redis_value.serialize())

        name: str = next(it).serialize().lower()
        CommandType = RedisCommand._name2command[name]

        args, kwargs = CommandType.parse_args(list(it))
        return CommandType(*args, **kwargs)

    @abstractmethod
    def execute(self) -> RedisValue: ...

    @staticmethod
    def parse_args(args: List[RedisBulkStrings]) -> Tuple[List[Any], Dict[str, Any]]:
        return args, {}

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

    @staticmethod
    def parse_args(args: List[RedisBulkStrings]) -> Tuple[List[Any], Dict[str, Any]]:
        parsed_args = []
        parsed_kwargs = {}
        it = iter(args)
        parsed_args.append(next(it))  # key
        parsed_args.append(next(it))  # value

        for arg in it:
            match arg.serialize().lower():
                case "px":
                    expire_ms = int(next(it).serialize())
                    parsed_kwargs["expiration"] = expire_ms
                case "p":
                    expire_s = int(next(it).serialize())
                    parsed_kwargs["expiration"] = expire_s * 1000
                case _:
                    pass

        return parsed_args, parsed_kwargs

    def __init__(
        self,
        key: RedisBulkStrings,
        value: RedisBulkStrings,
        expiration: int = -1,
    ) -> None:
        self.key = key
        self.value = value
        self.expiration = expiration

    def execute(self) -> RedisValue:
        RedisCache.set(
            self.key,
            self.value,
            (
                None
                if self.expiration <= 0
                else datetime.now() + timedelta(milliseconds=self.expiration)
            ),
        )

        return RedisValue.from_value("OK")


class GetCommand(RedisCommand):
    name = "get"

    def __init__(self, key: RedisBulkStrings) -> None:
        self.key = key

    def execute(self) -> RedisValue:
        try:
            return RedisCache.get(self.key)
        except KeyError:
            return RedisValue.from_value(None)
