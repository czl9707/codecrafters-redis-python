from typing import TYPE_CHECKING, Dict, List, Tuple, Type, Any
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from .redis_value import (
    RedisValue,
    RedisArray,
    RedisBulkStrings,
)

if TYPE_CHECKING:
    from .redis_cache import RedisCache


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
    def execute(self, redis_cache: "RedisCache") -> RedisValue: ...

    @staticmethod
    def parse_args(args: List[RedisBulkStrings]) -> Tuple[List[Any], Dict[str, Any]]:
        return args, {}

    def __init_subclass__(cls) -> None:
        RedisCommand._name2command[cls.name] = cls


class PingCommand(RedisCommand):
    name = "ping"

    def __init__(self) -> None:
        return

    def execute(self, redis_cache: "RedisCache") -> RedisValue:
        return RedisValue.from_value("PONG")


class EchoCommand(RedisCommand):
    name = "echo"

    def __init__(self, content: RedisBulkStrings) -> None:
        self.content = content

    def execute(self, redis_cache: "RedisCache") -> RedisValue:
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

    def execute(self, redis_cache: "RedisCache") -> RedisValue:
        redis_cache.set(
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

    def execute(self, redis_cache: "RedisCache") -> RedisValue:
        try:
            return redis_cache.get(self.key)
        except KeyError:
            return RedisValue.from_value(None)


class InfoCommand(RedisCommand):
    name = "info"

    def __init__(self, arg: RedisBulkStrings) -> None:
        self.arg = arg

    def execute(self, redis_cache: "RedisCache") -> RedisValue:
        if self.arg.serialize().lower() == "replication":
            pairs = {}
            pairs["role"] = "master" if redis_cache.is_master else "slave"
            if redis_cache.is_master:
                pairs["master_replid"] = redis_cache.master_replid
                pairs["master_repl_offset"] = redis_cache.master_repl_offset

            return RedisValue.from_value(
                "\r\n".join(f"{key}:{value}" for key, value in pairs.items())
            )
        else:
            return RedisValue.from_value(None)
