from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Type, Any
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from .redis_value import (
    RedisSimpleString,
    RedisValue,
    RedisArray,
    RedisBulkStrings,
)

if TYPE_CHECKING:
    from .redis_server import RedisServer


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
    def execute(self, server: "RedisServer") -> RedisValue: ...

    @staticmethod
    def parse_args(args: List[RedisBulkStrings]) -> Tuple[List[Any], Dict[str, Any]]:
        return args, {}

    def __init_subclass__(cls) -> None:
        RedisCommand._name2command[cls.name] = cls


class PingCommand(RedisCommand):
    name = "ping"

    def __init__(self) -> None:
        return

    def execute(self, server: "RedisServer") -> RedisValue:
        return RedisValue.from_value("PONG")


class EchoCommand(RedisCommand):
    name = "echo"

    def __init__(self, content: RedisBulkStrings) -> None:
        self.content = content

    def execute(self, server: "RedisServer") -> RedisValue:
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

    def execute(self, server: "RedisServer") -> RedisValue:
        server.set(
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

    def execute(self, server: "RedisServer") -> RedisValue:
        try:
            return server.get(self.key)
        except KeyError:
            return RedisValue.from_value(None)


class InfoCommand(RedisCommand):
    name = "info"

    def __init__(self, arg: RedisBulkStrings) -> None:
        self.arg = arg.serialize().lower()

    def execute(self, server: "RedisServer") -> RedisValue:
        if self.arg == "replication":
            pairs = {}
            if server.is_master:
                pairs["role"] = "master"
                pairs["master_replid"] = server.master_replid
                pairs["master_repl_offset"] = server.master_repl_offset
            else:
                pairs["role"] = "slave"

            return RedisValue.from_value(
                "\r\n".join(f"{key}:{value}" for key, value in pairs.items())
            )
        else:
            return RedisValue.from_value(None)


class ReplConfCommand(RedisCommand):
    name = "replconf"

    @staticmethod
    def parse_args(args: List[RedisBulkStrings]) -> Tuple[List[Any], Dict[str, Any]]:
        parsed_kwargs = {
            "capabilities": [],
        }
        it = iter(args)

        for arg in it:
            match arg.serialize().lower():
                case "listening-port":
                    port = int(next(it).serialize())
                    parsed_kwargs["listening_port"] = port
                case "capa":
                    capa = next(it).serialize()
                    parsed_kwargs["capabilities"].append(capa)
                case _:
                    pass

        return [], parsed_kwargs

    def __init__(
        self,
        listening_port: Optional[int] = None,
        capabilities: List[str] = [],
    ) -> None:
        self.listening_port = listening_port
        self.capabilities = capabilities

    def execute(self, server: "RedisServer") -> RedisValue:
        # ok = RedisSimpleString()
        # ok.value = "OK"
        # return ok
        return RedisValue.from_value("OK")
