from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple, Type, Any
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from .redis_value import (
    RedisValue,
    RedisArray,
    RedisBulkStrings,
)

if TYPE_CHECKING:
    from .redis_server import RedisServer, MasterServer

EMPTYRDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="


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
    def execute(self, server: "RedisServer") -> Iterator[RedisValue]: ...

    @staticmethod
    def parse_args(args: List[RedisBulkStrings]) -> Tuple[List[Any], Dict[str, Any]]:
        return args, {}

    def __init_subclass__(cls) -> None:
        RedisCommand._name2command[cls.name] = cls


class PingCommand(RedisCommand):
    name = "ping"

    def __init__(self) -> None:
        return

    def execute(self, server: "RedisServer") -> Iterator[RedisValue]:
        yield RedisValue.from_value("PONG")


class EchoCommand(RedisCommand):
    name = "echo"

    def __init__(self, content: RedisBulkStrings) -> None:
        self.content = content

    def execute(self, server: "RedisServer") -> Iterator[RedisValue]:
        yield self.content


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

    def execute(self, server: "RedisServer") -> Iterator[RedisValue]:
        server.set(
            self.key,
            self.value,
            (
                None
                if self.expiration <= 0
                else datetime.now() + timedelta(milliseconds=self.expiration)
            ),
        )

        yield RedisValue.from_value("OK")


class GetCommand(RedisCommand):
    name = "get"

    def __init__(self, key: RedisBulkStrings) -> None:
        self.key = key

    def execute(self, server: "RedisServer") -> Iterator[RedisValue]:
        try:
            yield server.get(self.key)
        except KeyError:
            yield RedisValue.from_value(None)


class InfoCommand(RedisCommand):
    name = "info"

    def __init__(self, arg: RedisBulkStrings) -> None:
        self.arg = arg.serialize().lower()

    def execute(self, server: "RedisServer") -> Iterator[RedisValue]:
        if self.arg == "replication":
            pairs = {}
            if server.is_master:
                pairs["role"] = "master"
                pairs["master_replid"] = server.master_replid
                pairs["master_repl_offset"] = server.master_repl_offset
            else:
                pairs["role"] = "slave"

            yield RedisValue.from_value(
                "\r\n".join(f"{key}:{value}" for key, value in pairs.items())
            )
        else:
            yield RedisValue.from_value(None)


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

    def execute(self, server: "MasterServer") -> Iterator[RedisValue]:
        assert server.is_master

        yield RedisValue.from_value("OK")


class PsyncCommand(RedisCommand):
    name = "psync"

    def __init__(
        self,
        replication_id: RedisBulkStrings,
        replication_offset: RedisBulkStrings,
    ) -> None:
        self.replication_id = replication_id.serialize()
        self.replication_offset = int(replication_offset.serialize())

    def execute(self, server: "MasterServer") -> Iterator[RedisValue]:
        assert server.is_master

        replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        offset = server.master_repl_offset
        server.master_repl_offset += 1

        yield RedisValue.from_value(f"FULLRESYNC {replication_id} {offset}")
        yield RedisValue.from_value(EMPTYRDB)
