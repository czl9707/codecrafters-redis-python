from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Self, Type
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from .redis_value import (
    RedisRDBFile,
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
    def from_bytes(b: bytes) -> "RedisCommand":
        redis_value = RedisValue.from_bytes(b)
        assert isinstance(redis_value, RedisArray)
        for v in redis_value.serialize():
            assert isinstance(v, RedisBulkStrings)

        return RedisCommand.from_redis_value(redis_value.value)

    def deserialize(self) -> bytes:
        return self.as_redis_value().deserialize()

    @staticmethod
    @abstractmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        it = iter(args)

        name: str = next(it).serialize().lower()
        CommandType = RedisCommand._name2command[name]

        return CommandType.from_redis_value(it)

    @abstractmethod
    def execute(self, server: "RedisServer") -> Iterator[RedisValue]: ...

    @abstractmethod
    def as_redis_value(self) -> RedisValue: ...

    def __init_subclass__(cls) -> None:
        RedisCommand._name2command[cls.name] = cls


class PingCommand(RedisCommand):
    name = "ping"

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        return PingCommand()

    def execute(self, server: "RedisServer") -> Iterator[RedisValue]:
        yield RedisBulkStrings.from_value("PONG")

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
            ]
        )


class EchoCommand(RedisCommand):
    name = "echo"

    def __init__(self, content: RedisBulkStrings) -> None:
        self.content = content

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        return EchoCommand(next(args))

    def execute(self, server: "RedisServer") -> Iterator[RedisValue]:
        yield self.content

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                self.content,
            ]
        )


class SetCommand(RedisCommand):
    name = "set"

    def __init__(
        self,
        key: RedisBulkStrings,
        value: RedisBulkStrings,
        expiration: int = -1,
    ) -> None:
        self.key = key
        self.value = value
        self.expiration = expiration

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        kwargs = {}
        kwargs["key"] = next(args)
        kwargs["value"] = next(args)

        for arg in args:
            match arg.serialize().lower():
                case "px":
                    expire_ms = int(next(args).serialize())
                    kwargs["expiration"] = expire_ms
                case "p":
                    expire_s = int(next(args).serialize())
                    kwargs["expiration"] = expire_s * 1000
                case _:
                    raise Exception(f"Unknown arg: {arg.serialize()}")

        return SetCommand(**kwargs)

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

        yield RedisBulkStrings.from_value("OK")

    def as_redis_value(self) -> RedisValue:
        s = [
            RedisBulkStrings.from_value(self.name),
            self.key,
            self.value,
        ]

        if self.expiration > 0:
            s.append(RedisBulkStrings.from_value("px"))
            s.append(RedisBulkStrings.from_value(str(self.expiration)))

        return RedisArray.from_value(s)


class GetCommand(RedisCommand):
    name = "get"

    def __init__(self, key: RedisBulkStrings) -> None:
        self.key = key

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        return GetCommand(next(args))

    def execute(self, server: "RedisServer") -> Iterator[RedisValue]:
        try:
            yield server.get(self.key)
        except KeyError:
            yield RedisBulkStrings.from_value(None)

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                self.key,
            ]
        )


class InfoCommand(RedisCommand):
    name = "info"

    def __init__(self, arg: str) -> None:
        self.arg = arg

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        return InfoCommand(next(args).serialize().lower())

    def execute(self, server: "RedisServer") -> Iterator[RedisValue]:
        if self.arg == "replication":
            pairs = {}
            if server.is_master:
                pairs["role"] = "master"
                pairs["master_replid"] = server.master_replid
                pairs["master_repl_offset"] = server.master_repl_offset
            else:
                pairs["role"] = "slave"

            yield RedisBulkStrings.from_value(
                "\r\n".join(f"{key}:{value}" for key, value in pairs.items())
            )
        else:
            yield RedisBulkStrings.from_value(None)

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                self.arg,
            ]
        )


class ReplConfCommand(RedisCommand):
    name = "replconf"

    def __init__(
        self,
        listening_port: Optional[int] = None,
        capabilities: List[str] = [],
    ) -> None:
        self.listening_port = listening_port
        self.capabilities = capabilities

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        kwargs = {
            "capabilities": [],
        }

        for arg in args:
            match arg.serialize().lower():
                case "listening-port":
                    port = int(next(args).serialize())
                    kwargs["listening_port"] = port
                case "capa":
                    capa = next(args).serialize()
                    kwargs["capabilities"].append(capa)
                case _:
                    pass

        return ReplConfCommand(**kwargs)

    def execute(self, server: "MasterServer") -> Iterator[RedisValue]:
        assert server.is_master

        yield RedisBulkStrings.from_value("OK")

    def as_redis_value(self) -> RedisValue:
        s = [RedisBulkStrings.from_value(self.name)]

        if self.listening_port is not None:
            s.append(RedisBulkStrings.from_value("listening-port"))
            s.append(RedisBulkStrings.from_value(str(self.listening_port)))
        for capa in self.capabilities:
            s.append(RedisBulkStrings.from_value("capa"))
            s.append(RedisBulkStrings.from_value(capa))

        return RedisArray.from_value(s)


class PsyncCommand(RedisCommand):
    name = "psync"

    def __init__(
        self,
        replication_id: str,
        replication_offset: int,
    ) -> None:
        self.replication_id = replication_id
        self.replication_offset = replication_offset

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        return PsyncCommand(next(args).serialize(), int(next(args).serialize()))

    def execute(self, server: "MasterServer") -> Iterator[RedisValue]:
        assert server.is_master

        replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        offset = server.master_repl_offset
        server.master_repl_offset += 1

        yield RedisBulkStrings.from_value(f"FULLRESYNC {replication_id} {offset}")
        file = RedisRDBFile(EMPTYRDB)
        yield file

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                RedisBulkStrings.from_value(self.replication_id),
                RedisBulkStrings.from_value(str(self.replication_offset)),
            ]
        )
