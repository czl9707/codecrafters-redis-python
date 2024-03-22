from typing import TYPE_CHECKING, Iterator, Optional, Self, Set
from datetime import datetime, timedelta

from ..helper import get_random_replication_id
from ..redis_values import (
    RedisRDBFile,
    RedisValue,
    RedisArray,
    RedisBulkStrings,
)
from ._base import RedisCommand, write

if TYPE_CHECKING:
    from ..redis_server import RedisServer, MasterServer, ConnectionSession


EMPTYRDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="


class PingCommand(RedisCommand):
    name = "ping"

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        return PingCommand()

    def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> Iterator[RedisValue]:
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

    def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> Iterator[RedisValue]:
        yield self.content

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                self.content,
            ]
        )


@write
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

    def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> Iterator[RedisValue]:
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

    def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> Iterator[RedisValue]:
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

    def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> Iterator[RedisValue]:
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
        capabilities: Optional[Set[str]] = None,
        listening_port: Optional[int] = None,
    ) -> None:
        self.listening_port = listening_port
        self.capabilities = capabilities if capabilities is not None else set()

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        kwargs = {
            "capabilities": set(),
        }

        for arg in args:
            match arg.serialize().lower():
                case "listening-port":
                    port = int(next(args).serialize())
                    kwargs["listening_port"] = port
                case "capa":
                    capa = next(args).serialize()
                    kwargs["capabilities"].add(capa)
                case _:
                    pass

        return ReplConfCommand(**kwargs)

    def execute(
        self, server: "MasterServer", session: "ConnectionSession"
    ) -> Iterator[RedisValue]:
        assert server.is_master

        for capa in self.capabilities:
            session.replica_record.capabilities.add(capa)
        if self.listening_port is not None:
            session.replica_record.listening_port = self.listening_port

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

    def execute(
        self, server: "MasterServer", session: "ConnectionSession"
    ) -> Iterator[RedisValue]:
        assert server.is_master

        replication_id = get_random_replication_id()
        offset = server.master_repl_offset
        server.master_repl_offset += 1

        session.replica_record.replication_id = replication_id
        session.replica_record.replication_offset = offset

        yield RedisBulkStrings.from_value(f"FULLRESYNC {replication_id} {offset}")
        yield RedisRDBFile.from_value(EMPTYRDB)

        # after the replica receive file, then it really become a replica
        server.registrate_replica(session.replica_record)

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                RedisBulkStrings.from_value(self.replication_id),
                RedisBulkStrings.from_value(str(self.replication_offset)),
            ]
        )
