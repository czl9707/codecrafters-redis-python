from typing import TYPE_CHECKING, AsyncGenerator, Iterator, Optional, Self, Set, cast
from datetime import datetime, timedelta

from ..helper import get_random_replication_id
from ..redis_values import (
    RedisRDBFile,
    RedisValue,
    RedisArray,
    RedisBulkStrings,
)
from ._base import RedisCommand, write, replica_reply

if TYPE_CHECKING:
    from ..redis_server import (
        RedisServer,
        MasterServer,
        ReplicaServer,
        ConnectionSession,
    )


EMPTYRDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="


class PingCommand(RedisCommand):
    name = "ping"

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        return PingCommand()

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
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

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
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

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
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

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
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

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
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


@replica_reply
class ReplConfCommand(RedisCommand):
    name = "replconf"

    def __init__(
        self,
        capabilities: Optional[Set[str]] = None,
        listening_port: Optional[int] = None,
        get_ack: Optional[bool] = None,
        ack: Optional[int] = None,
    ) -> None:
        self.listening_port = listening_port
        self.capabilities = capabilities if capabilities is not None else set()
        self.get_ack = get_ack
        self.ack_offset = ack

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
                case "getack":
                    kwargs["get_ack"] = True
                case "ack":
                    offset = int(next(args).serialize())
                    kwargs["ack"] = offset
                case _:
                    raise AttributeError(f"Unexpected Value {arg.serialize().lower()}")

        return ReplConfCommand(**kwargs)

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
        if self.capabilities or self.listening_port:
            assert server.is_master

            for capa in self.capabilities:
                session.replica_record.capabilities.add(capa)
            if self.listening_port is not None:
                session.replica_record.listening_port = self.listening_port

            yield RedisBulkStrings.from_value("OK")
        elif self.get_ack:
            assert not server.is_master
            server = cast("ReplicaServer", server)

            yield ReplConfCommand(ack=server.replica_offset).as_redis_value()

    def as_redis_value(self) -> RedisValue:
        s = [RedisBulkStrings.from_value(self.name)]

        if self.listening_port is not None:
            s.append(RedisBulkStrings.from_value("listening-port"))
            s.append(RedisBulkStrings.from_value(str(self.listening_port)))
        if self.capabilities:
            for capa in self.capabilities:
                s.append(RedisBulkStrings.from_value("capa"))
                s.append(RedisBulkStrings.from_value(capa))
        if self.get_ack:
            s.append(RedisBulkStrings.from_value("getack"))
        if self.ack_offset is not None:
            s.append(RedisBulkStrings.from_value("ack"))
            s.append(RedisBulkStrings.from_value(str(self.ack_offset)))

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

    async def execute(
        self, server: "MasterServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
        assert server.is_master

        replication_id = get_random_replication_id()

        session.replica_record.replication_id = replication_id
        session.replica_record.replication_offset = server.master_repl_offset

        yield RedisBulkStrings.from_value(
            f"FULLRESYNC {replication_id} {server.master_repl_offset}"
        )
        yield RedisRDBFile.from_value(EMPTYRDB)

        # after the replica receive file, then it really become a replica
        await server.registrate_replica(session.replica_record)

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                RedisBulkStrings.from_value(self.replication_id),
                RedisBulkStrings.from_value(str(self.replication_offset)),
            ]
        )
