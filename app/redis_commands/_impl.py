from typing import (
    TYPE_CHECKING,
    AsyncGenerator,
    Dict,
    Iterator,
    List,
    Optional,
    Self,
    Set,
)
from datetime import datetime, timedelta, timezone
from bisect import bisect_left, bisect_right

from ..helper import get_random_replication_id, wait_for_n_finish
from ..redis_values import (
    RedisRDBFile,
    RedisValue,
    RedisArray,
    RedisSimpleString,
    RedisBulkStrings,
    RedisInteger,
    RedisStream,
    RedisSimpleErrors,
)
from ._base import RedisCommand, write, replica_reply

EMPTYRDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

INFINITY = (1 << 64) - 1

if TYPE_CHECKING:
    from ..redis_server import (
        RedisServer,
        MasterServer,
        ReplicaServer,
        ConnectionSession,
    )
    from ..redis_server._base import ReplicaRecord


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
                else datetime.now(tz=timezone.utc)
                + timedelta(milliseconds=self.expiration)
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
        yield server.get(self.key)

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
                pairs["master_replid"] = server.replica_id
                pairs["master_repl_offset"] = server.replica_offset
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
        get_ack: Optional[str] = None,
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
                    get_ack_string = next(args).serialize()
                    kwargs["get_ack"] = get_ack_string
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
            s.append(RedisBulkStrings.from_value("*"))
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
        session.replica_record.replication_offset = 0

        yield RedisBulkStrings.from_value(
            f"FULLRESYNC {replication_id} {server.replica_offset}"
        )
        yield RedisRDBFile.from_value(EMPTYRDB)

        # after the replica receive file, then it really become a replica
        # this line block the server connection handler, and hand over all stuff to heartbeat
        await server.registrate_replica(session.replica_record)

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                RedisBulkStrings.from_value(self.replication_id),
                RedisBulkStrings.from_value(str(self.replication_offset)),
            ]
        )


class WaitCommand(RedisCommand):
    name = "wait"

    def __init__(
        self,
        replica_num: int,
        timeout: int,
    ) -> None:
        self.replica_num = replica_num
        self.timeout = timeout

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        return WaitCommand(int(next(args).serialize()), int(next(args).serialize()))

    async def execute(
        self, server: "MasterServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
        assert server.is_master

        async def _wait_for_single_replia(replica: "ReplicaRecord"):
            while True:
                expected_offset = replica.expected_offset
                if replica.replication_offset == expected_offset:
                    break

                await replica.write(ReplConfCommand(get_ack="*").deserialize())
                ack_response_command = RedisCommand.from_redis_value(
                    await replica.read()
                )
                replica.replication_offset = ack_response_command.ack_offset

        finished, _ = await wait_for_n_finish(
            [
                _wait_for_single_replia(replica)
                for replica in server.registrated_replicas.values()
            ],
            self.replica_num,
            self.timeout / 1000,
        )

        yield RedisInteger.from_value(len(finished))

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                RedisBulkStrings.from_value(str(self.replica_num)),
                RedisBulkStrings.from_value(str(self.timeout)),
            ]
        )


class ConfigCommand(RedisCommand):
    name = "config"

    def __init__(
        self,
        get: Optional[str],
    ) -> None:
        self.get = get

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        kwargs = {}

        for arg in args:
            match arg.serialize().lower():
                case "get":
                    name = next(args).serialize()
                    kwargs["get"] = name
                case _:
                    raise AttributeError(f"Unexpected Value {arg.serialize().lower()}")

        return ConfigCommand(**kwargs)

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
        if self.get:
            yield RedisArray.from_value(
                [
                    RedisBulkStrings.from_value(self.get),
                    RedisBulkStrings.from_value(str(server.config[self.get])),
                ]
            )

    def as_redis_value(self) -> RedisValue:
        s = [RedisBulkStrings.from_value(self.name)]

        if self.get is not None:
            s.append(RedisBulkStrings.from_value("get"))
            s.append(RedisBulkStrings.from_value(self.get))

        return RedisArray.from_value(s)


class KeysCommand(RedisCommand):
    name = "keys"

    def __init__(
        self,
        wildcard: str,
    ) -> None:
        self.wildcard = wildcard

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        return KeysCommand(next(args).serialize())

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
        yield RedisArray.from_value(list(server.keys(self.wildcard)))

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                self.wildcard,
            ]
        )


class TypeCommand(RedisCommand):
    name = "type"

    def __init__(self, key: RedisBulkStrings) -> None:
        self.key = key

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        return TypeCommand(next(args))

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
        yield RedisSimpleString.from_value(server.get(self.key).redis_type)

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                self.key,
            ]
        )


class XaddCommand(RedisCommand):
    name = "xadd"

    def __init__(
        self,
        key: RedisBulkStrings,
        entry_id: RedisStream.StreamEntryId,
        entries: Dict[RedisBulkStrings, RedisBulkStrings],
    ) -> None:
        self.key = key
        self.entry_id = entry_id
        self.entries = entries

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        key = next(args)
        entry_id = RedisStream.StreamEntryId.from_string(next(args).serialize())
        entries = {}
        for entry_key in args:
            entries[entry_key] = next(args)

        return XaddCommand(key, entry_id, entries)

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
        stream = server.get(self.key)
        if not isinstance(stream, RedisStream):
            stream = RedisStream()
            server.set(self.key, stream)

        try:
            last_entry_id = next(reversed(stream.value)) if len(stream.value) else None
            self.entry_id = self.entry_id.validate(last_entry_id)
        except ValueError as e:
            yield RedisSimpleErrors.from_value(*e.args)
            return

        stream.value[self.entry_id] = self.entries
        yield RedisBulkStrings.from_value(self.entry_id.as_string())

    def as_redis_value(self) -> RedisValue:
        s = [
            RedisBulkStrings.from_value(self.name),
            self.key,
            RedisBulkStrings.from_value(self.entry_id.as_string(use_star=True)),
        ]

        for k, v in self.entries.items():
            s.append(k)
            s.append(v)

        return RedisArray.from_value(s)


class XrangeCommand(RedisCommand):
    name = "xrange"

    def __init__(
        self,
        key: RedisBulkStrings,
        start_entry_id: RedisStream.StreamEntryId,
        end_entry_id: RedisStream.StreamEntryId,
    ) -> None:
        self.key = key
        self.start_entry_id = start_entry_id
        self.end_entry_id = end_entry_id

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        key = next(args)

        id_string = next(args).serialize()
        if id_string == "-":
            parts = (0, 0)
        else:
            parts = [int(s) for s in id_string.split("-")]
            if len(parts) == 1:
                parts.append(0)
        start_entry_id = RedisStream.StreamEntryId(*parts)

        id_string = next(args).serialize()
        if id_string == "+":
            parts = (INFINITY, INFINITY)
        else:
            parts = [int(s) for s in id_string.split("-")]
            if len(parts) == 1:
                parts.append(INFINITY)
        end_entry_id = RedisStream.StreamEntryId(*parts)

        return XrangeCommand(key, start_entry_id, end_entry_id)

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
        assert self.start_entry_id <= self.end_entry_id

        stream = server.get(self.key)
        if not isinstance(stream, RedisStream):
            yield RedisArray.from_value([])
            return

        keys = stream.entry_ids()
        result_list: List[RedisArray] = []

        index = bisect_left(keys, self.start_entry_id)
        while index < len(keys) and keys[index] <= self.end_entry_id:
            entry_id = keys[index]
            result_list.append(stream.entry_as_redis_value(entry_id))
            index += 1

        yield RedisArray.from_value(result_list)

    def as_redis_value(self) -> RedisValue:
        s = [
            RedisBulkStrings.from_value(self.name),
            self.key,
            RedisBulkStrings.from_value(self.start_entry_id.as_string()),
            RedisBulkStrings.from_value(self.end_entry_id.as_string()),
        ]

        return RedisArray.from_value(s)


class XreadCommand(RedisCommand):
    name = "xread"

    def __init__(
        self,
        key_entry_pairs: Dict[RedisBulkStrings, RedisStream.StreamEntryId],
    ) -> None:
        self.key_entry_pairs = key_entry_pairs

    @staticmethod
    def from_redis_value(args: Iterator[RedisBulkStrings]) -> Self:
        key_entry_pairs = {}

        for arg in args:
            match arg.serialize().lower():
                case "count":
                    raise NotImplemented
                case "block":
                    raise NotImplemented
                case "streams":
                    rest = list(args)
                    for key, entry_string in zip(
                        rest[: len(rest) // 2], rest[len(rest) // 2 :]
                    ):
                        entry_id = RedisStream.StreamEntryId.from_string(
                            entry_string.serialize()
                        )
                        key_entry_pairs[key] = entry_id
                    break

        return XreadCommand(key_entry_pairs)

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncGenerator[RedisValue, None]:
        results = []

        for key, start_entry_id in self.key_entry_pairs.items():
            stream = server.get(key)
            if not isinstance(stream, RedisStream):
                results.append(RedisArray.from_value([]))

            keys = stream.entry_ids()
            entry_list: List[RedisArray] = []

            index = bisect_right(keys, start_entry_id)
            while index < len(keys):
                entry_id = keys[index]
                entry_list.append(stream.entry_as_redis_value(entry_id))
                index += 1

            results.append(
                RedisArray.from_value(
                    [
                        key,
                        RedisArray.from_value(entry_list),
                    ]
                )
            )

        yield RedisArray.from_value(results)

    def as_redis_value(self) -> RedisValue:
        keys = []
        entries = []

        for k, v in self.key_entry_pairs.items():
            keys.append(k)
            entries.append(RedisBulkStrings.from_value(v.as_string()))

        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                RedisBulkStrings.from_value("STREAMS"),
                *keys,
                *entries,
            ]
        )
