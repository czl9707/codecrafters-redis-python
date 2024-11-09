import asyncio
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterator,
    Optional,
    Self,
    Set,
    Type,
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
    name = "PING"

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        return cls()

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
        yield RedisBulkStrings.from_value("PONG")

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
            ]
        )


class EchoCommand(RedisCommand):
    name = "ECHO"

    def __init__(self, content: RedisBulkStrings) -> None:
        self.content = content

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        return cls(next(args))

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
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
    name = "SET"

    def __init__(
        self,
        key: RedisBulkStrings,
        value: RedisBulkStrings,
        expiration: int = -1,
    ) -> None:
        self.key = key
        self.value = value
        self.expiration = expiration

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        kwargs = {}
        kwargs["key"] = next(args)
        kwargs["value"] = next(args)

        for arg in args:
            match arg.serialize().lower(): # type: ignore
                case "px":
                    expire_ms = int(next(args).serialize()) # type: ignore
                    kwargs["expiration"] = expire_ms
                case "p":
                    expire_s = int(next(args).serialize()) # type: ignore
                    kwargs["expiration"] = expire_s * 1000
                case _:
                    raise Exception(f"Unknown arg: {arg.serialize()}")

        return cls(**kwargs)

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
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
    name = "GET"

    def __init__(self, key: RedisBulkStrings) -> None:
        self.key = key

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        return cls(next(args))

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
        yield server.get(self.key)

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                self.key,
            ]
        )


class InfoCommand(RedisCommand):
    name = "INFO"

    def __init__(self, arg: str) -> None:
        self.arg = arg

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        return cls(next(args).serialize().lower()) # type: ignore

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
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
                RedisBulkStrings.from_value(self.arg),
            ]
        )


@replica_reply
class ReplConfCommand(RedisCommand):
    name = "REPLCONF"

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

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        kwargs = {
            "capabilities": set(),
        }

        for arg in args:
            match arg.serialize().lower(): # type: ignore
                case "listening-port":
                    port = int(next(args).serialize()) # type: ignore
                    kwargs["listening_port"] = port
                case "capa":
                    capa = next(args).serialize()
                    kwargs["capabilities"].add(capa)
                case "getack":
                    get_ack_string = next(args).serialize()
                    kwargs["get_ack"] = get_ack_string
                case "ack":
                    offset = int(next(args).serialize()) # type: ignore
                    kwargs["ack"] = offset
                case _:
                    raise AttributeError(f"Unexpected Value {arg.serialize().lower()}")

        return cls(**kwargs)

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
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
            s.append(RedisBulkStrings.from_value("GETACK"))
            s.append(RedisBulkStrings.from_value("*"))
        if self.ack_offset is not None:
            s.append(RedisBulkStrings.from_value("ack"))
            s.append(RedisBulkStrings.from_value(str(self.ack_offset)))

        return RedisArray.from_value(s)


class PsyncCommand(RedisCommand):
    name = "PSYNC"

    def __init__(
        self,
        replication_id: str,
        replication_offset: int,
    ) -> None:
        self.replication_id = replication_id
        self.replication_offset = replication_offset

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        return cls(next(args).serialize(), int(next(args).serialize())) # type: ignore

    async def execute(
        self, server: "MasterServer", session: "ConnectionSession"  # type: ignore
    ) -> AsyncIterator[RedisValue]:
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
    name = "WAIT"

    def __init__(
        self,
        replica_num: int,
        timeout: int,
    ) -> None:
        self.replica_num = replica_num
        self.timeout = timeout

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        return cls(int(next(args).serialize()), int(next(args).serialize())) # type: ignore

    async def execute(
        self, server: "MasterServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
        assert server.is_master

        def _wait_for_single_replica_wrap(replica: "ReplicaRecord") -> Callable[[asyncio.Event], Coroutine]:
            async def _wait_for_single_replica(event: asyncio.Event) -> None:
                if replica.is_synced:
                    return
                
                await replica.sync()
                while not replica.is_synced and not event.is_set():
                    await asyncio.sleep(0.01)
            
            return _wait_for_single_replica
                
        await wait_for_n_finish(
            [
                _wait_for_single_replica_wrap(replica)
                for replica in server.registrated_replicas.values()
            ],
            self.replica_num,
            self.timeout / 1000,
        )

        yield RedisInteger.from_value(len([0 for replica in server.registrated_replicas.values() if replica.is_synced]))

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                RedisBulkStrings.from_value(str(self.replica_num)),
                RedisBulkStrings.from_value(str(self.timeout)),
            ]
        )


class ConfigCommand(RedisCommand):
    name = "CONFIG"

    def __init__(
        self,
        get: Optional[str],
    ) -> None:
        self.get = get

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        kwargs = {}

        for arg in args:
            match arg.serialize().lower(): # type: ignore
                case "get":
                    name = next(args).serialize()
                    kwargs["get"] = name
                case _:
                    raise AttributeError(f"Unexpected Value {arg.serialize().lower()}") # type: ignore

        return cls(**kwargs)

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
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
    name = "KEYS"

    def __init__(
        self,
        wildcard: str,
    ) -> None:
        self.wildcard = wildcard

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        return cls(next(args).serialize())

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
        yield RedisArray.from_value(list(server.keys(self.wildcard)))

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                self.wildcard,
            ]
        )


class TypeCommand(RedisCommand):
    name = "TYPE"

    def __init__(self, key: RedisBulkStrings) -> None:
        self.key = key

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        return cls(next(args))

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
        yield RedisSimpleString.from_value(server.get(self.key).redis_type)

    def as_redis_value(self) -> RedisValue:
        return RedisArray.from_value(
            [
                RedisBulkStrings.from_value(self.name),
                self.key,
            ]
        )


class XaddCommand(RedisCommand):
    name = "XADD"

    def __init__(
        self,
        key: RedisBulkStrings,
        entry_id: RedisStream.StreamEntryId,
        entries: dict[RedisBulkStrings, RedisBulkStrings],
    ) -> None:
        self.key = key
        self.entry_id = entry_id
        self.entries = entries

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        key = next(args)
        entry_id = RedisStream.StreamEntryId.from_string(next(args).serialize()) # type: ignore
        entries = {}
        for entry_key in args:
            entries[entry_key] = next(args)

        return cls(key, entry_id, entries)

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
        stream = server.get(self.key)
        if not isinstance(stream, RedisStream):
            stream = RedisStream()
            server.set(self.key, stream)

        try:
            self.entry_id = self.entry_id.validate(stream.last_entry_id())
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
    name = "XRANGE"

    def __init__(
        self,
        key: RedisBulkStrings,
        start_entry_id: RedisStream.StreamEntryId,
        end_entry_id: RedisStream.StreamEntryId,
    ) -> None:
        self.key = key
        self.start_entry_id = start_entry_id
        self.end_entry_id = end_entry_id

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
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

        return cls(key, start_entry_id, end_entry_id)

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
        assert self.start_entry_id <= self.end_entry_id

        stream = server.get(self.key)
        if not isinstance(stream, RedisStream):
            yield RedisArray.from_value([])
            return

        keys = stream.entry_ids()
        result_list: list[RedisArray] = []

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
    name = "XREAD"

    def __init__(
        self,
        keys: list[RedisBulkStrings],
        start_entry_strs: list[str],
        block: int = -1,
    ) -> None:
        self.keys = keys
        self.start_entry_strs = start_entry_strs
        self.block = block

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        block = -1

        for arg in args:
            match arg.serialize().lower():
                case "count":
                    raise NotImplemented
                case "block":
                    block = int(next(args).serialize())
                case "streams":
                    rest = list(args)
                    keys = rest[: len(rest) // 2]
                    start_entry_strs = [v.serialize() for v in rest[len(rest) // 2 :]]
                    break

        return cls(
            keys=keys,
            start_entry_strs=start_entry_strs, # type: ignore
            block=block,
        )

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
        async def wait_for_data(
            stream: RedisStream, start_entry_id: RedisStream.StreamEntryId
        ):
            while True:
                last_entry_id = stream.last_entry_id()
                last_entry_id = (
                    last_entry_id if last_entry_id else RedisStream.StreamEntryId(0, 1)
                )
                if last_entry_id <= start_entry_id:
                    await asyncio.sleep(0)
                else:
                    return

        start_entry_ids: list[RedisStream.StreamEntryId] = []
        streams: list[RedisStream] = [server.get(key) for key in self.keys]
        for entry_string, stream in zip(self.start_entry_strs, streams):
            if entry_string == "$":
                last_entry_id = stream.last_entry_id()
                start_entry_ids.append(
                    last_entry_id if last_entry_id else RedisStream.StreamEntryId(0, 1)
                )
            else:
                start_entry_ids.append(
                    RedisStream.StreamEntryId.from_string(entry_string)
                )

        if self.block >= 0:
            wait_task = asyncio.gather(
                *[
                    wait_for_data(stream, start_entry_id)
                    for stream, start_entry_id in zip(streams, start_entry_ids)
                ]
            )

            try:
                await asyncio.wait_for(
                    wait_task,
                    timeout=self.block / 1000 if self.block > 0 else None,
                )
            except TimeoutError:
                pass

        results = []

        for stream, start_entry_id, key in zip(streams, start_entry_ids, self.keys):
            if not isinstance(stream, RedisStream):
                results.append(RedisArray.from_value([]))

            all_keys = stream.entry_ids()
            entry_list: list[RedisArray] = []

            index = bisect_right(all_keys, start_entry_id)
            while index < len(all_keys):
                entry_id = all_keys[index]
                entry_list.append(stream.entry_as_redis_value(entry_id))
                index += 1

            if entry_list:
                results.append(
                    RedisArray.from_value(
                        [
                            key,
                            RedisArray.from_value(entry_list),
                        ]
                    )
                )

        if results:
            yield RedisArray.from_value(results)
        else:
            yield RedisBulkStrings.from_value(None)

    def as_redis_value(self) -> RedisValue: # type: ignore
        pass
        # TODO: not implemented properly
        # keys = []
        # entries = []

        # for k, v in self.key_entry_pairs.items():
        #     keys.append(k)
        #     entries.append(RedisBulkStrings.from_value(v))

        # return RedisArray.from_value(
        #     [
        #         RedisBulkStrings.from_value(self.name),
        #         RedisBulkStrings.from_value("STREAMS"),
        #         *keys,
        #         *entries,
        #     ]
        # )



@write
class IncrCommand(RedisCommand):
    name = "INCR"

    def __init__(
        self,
        key: RedisBulkStrings,
    ) -> None:
        self.key = key

    @classmethod
    def from_redis_value_iter(cls: Type[Self], args: Iterator[RedisBulkStrings]) -> Self:
        return cls(next(args))

    async def execute(
        self, server: "RedisServer", session: "ConnectionSession"
    ) -> AsyncIterator[RedisValue]:
        value = server.get(self.key)

        if isinstance(value, RedisBulkStrings) and value.serialize() is None:
            value = RedisBulkStrings.from_value("1")
        elif isinstance(value, RedisBulkStrings) and value.serialize().isnumeric():
            value = RedisBulkStrings.from_value(str(int(value.serialize()) + 1))
        
        server.set(self.key, value)
        yield RedisInteger.from_value(int(value.serialize()))

    def as_redis_value(self) -> RedisValue:
        s = [
            RedisBulkStrings.from_value(self.name),
            self.key,
        ]

        return RedisArray.from_value(s) 
