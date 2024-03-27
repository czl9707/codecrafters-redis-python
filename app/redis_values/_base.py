from abc import ABC, abstractmethod
import asyncio
from typing import (
    Any,
    AsyncIterator,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    Deque,
)
from collections import deque

BUFFER_SIZE = 1024
CRLF = b"\r\n"
EOF = b""

TBase = TypeVar("TBase")


class RedisValue(ABC, Generic[TBase]):
    _symbol2value: Dict[str, Type["RedisValue"]] = {}
    _type2value: Dict[type, Type["RedisValue"]] = {}

    # child class overwrite these!
    symbol: bytes
    value_types: List[type] = []

    __slots__ = ["tokens", "value"]
    tokens: Optional[List[bytes]]
    value: Optional[TBase]

    def __init__(self) -> None:
        self.tokens = None
        self.value = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.serialize()})"

    def __hash__(self) -> int:
        return hash(self.serialize())

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, RedisValue):
            return self.serialize() == other.serialize()
        return False

    def __init_subclass__(cls: Type["RedisValue"]) -> None:
        if hasattr(cls, "symbol"):
            RedisValue._symbol2value[cls.symbol] = cls
        for t in cls.value_types:
            RedisValue._type2value[t] = cls

    @staticmethod
    def _from_symbol(sym: bytes) -> "RedisValue":
        return RedisValue._symbol2value[sym]()

    @staticmethod
    def _from_type(t: type) -> "RedisValue":
        return RedisValue._type2value[t]()

    @staticmethod
    def from_bytes(tokens: Deque[bytes] | bytes) -> "RedisValue":
        if isinstance(tokens, bytes):
            tokens = deque(tokens.split(CRLF))
        assert isinstance(tokens, deque)

        redis_value = RedisValue._from_symbol(tokens[0][:1])
        redis_value.tokens = redis_value._prepare(tokens)
        return redis_value

    @classmethod
    def from_value(cls, value: TBase) -> "RedisValue":
        if cls == RedisValue:
            redis_value = RedisValue._from_type(value.__class__)
        else:
            redis_value = cls()
        redis_value.value = value
        return redis_value

    def serialize(self) -> TBase:
        if self.value is None:
            self.value = self._serialize(self.tokens)

        return self.value

    def deserialize(self) -> bytes:
        if self.tokens is None:
            self.tokens = self._deserialize(self.value)

        return CRLF.join([*self.tokens, EOF])

    @property
    def bytes_size(self) -> int:
        if self.tokens is None:
            self.tokens = self._deserialize(self.value)
        return sum(len(t) for t in self.tokens) + len(self.tokens) * len(CRLF)

    @classmethod
    @abstractmethod
    def _serialize(cls, tokens: List[bytes]) -> TBase: ...

    @classmethod
    @abstractmethod
    def _deserialize(cls, value: TBase) -> List[bytes]: ...

    @classmethod
    @abstractmethod
    def _prepare(cls, tokens: Deque[bytes]) -> List[bytes]: ...

    @property
    @abstractmethod
    def redis_type(self) -> str: ...


class RedisValueReader:
    def __init__(self, reader: asyncio.StreamReader) -> None:
        self.reader = reader
        self._deque = deque()

    def __aiter__(self) -> AsyncIterator[RedisValue]:
        return self

    async def read(self) -> RedisValue:
        while not self.reader.at_eof():
            while len(self._deque) and self._deque[0] == EOF:
                self._deque.popleft()
            if len(self._deque) == 0:
                request_bytes = await self.reader.read(BUFFER_SIZE)
                if not request_bytes:
                    continue

                print(request_bytes)
                self._deque.extend(request_bytes.split(CRLF))

            redis_value = RedisValue.from_bytes(self._deque)
            return redis_value

    async def __anext__(self) -> RedisValue:
        while True:
            value = await self.read()
            if value:
                return value
            else:
                raise StopAsyncIteration
