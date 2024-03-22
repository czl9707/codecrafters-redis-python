from abc import ABC, abstractmethod
from typing import Any, Dict, Deque, Generic, List, Optional, Type, TypeVar
from collections import deque

CRLF = b"\r\n"

TBase = TypeVar("TBase")


class RedisValue(ABC, Generic[TBase]):
    _symbol2value: Dict[str, Type["RedisValue"]] = {}
    _type2value: Dict[type, Type["RedisValue"]] = {}
    symbol: bytes
    value_types: List[type] = []

    bytes_value: Optional[bytes]
    value: Optional[TBase]

    def __init__(self) -> None:
        self.bytes_value = None
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
    def from_bytes(b: bytes) -> "RedisValue":
        redis_value = RedisValue._from_symbol(b[0:1])
        redis_value.bytes_value = b
        return redis_value

    @classmethod
    def from_value(cls, value: TBase) -> "RedisValue":
        if cls == RedisValue:
            redis_value = RedisValue._from_type(value.__class__)
        else:
            redis_value = cls()
        redis_value.value = value
        return redis_value

    @staticmethod
    def from_serialization(bytes_lines: Deque[bytes]) -> "RedisValue":
        redis_value = RedisValue._from_symbol(bytes_lines[0][0:1])
        used_tokens = []
        redis_value.value = redis_value._serialize(bytes_lines, used_tokens)
        redis_value.bytes_value = b"".join(used_tokens)
        return redis_value

    def serialize(self) -> TBase:
        if self.value is None:
            self.value = self._serialize(
                deque(self.bytes_value.split(CRLF)),
            )

        return self.value

    def deserialize(self) -> bytes:
        if self.bytes_value is None:
            self.bytes_value = self._deserialize(self.value)

        return self.bytes_value

    @classmethod
    @abstractmethod
    def _serialize(
        cls,
        bytes_lines: Deque[bytes],
        used_tokens: Optional[List[bytes]] = None,
    ) -> TBase: ...

    @classmethod
    @abstractmethod
    def _deserialize(cls, value: TBase) -> bytes: ...
