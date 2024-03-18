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

    @staticmethod
    def from_value(value: TBase) -> "RedisValue":
        redis_value = RedisValue._from_type(value.__class__)
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
            assert self.bytes_value
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


class RedisSimpleString(RedisValue[str]):
    symbol = b"+"

    @classmethod
    def _serialize(
        cls,
        bytes_lines: Deque[bytes],
        used_tokens: Optional[List[bytes]] = None,
    ) -> str:
        l = bytes_lines.popleft()
        if used_tokens is not None:
            used_tokens.append(l)
            used_tokens.append(CRLF)

        return l.decode()[1:]

    @classmethod
    def _deserialize(cls, value: str) -> bytes:
        return b"".join(
            [
                cls.symbol,
                value.encode(),
                CRLF,
            ]
        )


class RedisBulkStrings(RedisValue[Optional[str]]):
    symbol = b"$"
    value_types = [str, None.__class__]

    @classmethod
    def _serialize(
        cls,
        bytes_lines: Deque[bytes],
        used_tokens: Optional[List[bytes]] = None,
    ) -> Optional[str]:
        l = bytes_lines.popleft()
        if used_tokens is not None:
            used_tokens.append(l)
            used_tokens.append(CRLF)

        size = int(l.decode()[1:])
        if size < 0:
            return None  # Handle NoneBulkString

        total_length = 0
        tokens = []
        while total_length < size:
            line = bytes_lines.popleft()
            tokens.append(line)
            tokens.append(CRLF)

            total_length += len(line) + 2

        bytes_string = b"".join(tokens)[:size]
        if used_tokens is not None:
            used_tokens.append(bytes_string)
            used_tokens.append(CRLF)

        return bytes_string.decode()

    @classmethod
    def _deserialize(cls, value: str) -> bytes:
        if value is not None:
            return b"".join(
                [
                    cls.symbol,
                    str(len(value)).encode(),
                    CRLF,
                    value.encode(),
                    CRLF,
                ]
            )
        else:
            return b"".join(
                [
                    cls.symbol,
                    b"-1",
                    CRLF,
                ]
            )


class RedisArray(RedisValue[List[RedisValue]]):
    symbol = b"*"
    value_types = [list]

    @classmethod
    def _serialize(
        cls,
        bytes_lines: Deque[bytes],
        used_tokens: Optional[List[bytes]] = None,
    ) -> List[RedisValue]:
        children: List[RedisValue] = []

        l = bytes_lines.popleft()
        size = int(l.decode()[1:])
        if used_tokens is not None:
            used_tokens.append(l)

        for _ in range(size):
            redis_value = RedisValue.from_serialization(bytes_lines)
            children.append(redis_value)

            if used_tokens is not None:
                used_tokens.extend(redis_value.deserialize())

        return children

    @classmethod
    def _deserialize(cls, value: List[RedisValue]) -> bytes:
        return b"".join(
            [
                cls.symbol,
                str(len(value)).encode(),
                CRLF,
                *[v.deserialize() for v in value],
            ]
        )


# RDBFile is same as BulkStrings, but without CRLF at the end
# and no need for serialization... For now
class RedisRDBFile(RedisValue[str]):
    def __init__(self, value: str) -> None:
        self.value = value

    @classmethod
    def _deserialize(cls, value: str) -> bytes:
        return b"".join(
            [
                cls.symbol,
                str(len(value)).encode(),
                CRLF,
                value.encode(),
            ]
        )
