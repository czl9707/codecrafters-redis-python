from abc import ABC, abstractmethod
from typing import Dict, Deque, List, Self, Type, Any
from collections import deque

CRLF = "\r\n"


class RedisValue(ABC):
    _symbol2value: Dict[str, Type["RedisValue"]] = {}
    _type2value: Dict[type, Type["RedisValue"]] = {}
    symbol: str
    value_type: type
    value: Any

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.value})"

    def __init_subclass__(cls: Type["RedisValue"]) -> None:
        RedisValue._symbol2value[cls.symbol] = cls
        if hasattr(cls, "value_type"):
            RedisValue._type2value[cls.value_type] = cls

    @staticmethod
    def from_bytes(bs: bytes) -> "RedisValue":
        lines = bs.decode().split(CRLF)
        return RedisValue.serialize(deque(lines))

    @staticmethod
    @abstractmethod
    def serialize(lines: Deque[str]) -> Self:
        sym = lines[0][0]

        return RedisValue._symbol2value[sym].serialize(lines)

    @abstractmethod
    def deserialize(self) -> bytes: ...


class RedisSimpleString(RedisValue):
    symbol = "+"

    value: str

    def __init__(self, value: str) -> None:
        self.value = value

    @staticmethod
    def serialize(lines: Deque[str]) -> Self:
        s = lines.popleft()
        redisvalue = RedisSimpleString(s[1:])

        return redisvalue

    def deserialize(self) -> bytes:
        return f"{self.symbol}{self.value}{CRLF}".encode()


class RedisBulkStrings(RedisValue):
    symbol = "$"
    value_type = str

    value: str

    def __init__(self, value: str) -> None:
        self.value = value

    @staticmethod
    def serialize(lines: Deque[str]) -> Self:
        lines.popleft()
        s = lines.popleft()
        redisvalue = RedisBulkStrings(s)

        return redisvalue

    def deserialize(self) -> bytes:
        s = f"{self.symbol}{len(self.value)}{CRLF}"
        s += self.value + CRLF
        return s.encode()


class RedisArray(RedisValue):
    symbol = "*"
    value_type = list

    value: List[RedisValue]

    def __init__(self, value: List[RedisValue]) -> None:
        self.value = value

    @staticmethod
    def serialize(lines: Deque[str]) -> Self:
        count = int(lines.popleft()[1:])
        v = [RedisValue.serialize(lines) for _ in range(count)]

        redisvalue = RedisArray(v)

        return redisvalue

    def deserialize(self) -> bytes:
        lines = [
            f"{self.symbol}{len(self.value)}{CRLF}".encode(),
        ]

        for v in self.value:
            lines.append(v.deserialize())

        return b"".join(lines)
