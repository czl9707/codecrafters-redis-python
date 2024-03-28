import base64
import collections
from typing import Deque, Dict, List, Never, Optional, OrderedDict, NamedTuple

from ._base import RedisValue, CRLF


class RedisSimpleString(RedisValue[str]):
    symbol = b"+"

    @classmethod
    def _prepare(cls, tokens: Deque[bytes]) -> List[bytes]:
        return [tokens.popleft()]

    @classmethod
    def _serialize(cls, tokens: List[bytes]) -> str:
        return tokens[0].decode()[1:]

    @classmethod
    def _deserialize(cls, value: str) -> List[bytes]:
        return [
            cls.symbol + value.encode(),
        ]

    @property
    def redis_type(self) -> str:
        return "string"


class RedisInteger(RedisValue[int]):
    symbol = b":"
    value_types = [int]

    @classmethod
    def _prepare(cls, tokens: Deque[bytes]) -> List[bytes]:
        return [tokens.popleft()]

    @classmethod
    def _serialize(cls, tokens: List[bytes]) -> int:
        return int(tokens[0].decode()[1:])

    @classmethod
    def _deserialize(cls, value: int) -> List[bytes]:
        return [
            cls.symbol + str(value).encode(),
        ]

    @property
    def redis_type(self) -> str:
        return "integer"


class RedisBulkStrings(RedisValue[Optional[str]]):
    symbol = b"$"
    value_types = [str, None.__class__]

    # RDB file will go into this catalog when inbound
    @classmethod
    def _prepare(cls, tokens: Deque[bytes]) -> List[bytes]:
        header = tokens.popleft()
        size = int(header.decode()[1:])

        total_length = 0
        string_bytes = []
        while total_length < size:
            s = tokens.popleft()
            string_bytes.append(s)
            total_length += len(s)

            if total_length < size:
                string_bytes.append(CRLF)
            else:
                break

        bs = b"".join(string_bytes)
        if len(bs) > size:
            unused = bs[size:]
            tokens.appendleft(unused)
            s = bs[:size]

        return [header, bs]

    @classmethod
    def _serialize(cls, tokens: List[bytes]) -> Optional[str]:
        header = tokens[0]
        size = int(header.decode()[1:])
        if size < 0:
            return None  # Handle NoneBulkString

        return tokens[1].decode()

    @classmethod
    def _deserialize(cls, value: str) -> List[bytes]:
        if value is not None:
            return [
                cls.symbol + str(len(value)).encode(),
                value.encode(),
            ]
        else:
            return [
                cls.symbol + b"-1",
            ]

    @property
    def redis_type(self) -> str:
        if self.value is None:
            if self.tokens is None:
                return "none"
            if self.tokens is not None and self.tokens[0] == b"$-1":
                return "none"

        return "string"


class RedisArray(RedisValue[List[RedisValue]]):
    symbol = b"*"
    value_types = [list]

    @classmethod
    def _prepare(cls, tokens: Deque[bytes]) -> List[bytes]:
        header = tokens.popleft()
        size = int(header.decode()[1:])

        values = [header]
        for _ in range(size):
            redis_value = RedisValue._from_symbol(tokens[0][:1])
            values.extend(redis_value._prepare(tokens))

        return values

    @classmethod
    def _serialize(cls, tokens: List[bytes]) -> List[RedisValue]:
        children: List[RedisValue] = []

        header = tokens[0]
        size = int(header.decode()[1:])

        children_bytes = collections.deque(tokens[1:])
        for _ in range(size):
            redis_value = RedisValue.from_bytes(children_bytes)
            children.append(redis_value)

        return children

    @classmethod
    def _deserialize(cls, value: List[RedisValue]) -> List[bytes]:
        bytes_values = [cls.symbol + str(len(value)).encode()]
        for v in value:
            v.deserialize()  # ensure value get .tokens
            bytes_values.extend(v.tokens)  # type: ignore
        return bytes_values

    @property
    def redis_type(self) -> str:
        return "list"


class RedisStream(
    RedisValue[
        OrderedDict[
            "RedisStream.StreamEntryId",
            Dict[RedisBulkStrings, RedisValue],
        ]
    ]
):
    class StreamEntryId(NamedTuple):
        timestamp: int
        sequence: int

        @staticmethod
        def from_string(s: str) -> "RedisStream.StreamEntryId":
            timestamp, sequence = s.split("-")
            return RedisStream.StreamEntryId(
                timestamp=int(timestamp), sequence=int(sequence)
            )

        def as_string(self) -> str:
            return f"{self.timestamp}-{self.sequence}"

    value: OrderedDict[
        "RedisStream.StreamEntryId",
        Dict[RedisBulkStrings, RedisValue],
    ]

    def __init__(self) -> None:
        super().__init__()
        self.value = collections.OrderedDict()

    @classmethod
    def _prepare(cls, tokens: Deque[bytes]) -> Never:
        raise NotImplemented

    @classmethod
    def _serialize(cls, tokens: List[bytes]) -> Never:
        raise NotImplemented

    @property
    def bytes_size(self) -> int:
        self.value
        raise NotImplemented

    @classmethod
    def _deserialize(cls, value: str) -> Never:
        raise NotImplemented

    @property
    def redis_type(self) -> str:
        return "stream"


# RDBFile is same as BulkStrings, but without CRLF at the end
# should not be serialize from value to bytes
class RedisRDBFile(RedisValue[str]):
    @classmethod
    def _prepare(cls, tokens: Deque[bytes]) -> List[bytes]:
        raise NotImplemented

    @classmethod
    def _serialize(cls, tokens: List[bytes]) -> Never:
        raise NotImplemented

    @property
    def bytes_size(self) -> int:
        if self.tokens is None:
            self.tokens = self._deserialize(self.value)
        return sum(len(t) for t in self.tokens) + len(CRLF)

    @classmethod
    def _deserialize(cls, value: str) -> List[bytes]:
        bytes_value = base64.b64decode(value)

        return [
            b"$" + str(len(bytes_value)).encode(),
            bytes_value,
        ]

    def deserialize(self) -> bytes:
        if self.tokens is None:
            self.tokens = self._deserialize(self.value)

        return CRLF.join(self.tokens)

    @property
    def redis_type(self) -> str:
        raise NotImplemented
