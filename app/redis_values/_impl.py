import base64
from typing import Deque, List, Optional

from ._base import RedisValue, CRLF


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
class RedisRDBFile(RedisValue[str]):
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

        return bytes_string.decode()

    @classmethod
    def _deserialize(cls, value: str) -> bytes:
        bytes_value = base64.b64decode(value)

        return b"".join(
            [
                b"$",
                str(len(bytes_value)).encode(),
                CRLF,
                bytes_value,
            ]
        )
