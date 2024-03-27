import io
import pathlib
from datetime import datetime, timezone
from typing import Any, Callable, Dict, TYPE_CHECKING, Optional

from ..redis_values import RedisBulkStrings, RedisValue

if TYPE_CHECKING:
    from ._base import RedisEntry


OPCODE_EOF = b"\xFF"
OPCODE_SELECTDB = b"\xFE"
OPCODE_EXPIRETIME = b"\xFD"
OPCODE_EXPIRETIMEMS = b"\xFC"
OPCODE_RESIZEDB = b"\xFB"
OPCODE_AUX = b"\xFA"
OPCODE_STRING_TYPE = b"\x00"


class DatabaseParser:
    def __init__(self, rdb_path: pathlib.Path) -> None:
        self._op2parser = {
            OPCODE_AUX: self._parse_aux,
            OPCODE_RESIZEDB: self._parse_resizedb,
            OPCODE_EXPIRETIME: self._parse_expiretime,
            OPCODE_EXPIRETIMEMS: self._parse_expiretime_ms,
            OPCODE_SELECTDB: self._parse_selectdb,
            OPCODE_STRING_TYPE: lambda file: self._parse_key_value_pair(
                file, self._parse_string
            ),
        }

        self.rdb_path = rdb_path
        self.aux = {}
        self.db_size = 0
        self.expiry_size = 0
        self.db_number = None
        self.redis_entries: Dict[RedisBulkStrings, "RedisEntry"] = {}
        if not self.rdb_path.exists():
            return

        with open(self.rdb_path, "rb") as file:
            self.parse(file)

    def parse(self, file: io.BufferedReader):

        self._check_magic_string(file)
        self._check_version(file)

        while (opcode := file.read(1)) != OPCODE_EOF:
            self._op2parser[opcode](file)

    def _check_magic_string(self, file: io.BufferedReader) -> None:
        if file.read(5) != b"REDIS":
            raise Exception("Invalid file format")

    def _check_version(self, file: io.BufferedReader) -> None:
        version_str = file.read(4)
        version = int(version_str)
        if version < 1:
            raise Exception(f"Invalid version: {version}")

    def _parse_length(self, file: io.BufferedReader) -> int:
        b = file.read(1)
        match int.from_bytes(b, "little") >> 6:
            case 0b00:
                pass
            case 0b01:
                b &= 0b00111111
                b += file.read()
            case 0b10:
                b = file.read(4)
            case 0b11:
                return -1
            case _:
                raise NotImplemented

        return int.from_bytes(b, "little")

    def _parse_string(self, file: io.BufferedReader) -> str:
        length = self._parse_length(file)
        bs = file.read(length)
        return bs.decode()

    def _parse_aux(self, file: io.BufferedReader) -> None:
        key = self._parse_string(file)
        if key in ("redis-ver"):
            value = self._parse_string(file)
        if key in ("redis-bits", "aof-preamble", "aof-base"):
            file.read(1)
            value = int.from_bytes(file.read(1), "little")
        elif key in ("ctime", "used-mem"):
            file.read(5)
            value = None
            # place holder

        self.aux[key] = value

    def _parse_resizedb(self, file: io.BufferedReader) -> None:
        self.db_size = self._parse_length(file)
        self.expiry_size = self._parse_length(file)

    def _parse_selectdb(self, file: io.BufferedReader) -> None:
        self.db_number = self._parse_length(file)

    def _parse_key_value_pair(
        self,
        file: io.BufferedReader,
        value_parser: Callable[[io.BufferedReader], Any],
    ) -> RedisBulkStrings:
        key = RedisBulkStrings.from_value(self._parse_string(file))
        value = RedisValue.from_value(value_parser(file))

        if key not in self.redis_entries:
            entry = RedisEntry(value, None)
            self.redis_entries[key] = entry
        else:
            self.redis_entries[key].value = value
        return key

    def _parse_expiretime(self, file: io.BufferedReader) -> None:
        sec = int.from_bytes(file.read(4), "little")

        opcode = file.read(1)
        key: RedisBulkStrings = self._op2parser[opcode](file)
        self.redis_entries[key].expiration = datetime.fromtimestamp(
            sec, tz=timezone.utc
        )

        return key

    def _parse_expiretime_ms(self, file: io.BufferedReader) -> None:
        msec = int.from_bytes(file.read(8), "little")

        opcode = file.read(1)
        key: RedisBulkStrings = self._op2parser[opcode](file)
        self.redis_entries[key].expiration = datetime.fromtimestamp(
            msec / 1000, tz=timezone.utc
        )
        return key


class RedisEntry:
    def __init__(
        self,
        value: RedisValue,
        expiration: Optional[datetime],
    ) -> None:
        self.value = value
        self.expiration = expiration
