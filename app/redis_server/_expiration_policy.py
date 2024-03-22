from abc import ABC
from datetime import datetime
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from ._base import RedisEntry


class ExpirationPolicy(ABC):
    ALL: List["ExpirationPolicy"] = []

    @staticmethod
    def is_expired(entry: "RedisEntry") -> bool:
        return any(policy.is_expired(entry) for policy in ExpirationPolicy.ALL)

    def __init_subclass__(cls) -> None:
        ExpirationPolicy.ALL.append(cls)


class EndOfLifePolicy(ExpirationPolicy):
    @staticmethod
    def is_expired(entry: "RedisEntry") -> bool:
        if entry.expiration is not None and entry.expiration < datetime.now():
            return True
        return False
