from __future__ import annotations

from .base import DatabaseAdapter, SetupError
from .dragonfly import DragonflyAdapter
from .redis import RedisAdapter
from .valkey import ValkeyAdapter
from .vortex import VortexAdapter

ADAPTERS: dict[str, DatabaseAdapter] = {
    "vortex": VortexAdapter(),
    "redis": RedisAdapter(),
    "dragonfly": DragonflyAdapter(),
    "valkey": ValkeyAdapter(),
}


def get_adapter(name: str) -> DatabaseAdapter:
    try:
        return ADAPTERS[name]
    except KeyError as error:
        supported = ", ".join(sorted(ADAPTERS))
        raise SetupError(f"unsupported database '{name}'. Supported databases: {supported}") from error
