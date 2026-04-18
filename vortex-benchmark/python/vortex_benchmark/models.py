from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

SUPPORTED_DATABASES = ("vortex", "redis", "dragonfly", "valkey")
NATIVE_DATABASES = ("vortex", "redis")
CONTAINER_DATABASES = SUPPORTED_DATABASES
DEFAULT_PORT_BASE = 16379
DEFAULT_CPUS = 4
DEFAULT_MEMORY = "2g"
DEFAULT_AOF_FSYNC = "everysec"
DEFAULT_EVICTION_POLICY = "noeviction"
SUPPORTED_AOF_FSYNC_POLICIES = ("always", "everysec", "no")
SUPPORTED_EVICTION_POLICIES = (
    "noeviction",
    "allkeys-lru",
    "volatile-lru",
    "allkeys-random",
    "volatile-random",
    "volatile-ttl",
)


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def sanitize_identifier(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in value)


def build_environment_id(mode: str, databases: list[str]) -> str:
    return sanitize_identifier(f"{timestamp_slug()}-{mode}-{'-'.join(databases)}")


def split_csv_values(values: Optional[list[str]]) -> list[str]:
    if not values:
        return []

    seen: set[str] = set()
    ordered: list[str] = []
    for raw_value in values:
        for part in raw_value.split(","):
            value = part.strip()
            if not value or value in seen:
                continue
            seen.add(value)
            ordered.append(value)
    return ordered


@dataclass
class ServiceState:
    database: str
    mode: str
    host: str
    port: int
    ready: bool
    log_path: str
    started_at: str
    ready_at: Optional[str]
    status: str = "running"
    external: bool = False
    pid: Optional[int] = None
    process_group: Optional[int] = None
    container_id: Optional[str] = None
    stopped_at: Optional[str] = None
    stop_signal: Optional[str] = None
    resource_config: dict[str, Any] = field(default_factory=dict)
    runtime_config: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ServiceState":
        return cls(**data)


@dataclass
class EnvironmentState:
    environment_id: str
    created_at: str
    updated_at: str
    project_root: str
    benchmark_root: str
    artifact_root: str
    state_file: str
    mode: str
    requested_databases: list[str]
    status: str
    services: list[ServiceState]
    runtime_config: dict[str, Any] = field(default_factory=dict)
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["services"] = [service.to_dict() for service in self.services]
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EnvironmentState":
        payload = dict(data)
        payload["services"] = [
            ServiceState.from_dict(service) for service in payload.get("services", [])
        ]
        return cls(**payload)
