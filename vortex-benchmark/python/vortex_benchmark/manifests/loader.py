from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

from vortex_benchmark.models import (
    SUPPORTED_AOF_FSYNC_POLICIES,
    SUPPORTED_EVICTION_POLICIES,
)


MANIFEST_TOP_LEVEL_KEYS = {
    "schema_version",
    "name",
    "description",
    "databases",
    "workloads",
    "commands",
    "command_groups",
    "backends",
    "duration",
    "environment",
    "resource_config",
    "runtime_config",
    "settings",
}
ENVIRONMENT_KEYS = {"mode", "output_dir", "state_file", "port_base", "build_vortex"}
RESOURCE_CONFIG_KEYS = {"cpus", "memory", "threads"}
RUNTIME_CONFIG_KEYS = {"aof_enabled", "aof_fsync", "maxmemory", "eviction_policy"}
SIZE_LITERAL_RE = re.compile(r"^\d+(?:[kmgt]i?b?|[kmgt]b?)?$", re.IGNORECASE)


@dataclass
class BenchmarkManifest:
    source_path: str
    source_format: str
    schema_version: int = 1
    name: Optional[str] = None
    description: Optional[str] = None
    databases: list[str] = field(default_factory=list)
    workloads: list[str] = field(default_factory=list)
    commands: list[str] = field(default_factory=list)
    command_groups: list[str] = field(default_factory=list)
    backends: list[str] = field(default_factory=list)
    duration: Optional[str] = None
    environment: dict[str, Any] = field(default_factory=dict)
    resource_config: dict[str, Any] = field(default_factory=dict)
    runtime_config: dict[str, Any] = field(default_factory=dict)
    settings: dict[str, Any] = field(default_factory=dict)


def _require_mapping(value: Any, label: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a mapping")
    return dict(value)


def _optional_string(value: Any, label: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string when provided")
    return value.strip()


def _optional_string_list(value: Any, label: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list of strings")

    resolved: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{label} entries must be non-empty strings")
        resolved.append(item.strip())
    return resolved


def _optional_positive_int(value: Any, label: str) -> Optional[int]:
    if value is None:
        return None
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _optional_bool(value: Any, label: str) -> Optional[bool]:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ValueError(f"{label} must be a boolean when provided")
    return value


def _optional_size_string(value: Any, label: str) -> Optional[str]:
    text = _optional_string(value, label)
    if text is None:
        return None
    if not SIZE_LITERAL_RE.fullmatch(text):
        raise ValueError(
            f"{label} must be a size literal such as 4194304, 4mb, 2g, or 512k"
        )
    return text.lower()


def _validate_environment(payload: dict[str, Any]) -> dict[str, Any]:
    unknown = sorted(set(payload) - ENVIRONMENT_KEYS)
    if unknown:
        raise ValueError(f"environment contains unsupported keys: {', '.join(unknown)}")

    mode = payload.get("mode")
    if mode is not None:
        if not isinstance(mode, str) or mode.strip() not in {"native", "container"}:
            raise ValueError("environment.mode must be either 'native' or 'container'")
        payload["mode"] = mode.strip()

    output_dir = _optional_string(payload.get("output_dir"), "environment.output_dir")
    state_file = _optional_string(payload.get("state_file"), "environment.state_file")
    port_base = _optional_positive_int(payload.get("port_base"), "environment.port_base")
    build_vortex = _optional_bool(payload.get("build_vortex"), "environment.build_vortex")

    normalized: dict[str, Any] = {}
    if mode is not None:
        normalized["mode"] = payload["mode"]
    if output_dir is not None:
        normalized["output_dir"] = output_dir
    if state_file is not None:
        normalized["state_file"] = state_file
    if port_base is not None:
        normalized["port_base"] = port_base
    if build_vortex is not None:
        normalized["build_vortex"] = build_vortex
    return normalized


def _validate_resource_config(payload: dict[str, Any]) -> dict[str, Any]:
    unknown = sorted(set(payload) - RESOURCE_CONFIG_KEYS)
    if unknown:
        raise ValueError(f"resource_config contains unsupported keys: {', '.join(unknown)}")

    cpus = _optional_positive_int(payload.get("cpus"), "resource_config.cpus")
    threads = _optional_positive_int(payload.get("threads"), "resource_config.threads")
    memory = _optional_string(payload.get("memory"), "resource_config.memory")

    normalized: dict[str, Any] = {}
    if cpus is not None:
        normalized["cpus"] = cpus
    if threads is not None:
        normalized["threads"] = threads
    if memory is not None:
        normalized["memory"] = memory
    return normalized


def _validate_runtime_config(payload: dict[str, Any]) -> dict[str, Any]:
    unknown = sorted(set(payload) - RUNTIME_CONFIG_KEYS)
    if unknown:
        raise ValueError(f"runtime_config contains unsupported keys: {', '.join(unknown)}")

    aof_enabled = _optional_bool(payload.get("aof_enabled"), "runtime_config.aof_enabled")
    aof_fsync = _optional_string(payload.get("aof_fsync"), "runtime_config.aof_fsync")
    if aof_fsync is not None and aof_fsync not in SUPPORTED_AOF_FSYNC_POLICIES:
        supported = ", ".join(SUPPORTED_AOF_FSYNC_POLICIES)
        raise ValueError(f"runtime_config.aof_fsync must be one of: {supported}")

    maxmemory = _optional_size_string(payload.get("maxmemory"), "runtime_config.maxmemory")
    eviction_policy = _optional_string(
        payload.get("eviction_policy"), "runtime_config.eviction_policy"
    )
    if eviction_policy is not None and eviction_policy not in SUPPORTED_EVICTION_POLICIES:
        supported = ", ".join(SUPPORTED_EVICTION_POLICIES)
        raise ValueError(f"runtime_config.eviction_policy must be one of: {supported}")

    normalized: dict[str, Any] = {}
    if aof_enabled is not None:
        normalized["aof_enabled"] = aof_enabled
    if aof_fsync is not None:
        normalized["aof_fsync"] = aof_fsync
    if maxmemory is not None:
        normalized["maxmemory"] = maxmemory
    if eviction_policy is not None:
        normalized["eviction_policy"] = eviction_policy
    return normalized


def _load_raw_document(path: Path) -> tuple[dict[str, Any], str]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        source_format = "json"
    elif suffix in {".yaml", ".yml"}:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        source_format = "yaml"
    else:
        raise ValueError("workload manifest must end in .json, .yaml, or .yml")

    if not isinstance(payload, dict):
        raise ValueError("workload manifest root must be a mapping")

    return dict(payload), source_format


def load_manifest(path: Path) -> BenchmarkManifest:
    resolved_path = path.expanduser().resolve()
    if not resolved_path.exists():
        raise ValueError(f"workload manifest does not exist: {resolved_path}")

    payload, source_format = _load_raw_document(resolved_path)
    unknown = sorted(set(payload) - MANIFEST_TOP_LEVEL_KEYS)
    if unknown:
        raise ValueError(f"workload manifest contains unsupported keys: {', '.join(unknown)}")

    schema_version = payload.get("schema_version", 1)
    if not isinstance(schema_version, int) or schema_version <= 0:
        raise ValueError("schema_version must be a positive integer")

    return BenchmarkManifest(
        source_path=str(resolved_path),
        source_format=source_format,
        schema_version=schema_version,
        name=_optional_string(payload.get("name"), "name"),
        description=_optional_string(payload.get("description"), "description"),
        databases=_optional_string_list(payload.get("databases"), "databases"),
        workloads=_optional_string_list(payload.get("workloads"), "workloads"),
        commands=_optional_string_list(payload.get("commands"), "commands"),
        command_groups=_optional_string_list(payload.get("command_groups"), "command_groups"),
        backends=_optional_string_list(payload.get("backends"), "backends"),
        duration=_optional_string(payload.get("duration"), "duration"),
        environment=_validate_environment(_require_mapping(payload.get("environment"), "environment")),
        resource_config=_validate_resource_config(
            _require_mapping(payload.get("resource_config"), "resource_config")
        ),
        runtime_config=_validate_runtime_config(
            _require_mapping(payload.get("runtime_config"), "runtime_config")
        ),
        settings=_require_mapping(payload.get("settings"), "settings"),
    )
