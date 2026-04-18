from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from vortex_benchmark.catalog import (
    expand_command_groups,
    get_workload_definition,
    normalize_command_name,
    resolve_backend_name,
    resolve_command_group_name,
    resolve_workload_name,
)
from vortex_benchmark.manifests.loader import BenchmarkManifest, load_manifest
from vortex_benchmark.models import (
    DEFAULT_CPUS,
    DEFAULT_MEMORY,
    DEFAULT_PORT_BASE,
    SUPPORTED_DATABASES,
    SUPPORTED_AOF_FSYNC_POLICIES,
    SUPPORTED_EVICTION_POLICIES,
    split_csv_values,
)


@dataclass
class ResolvedBenchmarkSpec:
    manifest: Optional[BenchmarkManifest]
    databases: list[str]
    workloads: list[str]
    workload_definitions: list[dict[str, object]]
    workload_default_commands: list[str]
    commands: list[str]
    command_groups: list[str]
    expanded_group_commands: list[str]
    resolved_commands: list[str]
    effective_commands: list[str]
    backends: list[str]
    duration: Optional[str]
    mode: Optional[str]
    output_dir: Optional[str]
    state_file: Optional[str]
    port_base: int
    cpus: int
    memory: str
    threads: Optional[int]
    build_vortex: bool
    settings: dict[str, Any]
    environment: dict[str, Any]
    resource_config: dict[str, Any]
    runtime_config: dict[str, Any]

    @property
    def manifest_path(self) -> Optional[str]:
        if self.manifest is None:
            return None
        return self.manifest.source_path

    @property
    def manifest_format(self) -> Optional[str]:
        if self.manifest is None:
            return None
        return self.manifest.source_format

    @property
    def manifest_name(self) -> Optional[str]:
        if self.manifest is None:
            return None
        return self.manifest.name


def _dedupe_ordered(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _coalesce_list(cli_values: list[str], manifest_values: list[str]) -> list[str]:
    return cli_values if cli_values else list(manifest_values)


def _coalesce_scalar(cli_value, manifest_value, default_value):
    if cli_value is not None:
        return cli_value
    if manifest_value is not None:
        return manifest_value
    return default_value


def _resolve_manifest(args) -> Optional[BenchmarkManifest]:
    if not getattr(args, "workload_manifest", None):
        return None
    return load_manifest(Path(getattr(args, "workload_manifest")))


def _resolve_databases(raw_databases: list[str]) -> list[str]:
    resolved: list[str] = []
    seen: set[str] = set()
    for database in raw_databases:
        normalized = database.strip().lower()
        if normalized not in SUPPORTED_DATABASES:
            supported = ", ".join(SUPPORTED_DATABASES)
            raise ValueError(
                f"unsupported database '{database}'. Supported databases: {supported}"
            )
        if normalized in seen:
            continue
        seen.add(normalized)
        resolved.append(normalized)
    return resolved


def _validate_duration(duration: Optional[str]) -> Optional[str]:
    if duration is None:
        return None
    if not re.fullmatch(r"\d+(ms|s|m|h)", duration):
        raise ValueError("duration must use a simple time literal such as 500ms, 60s, 5m, or 1h")
    return duration


def _validate_runtime_config(runtime_config: dict[str, Any]) -> dict[str, Any]:
    aof_enabled = runtime_config.get("aof_enabled")
    if aof_enabled is not None and not isinstance(aof_enabled, bool):
        raise ValueError("aof_enabled must be a boolean when provided")

    aof_fsync = runtime_config.get("aof_fsync")
    if aof_fsync is not None and aof_fsync not in SUPPORTED_AOF_FSYNC_POLICIES:
        supported = ", ".join(SUPPORTED_AOF_FSYNC_POLICIES)
        raise ValueError(f"aof_fsync must be one of: {supported}")

    maxmemory = runtime_config.get("maxmemory")
    if maxmemory is not None and (not isinstance(maxmemory, str) or not maxmemory.strip()):
        raise ValueError("maxmemory must be a non-empty size literal when provided")

    eviction_policy = runtime_config.get("eviction_policy")
    if eviction_policy is not None and eviction_policy not in SUPPORTED_EVICTION_POLICIES:
        supported = ", ".join(SUPPORTED_EVICTION_POLICIES)
        raise ValueError(f"eviction_policy must be one of: {supported}")

    return runtime_config


def resolve_benchmark_spec(args) -> ResolvedBenchmarkSpec:
    manifest = _resolve_manifest(args)
    manifest_databases = manifest.databases if manifest else []
    manifest_workloads = manifest.workloads if manifest else []
    manifest_commands = manifest.commands if manifest else []
    manifest_groups = manifest.command_groups if manifest else []
    manifest_backends = manifest.backends if manifest else []
    manifest_environment = manifest.environment if manifest else {}
    manifest_resource_config = manifest.resource_config if manifest else {}
    manifest_runtime_config = manifest.runtime_config if manifest else {}
    manifest_settings = dict(manifest.settings) if manifest else {}

    cli_databases = split_csv_values(getattr(args, "databases", []))
    cli_workloads = split_csv_values(getattr(args, "workloads", []))
    cli_commands = split_csv_values(getattr(args, "commands", []))
    cli_groups = split_csv_values(getattr(args, "command_groups", []))
    cli_backends = split_csv_values(getattr(args, "backends", []))

    raw_databases = _coalesce_list(cli_databases, manifest_databases)
    raw_workloads = _coalesce_list(cli_workloads, manifest_workloads)
    raw_commands = _coalesce_list(cli_commands, manifest_commands)
    raw_groups = _coalesce_list(cli_groups, manifest_groups)
    raw_backends = _coalesce_list(cli_backends, manifest_backends)

    workloads = _dedupe_ordered([resolve_workload_name(value) for value in raw_workloads])
    workload_definitions = [get_workload_definition(name).to_dict() for name in workloads]
    workload_default_commands = _dedupe_ordered(
        [
            command
            for name in workloads
            for command in get_workload_definition(name).default_commands
        ]
    )
    commands = _dedupe_ordered([normalize_command_name(value) for value in raw_commands])
    command_groups = _dedupe_ordered([resolve_command_group_name(value) for value in raw_groups])
    expanded_group_commands = expand_command_groups(command_groups)
    resolved_commands = _dedupe_ordered(commands + expanded_group_commands)
    effective_commands = _dedupe_ordered(resolved_commands + workload_default_commands)
    backends = _dedupe_ordered([resolve_backend_name(value) for value in raw_backends])

    mode = None
    if getattr(args, "native", False):
        mode = "native"
    elif getattr(args, "container", False):
        mode = "container"
    elif manifest_environment.get("mode") is not None:
        mode = manifest_environment["mode"]

    output_dir = _coalesce_scalar(
        getattr(args, "output_dir", None), manifest_environment.get("output_dir"), None
    )
    state_file = _coalesce_scalar(
        getattr(args, "state_file", None), manifest_environment.get("state_file"), None
    )
    port_base = _coalesce_scalar(
        getattr(args, "port_base", None), manifest_environment.get("port_base"), DEFAULT_PORT_BASE
    )
    cpus = _coalesce_scalar(getattr(args, "cpus", None), manifest_resource_config.get("cpus"), DEFAULT_CPUS)
    memory = _coalesce_scalar(
        getattr(args, "memory", None), manifest_resource_config.get("memory"), DEFAULT_MEMORY
    )
    threads = _coalesce_scalar(
        getattr(args, "threads", None), manifest_resource_config.get("threads"), None
    )
    build_vortex = _coalesce_scalar(
        getattr(args, "build_vortex", None), manifest_environment.get("build_vortex"), True
    )
    aof_enabled = _coalesce_scalar(
        getattr(args, "aof_enabled", None), manifest_runtime_config.get("aof_enabled"), None
    )
    aof_fsync = _coalesce_scalar(
        getattr(args, "aof_fsync", None), manifest_runtime_config.get("aof_fsync"), None
    )
    maxmemory = _coalesce_scalar(
        getattr(args, "maxmemory", None), manifest_runtime_config.get("maxmemory"), None
    )
    eviction_policy = _coalesce_scalar(
        getattr(args, "eviction_policy", None),
        manifest_runtime_config.get("eviction_policy"),
        None,
    )

    if port_base <= 0:
        raise ValueError("port base must be a positive integer")
    if cpus <= 0:
        raise ValueError("cpus must be a positive integer")
    if threads is not None and threads <= 0:
        raise ValueError("threads must be a positive integer when provided")
    if not isinstance(memory, str) or not memory.strip():
        raise ValueError("memory must be a non-empty string")

    environment = {
        "mode": mode,
        "output_dir": output_dir,
        "state_file": state_file,
        "port_base": port_base,
        "build_vortex": build_vortex,
    }
    resource_config = {
        "cpus": cpus,
        "memory": memory,
        "threads": threads,
    }
    runtime_config = _validate_runtime_config(
        {
            key: value
            for key, value in {
                "aof_enabled": aof_enabled,
                "aof_fsync": aof_fsync,
                "maxmemory": maxmemory.strip() if isinstance(maxmemory, str) else maxmemory,
                "eviction_policy": eviction_policy,
            }.items()
            if value is not None
        }
    )

    return ResolvedBenchmarkSpec(
        manifest=manifest,
        databases=_resolve_databases(raw_databases),
        workloads=workloads,
        workload_definitions=workload_definitions,
        workload_default_commands=workload_default_commands,
        commands=commands,
        command_groups=command_groups,
        expanded_group_commands=expanded_group_commands,
        resolved_commands=resolved_commands,
        effective_commands=effective_commands,
        backends=backends,
        duration=_validate_duration(_coalesce_scalar(getattr(args, "duration", None), manifest.duration if manifest else None, None)),
        mode=mode,
        output_dir=output_dir,
        state_file=state_file,
        port_base=port_base,
        cpus=cpus,
        memory=memory.strip(),
        threads=threads,
        build_vortex=bool(build_vortex),
        settings=manifest_settings,
        environment=environment,
        resource_config=resource_config,
        runtime_config=runtime_config,
    )


def validate_run_inputs(spec: ResolvedBenchmarkSpec, selected_databases: list[str]) -> None:
    if not selected_databases:
        raise ValueError("run requires at least one database selection via --db, manifest, or state file")

    if not (spec.workloads or spec.commands or spec.command_groups):
        raise ValueError(
            "run requires at least one workload, command, or command group from CLI flags or the workload manifest"
        )
