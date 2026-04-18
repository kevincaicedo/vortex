from __future__ import annotations

from pathlib import Path
from typing import Optional

from vortex_benchmark.db import get_adapter, start_service, stop_service
from vortex_benchmark.db.base import StartRequest
from vortex_benchmark.env import build_layout, resolve_benchmark_root, resolve_repo_root, save_environment_state
from vortex_benchmark.manifests import resolve_benchmark_spec
from vortex_benchmark.models import (
    CONTAINER_DATABASES,
    NATIVE_DATABASES,
    EnvironmentState,
    build_environment_id,
    utc_now,
)


def resolve_mode(databases: list[str], *, requested_mode: Optional[str]) -> str:
    if requested_mode == "native":
        unsupported = [database for database in databases if database not in NATIVE_DATABASES]
        if unsupported:
            raise ValueError(
                f"native mode only supports {', '.join(NATIVE_DATABASES)}; unsupported: {', '.join(unsupported)}"
            )
        return "native"

    if requested_mode == "container":
        unsupported = [database for database in databases if database not in CONTAINER_DATABASES]
        if unsupported:
            raise ValueError(
                f"container mode only supports {', '.join(CONTAINER_DATABASES)}; unsupported: {', '.join(unsupported)}"
            )
        return "container"

    return "native" if all(database in NATIVE_DATABASES for database in databases) else "container"


def _resolve_state_path(
    raw_state_file: Optional[str], environments_dir: Path, environment_id: str
) -> Path:
    if raw_state_file:
        return Path(raw_state_file).expanduser().resolve()
    return environments_dir / f"{environment_id}.json"


def execute_setup(args) -> EnvironmentState:
    spec = resolve_benchmark_spec(args)
    databases = spec.databases
    if not databases:
        raise ValueError("setup requires at least one database via --db or workload manifest")

    mode = resolve_mode(databases, requested_mode=spec.mode)
    environment_id = build_environment_id(mode, databases)
    layout = build_layout(spec.output_dir)
    state_path = _resolve_state_path(spec.state_file, layout.environments_dir, environment_id)

    started_services = []
    project_root = resolve_repo_root()
    benchmark_root = resolve_benchmark_root()

    try:
        port = spec.port_base
        for database in databases:
            adapter = get_adapter(database)
            request = StartRequest(
                database=database,
                mode=mode,
                host="127.0.0.1",
                port=port,
                cpus=spec.cpus,
                memory=spec.memory,
                threads=spec.threads,
                environment_id=environment_id,
                project_root=project_root,
                benchmark_root=benchmark_root,
                artifact_root=layout.root,
                runtime_dir=layout.runtime_dir / environment_id / database,
                log_path=layout.logs_dir / f"{environment_id}-{database}.log",
                build_vortex=spec.build_vortex,
                runtime_config=dict(spec.runtime_config),
            )
            started_services.append(start_service(adapter, request))
            port += 1
    except Exception:
        for service in reversed(started_services):
            stop_service(service)
        raise

    now = utc_now()
    state = EnvironmentState(
        environment_id=environment_id,
        created_at=now,
        updated_at=now,
        project_root=str(project_root),
        benchmark_root=str(benchmark_root),
        artifact_root=str(layout.root),
        state_file=str(state_path),
        mode=mode,
        requested_databases=databases,
        status="ready",
        services=started_services,
        runtime_config=dict(spec.runtime_config),
    )
    save_environment_state(state, state_path)

    print(f"environment ready: {state.environment_id}")
    print(f"state file: {state.state_file}")
    for service in state.services:
        handle = service.container_id or str(service.pid)
        print(
            f"- {service.database}: {service.mode} {service.host}:{service.port} handle={handle} log={service.log_path}"
        )
        if service.runtime_config:
            formatted_runtime = ", ".join(
                f"{key}={value}" for key, value in sorted(service.runtime_config.items())
            )
            print(f"  runtime: {formatted_runtime}")

    return state
