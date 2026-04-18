from __future__ import annotations

from pathlib import Path
from typing import Optional

from vortex_benchmark.db.base import append_log_header, best_effort_command_output
from vortex_benchmark.env import (
    build_layout,
    probe_redis_endpoint,
    resolve_benchmark_root,
    resolve_repo_root,
    save_environment_state,
)
from vortex_benchmark.manifests import resolve_benchmark_spec
from vortex_benchmark.models import (
    EnvironmentState,
    ServiceState,
    sanitize_identifier,
    timestamp_slug,
    utc_now,
)


def _resolve_state_path(
    raw_state_file: Optional[str], environments_dir: Path, environment_id: str
) -> Path:
    if raw_state_file:
        return Path(raw_state_file).expanduser().resolve()
    return environments_dir / f"{environment_id}.json"


def _resolve_log_path(
    raw_log_path: Optional[str], logs_dir: Path, environment_id: str, database: str
) -> Path:
    if raw_log_path:
        return Path(raw_log_path).expanduser().resolve()
    return logs_dir / f"{environment_id}-{database}-attach.log"


def _resolve_binary_path(raw_binary_path: Optional[str]) -> Optional[Path]:
    if not raw_binary_path:
        return None
    return Path(raw_binary_path).expanduser().resolve()


def execute_attach(args) -> EnvironmentState:
    spec = resolve_benchmark_spec(args)
    databases = spec.databases

    if len(databases) != 1:
        raise ValueError("attach requires exactly one database via --db")
    if spec.mode == "container":
        raise ValueError("attach only supports native or already-running host processes")
    if not getattr(args, "host", None):
        raise ValueError("attach requires --host")
    if getattr(args, "port", None) is None or args.port <= 0:
        raise ValueError("attach requires a positive --port")
    if getattr(args, "pid", None) is None or args.pid <= 0:
        raise ValueError("attach requires a positive --pid")

    database = databases[0]
    host = str(args.host).strip()
    port = int(args.port)
    pid = int(args.pid)
    label = sanitize_identifier(getattr(args, "label", None) or "external")

    if not probe_redis_endpoint(host, port):
        raise ValueError(
            f"attach target {database} at {host}:{port} is not reachable via RESP ping"
        )

    environment_id = sanitize_identifier(
        f"{timestamp_slug()}-native-{database}-attach-{label}"
    )
    layout = build_layout(spec.output_dir)
    state_path = _resolve_state_path(spec.state_file, layout.environments_dir, environment_id)
    log_path = _resolve_log_path(
        getattr(args, "log_path", None), layout.logs_dir, environment_id, database
    )
    binary_path = _resolve_binary_path(getattr(args, "binary_path", None))
    binary_display = str(binary_path) if binary_path is not None else "external"
    version = (
        best_effort_command_output([str(binary_path), "--version"])
        if binary_path is not None and binary_path.exists()
        else None
    )

    append_log_header(
        log_path,
        f"attached external service: database={database} host={host} port={port} pid={pid} label={label}",
    )
    if binary_path is not None:
        append_log_header(log_path, f"binary path: {binary_path}")
    if spec.runtime_config:
        formatted_runtime = ", ".join(
            f"{key}={value}" for key, value in sorted(spec.runtime_config.items())
        )
        append_log_header(log_path, f"runtime_config: {formatted_runtime}")

    now = utc_now()
    state = EnvironmentState(
        environment_id=environment_id,
        created_at=now,
        updated_at=now,
        project_root=str(resolve_repo_root()),
        benchmark_root=str(resolve_benchmark_root()),
        artifact_root=str(layout.root),
        state_file=str(state_path),
        mode="native",
        requested_databases=[database],
        status="ready",
        services=[
            ServiceState(
                database=database,
                mode="native",
                host=host,
                port=port,
                ready=True,
                log_path=str(log_path),
                started_at=now,
                ready_at=now,
                status="running",
                external=True,
                pid=pid,
                process_group=pid,
                stop_signal="external-managed",
                resource_config={
                    "cpus": spec.cpus,
                    "memory": spec.memory,
                    "threads": spec.threads,
                },
                runtime_config=dict(spec.runtime_config),
                metadata={
                    "bind": f"{host}:{port}",
                    "binary": binary_display,
                    "external": True,
                    "label": label,
                    "version": version,
                },
            )
        ],
        runtime_config=dict(spec.runtime_config),
    )
    save_environment_state(state, state_path)

    print(f"environment attached: {state.environment_id}")
    print(f"state file: {state.state_file}")
    print(
        f"- {database}: external {host}:{port} pid={pid} label={label} log={log_path}"
    )
    if spec.runtime_config:
        formatted_runtime = ", ".join(
            f"{key}={value}" for key, value in sorted(spec.runtime_config.items())
        )
        print(f"  runtime: {formatted_runtime}")

    return state