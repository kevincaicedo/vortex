from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from vortex_benchmark.backends import BackendRunContext, execute_backend, resolve_backend_names
from vortex_benchmark.backends.base import BackendExecutionRecord
from vortex_benchmark.db.vortex import normalize_vortex_runtime_config
from vortex_benchmark.env import (
    build_layout,
    load_environment_state,
    probe_redis_endpoint,
    resolve_benchmark_root,
    resolve_repo_root,
    save_environment_state,
)
from vortex_benchmark.manifests import resolve_benchmark_spec, validate_run_inputs
from vortex_benchmark.models import (
    EnvironmentState,
    ServiceState,
    sanitize_identifier,
    split_csv_values,
    timestamp_slug,
    utc_now,
)
from vortex_benchmark.preflight import build_preflight_summary, write_preflight
from vortex_benchmark.progress import ProgressReporter
from vortex_benchmark.session import (
    build_ssh_options,
    build_session_header,
    command_line,
    copy_source_to_ssh,
    copy_ssh_path,
    make_session_dir,
    run_ssh,
    update_session_json,
    write_session_json,
)
from vortex_benchmark.telemetry import capture_host_metadata, capture_run_validity


VORTEX_ONLY_RUNTIME_KEYS = {
    "io_backend",
    "telemetry_mode",
    "telemetry_local_sample_rate",
    "telemetry_flush_interval_ms",
    "shard_count",
    "ring_size",
    "fixed_buffers",
    "fixed_buffer_registration",
    "sqpoll_idle_ms",
}


def _replicate_id(replicate_index: int) -> str:
    return f"rep{replicate_index:02d}"


def _annotate_record_with_replicate(
    record: BackendExecutionRecord,
    *,
    suite_run_id: str,
    replicate_index: int,
    replicate_count: int,
) -> None:
    replicate_id = _replicate_id(replicate_index)
    record.selection = {
        **(record.selection or {}),
        "replicate_index": replicate_index,
        "replicate_count": replicate_count,
        "replicate_id": replicate_id,
    }
    record.artifacts = {
        **(record.artifacts or {}),
        "suite_run_id": suite_run_id,
        "replicate_run_id": record.artifacts.get("replicate_run_id") if record.artifacts else record.backend,
        "replicate_id": replicate_id,
    }
    record.artifacts["replicate_run_id"] = record.artifacts.get("replicate_run_id") or f"{suite_run_id}-{replicate_id}"

    for item in record.items:
        item["suite_run_id"] = suite_run_id
        item["replicate_index"] = replicate_index
        item["replicate_count"] = replicate_count
        item["replicate_id"] = replicate_id
        item["replicate_run_id"] = record.artifacts["replicate_run_id"]


def has_run_selection(args) -> bool:
    return any(
        (
            split_csv_values(args.workloads),
            split_csv_values(args.commands),
            split_csv_values(args.command_groups),
            split_csv_values(args.backends),
            args.workload_manifest,
            args.duration,
        )
    )


def _resolve_state(state_file: Optional[str], preloaded_state: Optional[EnvironmentState]) -> EnvironmentState:
    if preloaded_state is not None:
        return preloaded_state
    if not state_file:
        raise ValueError("run requires --state-file when no environment has just been set up")
    return load_environment_state(Path(state_file).expanduser().resolve())


def _profile_defaults(args) -> None:
    profile = getattr(args, "profile", None)
    if not profile:
        return
    if profile == "quick":
        if getattr(args, "repeat", None) is None:
            args.repeat = 1
        args.evidence_tier = "exploratory"
    elif profile == "engineering":
        args.evidence_tier = "engineering"
    elif profile == "citation":
        if getattr(args, "repeat", None) is None:
            args.repeat = 3
        args.evidence_tier = "citation-grade"
    elif profile == "diagnostic":
        args.evidence_tier = "engineering"


def _infer_target_mode(args, *, has_state_file: bool) -> str:
    requested = getattr(args, "target_mode", None)
    if requested:
        return requested
    if has_state_file:
        return "host-port"
    if getattr(args, "ssh_start_command", None):
        return "ssh-managed"
    if getattr(args, "ssh_target", None):
        return "ssh-attach"
    if getattr(args, "target_url", None) or getattr(args, "target_host", None):
        return "host-port"
    return "local"


def _parse_target_endpoint(args) -> tuple[str, int]:
    raw_url = getattr(args, "target_url", None)
    if raw_url:
        parsed = urlparse(raw_url if "://" in raw_url else f"redis://{raw_url}")
        if not parsed.hostname or parsed.port is None:
            raise ValueError("--target-url must include host and port")
        return parsed.hostname, parsed.port

    host = getattr(args, "target_host", None)
    port = getattr(args, "target_port", None)
    if not host or port is None:
        raise ValueError("attach target modes require --target-url or --target-host with --target-port")
    if port <= 0:
        raise ValueError("--target-port must be positive")
    return str(host), int(port)


def _write_explain(path: Path, payload: dict[str, object]) -> None:
    lines = [
        "# Resolved Benchmark Plan",
        "",
        f"- Target mode: `{payload.get('target_mode')}`",
        f"- Databases: `{', '.join(payload.get('selected_databases') or [])}`",
        f"- Backends: `{', '.join(payload.get('resolved_backends') or [])}`",
        f"- Commands: `{', '.join(payload.get('effective_commands') or [])}`",
        f"- Workloads: `{', '.join(payload.get('workloads') or [])}`",
        f"- Repeat: `{payload.get('repeat_count')}`",
        f"- Duration: `{payload.get('duration') or 'n/a'}`",
        f"- Artifact root: `{payload.get('artifact_root')}`",
        f"- Remote load host: `{payload.get('remote_load_host') or 'n/a'}`",
        f"- SSH workdir: `{payload.get('ssh_workdir') or 'n/a'}`",
        f"- SSH port: `{payload.get('ssh_port') or 'default'}`",
        f"- SSH identity file: `{payload.get('ssh_identity_file') or 'n/a'}`",
        f"- SSH config: `{payload.get('ssh_config') or 'n/a'}`",
        f"- SSH options: `{', '.join(payload.get('ssh_option') or []) or 'n/a'}`",
        f"- SSH connect timeout: `{payload.get('ssh_connect_timeout') or 'default'}`",
        f"- SSH copy source: `{payload.get('ssh_copy_source')!s}`",
        f"- SSH build command: `{payload.get('ssh_build_command') or 'n/a'}`",
        f"- Remote artifact return: `{payload.get('remote_artifact_return') or 'n/a'}`",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _append_optional(argv: list[str], flag: str, value: object | None) -> None:
    if value is not None and value != "":
        argv.extend([flag, str(value)])


def _append_csv(argv: list[str], flag: str, values: list[str] | tuple[str, ...]) -> None:
    if values:
        argv.extend([flag, ",".join(str(value) for value in values)])


def _append_runtime_args(argv: list[str], args, spec) -> None:
    _append_optional(argv, "--threads", getattr(args, "threads", None))
    if getattr(args, "aof_enabled", None) is True:
        argv.append("--aof-enabled")
    elif getattr(args, "aof_enabled", None) is False:
        argv.append("--aof-disabled")
    _append_optional(argv, "--aof-fsync", getattr(args, "aof_fsync", None))
    _append_optional(
        argv,
        "--aof-max-pending-fsync-bytes",
        getattr(args, "aof_max_pending_fsync_bytes", None),
    )
    _append_optional(argv, "--maxmemory", getattr(args, "maxmemory", None))
    _append_optional(argv, "--eviction-policy", getattr(args, "eviction_policy", None))
    _append_optional(argv, "--io-backend", getattr(args, "io_backend", None))
    _append_optional(argv, "--telemetry-mode", getattr(args, "telemetry_mode", None))
    _append_optional(
        argv,
        "--telemetry-local-sample-rate",
        getattr(args, "telemetry_local_sample_rate", None),
    )
    _append_optional(
        argv,
        "--telemetry-flush-interval-ms",
        getattr(args, "telemetry_flush_interval_ms", None),
    )
    _append_optional(argv, "--ring-size", getattr(args, "ring_size", None))
    _append_optional(argv, "--fixed-buffers", getattr(args, "fixed_buffers", None))
    _append_optional(
        argv,
        "--fixed-buffer-registration",
        getattr(args, "fixed_buffer_registration", None),
    )
    _append_optional(argv, "--sqpoll-idle-ms", getattr(args, "sqpoll_idle_ms", None))


def _remote_manifest_path(manifest_path: str | None) -> str | None:
    if not manifest_path:
        return None
    path = Path(manifest_path).expanduser()
    try:
        return path.resolve().relative_to(resolve_repo_root()).as_posix()
    except (OSError, ValueError):
        return manifest_path


def _remote_load_host(args) -> str | None:
    return getattr(args, "ssh_load_host", None) or None


def _remote_workdir(args) -> str:
    workdir = getattr(args, "ssh_workdir", None)
    if not workdir:
        raise ValueError("remote benchmark copy/build/load execution requires --ssh-workdir")
    return str(workdir)


def _remote_artifact_root(session_dir: Path) -> str:
    return f".artifacts/benchmarks/remote-load/{session_dir.name}"


def _remote_artifact_return_dir(layout_root: Path, session_dir: Path) -> Path:
    return layout_root / "remote" / session_dir.name


def _ssh_options(args, *, scp: bool = False) -> list[str]:
    return build_ssh_options(
        port=getattr(args, "ssh_port", None),
        identity_file=getattr(args, "ssh_identity_file", None),
        config_file=getattr(args, "ssh_config", None),
        extra_options=getattr(args, "ssh_option", []) or [],
        connect_timeout=getattr(args, "ssh_connect_timeout", None),
        scp=scp,
    )


def _ssh_transport_metadata(args) -> dict[str, object]:
    return {
        "port": getattr(args, "ssh_port", None),
        "identity_file": getattr(args, "ssh_identity_file", None),
        "config_file": getattr(args, "ssh_config", None),
        "options": list(getattr(args, "ssh_option", []) or []),
        "connect_timeout": getattr(args, "ssh_connect_timeout", None),
    }


def _remote_benchmark_argv(args, spec, remote_artifact_root: str) -> list[str]:
    host, port = _parse_target_endpoint(args)
    argv = [
        "run",
        "--target-mode",
        "host-port",
        "--target-url",
        f"{host}:{port}",
        "--artifact-root",
        remote_artifact_root,
        "--no-color",
    ]
    _append_csv(argv, "--db", list(spec.databases))
    argv.append("--native")
    _append_csv(argv, "--backend", resolve_backend_names(spec))
    _append_csv(argv, "--workload", list(spec.workloads))
    _append_csv(argv, "--command", list(spec.effective_commands))
    _append_csv(argv, "--command-group", list(spec.command_groups))
    _append_optional(argv, "--workload-manifest", _remote_manifest_path(spec.manifest_path))
    _append_optional(argv, "--duration", spec.duration)
    _append_optional(argv, "--repeat", spec.repeat_count)
    _append_optional(argv, "--profile", getattr(args, "profile", None))
    _append_optional(argv, "--evidence-tier", spec.evidence_tier)
    _append_runtime_args(argv, args, spec)
    if getattr(args, "json", False):
        argv.append("--json")
    if getattr(args, "no_report", False):
        argv.append("--no-report")
    return argv


def _remote_python_command(workdir: str, argv: list[str]) -> str:
    return (
        f"cd {command_line([workdir])} && "
        f"PYTHONPATH=vortex-benchmark/python python3 -m vortex_benchmark {command_line(argv)}"
    )


def _remote_prepare_hosts(args, load_host: str | None) -> list[str]:
    hosts = [str(getattr(args, "ssh_target"))]
    if load_host and load_host not in hosts:
        hosts.append(load_host)
    return hosts


def _prepare_remote_checkout(args, session_dir: Path, load_host: str | None, progress: ProgressReporter) -> list[dict[str, object]]:
    if not (getattr(args, "ssh_copy_source", False) or getattr(args, "ssh_build_command", None)):
        return []
    workdir = _remote_workdir(args)
    records: list[dict[str, object]] = []
    redactions = getattr(args, "ssh_redact", [])
    ssh_options = _ssh_options(args)
    for host in _remote_prepare_hosts(args, load_host):
        progress.phase("remote-prepare", "start", f"preparing {host}")
        mkdir_record = run_ssh(
            host,
            f"mkdir -p {command_line([workdir])}",
            redactions=redactions,
            ssh_options=ssh_options,
        )
        records.append({"host": host, "step": "mkdir", **mkdir_record})
        if mkdir_record.get("exit_code") != 0:
            raise RuntimeError(f"remote mkdir failed on {host}: {mkdir_record.get('stderr')}")
        if getattr(args, "ssh_copy_source", False):
            copy_record = copy_source_to_ssh(
                host,
                resolve_repo_root(),
                workdir,
                redactions=redactions,
                ssh_options=ssh_options,
            )
            records.append({"host": host, "step": "copy_source", **copy_record})
            if copy_record.get("exit_code") != 0:
                raise RuntimeError(f"remote source copy failed on {host}: {copy_record.get('stderr')}")
        build_command = getattr(args, "ssh_build_command", None)
        if build_command:
            build_record = run_ssh(
                host,
                f"cd {command_line([workdir])} && {build_command}",
                timeout=900.0,
                redactions=redactions,
                ssh_options=ssh_options,
            )
            records.append({"host": host, "step": "build", **build_record})
            if build_record.get("exit_code") != 0:
                raise RuntimeError(f"remote build command failed on {host}: {build_record.get('stderr')}")
        progress.phase("remote-prepare", "ok", f"prepared {host}")
    (session_dir / "remote-prepare.json").write_text(
        json.dumps(records, indent=2) + "\n",
        encoding="utf-8",
    )
    return records


def _execute_remote_load_run(
    args,
    spec,
    layout_root: Path,
    session_dir: Path,
    progress: ProgressReporter,
) -> dict[str, object]:
    load_host = _remote_load_host(args)
    if not load_host:
        raise ValueError("remote load execution requires --ssh-load-host")
    workdir = _remote_workdir(args)
    remote_root = _remote_artifact_root(session_dir)
    remote_argv = _remote_benchmark_argv(args, spec, remote_root)
    remote_command = _remote_python_command(workdir, remote_argv)
    redactions = getattr(args, "ssh_redact", [])
    ssh_options = _ssh_options(args)
    command_path = session_dir / "remote-load-command.txt"
    command_path.write_text(remote_command + "\n", encoding="utf-8")

    progress.phase("remote-load", "start", f"running benchmark load on {load_host}")
    run_record = run_ssh(
        load_host,
        remote_command,
        timeout=7200.0,
        redactions=redactions,
        ssh_options=ssh_options,
    )
    (session_dir / "remote-load-run.json").write_text(
        json.dumps(run_record, indent=2) + "\n",
        encoding="utf-8",
    )
    if run_record.get("exit_code") != 0:
        progress.phase("remote-load", "fail", f"remote load failed on {load_host}")
        raise RuntimeError(f"remote load command failed on {load_host}: {run_record.get('stderr')}")
    progress.phase("remote-load", "ok", f"remote load completed on {load_host}")

    progress.phase("artifact-return", "start", "copying remote load artifacts")
    local_return_dir = _remote_artifact_return_dir(layout_root, session_dir)
    copy_record = copy_ssh_path(
        load_host,
        remote_root,
        local_return_dir,
        timeout=300.0,
        redactions=redactions,
        scp_options=_ssh_options(args, scp=True),
    )
    (session_dir / "remote-load-artifact-copy.json").write_text(
        json.dumps(copy_record, indent=2) + "\n",
        encoding="utf-8",
    )
    if copy_record.get("exit_code") != 0:
        progress.phase("artifact-return", "warn", f"remote load artifact copy failed: {copy_record.get('stderr')}")
    else:
        progress.phase("artifact-return", "ok", "remote load artifacts copied")
    return {
        "host": load_host,
        "workdir": workdir,
        "remote_artifact_root": remote_root,
        "command_file": str(command_path),
        "local_artifact_dir": str(local_return_dir),
        "run": run_record,
        "artifact_copy": copy_record,
    }


def _estimate_progress_steps(spec, target_mode: str, args) -> int:
    backend_count = max(len(resolve_backend_names(spec)), 1)
    database_count = max(len(spec.databases), 1)
    repeat_count = max(spec.repeat_count, 1)
    base = 5
    if target_mode.startswith("ssh") and (
        getattr(args, "ssh_copy_source", False) or getattr(args, "ssh_build_command", None)
    ):
        base += len(_remote_prepare_hosts(args, _remote_load_host(args)))
    if target_mode.startswith("ssh") and _remote_load_host(args):
        base += 3
    else:
        base += backend_count * database_count * repeat_count + 1
    if not getattr(args, "no_report", False):
        base += 1
    if target_mode == "local" or getattr(args, "ssh_stop_command", None):
        base += 1
    return base


def _target_metadata(args, target_mode: str) -> dict[str, object]:
    metadata: dict[str, object] = {"mode": target_mode, "ownership": "managed"}
    if target_mode in {"host-port", "ssh-attach"}:
        host, port = _parse_target_endpoint(args)
        metadata.update({"host": host, "port": port, "ownership": "external"})
    if target_mode.startswith("ssh"):
        if not getattr(args, "ssh_target", None):
            raise ValueError(f"{target_mode} requires --ssh-target")
        metadata.update(
            {
                "ssh_target": getattr(args, "ssh_target", None),
                "service_host": getattr(args, "ssh_service_host", None),
                "load_host": getattr(args, "ssh_load_host", None),
                "workdir": getattr(args, "ssh_workdir", None),
                "artifact_path": getattr(args, "ssh_artifact_path", None),
                "transport": _ssh_transport_metadata(args),
            }
        )
    if target_mode == "ssh-managed":
        if not getattr(args, "ssh_start_command", None):
            raise ValueError("ssh-managed requires --ssh-start-command")
        metadata["ownership"] = "managed"
        metadata["start_command"] = getattr(args, "ssh_start_command", None)
        metadata["stop_command"] = getattr(args, "ssh_stop_command", None)
    return metadata


def _attach_state_for_target(args, spec, layout, target_mode: str) -> EnvironmentState:
    databases = spec.databases
    if len(databases) != 1:
        raise ValueError(f"{target_mode} requires exactly one database via --db")
    database = databases[0]
    host, port = _parse_target_endpoint(args)
    deadline = time.monotonic() + 20.0
    while not probe_redis_endpoint(host, port):
        if time.monotonic() >= deadline:
            break
        time.sleep(0.25)
    if not probe_redis_endpoint(host, port):
        raise ValueError(f"target {database} at {host}:{port} is not reachable via RESP ping")

    now = utc_now()
    label = sanitize_identifier(target_mode)
    environment_id = sanitize_identifier(f"{timestamp_slug()}-{database}-{label}")
    state_path = layout.environments_dir / f"{environment_id}.json"
    log_path = layout.logs_dir / f"{environment_id}-{database}-attach.log"
    log_path.write_text(
        f"attached target: database={database} host={host} port={port} mode={target_mode}\n",
        encoding="utf-8",
    )
    service = ServiceState(
        database=database,
        mode="remote" if target_mode.startswith("ssh") else "native",
        host=host,
        port=port,
        ready=True,
        log_path=str(log_path),
        started_at=now,
        ready_at=now,
        status="running",
        external=target_mode in {"host-port", "ssh-attach"},
        pid=getattr(args, "target_pid", None),
        process_group=getattr(args, "target_pid", None),
        stop_signal="external-managed" if target_mode in {"host-port", "ssh-attach"} else None,
        resource_config={
            "cpus": spec.cpus,
            "memory": spec.memory,
            "threads": spec.threads,
        },
        runtime_config=dict(spec.runtime_config),
        metadata={
            "bind": f"{host}:{port}",
            "target_mode": target_mode,
            "ssh_target": getattr(args, "ssh_target", None),
            "external": target_mode in {"host-port", "ssh-attach"},
        },
    )
    state = EnvironmentState(
        environment_id=environment_id,
        created_at=now,
        updated_at=now,
        project_root=str(resolve_repo_root()),
        benchmark_root=str(resolve_benchmark_root()),
        artifact_root=str(layout.root),
        state_file=str(state_path),
        mode="remote" if target_mode.startswith("ssh") else "native",
        requested_databases=[database],
        status="ready",
        services=[service],
        runtime_config=dict(spec.runtime_config),
    )
    save_environment_state(state, state_path)
    return state


def _validate_runtime_config(state: EnvironmentState, spec, selected_databases: list[str]) -> None:
    if not spec.runtime_config:
        return

    mismatches: list[str] = []
    for service in state.services:
        if service.database not in selected_databases:
            continue
        expected_runtime = dict(spec.runtime_config)
        if service.database == "vortex":
            expected_runtime = normalize_vortex_runtime_config(expected_runtime, mode=state.mode)
        for key, expected in expected_runtime.items():
            if service.database != "vortex" and key in VORTEX_ONLY_RUNTIME_KEYS:
                continue
            actual = (service.runtime_config or {}).get(key)
            if actual != expected:
                mismatches.append(
                    f"{service.database}: expected {key}={expected!r}, state file has {actual!r}"
                )

    if mismatches:
        raise ValueError(
            "requested runtime config does not match the selected environment state; rerun setup with the same AOF or eviction settings.\n"
            + "\n".join(mismatches)
        )


def _execute_backend_run(
    args,
    preloaded_state: Optional[EnvironmentState] = None,
    progress: ProgressReporter | None = None,
) -> Path:
    progress = progress or ProgressReporter(
        json_mode=getattr(args, "json", False),
        no_color=getattr(args, "no_color", False),
    )
    spec = resolve_benchmark_spec(args)
    state = _resolve_state(spec.state_file, preloaded_state)
    selected_databases = spec.databases or list(state.requested_databases)
    validate_run_inputs(spec, selected_databases)
    _validate_runtime_config(state, spec, selected_databases)
    resolved_backends = resolve_backend_names(spec)
    if progress.total_steps is None:
        progress.set_total_steps(
            3 + max(len(selected_databases), 1) * max(len(resolved_backends), 1) * max(spec.repeat_count, 1)
        )
    progress.phase(
        "preflight",
        "start",
        "validating target reachability and runtime config",
        databases=selected_databases,
        backends=resolved_backends,
    )

    known_databases = {service.database for service in state.services}
    unknown = [database for database in selected_databases if database not in known_databases]
    if unknown:
        raise ValueError(
            f"state file does not contain the requested databases: {', '.join(unknown)}"
        )

    unreachable = [
        service.database
        for service in state.services
        if service.database in selected_databases
        and service.status == "running"
        and not probe_redis_endpoint(service.host, service.port)
    ]
    if unreachable:
        raise ValueError(
            f"the following services are not reachable via RESP ping: {', '.join(unreachable)}"
        )
    progress.phase("preflight", "ok", "target services are reachable")

    layout = build_layout(spec.output_dir or state.artifact_root)
    request_path = layout.requests_dir / f"{timestamp_slug()}-{state.environment_id}-run-request.json"
    selected_services = [
        service
        for service in state.services
        if service.database in selected_databases
    ]
    request_validity = capture_run_validity(
        services=selected_services,
        repo_root=Path(state.project_root).expanduser().resolve(),
        evidence_tier=spec.evidence_tier,
        requested_repeat_count=spec.repeat_count,
        aggregates_multiple_replicates=spec.repeat_count > 1,
    )
    payload = {
        "schema_version": 1,
        "generated_at": utc_now(),
        "environment_id": state.environment_id,
        "state_file": state.state_file,
        "host_metadata": capture_host_metadata(),
        "validity": request_validity,
        "mode": state.mode,
        "selected_databases": selected_databases,
        "duration": spec.duration,
        "workloads": spec.workloads,
        "workload_definitions": spec.workload_definitions,
        "workload_default_commands": spec.workload_default_commands,
        "commands": spec.commands,
        "command_groups": spec.command_groups,
        "expanded_group_commands": spec.expanded_group_commands,
        "resolved_commands": spec.resolved_commands,
        "effective_commands": spec.effective_commands,
        "workload_manifest": spec.manifest_path,
        "manifest_format": spec.manifest_format,
        "manifest_name": spec.manifest_name,
        "requested_backends": spec.backends,
        "resolved_backends": resolved_backends,
        "environment": spec.environment,
        "resource_config": spec.resource_config,
        "runtime_config": spec.runtime_config,
        "settings": spec.settings,
        "execution_mode": "executed",
        "services": [
            {
                "database": service.database,
                "mode": service.mode,
                "host": service.host,
                "port": service.port,
                "log_path": service.log_path,
                "resource_config": service.resource_config,
                "runtime_config": service.runtime_config,
            }
            for service in selected_services
        ],
    }
    request_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    progress.phase("request", "ok", f"resolved request written to {request_path}")

    service_by_database = {
        service.database: service
        for service in state.services
        if service.database in selected_databases
    }
    run_id = request_path.stem.removesuffix("-run-request")
    records: list[BackendExecutionRecord] = []
    failures: list[str] = []

    for replicate_index in range(1, spec.repeat_count + 1):
        replicate_run_id = (
            run_id
            if spec.repeat_count == 1
            else f"{run_id}-{_replicate_id(replicate_index)}"
        )
        for database in selected_databases:
            service = service_by_database[database]
            progress.phase(
                "run",
                "start",
                f"{database} replicate {_replicate_id(replicate_index)}",
                database=database,
                replicate_index=replicate_index,
                replicate_count=spec.repeat_count,
            )
            context = BackendRunContext(
                spec=spec,
                state=state,
                service=service,
                layout=layout,
                request_path=request_path,
                run_id=replicate_run_id,
                suite_run_id=run_id,
                replicate_index=replicate_index,
                replicate_count=spec.repeat_count,
                selected_workloads=list(spec.workloads),
                selected_commands=list(spec.effective_commands),
            )
            for backend_name in resolved_backends:
                backend_started = utc_now()
                try:
                    progress.phase(
                        "backend",
                        "start",
                        f"{backend_name} on {database}",
                        backend=backend_name,
                        database=database,
                    )
                    record = execute_backend(backend_name, context)
                    progress.phase(
                        "backend",
                        "ok",
                        f"{backend_name} on {database}",
                        backend=backend_name,
                        database=database,
                    )
                except Exception as error:
                    progress.phase(
                        "backend",
                        "fail",
                        f"{backend_name} on {database}: {error}",
                        backend=backend_name,
                        database=database,
                    )
                    failures.append(
                        f"{backend_name}:{database}:{_replicate_id(replicate_index)}: {error}"
                    )
                    record = BackendExecutionRecord(
                        backend=backend_name,
                        database=database,
                        status="failed",
                        started_at=backend_started,
                        completed_at=utc_now(),
                        duration_seconds=0.0,
                        selection={
                            "commands": context.selected_commands,
                            "workloads": context.selected_workloads,
                        },
                        artifacts={
                            "replicate_run_id": replicate_run_id,
                        },
                        notes=[str(error)],
                    )
                _annotate_record_with_replicate(
                    record,
                    suite_run_id=run_id,
                    replicate_index=replicate_index,
                    replicate_count=spec.repeat_count,
                )
                records.append(record)

    summary_path = layout.results_dir / f"{run_id}-summary.json"
    telemetry_summary_paths = sorted(
        {
            telemetry.get("summary_path")
            for record in records
            for item in record.items
            for telemetry in [
                ((item.get("observability") or {}).get("host_telemetry") or {})
            ]
            if telemetry.get("summary_path")
        }
    )
    summary_validity = {
        **request_validity,
        "repeat_count_executed": spec.repeat_count,
        "aggregates_multiple_replicates": spec.repeat_count > 1,
        "host_telemetry_captured": bool(telemetry_summary_paths),
        "host_telemetry_summary_paths": telemetry_summary_paths,
    }
    summary_payload = {
        "schema_version": 1,
        "generated_at": utc_now(),
        "run_id": run_id,
        "environment_id": state.environment_id,
        "request_path": str(request_path),
        "summary_path": str(summary_path),
        "validity": summary_validity,
        "selected_databases": selected_databases,
        "resolved_backends": resolved_backends,
        "failure_count": len(failures),
        "failures": failures,
        "results": [record.to_dict() for record in records],
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2) + "\n", encoding="utf-8")

    if not getattr(args, "json", False):
        print(f"run request written: {request_path}")
        print(f"run summary written: {summary_path}")
        print(
            f"executed backends: {', '.join(resolved_backends)} across {len(selected_databases)} database target(s)"
        )
    progress.phase("summary", "ok", f"run summary written to {summary_path}")
    if failures:
        raise RuntimeError(
            f"{len(failures)} backend execution(s) failed; inspect {summary_path} for details"
        )
    return summary_path


def _execute_one_command_run(args) -> Path:
    _profile_defaults(args)
    spec = resolve_benchmark_spec(args)
    target_mode = _infer_target_mode(args, has_state_file=bool(spec.state_file))
    layout = build_layout(spec.output_dir)
    session_dir = make_session_dir(layout.root, target_mode)
    progress = ProgressReporter(
        json_mode=getattr(args, "json", False),
        no_color=getattr(args, "no_color", False),
        total_steps=3
        if getattr(args, "dry_run", False)
        else _estimate_progress_steps(spec, target_mode, args),
    )
    started_at = utc_now()
    target = _target_metadata(args, target_mode)
    preflight_path = write_preflight(
        session_dir / "preflight.json",
        build_preflight_summary(target_mode=target_mode),
    )
    remote_metadata: list[dict[str, object]] = []
    if target_mode.startswith("ssh") and getattr(args, "ssh_target", None):
        for remote_command in (
            "uname -a",
            "nproc 2>/dev/null || true",
            "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || true",
            "command -v perf >/dev/null 2>&1 && perf --version || true",
        ):
            remote_metadata.append(
                run_ssh(
                    str(getattr(args, "ssh_target")),
                    remote_command,
                    timeout=5.0,
                    redactions=getattr(args, "ssh_redact", []),
                    ssh_options=_ssh_options(args),
                )
            )
        (session_dir / "remote-metadata.json").write_text(
            json.dumps(remote_metadata, indent=2) + "\n",
            encoding="utf-8",
        )
    session_payload = build_session_header(
        session_dir=session_dir,
        artifact_root=layout.root,
        target_mode=target_mode,
        command=command_line(sys.argv),
        profile=getattr(args, "profile", None),
        target=target,
        workload_contract={
            "databases": spec.databases,
            "workloads": spec.workloads,
            "commands": spec.effective_commands,
            "backends": spec.backends,
            "repeat_count": spec.repeat_count,
            "duration": spec.duration,
            "manifest_path": spec.manifest_path,
        },
        artifact_paths={
            "preflight": str(preflight_path),
            "remote_metadata": str(session_dir / "remote-metadata.json")
            if remote_metadata
            else None,
        },
        started_at=started_at,
    )
    write_session_json(session_dir, session_payload)
    progress.phase("session", "ok", f"session initialized at {session_dir}")

    explain_payload = {
        "target_mode": target_mode,
        "selected_databases": spec.databases,
        "resolved_backends": resolve_backend_names(spec),
        "effective_commands": spec.effective_commands,
        "workloads": spec.workloads,
        "repeat_count": spec.repeat_count,
        "duration": spec.duration,
        "artifact_root": str(layout.root),
        "remote_load_host": _remote_load_host(args),
        "ssh_workdir": getattr(args, "ssh_workdir", None),
        "ssh_port": getattr(args, "ssh_port", None),
        "ssh_identity_file": getattr(args, "ssh_identity_file", None),
        "ssh_config": getattr(args, "ssh_config", None),
        "ssh_option": list(getattr(args, "ssh_option", []) or []),
        "ssh_connect_timeout": getattr(args, "ssh_connect_timeout", None),
        "ssh_copy_source": getattr(args, "ssh_copy_source", False),
        "ssh_build_command": getattr(args, "ssh_build_command", None),
        "remote_artifact_return": str(_remote_artifact_return_dir(layout.root, session_dir))
        if target_mode.startswith("ssh")
        else None,
    }
    explain_path = session_dir / "plan.md"
    if getattr(args, "explain", False) or getattr(args, "dry_run", False):
        _write_explain(explain_path, explain_payload)
        progress.phase("plan", "ok", f"resolved plan written to {explain_path}")

    if getattr(args, "dry_run", False):
        update_session_json(
            session_dir,
            status="dry-run",
            exit_code=0,
            ended_at=utc_now(),
            artifact_paths={**session_payload["artifact_paths"], "plan": str(explain_path)},
        )
        progress.phase("session", "dry-run", f"dry-run session written to {session_dir}")
        if getattr(args, "json", False):
            print(json.dumps({"session": str(session_dir), "plan": str(explain_path)}))
        else:
            print(f"dry-run session written: {session_dir}")
        return session_dir / "session.json"

    managed_state: EnvironmentState | None = None
    remote_start: dict[str, object] | None = None
    remote_stop: dict[str, object] | None = None
    remote_copy: dict[str, object] | None = None
    remote_prepare: list[dict[str, object]] = []
    remote_load: dict[str, object] | None = None
    summary_path: Path | None = None
    report_path: Path | None = None
    try:
        if target_mode.startswith("ssh"):
            remote_prepare = _prepare_remote_checkout(
                args,
                session_dir,
                _remote_load_host(args),
                progress,
            )
        progress.phase("setup", "start", f"target mode {target_mode}")
        if target_mode == "local":
            from vortex_benchmark.commands.setup import execute_setup

            managed_state = execute_setup(args)
        elif target_mode == "ssh-managed":
            remote_start = run_ssh(
                str(getattr(args, "ssh_target")),
                str(getattr(args, "ssh_start_command")),
                redactions=getattr(args, "ssh_redact", []),
                ssh_options=_ssh_options(args),
            )
            if remote_start.get("exit_code") != 0:
                raise RuntimeError(f"remote start command failed: {remote_start.get('stderr')}")
            managed_state = _attach_state_for_target(args, spec, layout, target_mode)
        else:
            managed_state = _attach_state_for_target(args, spec, layout, target_mode)
        progress.phase("setup", "ok", f"state file {managed_state.state_file}")

        args.state_file = managed_state.state_file
        if target_mode.startswith("ssh") and _remote_load_host(args):
            remote_load = _execute_remote_load_run(args, spec, layout.root, session_dir, progress)
        else:
            summary_path = _execute_backend_run(args, preloaded_state=managed_state, progress=progress)

        if summary_path is not None and not getattr(args, "no_report", False):
            from argparse import Namespace
            from vortex_benchmark.commands.report import execute_report

            progress.phase("report", "start", "rendering benchmark report")
            report_path = execute_report(
                Namespace(
                    summary_files=[str(summary_path)],
                    results_dir=None,
                    output_dir=str(layout.root),
                    title=None,
                )
            )
            progress.phase("report", "ok", f"report written to {report_path}")

        if target_mode == "ssh-managed" and getattr(args, "ssh_stop_command", None):
            progress.phase("teardown", "start", "running remote stop command")
            remote_stop = run_ssh(
                str(getattr(args, "ssh_target")),
                str(getattr(args, "ssh_stop_command")),
                redactions=getattr(args, "ssh_redact", []),
                ssh_options=_ssh_options(args),
            )
            if remote_stop.get("exit_code") != 0:
                progress.phase("teardown", "warn", f"remote stop failed: {remote_stop.get('stderr')}")
            else:
                progress.phase("teardown", "ok", "remote stop completed")
        elif target_mode == "local":
            from argparse import Namespace
            from vortex_benchmark.commands.teardown import execute_teardown

            progress.phase("teardown", "start", "stopping local managed services")
            execute_teardown(Namespace(state_file=managed_state.state_file))
            progress.phase("teardown", "ok", "local managed services stopped")

        if target_mode.startswith("ssh") and getattr(args, "ssh_artifact_path", None):
            progress.phase("artifact-return", "start", "copying remote artifacts")
            local_return_dir = _remote_artifact_return_dir(layout.root, session_dir) / "service"
            remote_copy = copy_ssh_path(
                str(getattr(args, "ssh_target")),
                str(getattr(args, "ssh_artifact_path")),
                local_return_dir,
                redactions=getattr(args, "ssh_redact", []),
                scp_options=_ssh_options(args, scp=True),
            )
            if remote_copy.get("exit_code") != 0:
                progress.phase("artifact-return", "warn", f"scp failed: {remote_copy.get('stderr')}")
            else:
                progress.phase("artifact-return", "ok", "remote artifacts copied")

        artifact_paths = {
            **session_payload["artifact_paths"],
            "state_file": managed_state.state_file,
            "summary": str(summary_path) if summary_path else None,
            "report": str(report_path) if report_path else None,
            "plan": str(explain_path) if explain_path.exists() else None,
        }
        if remote_start is not None:
            artifact_paths["remote_start"] = remote_start
        if remote_stop is not None:
            artifact_paths["remote_stop"] = remote_stop
        if remote_copy is not None:
            artifact_paths["remote_copy"] = remote_copy
        if remote_prepare:
            artifact_paths["remote_prepare"] = str(session_dir / "remote-prepare.json")
        if remote_load is not None:
            artifact_paths["remote_load"] = remote_load
        update_session_json(
            session_dir,
            status="completed",
            exit_code=0,
            ended_at=utc_now(),
            artifact_paths=artifact_paths,
        )
        progress.phase("session", "ok", f"completed session {session_dir}")
        return report_path or summary_path or session_dir / "session.json"
    except Exception:
        failure_artifacts: dict[str, object] = {}
        if target_mode == "ssh-managed" and getattr(args, "ssh_stop_command", None):
            progress.phase("teardown", "start", "running remote stop command after failure")
            remote_stop = run_ssh(
                str(getattr(args, "ssh_target")),
                str(getattr(args, "ssh_stop_command")),
                redactions=getattr(args, "ssh_redact", []),
                ssh_options=_ssh_options(args),
            )
            failure_artifacts["remote_stop"] = remote_stop
            if remote_stop.get("exit_code") != 0:
                progress.phase("teardown", "warn", f"remote stop failed: {remote_stop.get('stderr')}")
            else:
                progress.phase("teardown", "ok", "remote stop completed")
        elif target_mode == "local" and managed_state is not None:
            from argparse import Namespace
            from vortex_benchmark.commands.teardown import execute_teardown

            progress.phase("teardown", "start", "stopping local managed services after failure")
            execute_teardown(Namespace(state_file=managed_state.state_file))
            progress.phase("teardown", "ok", "local managed services stopped")
        if remote_prepare:
            failure_artifacts["remote_prepare"] = str(session_dir / "remote-prepare.json")
        if remote_load is not None:
            failure_artifacts["remote_load"] = remote_load
        update_session_json(
            session_dir,
            status="failed",
            exit_code=1,
            ended_at=utc_now(),
            artifact_paths={**session_payload["artifact_paths"], **failure_artifacts},
        )
        raise


def _should_use_one_command(args, preloaded_state: Optional[EnvironmentState]) -> bool:
    if preloaded_state is not None:
        return False
    if getattr(args, "subcommand", None) != "run":
        return False
    if getattr(args, "state_file", None) and not getattr(args, "target_mode", None):
        return False
    return True


def execute_run(args, preloaded_state: Optional[EnvironmentState] = None) -> Path:
    if _should_use_one_command(args, preloaded_state):
        return _execute_one_command_run(args)
    _profile_defaults(args)
    return _execute_backend_run(args, preloaded_state=preloaded_state)
