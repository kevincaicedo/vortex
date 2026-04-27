from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from vortex_benchmark.backends import BackendRunContext, execute_backend, resolve_backend_names
from vortex_benchmark.backends.base import BackendExecutionRecord
from vortex_benchmark.env import build_layout, load_environment_state, probe_redis_endpoint
from vortex_benchmark.manifests import resolve_benchmark_spec, validate_run_inputs
from vortex_benchmark.models import EnvironmentState, split_csv_values, timestamp_slug, utc_now
from vortex_benchmark.telemetry import capture_host_metadata, capture_run_validity


VORTEX_ONLY_RUNTIME_KEYS = {"io_backend", "ring_size", "fixed_buffers", "sqpoll_idle_ms"}


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


def _validate_runtime_config(state: EnvironmentState, spec, selected_databases: list[str]) -> None:
    if not spec.runtime_config:
        return

    mismatches: list[str] = []
    for service in state.services:
        if service.database not in selected_databases:
            continue
        for key, expected in spec.runtime_config.items():
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


def execute_run(args, preloaded_state: Optional[EnvironmentState] = None) -> Path:
    spec = resolve_benchmark_spec(args)
    state = _resolve_state(spec.state_file, preloaded_state)
    selected_databases = spec.databases or list(state.requested_databases)
    validate_run_inputs(spec, selected_databases)
    _validate_runtime_config(state, spec, selected_databases)
    resolved_backends = resolve_backend_names(spec)

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
                    record = execute_backend(backend_name, context)
                except Exception as error:
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

    print(f"run request written: {request_path}")
    print(f"run summary written: {summary_path}")
    print(
        f"executed backends: {', '.join(resolved_backends)} across {len(selected_databases)} database target(s)"
    )
    if failures:
        raise RuntimeError(
            f"{len(failures)} backend execution(s) failed; inspect {summary_path} for details"
        )
    return summary_path
