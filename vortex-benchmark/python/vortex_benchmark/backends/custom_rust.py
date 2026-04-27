from __future__ import annotations

from pathlib import Path

from vortex_benchmark.backends.base import (
    BackendError,
    BackendExecutionRecord,
    BackendRunContext,
    DEFAULT_CUSTOM_KEYSPACE,
    DEFAULT_CUSTOM_OPS_PER_THREAD,
    DEFAULT_CUSTOM_THREAD_SWEEP,
    DEFAULT_CUSTOM_VALUE_SIZE,
    DEFAULT_CUSTOM_WARMUP_OPS,
    coerce_int_list,
    coerce_positive_int,
    get_setting,
    parse_duration_seconds,
    quote_command,
    read_json,
    run_process,
    write_json,
)
from vortex_benchmark.env import resolve_benchmark_root
from vortex_benchmark.models import sanitize_identifier, utc_now
from vortex_benchmark.telemetry import (
    capture_service_snapshot,
    diff_service_snapshots,
    start_host_telemetry_capture,
)


def _resolve_result_json(item_dir: Path) -> Path | None:
    candidates = [
        path
        for path in sorted(item_dir.glob("*.json"))
        if not path.name.endswith("-host-telemetry-summary.json")
    ]
    if not candidates:
        return None
    return candidates[0]


def run_custom_rust_backend(context: BackendRunContext) -> BackendExecutionRecord:
    started_at = utc_now()
    if not context.selected_workloads:
        raise BackendError("custom-rust requires one or more workload selections")

    benchmark_root = resolve_benchmark_root()
    manifest_path = benchmark_root / "rust" / "custom-loadgen" / "Cargo.toml"
    if not manifest_path.exists():
        raise BackendError(f"custom Rust backend manifest is missing: {manifest_path}")

    profile = str(get_setting(context.spec.settings, "custom-rust", "profile", "release")).strip()
    if profile not in {"debug", "release"}:
        raise BackendError("custom-rust.profile must be either 'debug' or 'release'")

    thread_sweep = coerce_int_list(
        get_setting(context.spec.settings, "custom-rust", "thread_sweep"),
        label="custom-rust.thread_sweep",
        default=DEFAULT_CUSTOM_THREAD_SWEEP,
    )
    keyspace_size = coerce_positive_int(
        get_setting(context.spec.settings, "custom-rust", "keyspace_size"),
        label="custom-rust.keyspace_size",
        default=DEFAULT_CUSTOM_KEYSPACE,
    )
    ops_per_thread = coerce_positive_int(
        get_setting(context.spec.settings, "custom-rust", "ops_per_thread"),
        label="custom-rust.ops_per_thread",
        default=DEFAULT_CUSTOM_OPS_PER_THREAD,
    )
    warmup_ops = coerce_positive_int(
        get_setting(context.spec.settings, "custom-rust", "warmup_ops"),
        label="custom-rust.warmup_ops",
        default=DEFAULT_CUSTOM_WARMUP_OPS,
    )
    value_size = coerce_positive_int(
        get_setting(context.spec.settings, "custom-rust", "value_size"),
        label="custom-rust.value_size",
        default=DEFAULT_CUSTOM_VALUE_SIZE,
    )
    duration_seconds = parse_duration_seconds(context.spec.duration)

    backend_dir = (
        context.layout.backend_runs_dir
        / context.run_id
        / sanitize_identifier(context.service.database)
        / "custom-rust"
    )
    backend_dir.mkdir(parents=True, exist_ok=True)

    build_stdout = backend_dir / "build.stdout.log"
    build_stderr = backend_dir / "build.stderr.log"
    build_command = ["cargo", "build", "--manifest-path", str(manifest_path)]
    if profile == "release":
        build_command.insert(2, "--release")
    run_process(build_command, stdout_path=build_stdout, stderr_path=build_stderr, cwd=benchmark_root)

    binary_name = "custom-loadgen"
    binary_path = manifest_path.parent / "target" / profile / binary_name
    if not binary_path.exists():
        raise BackendError(f"custom Rust backend binary was not built at: {binary_path}")

    items: list[dict[str, object]] = []
    total_elapsed = 0.0
    run_commands: list[str] = []
    for workload in context.selected_workloads:
        for thread_count in thread_sweep:
            item_dir = backend_dir / f"{sanitize_identifier(workload)}-t{thread_count}"
            item_dir.mkdir(parents=True, exist_ok=True)
            stdout_path = item_dir / "run.stdout.log"
            stderr_path = item_dir / "run.stderr.log"
            command = [
                str(binary_path),
                "--server",
                context.service.database,
                "--host",
                context.service.host,
                "--port",
                str(context.service.port),
                "--threads",
                str(thread_count),
                "--num-keys",
                str(keyspace_size),
                "--ops-per-thread",
                str(ops_per_thread),
                "--warmup-ops",
                str(warmup_ops),
                "--value-size",
                str(value_size),
                "--output-dir",
                str(item_dir),
                "--workload",
                workload,
            ]

            snapshot_before = capture_service_snapshot(context.service)
            host_telemetry = None
            telemetry = start_host_telemetry_capture(
                item_dir,
                label="custom-rust",
                service=context.service,
            )
            try:
                _, elapsed = run_process(command, stdout_path=stdout_path, stderr_path=stderr_path)
            finally:
                host_telemetry = telemetry.stop()
            snapshot_after = capture_service_snapshot(context.service)
            total_elapsed += elapsed
            run_commands.append(quote_command(command))

            result_file = _resolve_result_json(item_dir)
            if result_file is None:
                raise BackendError(
                    f"custom-rust did not produce a JSON result for workload {workload} thread count {thread_count}"
                )

            payload = read_json(result_file)
            items.append(
                {
                    "workload": payload.get("workload"),
                    "thread_count": payload.get("num_threads"),
                    "command": quote_command(command),
                    "duration_seconds": round(elapsed, 6),
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                    "json_path": str(result_file),
                    "metrics": {
                        "total_ops": payload.get("total_ops"),
                        "aggregate_throughput_ops_sec": payload.get("aggregate_throughput_ops_sec"),
                        "p50_ns": payload.get("p50_ns"),
                        "p95_ns": payload.get("p95_ns"),
                        "p99_ns": payload.get("p99_ns"),
                        "p99_9_ns": payload.get("p99_9_ns"),
                        "p99_999_ns": payload.get("p99_999_ns"),
                        "max_ns": payload.get("max_ns"),
                        "mean_ns": payload.get("mean_ns"),
                    },
                    "observability": {
                        "before": snapshot_before,
                        "after": snapshot_after,
                        "delta": diff_service_snapshots(snapshot_before, snapshot_after),
                        "host_telemetry": host_telemetry,
                    },
                }
            )

    if not items:
        raise BackendError("custom-rust completed without producing any JSON result files")

    notes: list[str] = []
    if duration_seconds is not None:
        notes.append(
            "custom-rust currently uses ops-per-thread execution; the requested duration was recorded for context but not enforced in Phase 4"
        )

    completed_at = utc_now()
    result_path = context.layout.results_dir / f"{context.run_id}-{context.service.database}-custom-rust.json"
    summary = {
        "schema_version": 1,
        "generated_at": completed_at,
        "backend": "custom-rust",
        "database": context.service.database,
        "environment_id": context.state.environment_id,
        "request_path": str(context.request_path),
        "selected_workloads": context.selected_workloads,
        "config": {
            "profile": profile,
            "thread_sweep": thread_sweep,
            "keyspace_size": keyspace_size,
            "ops_per_thread": ops_per_thread,
            "warmup_ops": warmup_ops,
            "value_size": value_size,
            "duration_seconds": duration_seconds,
        },
        "build_command": quote_command(build_command),
        "run_commands": run_commands,
        "items": items,
        "notes": notes,
    }
    write_json(result_path, summary)

    return BackendExecutionRecord(
        backend="custom-rust",
        database=context.service.database,
        status="succeeded",
        started_at=started_at,
        completed_at=completed_at,
        selection={
            "commands": context.selected_commands,
            "workloads": context.selected_workloads,
        },
        artifacts={
            "backend_dir": str(backend_dir),
            "result_json": str(result_path),
            "build_stdout": str(build_stdout),
            "build_stderr": str(build_stderr),
            "host_telemetry_summary_paths": [
                ((item.get("observability") or {}).get("host_telemetry") or {}).get("summary_path")
                for item in items
                if ((item.get("observability") or {}).get("host_telemetry") or {}).get("summary_path")
            ],
        },
        items=items,
        notes=notes,
        duration_seconds=round(total_elapsed, 6),
    )