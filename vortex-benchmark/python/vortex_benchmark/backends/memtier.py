from __future__ import annotations

import math
import subprocess
from pathlib import Path

from vortex_benchmark.backends.base import (
    BackendError,
    BackendExecutionRecord,
    BackendRunContext,
    DEFAULT_MEMTIER_CLIENTS,
    DEFAULT_MEMTIER_DATA_SIZE,
    DEFAULT_MEMTIER_KEYSPACE,
    DEFAULT_MEMTIER_PIPELINE,
    DEFAULT_MEMTIER_REQUESTS,
    DEFAULT_MEMTIER_THREAD_SWEEP,
    backend_settings,
    coerce_int_list,
    coerce_nonnegative_int,
    coerce_positive_int,
    get_setting,
    parse_duration_seconds,
    quote_command,
    read_json,
    run_process,
    send_resp_pipeline,
    write_json,
)
from vortex_benchmark.catalog import get_workload_definition
from vortex_benchmark.models import sanitize_identifier, utc_now
from vortex_benchmark.telemetry import (
    capture_service_snapshot,
    diff_service_snapshots,
    start_host_telemetry_capture,
)


def _redis_cli(service, args: list[str], *, input_text: str = "") -> subprocess.CompletedProcess[str]:
    command = ["redis-cli", "-h", service.host, "-p", str(service.port), *args]
    try:
        return subprocess.run(
            command,
            check=True,
            text=True,
            input=input_text or None,
            capture_output=True,
        )
    except FileNotFoundError as error:
        raise BackendError("required command not found in PATH: redis-cli") from error
    except subprocess.CalledProcessError as error:
        detail = (error.stderr or error.stdout or "").strip() or f"command exited with status {error.returncode}"
        raise BackendError(f"command failed: {quote_command(command)}\n{detail}") from error


def _seed_keyspace(service, *, key_prefix: str, keyspace_size: int, data_size: int) -> None:
    _redis_cli(service, ["FLUSHALL"])
    value = "v" * data_size
    commands = [["SET", f"{key_prefix}{index}", value] for index in range(1, keyspace_size + 1)]
    send_resp_pipeline(service.host, service.port, commands)


def _ratio_from_workload(definition) -> str:
    write_ratio = definition.write_ratio
    read_ratio = definition.read_ratio
    if write_ratio == 0 and read_ratio == 0:
        return "1:1"
    if write_ratio == 0:
        return "0:1"
    if read_ratio == 0:
        return "1:0"
    divisor = math.gcd(write_ratio, read_ratio)
    return f"{write_ratio // divisor}:{read_ratio // divisor}"


def _parse_percentile(stats: dict[str, object], percentile: str) -> object:
    percentiles = stats.get("Percentile Latencies") or {}
    if isinstance(percentiles, dict):
        return percentiles.get(percentile)
    return None


def _parse_memtier_json(path: Path) -> dict[str, object]:
    payload = read_json(path)
    all_stats = payload.get("ALL STATS") or {}
    totals = all_stats.get("Totals") or {}
    gets = all_stats.get("Gets") or {}
    sets = all_stats.get("Sets") or {}

    return {
        "totals": {
            "count": totals.get("Count"),
            "ops_sec": totals.get("Ops/sec"),
            "average_latency_ms": totals.get("Average Latency"),
            "min_latency_ms": totals.get("Min Latency"),
            "max_latency_ms": totals.get("Max Latency"),
            "p50_latency_ms": _parse_percentile(totals, "p50.00"),
            "p95_latency_ms": _parse_percentile(totals, "p95.00"),
            "p99_latency_ms": _parse_percentile(totals, "p99.00"),
            "p99_9_latency_ms": _parse_percentile(totals, "p99.90"),
            "p99_99_latency_ms": _parse_percentile(totals, "p99.99"),
            "p99_999_latency_ms": _parse_percentile(totals, "p99.999"),
        },
        "gets": {
            "ops_sec": gets.get("Ops/sec"),
            "average_latency_ms": gets.get("Average Latency"),
        },
        "sets": {
            "ops_sec": sets.get("Ops/sec"),
            "average_latency_ms": sets.get("Average Latency"),
        },
        "run_information": payload.get("run information") or {},
        "configuration": payload.get("configuration") or {},
    }


def run_memtier_backend(context: BackendRunContext) -> BackendExecutionRecord:
    started_at = utc_now()
    if not context.selected_workloads:
        raise BackendError("memtier_benchmark requires one or more workload selections")

    requests = coerce_positive_int(
        get_setting(context.spec.settings, "memtier_benchmark", "requests"),
        label="memtier_benchmark.requests",
        default=DEFAULT_MEMTIER_REQUESTS,
    )
    clients = coerce_positive_int(
        get_setting(context.spec.settings, "memtier_benchmark", "clients"),
        label="memtier_benchmark.clients",
        default=DEFAULT_MEMTIER_CLIENTS,
    )
    pipeline = coerce_positive_int(
        get_setting(context.spec.settings, "memtier_benchmark", "pipeline"),
        label="memtier_benchmark.pipeline",
        default=DEFAULT_MEMTIER_PIPELINE,
    )
    keyspace_size = coerce_positive_int(
        get_setting(context.spec.settings, "memtier_benchmark", "keyspace_size"),
        label="memtier_benchmark.keyspace_size",
        default=DEFAULT_MEMTIER_KEYSPACE,
    )
    data_size = coerce_positive_int(
        get_setting(context.spec.settings, "memtier_benchmark", "data_size"),
        label="memtier_benchmark.data_size",
        default=DEFAULT_MEMTIER_DATA_SIZE,
    )
    rate_limiting = coerce_nonnegative_int(
        get_setting(context.spec.settings, "memtier_benchmark", "rate_limiting"),
        label="memtier_benchmark.rate_limiting",
        default=None,
    )
    thread_sweep = coerce_int_list(
        get_setting(context.spec.settings, "memtier_benchmark", "thread_sweep"),
        label="memtier_benchmark.thread_sweep",
        default=DEFAULT_MEMTIER_THREAD_SWEEP,
    )
    test_time_seconds = parse_duration_seconds(context.spec.duration)
    backend_config = backend_settings(context.spec.settings, "memtier_benchmark")
    key_prefix = str(backend_config.get("key_prefix", "bench:mt:"))

    backend_dir = (
        context.layout.backend_runs_dir
        / context.run_id
        / sanitize_identifier(context.service.database)
        / "memtier_benchmark"
    )
    backend_dir.mkdir(parents=True, exist_ok=True)

    items: list[dict[str, object]] = []
    notes: list[str] = []
    unsupported: list[str] = []

    for workload_name in context.selected_workloads:
        definition = get_workload_definition(workload_name)
        if definition.transactional or definition.multi_key:
            unsupported.append(workload_name)
            continue

        ratio = str(backend_config.get("ratio", _ratio_from_workload(definition)))
        key_pattern = str(
            backend_config.get(
                "key_pattern",
                "G:G" if definition.distribution == "zipfian" or definition.hot_key else "R:R",
            )
        )
        median = int(backend_config.get("key_median", max(1, keyspace_size // 2)))
        stddev = int(backend_config.get("key_stddev", max(1, keyspace_size // 6)))

        for thread_count in thread_sweep:
            if definition.read_ratio > 0:
                _seed_keyspace(
                    context.service,
                    key_prefix=key_prefix,
                    keyspace_size=keyspace_size,
                    data_size=data_size,
                )
            else:
                _redis_cli(context.service, ["FLUSHALL"])

            stem = f"{sanitize_identifier(workload_name)}-t{thread_count}"
            stdout_path = backend_dir / f"{stem}.stdout.log"
            stderr_path = backend_dir / f"{stem}.stderr.log"
            json_path = backend_dir / f"{stem}.json"
            hdr_prefix = backend_dir / f"{stem}-hdr"
            command = [
                "memtier_benchmark",
                "-s",
                context.service.host,
                "-p",
                str(context.service.port),
                "--protocol=redis",
                "-t",
                str(thread_count),
                "-c",
                str(clients),
                "--pipeline",
                str(pipeline),
                "--data-size",
                str(data_size),
                "--ratio",
                ratio,
                "--key-pattern",
                key_pattern,
                "--key-prefix",
                key_prefix,
                "--key-minimum",
                "1",
                "--key-maximum",
                str(keyspace_size),
                "--distinct-client-seed",
                "--hide-histogram",
                "--command-stats-breakdown=command",
                "--print-percentiles",
                "50,95,99,99.9,99.99,99.999",
                "--json-out-file",
                str(json_path),
                "--hdr-file-prefix",
                str(hdr_prefix),
            ]
            if "G" in key_pattern:
                command.extend(["--key-median", str(median), "--key-stddev", str(stddev)])
            if test_time_seconds is not None:
                command.extend(["--test-time", str(test_time_seconds)])
            else:
                command.extend(["--requests", str(requests)])
            if rate_limiting is not None:
                command.extend(["--rate-limiting", str(rate_limiting)])

            snapshot_before = capture_service_snapshot(context.service)
            host_telemetry = None
            telemetry = start_host_telemetry_capture(
                backend_dir,
                label=stem,
                service=context.service,
            )
            try:
                _, elapsed = run_process(command, stdout_path=stdout_path, stderr_path=stderr_path)
            finally:
                host_telemetry = telemetry.stop()
            snapshot_after = capture_service_snapshot(context.service)
            metrics = _parse_memtier_json(json_path)
            hdr_files = sorted(str(path) for path in backend_dir.glob(f"{stem}-hdr*"))
            items.append(
                {
                    "workload": workload_name,
                    "thread_count": thread_count,
                    "command": quote_command(command),
                    "duration_seconds": round(elapsed, 6),
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                    "json_path": str(json_path),
                    "hdr_files": hdr_files,
                    "metrics": metrics,
                    "observability": {
                        "before": snapshot_before,
                        "after": snapshot_after,
                        "delta": diff_service_snapshots(snapshot_before, snapshot_after),
                        "host_telemetry": host_telemetry,
                    },
                }
            )

    if unsupported:
        notes.append(
            "memtier_benchmark skipped workload families that need multi-key or transactional orchestration: "
            + ", ".join(unsupported)
        )

    if not items:
        raise BackendError(
            "memtier_benchmark did not receive any workload that it can execute in Phase 4; use non-transaction single-key workloads or select the custom-rust backend"
        )

    completed_at = utc_now()
    result_path = context.layout.results_dir / f"{context.run_id}-{context.service.database}-memtier_benchmark.json"
    summary = {
        "schema_version": 1,
        "generated_at": completed_at,
        "backend": "memtier_benchmark",
        "database": context.service.database,
        "environment_id": context.state.environment_id,
        "request_path": str(context.request_path),
        "selected_workloads": context.selected_workloads,
        "config": {
            "requests": requests,
            "clients": clients,
            "pipeline": pipeline,
            "thread_sweep": thread_sweep,
            "keyspace_size": keyspace_size,
            "data_size": data_size,
            "duration_seconds": test_time_seconds,
            "rate_limiting": rate_limiting,
            "key_prefix": key_prefix,
        },
        "items": items,
        "notes": notes,
    }
    write_json(result_path, summary)

    return BackendExecutionRecord(
        backend="memtier_benchmark",
        database=context.service.database,
        status="partial" if unsupported else "succeeded",
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=round(sum(float(item["duration_seconds"]) for item in items), 6),
        selection={
            "commands": context.selected_commands,
            "workloads": context.selected_workloads,
        },
        artifacts={
            "backend_dir": str(backend_dir),
            "result_json": str(result_path),
            "host_telemetry_summary_paths": [
                ((item.get("observability") or {}).get("host_telemetry") or {}).get("summary_path")
                for item in items
                if ((item.get("observability") or {}).get("host_telemetry") or {}).get("summary_path")
            ],
        },
        items=items,
        notes=notes,
    )