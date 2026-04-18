from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from vortex_benchmark.env import build_layout
from vortex_benchmark.telemetry import capture_host_metadata


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_summary_paths(summary_files: list[str], results_dir: Optional[str]) -> list[Path]:
    resolved: list[Path] = []
    seen: set[Path] = set()

    for raw_path in summary_files:
        path = Path(raw_path).expanduser().resolve()
        if not path.exists():
            raise ValueError(f"summary file does not exist: {path}")
        if path in seen:
            continue
        seen.add(path)
        resolved.append(path)

    if results_dir:
        root = Path(results_dir).expanduser().resolve()
    elif not resolved:
        root = build_layout(None).results_dir
    else:
        root = None

    if root is not None:
        if not root.exists():
            raise ValueError(f"results directory does not exist: {root}")
        for path in sorted(root.glob("*-summary.json")):
            if path in seen:
                continue
            seen.add(path)
            resolved.append(path.resolve())

    if not resolved:
        raise ValueError("no summary files were found; pass --summary-file or --results-dir")
    return resolved


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _service_record(state: dict[str, Any], database: str) -> dict[str, Any]:
    for service in state.get("services", []):
        if service.get("database") == database:
            return service
    return {}


def _runtime_fields(service: dict[str, Any]) -> dict[str, Any]:
    runtime_config = service.get("runtime_config") or {}
    return {
        "configured_aof_enabled": runtime_config.get("aof_enabled"),
        "configured_aof_fsync": runtime_config.get("aof_fsync"),
        "configured_maxmemory": runtime_config.get("maxmemory"),
        "configured_eviction_policy": runtime_config.get("eviction_policy"),
    }


def _observability_fields(item: dict[str, Any]) -> dict[str, Any]:
    observability = item.get("observability") or {}
    before = observability.get("before") or {}
    after = observability.get("after") or {}
    delta = observability.get("delta") or {}
    return {
        "used_memory_before_bytes": before.get("used_memory_bytes"),
        "used_memory_after_bytes": after.get("used_memory_bytes"),
        "used_memory_delta_bytes": delta.get("used_memory_bytes_delta"),
        "used_memory_rss_before_bytes": before.get("used_memory_rss_bytes"),
        "used_memory_rss_after_bytes": after.get("used_memory_rss_bytes"),
        "used_memory_rss_delta_bytes": delta.get("used_memory_rss_bytes_delta"),
        "process_memory_before_bytes": before.get("process_memory_bytes"),
        "process_memory_after_bytes": after.get("process_memory_bytes"),
        "process_memory_delta_bytes": delta.get("process_memory_bytes_delta"),
        "keyspace_hits_delta": delta.get("keyspace_hits_delta"),
        "keyspace_misses_delta": delta.get("keyspace_misses_delta"),
        "cache_hit_rate": delta.get("cache_hit_rate"),
        "cache_efficiency": delta.get("cache_efficiency"),
        "evicted_keys_delta": delta.get("evicted_keys_delta"),
        "expired_keys_delta": delta.get("expired_keys_delta"),
        "aof_enabled_before": delta.get("aof_enabled_before"),
        "aof_enabled_after": delta.get("aof_enabled_after"),
        "aof_current_size_before_bytes": before.get("aof_current_size_bytes"),
        "aof_current_size_after_bytes": after.get("aof_current_size_bytes"),
        "aof_current_size_delta_bytes": delta.get("aof_current_size_bytes_delta"),
    }


def _normalize_redis_rows(
    result: dict[str, Any],
    summary: dict[str, Any],
    request: dict[str, Any],
    service: dict[str, Any],
    summary_path: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in result.get("items", []):
        metrics = item.get("metrics") or {}
        rows.append(
            {
                "run_id": summary.get("run_id"),
                "environment_id": summary.get("environment_id"),
                "benchmark_date_time": summary.get("generated_at"),
                "database": result.get("database"),
                "database_mode": service.get("mode"),
                "database_version": (service.get("metadata") or {}).get("version"),
                "backend": result.get("backend"),
                "series_kind": "command",
                "series_label": item.get("label"),
                "thread_count": None,
                "throughput_ops_sec": _coerce_float(metrics.get("rps")),
                "average_latency_ms": _coerce_float(metrics.get("avg_latency_ms")),
                "p50_latency_ms": _coerce_float(metrics.get("p50_latency_ms")),
                "p99_latency_ms": _coerce_float(metrics.get("p99_latency_ms")),
                "p99_9_latency_ms": _coerce_float(metrics.get("p99_9_latency_ms")),
                "p99_999_latency_ms": _coerce_float(metrics.get("p99_999_latency_ms")),
                "workload_details": None,
                "host_os": (request.get("host_metadata") or {}).get("os"),
                "host_architecture": (request.get("host_metadata") or {}).get("architecture"),
                "source_summary": str(summary_path),
                **_runtime_fields(service),
                **_observability_fields(item),
            }
        )
    return rows


def _normalize_memtier_rows(
    result: dict[str, Any],
    summary: dict[str, Any],
    request: dict[str, Any],
    service: dict[str, Any],
    summary_path: Path,
) -> list[dict[str, Any]]:
    definitions = {
        definition.get("name"): definition
        for definition in request.get("workload_definitions", [])
        if isinstance(definition, dict)
    }
    rows: list[dict[str, Any]] = []
    for item in result.get("items", []):
        totals = ((item.get("metrics") or {}).get("totals") or {})
        workload = item.get("workload")
        rows.append(
            {
                "run_id": summary.get("run_id"),
                "environment_id": summary.get("environment_id"),
                "benchmark_date_time": summary.get("generated_at"),
                "database": result.get("database"),
                "database_mode": service.get("mode"),
                "database_version": (service.get("metadata") or {}).get("version"),
                "backend": result.get("backend"),
                "series_kind": "workload",
                "series_label": workload,
                "thread_count": _coerce_int(item.get("thread_count")),
                "throughput_ops_sec": _coerce_float(totals.get("ops_sec")),
                "average_latency_ms": _coerce_float(totals.get("average_latency_ms")),
                "p50_latency_ms": _coerce_float(totals.get("p50_latency_ms")),
                "p99_latency_ms": _coerce_float(totals.get("p99_latency_ms")),
                "p99_9_latency_ms": _coerce_float(totals.get("p99_9_latency_ms")),
                "p99_999_latency_ms": _coerce_float(totals.get("p99_999_latency_ms")),
                "workload_details": definitions.get(workload),
                "host_os": (request.get("host_metadata") or {}).get("os"),
                "host_architecture": (request.get("host_metadata") or {}).get("architecture"),
                "source_summary": str(summary_path),
                **_runtime_fields(service),
                **_observability_fields(item),
            }
        )
    return rows


def _normalize_custom_rows(
    result: dict[str, Any],
    summary: dict[str, Any],
    request: dict[str, Any],
    service: dict[str, Any],
    summary_path: Path,
) -> list[dict[str, Any]]:
    definitions = {
        definition.get("name"): definition
        for definition in request.get("workload_definitions", [])
        if isinstance(definition, dict)
    }
    rows: list[dict[str, Any]] = []
    for item in result.get("items", []):
        metrics = item.get("metrics") or {}
        workload = item.get("workload")
        rows.append(
            {
                "run_id": summary.get("run_id"),
                "environment_id": summary.get("environment_id"),
                "benchmark_date_time": summary.get("generated_at"),
                "database": result.get("database"),
                "database_mode": service.get("mode"),
                "database_version": (service.get("metadata") or {}).get("version"),
                "backend": result.get("backend"),
                "series_kind": "workload",
                "series_label": workload,
                "thread_count": _coerce_int(item.get("thread_count")),
                "throughput_ops_sec": _coerce_float(metrics.get("aggregate_throughput_ops_sec")),
                "average_latency_ms": None,
                "p50_latency_ms": _coerce_float(metrics.get("p50_ns")) / 1_000_000.0 if metrics.get("p50_ns") is not None else None,
                "p99_latency_ms": _coerce_float(metrics.get("p99_ns")) / 1_000_000.0 if metrics.get("p99_ns") is not None else None,
                "p99_9_latency_ms": _coerce_float(metrics.get("p99_9_ns")) / 1_000_000.0 if metrics.get("p99_9_ns") is not None else None,
                "p99_999_latency_ms": _coerce_float(metrics.get("p99_999_ns")) / 1_000_000.0 if metrics.get("p99_999_ns") is not None else None,
                "workload_details": definitions.get(workload),
                "host_os": (request.get("host_metadata") or {}).get("os"),
                "host_architecture": (request.get("host_metadata") or {}).get("architecture"),
                "source_summary": str(summary_path),
                **_runtime_fields(service),
                **_observability_fields(item),
            }
        )
    return rows


def _normalize_rows(summary_path: Path, summary: dict[str, Any], request: dict[str, Any], state: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in summary.get("results", []):
        result_json = Path((record.get("artifacts") or {}).get("result_json", "")).expanduser()
        backend_result = _read_json(result_json) if result_json.exists() else record
        service = _service_record(state, record.get("database", ""))
        backend = record.get("backend")
        if backend == "redis-benchmark":
            rows.extend(_normalize_redis_rows(backend_result, summary, request, service, summary_path))
        elif backend == "memtier_benchmark":
            rows.extend(_normalize_memtier_rows(backend_result, summary, request, service, summary_path))
        elif backend == "custom-rust":
            rows.extend(_normalize_custom_rows(backend_result, summary, request, service, summary_path))
    return rows


def _build_aof_overhead(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    baseline: dict[tuple[Any, ...], dict[str, Any]] = {}
    comparisons: list[dict[str, Any]] = []
    for row in rows:
        key = (row.get("database"), row.get("series_label"), row.get("thread_count"), row.get("backend"))
        aof_enabled = row.get("aof_enabled_after")
        if aof_enabled is False:
            baseline[key] = row
            continue
        if aof_enabled is not True:
            continue
        reference = baseline.get(key)
        if not reference:
            continue
        baseline_throughput = _coerce_float(reference.get("throughput_ops_sec"))
        current_throughput = _coerce_float(row.get("throughput_ops_sec"))
        baseline_p99 = _coerce_float(reference.get("p99_latency_ms"))
        current_p99 = _coerce_float(row.get("p99_latency_ms"))
        comparisons.append(
            {
                "database": row.get("database"),
                "series_label": row.get("series_label"),
                "thread_count": row.get("thread_count"),
                "backend": row.get("backend"),
                "throughput_overhead_pct": (
                    ((baseline_throughput - current_throughput) / baseline_throughput) * 100.0
                    if baseline_throughput and current_throughput is not None
                    else None
                ),
                "p99_overhead_pct": (
                    ((current_p99 - baseline_p99) / baseline_p99) * 100.0
                    if baseline_p99 and current_p99 is not None
                    else None
                ),
            }
        )
    return comparisons


def build_report_payload(summary_paths: list[Path], title: Optional[str] = None) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    host_metadata: dict[str, Any] | None = None
    database_records: dict[str, dict[str, Any]] = {}
    workloads: dict[str, dict[str, Any]] = {}
    source_runs: list[dict[str, Any]] = []

    for path in summary_paths:
        summary = _read_json(path)
        request = _read_json(Path(summary["request_path"]))
        state_path = Path(request["state_file"]).expanduser().resolve()
        state = _read_json(state_path) if state_path.exists() else {}

        if host_metadata is None:
            host_metadata = request.get("host_metadata") or capture_host_metadata()

        for service in state.get("services", []):
            database_records[service.get("database", "unknown")] = {
                "database": service.get("database"),
                "mode": service.get("mode"),
                "version": (service.get("metadata") or {}).get("version"),
                "bind": (service.get("metadata") or {}).get("bind"),
                "runtime_config": service.get("runtime_config") or {},
            }

        for definition in request.get("workload_definitions", []):
            if isinstance(definition, dict) and definition.get("name"):
                workloads[definition["name"]] = definition

        source_runs.append(
            {
                "summary_path": str(path),
                "request_path": summary.get("request_path"),
                "environment_id": summary.get("environment_id"),
                "generated_at": summary.get("generated_at"),
                "selected_databases": summary.get("selected_databases", []),
                "resolved_backends": summary.get("resolved_backends", []),
            }
        )
        rows.extend(_normalize_rows(path, summary, request, state))

    rows.sort(
        key=lambda row: (
            row.get("database") or "",
            row.get("backend") or "",
            row.get("series_label") or "",
            row.get("thread_count") or 0,
        )
    )

    return {
        "schema_version": 1,
        "title": title or "Vortex Benchmark Report",
        "source_runs": source_runs,
        "host_metadata": host_metadata or capture_host_metadata(),
        "databases": [database_records[key] for key in sorted(database_records)],
        "workloads": [workloads[key] for key in sorted(workloads)],
        "rows": rows,
        "aof_overhead": _build_aof_overhead(rows),
        "eviction_rows": [row for row in rows if row.get("evicted_keys_delta") not in {None, 0}],
    }