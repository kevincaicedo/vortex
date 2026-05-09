"""Benchmark interpretation gates for alpha optimization reports."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Optional


LIMITING_RESOURCE_CHOICES = (
    "CPU",
    "memory/cache/TLB",
    "lock contention",
    "scheduler",
    "IO backend",
    "disk/fsync",
    "network",
    "allocator",
    "benchmark client",
    "unknown",
)

CLIENT_CPU_FIELDS = (
    "load_generator_cpu_utilization_peak_pct",
    "client_cpu_utilization_peak_pct",
    "benchmark_client_cpu_peak_pct",
)

CORE_P99_TARGET_MS = 1.0
CORE_P999_TARGET_MS = 1.0
PRESSURE_P99_TARGET_MS = 1.0
PRESSURE_P999_TARGET_MS = 5.0
REDIS_THROUGHPUT_ALLOWED_RATIO = 1.50
REDIS_THROUGHPUT_NARROWED_RATIO = 1.00
CLIENT_CPU_SATURATION_PCT = 85.0


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


def _yes(value: Any) -> bool:
    return value is True or str(value).lower() in {"true", "yes", "1", "on"}


def _database(row: dict[str, Any]) -> str:
    return str(row.get("database") or "").lower()


def _maxmemory_normalized(value: Any) -> Any:
    if value is None or value == 0 or value == "0":
        return "unlimited"
    return value


def _scenario_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("backend"),
        row.get("series_kind"),
        row.get("series_label"),
        row.get("thread_count"),
    )


def _comparison_signature(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("configured_service_threads"),
        row.get("configured_aof_enabled"),
        row.get("configured_aof_fsync"),
        _maxmemory_normalized(row.get("configured_maxmemory")),
        row.get("configured_eviction_policy"),
        row.get("database_mode"),
    )


def _rss_bytes(row: dict[str, Any]) -> Optional[float]:
    for key in (
        "process_rss_peak_bytes",
        "full_server_process_rss_after_bytes",
        "process_memory_after_bytes",
        "used_memory_rss_after_bytes",
    ):
        value = _coerce_float(row.get(key))
        if value is not None and value > 0:
            return value
    return None


def _client_cpu(row: dict[str, Any]) -> tuple[Optional[float], str]:
    for field in CLIENT_CPU_FIELDS:
        value = _coerce_float(row.get(field))
        if value is not None:
            return value, field
    return None, "unavailable"


def _client_saturation(row: dict[str, Any]) -> tuple[str, str]:
    cpu, cpu_source = _client_cpu(row)
    network_issues = {
        "socket_recv_q_peak": _coerce_float(row.get("socket_recv_q_peak")),
        "socket_send_q_peak": _coerce_float(row.get("socket_send_q_peak")),
        "tcp_retrans_segs_delta": _coerce_float(row.get("tcp_retrans_segs_delta")),
        "tcp_listen_overflows_delta": _coerce_float(row.get("tcp_listen_overflows_delta")),
        "tcp_listen_drops_delta": _coerce_float(row.get("tcp_listen_drops_delta")),
        "network_total_rx_errors_delta": _coerce_float(row.get("network_total_rx_errors_delta")),
        "network_total_tx_errors_delta": _coerce_float(row.get("network_total_tx_errors_delta")),
    }
    positive_network = [
        key for key, value in network_issues.items() if value is not None and value > 0
    ]
    if positive_network:
        return (
            "saturated",
            "socket/TCP telemetry showed pressure: " + ", ".join(positive_network),
        )
    if cpu is not None and cpu >= CLIENT_CPU_SATURATION_PCT:
        return (
            "saturated",
            f"{cpu_source}={cpu:.1f}% >= {CLIENT_CPU_SATURATION_PCT:.1f}%",
        )
    if cpu is None:
        return (
            "unknown",
            "load-generator CPU telemetry is unavailable; socket/retransmit telemetry is still reported",
        )
    return (
        "clear",
        f"{cpu_source}={cpu:.1f}% and socket/retransmit telemetry did not show pressure",
    )


def _limiting_resource(row: dict[str, Any]) -> tuple[str, str]:
    client_verdict = row.get("benchmark_client_saturation_verdict")
    if client_verdict == "saturated":
        return "benchmark client", row.get("benchmark_client_saturation_reason") or "client saturated"

    network_keys = (
        "tcp_retrans_segs_delta",
        "tcp_listen_overflows_delta",
        "tcp_listen_drops_delta",
        "network_total_rx_errors_delta",
        "network_total_tx_errors_delta",
        "socket_recv_q_peak",
        "socket_send_q_peak",
    )
    if any((_coerce_float(row.get(key)) or 0.0) > 0.0 for key in network_keys):
        return "network", "socket queue, retransmit, or network error telemetry increased"

    disk_keys = (
        "aof_delayed_fsync_delta",
        "aof_pending_bio_fsync_after",
        "latency_aof_fsync_max_after_ms",
        "latency_aof_pending_fsync_max_after_ms",
        "system_mem_writeback_peak_bytes",
        "disk_io_time_delta_ms",
        "disk_io_in_progress_peak",
    )
    if any((_coerce_float(row.get(key)) or 0.0) > 0.0 for key in disk_keys):
        return "disk/fsync", "AOF, fsync, writeback, or disk telemetry increased"

    io_keys = (
        "reactor_submit_sq_full_retries_delta",
        "reactor_submit_failures_delta",
        "reactor_completion_budget_exhaustions_delta",
        "reactor_accept_budget_exhaustions_delta",
        "reactor_writev_budget_exhaustions_delta",
    )
    if any((_coerce_float(row.get(key)) or 0.0) > 0.0 for key in io_keys):
        return "IO backend", "reactor submit/completion telemetry showed pressure"

    scheduler_budget_keys = (
        "reactor_command_budget_exhaustions_delta",
        "reactor_maintenance_budget_exhaustions_delta",
    )
    if any((_coerce_float(row.get(key)) or 0.0) > 0.0 for key in scheduler_budget_keys):
        return "scheduler", "reactor command or maintenance budget exhausted during the run"

    runq = _coerce_float(row.get("system_procs_running_peak"))
    service_threads = _coerce_float(row.get("configured_service_threads"))
    if runq is not None and service_threads is not None and runq > service_threads * 1.5:
        return "scheduler", "run queue exceeded configured service-thread capacity"

    frag = _coerce_float(row.get("allocator_frag_ratio_after"))
    mem_frag = _coerce_float(row.get("mem_fragmentation_ratio_after"))
    if (frag is not None and frag >= 1.5) or (mem_frag is not None and mem_frag >= 1.5):
        return "allocator", "allocator or RSS fragmentation ratio is high"

    if (_coerce_float(row.get("system_vm_allocstall_delta")) or 0.0) > 0.0:
        return "memory/cache/TLB", "memory reclaim/allocstall telemetry increased"

    cpu_peak = _coerce_float(row.get("system_cpu_utilization_peak_pct"))
    if cpu_peak is not None and cpu_peak >= 90.0:
        return "CPU", "system CPU utilization peak was high"

    return "unknown", "no single limiting resource is identified by benchmark telemetry"


def _is_pressure_row(row: dict[str, Any]) -> bool:
    backend = row.get("backend")
    pipeline = _coerce_int(row.get("workload_pipeline")) or 1
    eviction = str(row.get("configured_eviction_policy") or "").lower()
    series = str(row.get("series_label") or "").lower()
    return (
        backend == "memtier_benchmark"
        or pipeline > 1
        or _yes(row.get("configured_aof_enabled"))
        or (eviction not in {"", "none", "noeviction"})
        or any(token in series for token in ("ttl", "expire", "eviction", "aof", "watch", "exec"))
    )


def _latency_gate(row: dict[str, Any]) -> tuple[str, str, str]:
    p99 = _coerce_float(row.get("p99_latency_ms"))
    p999 = _coerce_float(row.get("p99_9_latency_ms"))
    pressure = _is_pressure_row(row)
    scope = "alpha-pressure" if pressure else "core-sub-ms-p99.9"
    p99_target = PRESSURE_P99_TARGET_MS if pressure else CORE_P99_TARGET_MS
    p999_target = PRESSURE_P999_TARGET_MS if pressure else CORE_P999_TARGET_MS

    if p99 is None:
        return "Unknown", scope, "missing p99 latency"
    if p99 >= p99_target:
        return "Rejected", scope, f"p99 {p99:.3f} ms >= target {p99_target:.3f} ms"
    if p999 is None:
        return (
            "Narrowed",
            scope,
            f"p99 {p99:.3f} ms passes but p99.9 is unavailable; no p99.9 claim is allowed",
        )
    if p999 >= p999_target:
        return "Rejected", scope, f"p99.9 {p999:.3f} ms >= target {p999_target:.3f} ms"
    return (
        "Allowed",
        scope,
        f"p99 {p99:.3f} ms and p99.9 {p999:.3f} ms meet {scope} targets",
    )


def _memory_gate(row: dict[str, Any]) -> tuple[str, str]:
    if _database(row) != "vortex":
        return "n/a", "Redis/baseline row is not a Vortex memory claim"

    full_decision = row.get("full_server_rss_claim_decision")
    full_reason = row.get("full_server_rss_claim_reason")
    engine_decision = row.get("engine_memory_claim_decision")
    engine_reason = row.get("engine_memory_claim_reason")

    if full_decision in {"Allowed", "Narrowed"}:
        return str(full_decision), str(full_reason or "full-server RSS claim classified")
    if engine_decision in {"Allowed", "Narrowed"}:
        return (
            "Narrowed",
            str(engine_reason or "engine memory passes, but full-server Redis RSS comparison is missing"),
        )
    if full_decision:
        return str(full_decision), str(full_reason or "full-server memory claim rejected")
    return "Unknown", "memory attribution is unavailable"


def _redis_reference(rows: list[dict[str, Any]]) -> dict[tuple[Any, ...], dict[str, Any]]:
    references: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        if _database(row) != "redis":
            continue
        key = _scenario_key(row)
        current = references.get(key)
        if current is None or (
            (_coerce_float(row.get("throughput_ops_sec")) or 0.0)
            > (_coerce_float(current.get("throughput_ops_sec")) or 0.0)
        ):
            references[key] = row
    return references


def _redis_comparison(
    row: dict[str, Any], references: dict[tuple[Any, ...], dict[str, Any]]
) -> tuple[str, Optional[float], str]:
    if _database(row) != "vortex":
        return "n/a", None, "Redis/baseline row is not a Vortex-vs-Redis claim"
    if row.get("comparison_invalid") is True:
        return "Exploratory", None, "runtime settings differ across compared rows"

    reference = references.get(_scenario_key(row))
    if reference is None:
        return "Unknown", None, "no comparable Redis row is present"

    vortex_ops = _coerce_float(row.get("throughput_ops_sec"))
    redis_ops = _coerce_float(reference.get("throughput_ops_sec"))
    ratio = vortex_ops / redis_ops if vortex_ops is not None and redis_ops else None
    if ratio is None:
        return "Unknown", None, "missing Vortex or Redis throughput"

    latency_checks = []
    for key in ("p50_latency_ms", "p95_latency_ms", "p99_latency_ms", "p99_9_latency_ms"):
        vortex_latency = _coerce_float(row.get(key))
        redis_latency = _coerce_float(reference.get(key))
        if vortex_latency is None or redis_latency is None:
            continue
        latency_checks.append(vortex_latency <= redis_latency)
    latency_ok = bool(latency_checks) and all(latency_checks)

    if ratio >= REDIS_THROUGHPUT_ALLOWED_RATIO and latency_ok:
        return (
            "Allowed",
            ratio,
            f"throughput is {ratio:.2f}x Redis and compared latency percentiles are lower/equal",
        )
    if ratio >= REDIS_THROUGHPUT_NARROWED_RATIO and latency_ok:
        return (
            "Narrowed",
            ratio,
            f"Vortex beats Redis latency but throughput is {ratio:.2f}x, below 1.5x target",
        )
    if not latency_ok:
        return "Rejected", ratio, "Vortex did not beat Redis on compared latency percentiles"
    return "Rejected", ratio, f"throughput is {ratio:.2f}x Redis, below parity"


def _workload_contract(row: dict[str, Any], validity: dict[str, Any], host: dict[str, Any]) -> dict[str, Any]:
    host_validity = validity.get("host") or {}
    effective_io = (
        row.get("runtime_backend_effective_after")
        or row.get("io_backend_effective")
        or row.get("configured_io_backend")
        or "n/a"
    )
    flags = [
        f"aof={row.get('configured_aof_enabled')}",
        f"fsync={row.get('configured_aof_fsync')}",
        f"eviction={row.get('configured_eviction_policy')}",
        f"io={effective_io}",
    ]
    return {
        "feature_flags": ", ".join(flags),
        "backend_effective_mode": effective_io,
        "service_threads": row.get("configured_service_threads"),
        "load_threads": row.get("load_threads") or row.get("thread_count"),
        "client_cpu": _client_cpu(row)[0],
        "cpu_governor": host_validity.get("cpu_governor") or "unknown",
        "effective_cpu_power_mode": host_validity.get("effective_cpu_power_mode") or "unknown",
        "affinity_status": validity.get("affinity_status") or "unknown",
        "kernel": host.get("os_release") or host.get("platform") or "unknown",
        "value_size": row.get("workload_value_size"),
        "key_count": row.get("workload_key_count"),
        "pipeline": row.get("workload_pipeline"),
        "aof": row.get("configured_aof_enabled"),
        "eviction": row.get("configured_eviction_policy"),
        "maxmemory": row.get("configured_maxmemory"),
        "shard_count": row.get("engine_shard_count_after"),
        "fixed_buffer_count": row.get("io_fixed_buffer_count_after"),
        "fixed_buffer_size": row.get("io_fixed_buffer_size_after"),
        "repeat_count": row.get("replicate_count") or validity.get("requested_repeat_count"),
    }


def _evidence_tier(row: dict[str, Any], validity: dict[str, Any], host: dict[str, Any]) -> tuple[str, str]:
    reasons: list[str] = []
    repeat_count = _coerce_int(row.get("replicate_count") or validity.get("requested_repeat_count"))
    host_os = str(row.get("host_os") or host.get("os") or "")
    host_validity = validity.get("host") or {}

    if repeat_count is None or repeat_count < 3:
        reasons.append("single replicate or fewer than 3 repeats")
    if host_os.lower() not in {"linux"}:
        reasons.append("not a Linux run")
    io_effective = row.get("runtime_backend_effective_after") or row.get("io_backend_effective")
    if _database(row) == "vortex" and str(io_effective or "").lower() in {"", "unknown"}:
        reasons.append("missing Vortex backend effective mode")
    if _rss_bytes(row) is None:
        reasons.append("missing RSS attribution")
    if row.get("benchmark_client_saturation_verdict") == "saturated":
        reasons.append("benchmark client saturated")
    if row.get("comparison_invalid") is True:
        reasons.append("invalid cross-database comparison")

    if reasons:
        return "Exploratory", "; ".join(reasons)

    if (
        repeat_count >= 3
        and str(host_validity.get("effective_cpu_power_mode") or "").lower() == "performance"
        and host_validity.get("thermal_degraded") is False
    ):
        return "Citation-grade candidate", "Linux repeat run with clean host validity checks"

    return "Engineering", "usable for engineering triage, not a release claim"


def annotate_interpretation_rows(
    rows: list[dict[str, Any]],
    validity: Optional[dict[str, Any]] = None,
    host_metadata: Optional[dict[str, Any]] = None,
    comparison_validity: Optional[list[dict[str, Any]]] = None,
) -> None:
    validity = validity or {}
    host_metadata = host_metadata or {}
    invalid_keys = {
        (
            entry.get("backend"),
            entry.get("series_kind"),
            entry.get("series_label"),
            entry.get("thread_count"),
        )
        for entry in (comparison_validity or [])
    }
    references = _redis_reference(rows)

    for row in rows:
        if _scenario_key(row) in invalid_keys:
            row["comparison_invalid"] = True
        contract = _workload_contract(row, validity, host_metadata)
        for key, value in contract.items():
            row[f"workload_contract_{key}"] = value

        client_verdict, client_reason = _client_saturation(row)
        row["benchmark_client_saturation_verdict"] = client_verdict
        row["benchmark_client_saturation_reason"] = client_reason
        row["benchmark_client_cpu_available"] = _client_cpu(row)[0] is not None

        limiting, limiting_reason = _limiting_resource(row)
        row["limiting_resource_hypothesis"] = limiting
        row["limiting_resource_reason"] = limiting_reason

        latency_decision, latency_scope, latency_reason = _latency_gate(row)
        row["alpha_latency_gate_decision"] = latency_decision
        row["alpha_latency_claim_scope"] = latency_scope
        row["alpha_latency_gate_reason"] = latency_reason

        memory_decision, memory_reason = _memory_gate(row)
        row["alpha_memory_gate_decision"] = memory_decision
        row["alpha_memory_gate_reason"] = memory_reason

        redis_decision, redis_ratio, redis_reason = _redis_comparison(row, references)
        row["alpha_redis_comparison_decision"] = redis_decision
        row["alpha_throughput_vs_redis_ratio"] = redis_ratio
        row["alpha_redis_comparison_reason"] = redis_reason

        tier, tier_reason = _evidence_tier(row, validity, host_metadata)
        row["evidence_tier_row"] = tier
        row["evidence_tier_reason"] = tier_reason


def build_interpretation_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "database": row.get("database"),
            "backend": row.get("backend"),
            "series_label": row.get("series_label"),
            "thread_count": row.get("thread_count"),
            "service_threads": row.get("configured_service_threads"),
            "evidence_tier": row.get("evidence_tier_row"),
            "client_saturation": row.get("benchmark_client_saturation_verdict"),
            "limiting_resource_hypothesis": row.get("limiting_resource_hypothesis"),
            "alpha_latency_gate": row.get("alpha_latency_gate_decision"),
            "alpha_memory_gate": row.get("alpha_memory_gate_decision"),
            "redis_comparison_gate": row.get("alpha_redis_comparison_decision"),
            "throughput_vs_redis_ratio": row.get("alpha_throughput_vs_redis_ratio"),
            "reason": row.get("evidence_tier_reason")
            or row.get("alpha_latency_gate_reason")
            or row.get("limiting_resource_reason"),
        }
        for row in rows
    ]


def build_workload_contract_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "database": row.get("database"),
            "backend": row.get("backend"),
            "series_label": row.get("series_label"),
            "service_threads": row.get("workload_contract_service_threads"),
            "load_threads": row.get("workload_contract_load_threads"),
            "client_cpu": row.get("workload_contract_client_cpu"),
            "cpu_governor": row.get("workload_contract_cpu_governor"),
            "effective_cpu_power_mode": row.get("workload_contract_effective_cpu_power_mode"),
            "affinity_status": row.get("workload_contract_affinity_status"),
            "kernel": row.get("workload_contract_kernel"),
            "backend_effective_mode": row.get("workload_contract_backend_effective_mode"),
            "feature_flags": row.get("workload_contract_feature_flags"),
            "value_size": row.get("workload_contract_value_size"),
            "key_count": row.get("workload_contract_key_count"),
            "pipeline": row.get("workload_contract_pipeline"),
            "aof": row.get("workload_contract_aof"),
            "eviction": row.get("workload_contract_eviction"),
            "maxmemory": row.get("workload_contract_maxmemory"),
            "shard_count": row.get("workload_contract_shard_count"),
            "fixed_buffer_count": row.get("workload_contract_fixed_buffer_count"),
            "fixed_buffer_size": row.get("workload_contract_fixed_buffer_size"),
            "repeat_count": row.get("workload_contract_repeat_count"),
        }
        for row in rows
    ]


def _decision_counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts = Counter(str(row.get(key) or "Unknown") for row in rows)
    return dict(sorted(counts.items()))


def _gate_from_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "Unknown"
    if counts.get("Rejected", 0) > 0:
        return "Rejected"
    if counts.get("Exploratory", 0) > 0:
        return "Narrowed"
    if counts.get("Unknown", 0) > 0 or counts.get("Narrowed", 0) > 0:
        return "Narrowed"
    if counts.get("Allowed", 0) > 0:
        return "Allowed"
    return "Unknown"


def _scalability_gate(rows: list[dict[str, Any]]) -> tuple[str, str]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        service_threads = _coerce_int(row.get("configured_service_threads"))
        if service_threads is None:
            continue
        key = (
            row.get("database"),
            row.get("backend"),
            row.get("series_kind"),
            row.get("series_label"),
            row.get("thread_count"),
        )
        grouped[key].append(row)

    service_sweeps = []
    for group in grouped.values():
        unique_threads = sorted(
            {
                _coerce_int(row.get("configured_service_threads"))
                for row in group
                if _coerce_int(row.get("configured_service_threads")) is not None
            }
        )
        if len(unique_threads) >= 2:
            service_sweeps.append(group)

    if not service_sweeps:
        return "Unknown", "no service-thread sweep is present in this report"

    failures = 0
    for group in service_sweeps:
        ordered = sorted(group, key=lambda row: _coerce_int(row.get("configured_service_threads")) or 0)
        first, last = ordered[0], ordered[-1]
        first_threads = _coerce_float(first.get("configured_service_threads"))
        last_threads = _coerce_float(last.get("configured_service_threads"))
        first_ops = _coerce_float(first.get("throughput_ops_sec"))
        last_ops = _coerce_float(last.get("throughput_ops_sec"))
        if not first_threads or not last_threads or not first_ops or last_ops is None:
            failures += 1
            continue
        ideal = last_threads / first_threads
        observed = last_ops / first_ops
        efficiency = observed / ideal if ideal > 0 else 0.0
        if efficiency < 0.70:
            failures += 1
    if failures:
        return "Rejected", f"{failures} service-thread sweep(s) missed 70% scaling efficiency"
    return "Allowed", "service-thread sweeps meet the alpha efficiency floor"


def build_alpha_gate_summary(
    rows: list[dict[str, Any]],
    validity: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    validity = validity or {}
    vortex_rows = [row for row in rows if _database(row) == "vortex"]
    latency_scope_rows = vortex_rows or rows
    latency_counts = _decision_counts(latency_scope_rows, "alpha_latency_gate_decision")
    memory_counts = _decision_counts(vortex_rows, "alpha_memory_gate_decision")
    redis_counts = _decision_counts(vortex_rows, "alpha_redis_comparison_decision")
    evidence_counts = _decision_counts(rows, "evidence_tier_row")
    client_counts = _decision_counts(rows, "benchmark_client_saturation_verdict")
    scalability_decision, scalability_reason = _scalability_gate(rows)
    evidence_decision = "Unknown"
    if rows:
        evidence_decision = (
            "Allowed"
            if evidence_counts.get("Citation-grade candidate") == len(rows)
            else "Narrowed"
        )
    client_decision = "Unknown"
    if rows:
        if client_counts.get("saturated", 0):
            client_decision = "Rejected"
        elif client_counts.get("clear", 0) == len(rows):
            client_decision = "Allowed"
        else:
            client_decision = "Narrowed"

    gates = [
        {
            "gate": "correctness",
            "decision": "Unknown",
            "target": "required correctness matrix green",
            "reason": "benchmark report has no correctness-matrix artifact attached",
        },
        {
            "gate": "latency",
            "decision": _gate_from_counts(latency_counts),
            "target": "p99 < 1 ms and p99.9 < 5 ms for accepted Linux alpha workloads; core no-pressure rows target p99.9 < 1 ms",
            "reason": f"row decisions: {latency_counts or {'Unknown': 0}}",
        },
        {
            "gate": "memory",
            "decision": _gate_from_counts(memory_counts),
            "target": "scoped tiny/mixed KV moves toward <= 1.5x Redis full-server RSS",
            "reason": f"Vortex row decisions: {memory_counts or {'Unknown': 0}}",
        },
        {
            "gate": "redis_comparison",
            "decision": _gate_from_counts(redis_counts),
            "target": "Vortex lower latency than Redis and 1.5x-2x Redis throughput on comparable accepted rows",
            "reason": f"Vortex-vs-Redis row decisions: {redis_counts or {'Unknown': 0}}",
        },
        {
            "gate": "scalability",
            "decision": scalability_decision,
            "target": "service-thread scaling improves without p99.9 cliffs through planned thread counts",
            "reason": scalability_reason,
        },
        {
            "gate": "evidence",
            "decision": evidence_decision,
            "target": "3-5 clean Linux repeats, comparable settings, artifacts attached before release claims",
            "reason": f"row evidence tiers: {evidence_counts or {'Unknown': 0}}",
        },
        {
            "gate": "benchmark_client",
            "decision": client_decision,
            "target": "load-generator CPU, socket queue/retransmit telemetry, and affinity disclosure rule out client limits",
            "reason": f"client saturation verdicts: {client_counts or {'Unknown': 0}}",
        },
    ]
    return {
        "target_summary": "Alpha optimization targets lower latency than Redis on accepted rows, 1.5x-2x Redis throughput where comparable, p99 < 1 ms, p99.9 < 5 ms for pressure rows, and <= 1.5x Redis full-server RSS for scoped tiny/mixed KV.",
        "requested_repeat_count": validity.get("requested_repeat_count"),
        "gates": gates,
        "limiting_resource_choices": list(LIMITING_RESOURCE_CHOICES),
        "decision_counts": {
            "latency": latency_counts,
            "memory": memory_counts,
            "redis_comparison": redis_counts,
            "evidence": evidence_counts,
            "benchmark_client": client_counts,
        },
    }
