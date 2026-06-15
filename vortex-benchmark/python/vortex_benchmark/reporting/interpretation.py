"""Neutral benchmark interpretation helpers.

The benchmark tool classifies measurement quality, comparison compatibility,
client saturation, and likely limiting resources. Policy decisions belong in
the documents that consume the generated artifacts, not in this package.
"""

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
    "load_generator_cpu_utilization_of_capacity_pct",
    "load_generator_cpu_utilization_peak_pct",
    "load_generator_cpu_utilization_avg_pct",
    "client_cpu_utilization_peak_pct",
    "benchmark_client_cpu_peak_pct",
)

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
        row.get("workload_key_count"),
        row.get("workload_value_size"),
        row.get("workload_pipeline"),
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


def _latency_coverage(row: dict[str, Any]) -> tuple[str, str]:
    p99 = _coerce_float(row.get("p99_latency_ms"))
    p999 = _coerce_float(row.get("p99_9_latency_ms"))
    if p99 is None:
        return "missing", "p99 latency is unavailable"
    if p999 is None:
        return "partial", "p99 is available but p99.9 is unavailable"
    return "complete", "p99 and p99.9 latency are available"


def _comparison_references(rows: list[dict[str, Any]]) -> dict[tuple[Any, ...], dict[str, Any]]:
    references: dict[tuple[Any, ...], dict[str, Any]] = {}
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_scenario_key(row)].append(row)

    for scenario, group in grouped.items():
        for row in group:
            row_database = _database(row)
            peers = [
                peer
                for peer in group
                if _database(peer) != row_database
                and _comparison_signature(peer) == _comparison_signature(row)
            ]
            if not peers:
                continue
            references[(scenario, row_database)] = max(
                peers,
                key=lambda peer: _coerce_float(peer.get("throughput_ops_sec")) or 0.0,
            )
    return references


def _peer_comparison(
    row: dict[str, Any],
    references: dict[tuple[Any, ...], dict[str, Any]],
) -> tuple[str, Optional[str], Optional[float], str]:
    if row.get("comparison_invalid") is True:
        return "invalid", None, None, "comparison inputs differ for this scenario"

    reference = references.get((_scenario_key(row), _database(row)))
    if reference is None:
        return "missing-reference", None, None, "no peer row with matching workload signature is present"

    row_ops = _coerce_float(row.get("throughput_ops_sec"))
    reference_ops = _coerce_float(reference.get("throughput_ops_sec"))
    ratio = row_ops / reference_ops if row_ops is not None and reference_ops else None
    if ratio is None:
        return "missing-throughput", _database(reference), None, "missing row or reference throughput"

    compared_latencies = []
    for key in ("p50_latency_ms", "p95_latency_ms", "p99_latency_ms", "p99_9_latency_ms"):
        row_latency = _coerce_float(row.get(key))
        reference_latency = _coerce_float(reference.get(key))
        if row_latency is None or reference_latency is None:
            continue
        compared_latencies.append(row_latency <= reference_latency)

    latency_text = "latency percentiles were unavailable for peer comparison"
    if compared_latencies:
        latency_text = (
            "all comparable latency percentiles are lower/equal"
            if all(compared_latencies)
            else "one or more comparable latency percentiles are higher"
        )
    return (
        "comparable",
        _database(reference),
        ratio,
        f"throughput ratio {ratio:.2f}x vs {_database(reference)}; {latency_text}",
    )


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


def _measurement_tier(row: dict[str, Any], validity: dict[str, Any], host: dict[str, Any]) -> tuple[str, str]:
    reasons: list[str] = []
    repeat_count = _coerce_int(row.get("replicate_count") or validity.get("requested_repeat_count"))
    host_os = str(row.get("host_os") or host.get("os") or "")
    host_validity = validity.get("host") or {}

    if repeat_count is None or repeat_count < 3:
        reasons.append("single replicate or fewer than 3 repeats")
    if host_os.lower() not in {"linux"}:
        reasons.append("not a Linux run")
    if _rss_bytes(row) is None:
        reasons.append("missing RSS attribution")
    if row.get("benchmark_client_saturation_verdict") == "saturated":
        reasons.append("benchmark client saturated")
    if row.get("comparison_invalid") is True:
        reasons.append("invalid cross-database comparison")

    if reasons:
        return "exploratory", "; ".join(reasons)

    if (
        repeat_count >= 3
        and str(host_validity.get("effective_cpu_power_mode") or "").lower() == "performance"
        and host_validity.get("thermal_degraded") is False
    ):
        return "publication-candidate", "Linux repeat run with clean host validity checks"

    return "engineering", "usable for engineering triage"


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

        latency_status, latency_reason = _latency_coverage(row)
        row["latency_coverage_status"] = latency_status
        row["latency_coverage_reason"] = latency_reason

    references = _comparison_references(rows)
    for row in rows:
        status, reference_database, ratio, reason = _peer_comparison(row, references)
        row["peer_comparison_status"] = status
        row["peer_comparison_reference_database"] = reference_database
        row["throughput_vs_reference_ratio"] = ratio
        row["peer_comparison_reason"] = reason

        tier, tier_reason = _measurement_tier(row, validity, host_metadata)
        row["measurement_tier_row"] = tier
        row["measurement_tier_reason"] = tier_reason


def build_interpretation_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "database": row.get("database"),
            "backend": row.get("backend"),
            "series_label": row.get("series_label"),
            "thread_count": row.get("thread_count"),
            "service_threads": row.get("configured_service_threads"),
            "measurement_tier": row.get("measurement_tier_row"),
            "client_saturation": row.get("benchmark_client_saturation_verdict"),
            "limiting_resource_hypothesis": row.get("limiting_resource_hypothesis"),
            "latency_coverage": row.get("latency_coverage_status"),
            "peer_comparison": row.get("peer_comparison_status"),
            "reference_database": row.get("peer_comparison_reference_database"),
            "throughput_vs_reference_ratio": row.get("throughput_vs_reference_ratio"),
            "reason": row.get("measurement_tier_reason")
            or row.get("peer_comparison_reason")
            or row.get("latency_coverage_reason")
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


def _counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts = Counter(str(row.get(key) or "unknown") for row in rows)
    return dict(sorted(counts.items()))


def _status_from_counts(
    counts: dict[str, int],
    *,
    fail_values: set[str],
    warn_values: set[str],
    pass_values: set[str],
) -> str:
    if not counts:
        return "unknown"
    if any(counts.get(value, 0) > 0 for value in fail_values):
        return "fail"
    if any(counts.get(value, 0) > 0 for value in warn_values):
        return "warn"
    if sum(counts.get(value, 0) for value in pass_values) == sum(counts.values()):
        return "pass"
    return "unknown"


def _scalability_status(rows: list[dict[str, Any]]) -> tuple[str, str]:
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
        return "unknown", "no service-thread sweep is present in this report"

    regressions = 0
    for group in service_sweeps:
        ordered = sorted(group, key=lambda row: _coerce_int(row.get("configured_service_threads")) or 0)
        first, last = ordered[0], ordered[-1]
        first_ops = _coerce_float(first.get("throughput_ops_sec"))
        last_ops = _coerce_float(last.get("throughput_ops_sec"))
        if not first_ops or last_ops is None:
            continue
        if last_ops < first_ops:
            regressions += 1
    if regressions:
        return "warn", f"{regressions} service-thread sweep(s) regressed at the high-thread endpoint"
    return "pass", "service-thread sweeps did not regress at the high-thread endpoint"


def build_measurement_summary(
    rows: list[dict[str, Any]],
    validity: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    validity = validity or {}
    latency_counts = _counts(rows, "latency_coverage_status")
    evidence_counts = _counts(rows, "measurement_tier_row")
    client_counts = _counts(rows, "benchmark_client_saturation_verdict")
    comparison_counts = _counts(rows, "peer_comparison_status")
    limiting_counts = _counts(rows, "limiting_resource_hypothesis")
    scalability_status, scalability_reason = _scalability_status(rows)

    checks = [
        {
            "check": "latency_coverage",
            "status": _status_from_counts(
                latency_counts,
                fail_values={"missing"},
                warn_values={"partial"},
                pass_values={"complete"},
            ),
            "requirement": "p99 and p99.9 should be present when the backend can report them",
            "reason": f"row coverage: {latency_counts or {'unknown': 0}}",
        },
        {
            "check": "comparison_compatibility",
            "status": _status_from_counts(
                comparison_counts,
                fail_values={"invalid"},
                warn_values={"missing-reference", "missing-throughput"},
                pass_values={"comparable"},
            ),
            "requirement": "winner or ratio tables require matched workload signatures",
            "reason": f"peer comparison rows: {comparison_counts or {'unknown': 0}}",
        },
        {
            "check": "benchmark_client",
            "status": _status_from_counts(
                client_counts,
                fail_values={"saturated"},
                warn_values={"unknown"},
                pass_values={"clear"},
            ),
            "requirement": "load-generator CPU and socket telemetry should not be the bottleneck",
            "reason": f"client saturation verdicts: {client_counts or {'unknown': 0}}",
        },
        {
            "check": "repeatability",
            "status": "pass"
            if (validity.get("requested_repeat_count") or 0) >= 3
            else "warn",
            "requirement": "engineering and publication workflows should use repeated runs",
            "reason": f"requested repeat count: {validity.get('requested_repeat_count') or 'n/a'}",
        },
        {
            "check": "scalability_shape",
            "status": scalability_status,
            "requirement": "thread sweeps should be reviewed for high-thread regressions",
            "reason": scalability_reason,
        },
    ]
    return {
        "requested_repeat_count": validity.get("requested_repeat_count"),
        "checks": checks,
        "limiting_resource_choices": list(LIMITING_RESOURCE_CHOICES),
        "counts": {
            "latency_coverage": latency_counts,
            "measurement_tier": evidence_counts,
            "benchmark_client": client_counts,
            "peer_comparison": comparison_counts,
            "limiting_resource": limiting_counts,
        },
    }
