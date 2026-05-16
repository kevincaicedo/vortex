from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from vortex_benchmark.env import build_layout
from vortex_benchmark.telemetry import capture_host_metadata

ENGINE_BYTES_PER_KEY_ALLOWED = 192.0
ENGINE_BYTES_PER_KEY_NARROWED = 256.0
FULL_SERVER_RSS_ALLOWED_RATIO = 1.50
FULL_SERVER_RSS_NARROWED_RATIO = 2.00


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


def _infer_vortex_backend(service: dict[str, Any]) -> tuple[Any, Any]:
    if service.get("database") != "vortex":
        return None, None

    runtime = service.get("runtime_config") or {}
    metadata = service.get("metadata") or {}
    requested = runtime.get("io_backend") or "auto"
    command = str(metadata.get("command") or "")
    log_text = ""
    log_path = service.get("log_path")
    if log_path:
        try:
            log_text = Path(str(log_path)).read_text(
                encoding="utf-8", errors="replace"
            )
        except OSError:
            log_text = ""

    if requested in {"polling", "uring"}:
        effective = requested
    elif "backend_effective=\"polling\"" in log_text or "backend_effective=polling" in log_text:
        effective = "polling"
    elif "backend_effective=\"io_uring\"" in log_text or "backend_effective=io_uring" in log_text:
        effective = "io_uring"
    elif "--io-backend polling" in command or "io_backend=polling" in log_text:
        effective = "polling"
    elif "--io-backend uring" in command or "io_backend=uring" in log_text:
        effective = "uring"
    elif "falling back to polling" in log_text:
        effective = "polling"
    elif "io_backend=auto" in log_text and "reactor pool started" in log_text:
        effective = "uring"
    else:
        effective = "unknown"

    return requested, effective


def _runtime_fields(service: dict[str, Any]) -> dict[str, Any]:
    runtime_config = service.get("runtime_config") or {}
    resource_config = service.get("resource_config") or {}
    return {
        "configured_aof_enabled": runtime_config.get("aof_enabled"),
        "configured_aof_fsync": runtime_config.get("aof_fsync"),
        "configured_aof_max_pending_fsync_bytes": runtime_config.get(
            "aof_max_pending_fsync_bytes"
        ),
        "configured_shard_count": runtime_config.get("shard_count"),
        "configured_maxmemory": runtime_config.get("maxmemory"),
        "configured_maxmemory_bytes": runtime_config.get("maxmemory_bytes"),
        "configured_eviction_policy": runtime_config.get("eviction_policy"),
        "configured_io_backend": runtime_config.get("io_backend"),
        "configured_fixed_buffer_registration": runtime_config.get(
            "fixed_buffer_registration"
        ),
        "configured_telemetry_mode": runtime_config.get("telemetry_mode"),
        "configured_service_threads": resource_config.get("threads"),
        "configured_service_cpus": resource_config.get("cpus"),
        "configured_service_cpu_list": resource_config.get("service_cpus"),
        "configured_load_cpu_list": resource_config.get("load_cpus"),
    }


def _io_backend_fields(service: dict[str, Any]) -> dict[str, Any]:
    requested, effective = _infer_vortex_backend(service)
    return {
        "io_backend_requested": requested,
        "io_backend_effective": effective,
    }


def _settings_for_backend(request: dict[str, Any], backend: Any) -> dict[str, Any]:
    settings = request.get("settings") or {}
    if not isinstance(settings, dict):
        return {}

    aliases = [str(backend or "")]
    if backend == "memtier_benchmark":
        aliases.append("memtier-benchmark")
    elif backend == "redis-benchmark":
        aliases.append("redis_benchmark")
    elif backend == "custom-rust":
        aliases.append("custom_rust")

    for alias in aliases:
        value = settings.get(alias)
        if isinstance(value, dict):
            return value
    return {}


def _workload_contract_fields(
    backend: Any,
    item: dict[str, Any],
    request: dict[str, Any],
) -> dict[str, Any]:
    settings = _settings_for_backend(request, backend)
    value_size = (
        settings.get("value_size")
        or settings.get("value_size_bytes")
        or settings.get("data_size")
    )
    return {
        "workload_request_count": settings.get("requests"),
        "workload_client_count": settings.get("clients"),
        "workload_pipeline": settings.get("pipeline"),
        "workload_key_count": settings.get("keyspace_size"),
        "workload_value_size": value_size,
        "workload_rate_limit": settings.get("rate_limiting"),
        "workload_thread_sweep": settings.get("thread_sweep"),
        "workload_ops_per_thread": settings.get("ops_per_thread"),
        "workload_warmup_ops": settings.get("warmup_ops"),
        "workload_duration_seconds": request.get("duration"),
        "load_threads": _coerce_int(item.get("thread_count")),
    }


_FAIRNESS_COUNTER_FIELDS = [
    "backend_submit_syscalls",
    "backend_sq_occupancy_max",
    "backend_cq_occupancy_max",
    "backend_cq_overflows",
    "backend_sq_pressure_events",
    "backend_cq_pressure_events",
    "reactor_loop_iterations",
    "reactor_accept_eagain_rearms",
    "reactor_submit_sq_full_retries",
    "reactor_submit_failures",
    "reactor_completion_budget_exhaustions",
    "reactor_command_budget_exhaustions",
    "reactor_accept_budget_exhaustions",
    "reactor_writev_budget_exhaustions",
    "reactor_maintenance_budget_exhaustions",
    "reactor_yielded_connections",
    "reactor_parser_resumes",
    "reactor_writev_chunks",
    "reactor_writev_iovecs_total",
    "reactor_queued_response_bytes_total",
    "reactor_request_cap_exceeded",
    "reactor_response_cap_exceeded",
    "reactor_multi_queue_command_cap_exceeded",
    "reactor_multi_queue_bytes_cap_exceeded",
    "reactor_watch_cap_exceeded",
    "reactor_writev_chunk_cap_exceeded",
    "reactor_overload_accept_throttled",
    "reactor_overload_accept_resumed",
    "reactor_overload_read_disabled",
    "reactor_overload_read_resumed",
    "reactor_overload_command_deferred",
    "reactor_overload_command_resumed",
    "reactor_overload_connections_dropped",
    "reactor_accept_drain_runs",
    "reactor_accept_drain_accepted",
    "reactor_completion_batches",
    "reactor_completion_batch_total",
    "reactor_command_batches",
    "reactor_command_batch_total",
    "reactor_active_expiry_runs",
    "reactor_active_expiry_sampled",
    "reactor_active_expiry_expired",
    "reactor_completion_nanos_total",
    "reactor_close_drain_nanos_total",
    "reactor_active_expiry_nanos_total",
    "reactor_aof_append_nanos_total",
    "reactor_aof_fsync_nanos_total",
    "reactor_aof_fsync_requested",
    "reactor_aof_fsync_completed",
    "reactor_aof_fsync_failed",
    "reactor_aof_fsync_worker_saturation",
    "reactor_aof_backpressure_events",
    "reactor_aof_backpressure_nanos_total",
    "reactor_aof_fsync_latency_nanos_total",
    "reactor_maintenance_nanos_total",
    "reactor_metrics_flush_nanos_total",
    "eviction_nanos_total",
]

_FAIRNESS_PEAK_FIELDS = [
    "reactor_accept_drain_accepted_max",
    "reactor_completion_batch_max",
    "reactor_completion_batch_avg",
    "reactor_command_batch_max",
    "reactor_command_batch_avg",
    "reactor_completion_nanos_max",
    "reactor_writev_iovecs_max",
    "reactor_queued_response_bytes_max",
    "reactor_client_retained_bytes",
    "reactor_client_retained_bytes_max",
    "reactor_client_retained_bytes_peak",
    "reactor_overload_pending_response_bytes",
    "reactor_overload_pending_response_bytes_peak",
    "reactor_overload_parser_accumulator_bytes",
    "reactor_overload_parser_accumulator_bytes_peak",
    "reactor_overload_writev_backlog_bytes",
    "reactor_overload_writev_backlog_bytes_peak",
    "reactor_overload_aof_pending_bytes",
    "reactor_overload_aof_pending_bytes_peak",
    "reactor_overload_maintenance_debt",
    "reactor_overload_maintenance_debt_peak",
    "reactor_overload_read_disabled_connections",
    "reactor_overload_read_disabled_connections_peak",
    "reactor_overload_deferred_commands",
    "reactor_overload_deferred_commands_peak",
    "reactor_close_drain_nanos_max",
    "reactor_active_expiry_nanos_max",
    "reactor_aof_append_nanos_max",
    "reactor_aof_fsync_nanos_max",
    "reactor_aof_pending_bytes",
    "reactor_aof_pending_bytes_max",
    "reactor_aof_pending_writes",
    "reactor_aof_pending_writes_max",
    "reactor_aof_backpressure_nanos_max",
    "reactor_aof_last_appended_lsn",
    "reactor_aof_last_durable_lsn",
    "reactor_aof_durable_lsn_lag",
    "reactor_aof_fsync_latency_nanos_max",
    "reactor_aof_fsync_latency_le_100us",
    "reactor_aof_fsync_latency_le_500us",
    "reactor_aof_fsync_latency_le_1ms",
    "reactor_aof_fsync_latency_le_5ms",
    "reactor_aof_fsync_latency_le_10ms",
    "reactor_aof_fsync_latency_le_50ms",
    "reactor_aof_fsync_latency_le_100ms",
    "reactor_aof_fsync_latency_gt_100ms",
    "reactor_maintenance_nanos_max",
    "reactor_metrics_flush_nanos_max",
    "eviction_nanos_max",
]


def _fairness_observability_fields(
    before: dict[str, Any],
    after: dict[str, Any],
    delta: dict[str, Any],
    telemetry_summary: dict[str, Any],
) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for field in _FAIRNESS_COUNTER_FIELDS:
        fields[f"{field}_before"] = before.get(field)
        fields[f"{field}_after"] = after.get(field)
        fields[f"{field}_delta"] = delta.get(f"{field}_delta")
        fields[f"{field}_host_delta"] = telemetry_summary.get(f"{field}_delta")
    for field in _FAIRNESS_PEAK_FIELDS:
        fields[f"{field}_before"] = before.get(field)
        fields[f"{field}_after"] = after.get(field)
        fields[f"{field}_peak"] = telemetry_summary.get(f"{field}_peak")
    return fields


def _replicate_fields(record: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    selection = record.get("selection") or {}
    artifacts = record.get("artifacts") or {}
    return {
        "suite_run_id": item.get("suite_run_id") or artifacts.get("suite_run_id") or record.get("suite_run_id"),
        "replicate_run_id": item.get("replicate_run_id") or artifacts.get("replicate_run_id"),
        "replicate_index": item.get("replicate_index") or selection.get("replicate_index"),
        "replicate_count": item.get("replicate_count") or selection.get("replicate_count"),
        "replicate_id": item.get("replicate_id") or artifacts.get("replicate_id") or selection.get("replicate_id"),
    }


def _observability_fields(item: dict[str, Any]) -> dict[str, Any]:
    observability = item.get("observability") or {}
    before = observability.get("before") or {}
    after = observability.get("after") or {}
    delta = observability.get("delta") or {}
    host_telemetry = observability.get("host_telemetry") or {}
    telemetry_summary = host_telemetry.get("summary") or {}
    return {
        "used_memory_before_bytes": before.get("used_memory_bytes"),
        "used_memory_after_bytes": after.get("used_memory_bytes"),
        "used_memory_delta_bytes": delta.get("used_memory_bytes_delta"),
        "used_memory_rss_before_bytes": before.get("used_memory_rss_bytes"),
        "used_memory_rss_after_bytes": after.get("used_memory_rss_bytes"),
        "used_memory_rss_delta_bytes": delta.get("used_memory_rss_bytes_delta"),
        "used_memory_dataset_before_bytes": before.get("used_memory_dataset_bytes"),
        "used_memory_dataset_after_bytes": after.get("used_memory_dataset_bytes"),
        "used_memory_dataset_delta_bytes": delta.get("used_memory_dataset_bytes_delta"),
        "used_memory_overhead_before_bytes": before.get("used_memory_overhead_bytes"),
        "used_memory_overhead_after_bytes": after.get("used_memory_overhead_bytes"),
        "used_memory_overhead_delta_bytes": delta.get("used_memory_overhead_bytes_delta"),
        "memory_attribution_engine_scope": after.get("memory_attribution_engine_scope"),
        "memory_attribution_full_server_scope": after.get(
            "memory_attribution_full_server_scope"
        ),
        "engine_live_keys_before": before.get("engine_live_keys"),
        "engine_live_keys_after": after.get("engine_live_keys"),
        "engine_live_keys_delta": delta.get("engine_live_keys_delta"),
        "engine_logical_dataset_before_bytes": before.get(
            "engine_logical_dataset_bytes"
        ),
        "engine_logical_dataset_after_bytes": after.get(
            "engine_logical_dataset_bytes"
        ),
        "engine_logical_dataset_delta_bytes": delta.get(
            "engine_logical_dataset_bytes_delta"
        ),
        "engine_table_allocated_before_bytes": before.get(
            "engine_table_allocated_bytes"
        ),
        "engine_table_allocated_after_bytes": after.get(
            "engine_table_allocated_bytes"
        ),
        "engine_table_allocated_delta_bytes": delta.get(
            "engine_table_allocated_bytes_delta"
        ),
        "engine_table_total_slots_before": before.get("engine_table_total_slots"),
        "engine_table_total_slots_after": after.get("engine_table_total_slots"),
        "engine_table_total_slots_delta": delta.get(
            "engine_table_total_slots_delta"
        ),
        "engine_capacity_slack_slots_before": before.get(
            "engine_capacity_slack_slots"
        ),
        "engine_capacity_slack_slots_after": after.get(
            "engine_capacity_slack_slots"
        ),
        "engine_capacity_slack_slots_delta": delta.get(
            "engine_capacity_slack_slots_delta"
        ),
        "engine_tombstone_slots_before": before.get("engine_tombstone_slots"),
        "engine_tombstone_slots_after": after.get("engine_tombstone_slots"),
        "engine_tombstone_slots_delta": delta.get("engine_tombstone_slots_delta"),
        "engine_load_factor_before": before.get("engine_load_factor"),
        "engine_load_factor_after": after.get("engine_load_factor"),
        "engine_bytes_per_live_key_before": before.get("engine_bytes_per_live_key"),
        "engine_bytes_per_live_key_after": after.get("engine_bytes_per_live_key"),
        "engine_shard_count_after": after.get("engine_shard_count"),
        "io_fixed_buffer_reserved_after_bytes": after.get(
            "io_fixed_buffer_reserved_bytes"
        ),
        "io_fixed_buffer_committed_after_bytes": after.get(
            "io_fixed_buffer_committed_bytes"
        ),
        "io_fixed_buffer_active_after_bytes": after.get(
            "io_fixed_buffer_active_bytes"
        ),
        "io_fixed_buffer_count_after": after.get("io_fixed_buffer_count"),
        "io_fixed_buffer_size_after": after.get("io_fixed_buffer_size"),
        "runtime_backend_requested_after": after.get("backend_requested"),
        "runtime_backend_effective_after": after.get("backend_effective"),
        "runtime_telemetry_mode_after": after.get("runtime_telemetry_mode"),
        "runtime_profile_timers_available_after": after.get(
            "runtime_profile_timers_available"
        ),
        "runtime_local_flush_metrics_available_after": after.get(
            "runtime_local_flush_metrics_available"
        ),
        "shared_nothing_accepted_remote_after": after.get(
            "shared_nothing_accepted_remote"
        ),
        "shared_nothing_accepted_remote_delta": delta.get(
            "shared_nothing_accepted_remote_delta"
        ),
        "shared_nothing_remote_backpressure_after": after.get(
            "shared_nothing_remote_backpressure"
        ),
        "shared_nothing_remote_backpressure_delta": delta.get(
            "shared_nothing_remote_backpressure_delta"
        ),
        "shared_nothing_reply_backpressure_after": after.get(
            "shared_nothing_reply_backpressure"
        ),
        "shared_nothing_reply_backpressure_delta": delta.get(
            "shared_nothing_reply_backpressure_delta"
        ),
        "shared_nothing_deferred_replies_after": after.get(
            "shared_nothing_deferred_replies"
        ),
        "shared_nothing_deferred_replies_delta": delta.get(
            "shared_nothing_deferred_replies_delta"
        ),
        "shared_nothing_wakeups_sent_after": after.get("shared_nothing_wakeups_sent"),
        "shared_nothing_wakeups_sent_delta": delta.get(
            "shared_nothing_wakeups_sent_delta"
        ),
        "shared_nothing_wakeup_failures_after": after.get(
            "shared_nothing_wakeup_failures"
        ),
        "shared_nothing_wakeup_failures_delta": delta.get(
            "shared_nothing_wakeup_failures_delta"
        ),
        "shared_nothing_aggregates_accepted_after": after.get(
            "shared_nothing_aggregates_accepted"
        ),
        "shared_nothing_aggregates_accepted_delta": delta.get(
            "shared_nothing_aggregates_accepted_delta"
        ),
        "shared_nothing_aggregate_width_max_after": after.get(
            "shared_nothing_aggregate_width_max"
        ),
        "shared_nothing_txns_accepted_after": after.get(
            "shared_nothing_txns_accepted"
        ),
        "shared_nothing_txns_accepted_delta": delta.get(
            "shared_nothing_txns_accepted_delta"
        ),
        "shared_nothing_txn_prepare_messages_after": after.get(
            "shared_nothing_txn_prepare_messages"
        ),
        "shared_nothing_txn_prepare_messages_delta": delta.get(
            "shared_nothing_txn_prepare_messages_delta"
        ),
        "shared_nothing_txn_commit_messages_after": after.get(
            "shared_nothing_txn_commit_messages"
        ),
        "shared_nothing_txn_commit_messages_delta": delta.get(
            "shared_nothing_txn_commit_messages_delta"
        ),
        "shared_nothing_txn_abort_messages_after": after.get(
            "shared_nothing_txn_abort_messages"
        ),
        "shared_nothing_txn_abort_messages_delta": delta.get(
            "shared_nothing_txn_abort_messages_delta"
        ),
        "shared_nothing_txn_condition_aborts_after": after.get(
            "shared_nothing_txn_condition_aborts"
        ),
        "shared_nothing_txn_condition_aborts_delta": delta.get(
            "shared_nothing_txn_condition_aborts_delta"
        ),
        "shared_nothing_txn_conflict_aborts_after": after.get(
            "shared_nothing_txn_conflict_aborts"
        ),
        "shared_nothing_txn_conflict_aborts_delta": delta.get(
            "shared_nothing_txn_conflict_aborts_delta"
        ),
        "shared_nothing_prepared_key_waits_after": after.get(
            "shared_nothing_prepared_key_waits"
        ),
        "shared_nothing_prepared_key_waits_delta": delta.get(
            "shared_nothing_prepared_key_waits_delta"
        ),
        "shared_nothing_prepared_key_retries_after": after.get(
            "shared_nothing_prepared_key_retries"
        ),
        "shared_nothing_prepared_key_retries_delta": delta.get(
            "shared_nothing_prepared_key_retries_delta"
        ),
        "shared_nothing_prepared_key_wait_nanos_total_after": after.get(
            "shared_nothing_prepared_key_wait_nanos_total"
        ),
        "shared_nothing_prepared_key_wait_nanos_total_delta": delta.get(
            "shared_nothing_prepared_key_wait_nanos_total_delta"
        ),
        "shared_nothing_prepared_key_wait_nanos_max_after": after.get(
            "shared_nothing_prepared_key_wait_nanos_max"
        ),
        "service_invalid_delta_fields": delta.get("invalid_delta_fields"),
        "host_invalid_delta_fields": telemetry_summary.get("invalid_delta_fields"),
        "backend_plan_mixed_after": after.get("backend_plan_mixed"),
        "backend_fixed_buffers_capable_after": after.get(
            "backend_fixed_buffers_capable"
        ),
        "backend_fixed_buffers_registered_after": after.get(
            "backend_fixed_buffers_registered"
        ),
        "backend_sqpoll_after": after.get("backend_sqpoll"),
        "backend_multishot_accept_after": after.get("backend_multishot_accept"),
        "backend_accept4_after": after.get("backend_accept4"),
        "backend_close_opcode_after": after.get("backend_close_opcode"),
        "backend_cancel_support_after": after.get("backend_cancel_support"),
        "backend_nonblocking_drain_after": after.get("backend_nonblocking_drain"),
        "backend_requested_ring_size_after": after.get("backend_requested_ring_size"),
        "backend_effective_ring_size_after": after.get("backend_effective_ring_size"),
        "backend_submit_syscalls_before": before.get("backend_submit_syscalls"),
        "backend_submit_syscalls_after": after.get("backend_submit_syscalls"),
        "backend_submit_syscalls_delta": delta.get("backend_submit_syscalls_delta"),
        "backend_sq_occupancy_max_after": after.get("backend_sq_occupancy_max"),
        "backend_sq_capacity_after": after.get("backend_sq_capacity"),
        "backend_cq_occupancy_max_after": after.get("backend_cq_occupancy_max"),
        "backend_cq_capacity_after": after.get("backend_cq_capacity"),
        "backend_cq_overflows_delta": delta.get("backend_cq_overflows_delta"),
        "backend_completions_per_submit_syscall_x1000_after": after.get(
            "backend_completions_per_submit_syscall_x1000"
        ),
        "backend_sq_pressure_events_delta": delta.get(
            "backend_sq_pressure_events_delta"
        ),
        "backend_cq_pressure_events_delta": delta.get(
            "backend_cq_pressure_events_delta"
        ),
        "per_connection_state_after_bytes": after.get("per_connection_state_bytes"),
        "client_retained_after_bytes": after.get("client_retained_bytes"),
        "client_retained_max_after_bytes": after.get("client_retained_bytes_max"),
        "client_retained_peak_after_bytes": after.get("client_retained_bytes_peak"),
        "connection_capacity_after": after.get("connection_capacity"),
        "full_server_process_rss_after_bytes": after.get(
            "full_server_process_rss_bytes"
        ),
        "process_memory_before_bytes": before.get("process_memory_bytes"),
        "process_memory_after_bytes": after.get("process_memory_bytes"),
        "process_memory_delta_bytes": delta.get("process_memory_bytes_delta"),
        "allocator_allocated_before_bytes": before.get("allocator_allocated_bytes"),
        "allocator_allocated_after_bytes": after.get("allocator_allocated_bytes"),
        "allocator_allocated_delta_bytes": delta.get("allocator_allocated_bytes_delta"),
        "allocator_active_before_bytes": before.get("allocator_active_bytes"),
        "allocator_active_after_bytes": after.get("allocator_active_bytes"),
        "allocator_active_delta_bytes": delta.get("allocator_active_bytes_delta"),
        "allocator_resident_before_bytes": before.get("allocator_resident_bytes"),
        "allocator_resident_after_bytes": after.get("allocator_resident_bytes"),
        "allocator_resident_delta_bytes": delta.get("allocator_resident_bytes_delta"),
        "allocator_mapped_before_bytes": before.get("allocator_mapped_bytes"),
        "allocator_mapped_after_bytes": after.get("allocator_mapped_bytes"),
        "allocator_mapped_delta_bytes": delta.get("allocator_mapped_bytes_delta"),
        "allocator_retained_before_bytes": before.get("allocator_retained_bytes"),
        "allocator_retained_after_bytes": after.get("allocator_retained_bytes"),
        "allocator_retained_delta_bytes": delta.get("allocator_retained_bytes_delta"),
        "allocator_frag_ratio_before": before.get("allocator_frag_ratio"),
        "allocator_frag_ratio_after": after.get("allocator_frag_ratio"),
        "allocator_frag_bytes_before": before.get("allocator_frag_bytes"),
        "allocator_frag_bytes_after": after.get("allocator_frag_bytes"),
        "allocator_frag_bytes_delta": delta.get("allocator_frag_bytes_delta"),
        "allocator_rss_ratio_before": before.get("allocator_rss_ratio"),
        "allocator_rss_ratio_after": after.get("allocator_rss_ratio"),
        "allocator_rss_before_bytes": before.get("allocator_rss_bytes"),
        "allocator_rss_after_bytes": after.get("allocator_rss_bytes"),
        "allocator_rss_delta_bytes": delta.get("allocator_rss_bytes_delta"),
        "mem_fragmentation_ratio_before": before.get("mem_fragmentation_ratio"),
        "mem_fragmentation_ratio_after": after.get("mem_fragmentation_ratio"),
        "mem_fragmentation_bytes_before": before.get("mem_fragmentation_bytes"),
        "mem_fragmentation_bytes_after": after.get("mem_fragmentation_bytes"),
        "mem_fragmentation_bytes_delta": delta.get("mem_fragmentation_bytes_delta"),
        "keyspace_hits_delta": delta.get("keyspace_hits_delta"),
        "keyspace_misses_delta": delta.get("keyspace_misses_delta"),
        "cache_hit_rate": delta.get("cache_hit_rate"),
        "cache_efficiency": delta.get("cache_efficiency"),
        "evicted_keys_delta": delta.get("evicted_keys_delta"),
        "expired_keys_delta": delta.get("expired_keys_delta"),
        "reactor_loop_iterations_before": before.get("reactor_loop_iterations"),
        "reactor_loop_iterations_after": after.get("reactor_loop_iterations"),
        "reactor_loop_iterations_delta": delta.get("reactor_loop_iterations_delta"),
        "reactor_accept_eagain_rearms_before": before.get("reactor_accept_eagain_rearms"),
        "reactor_accept_eagain_rearms_after": after.get("reactor_accept_eagain_rearms"),
        "reactor_accept_eagain_rearms_delta": delta.get("reactor_accept_eagain_rearms_delta"),
        "reactor_completion_batches_before": before.get("reactor_completion_batches"),
        "reactor_completion_batches_after": after.get("reactor_completion_batches"),
        "reactor_completion_batches_delta": delta.get("reactor_completion_batches_delta"),
        "reactor_completion_batch_max_before": before.get("reactor_completion_batch_max"),
        "reactor_completion_batch_max_after": after.get("reactor_completion_batch_max"),
        "reactor_completion_batch_avg_before": before.get("reactor_completion_batch_avg"),
        "reactor_completion_batch_avg_after": after.get("reactor_completion_batch_avg"),
        "reactor_command_batches_before": before.get("reactor_command_batches"),
        "reactor_command_batches_after": after.get("reactor_command_batches"),
        "reactor_command_batches_delta": delta.get("reactor_command_batches_delta"),
        "reactor_command_batch_max_before": before.get("reactor_command_batch_max"),
        "reactor_command_batch_max_after": after.get("reactor_command_batch_max"),
        "reactor_command_batch_avg_before": before.get("reactor_command_batch_avg"),
        "reactor_command_batch_avg_after": after.get("reactor_command_batch_avg"),
        "reactor_active_expiry_runs_before": before.get("reactor_active_expiry_runs"),
        "reactor_active_expiry_runs_after": after.get("reactor_active_expiry_runs"),
        "reactor_active_expiry_runs_delta": delta.get("reactor_active_expiry_runs_delta"),
        "reactor_active_expiry_sampled_before": before.get("reactor_active_expiry_sampled"),
        "reactor_active_expiry_sampled_after": after.get("reactor_active_expiry_sampled"),
        "reactor_active_expiry_sampled_delta": delta.get("reactor_active_expiry_sampled_delta"),
        "reactor_active_expiry_expired_before": before.get("reactor_active_expiry_expired"),
        "reactor_active_expiry_expired_after": after.get("reactor_active_expiry_expired"),
        "reactor_active_expiry_expired_delta": delta.get("reactor_active_expiry_expired_delta"),
        "eviction_admissions_before": before.get("eviction_admissions"),
        "eviction_admissions_after": after.get("eviction_admissions"),
        "eviction_admissions_delta": delta.get("eviction_admissions_delta"),
        "eviction_shards_scanned_before": before.get("eviction_shards_scanned"),
        "eviction_shards_scanned_after": after.get("eviction_shards_scanned"),
        "eviction_shards_scanned_delta": delta.get("eviction_shards_scanned_delta"),
        "eviction_slots_sampled_before": before.get("eviction_slots_sampled"),
        "eviction_slots_sampled_after": after.get("eviction_slots_sampled"),
        "eviction_slots_sampled_delta": delta.get("eviction_slots_sampled_delta"),
        "eviction_bytes_freed_before": before.get("eviction_bytes_freed"),
        "eviction_bytes_freed_after": after.get("eviction_bytes_freed"),
        "eviction_bytes_freed_delta": delta.get("eviction_bytes_freed_delta"),
        "eviction_oom_after_scan_before": before.get("eviction_oom_after_scan"),
        "eviction_oom_after_scan_after": after.get("eviction_oom_after_scan"),
        "eviction_oom_after_scan_delta": delta.get("eviction_oom_after_scan_delta"),
        **_fairness_observability_fields(before, after, delta, telemetry_summary),
        "aof_enabled_before": delta.get("aof_enabled_before"),
        "aof_enabled_after": delta.get("aof_enabled_after"),
        "aof_current_size_before_bytes": before.get("aof_current_size_bytes"),
        "aof_current_size_after_bytes": after.get("aof_current_size_bytes"),
        "aof_current_size_delta_bytes": delta.get("aof_current_size_bytes_delta"),
        "aof_buffer_length_before_bytes": before.get("aof_buffer_length"),
        "aof_buffer_length_after_bytes": after.get("aof_buffer_length"),
        "aof_buffer_length_delta_bytes": delta.get("aof_buffer_length_delta"),
        "aof_pending_bio_fsync_before": before.get("aof_pending_bio_fsync"),
        "aof_pending_bio_fsync_after": after.get("aof_pending_bio_fsync"),
        "aof_pending_bio_fsync_delta": delta.get("aof_pending_bio_fsync_delta"),
        "aof_delayed_fsync_before": before.get("aof_delayed_fsync"),
        "aof_delayed_fsync_after": after.get("aof_delayed_fsync"),
        "aof_delayed_fsync_delta": delta.get("aof_delayed_fsync_delta"),
        "aof_pending_rewrite_before": before.get("aof_pending_rewrite"),
        "aof_pending_rewrite_after": after.get("aof_pending_rewrite"),
        "aof_pending_rewrite_delta": delta.get("aof_pending_rewrite_delta"),
        "aof_last_write_status_before": before.get("aof_last_write_status"),
        "aof_last_write_status_after": after.get("aof_last_write_status"),
        "latency_aof_write_latest_before_ms": before.get("latency_aof_write_latest_ms"),
        "latency_aof_write_latest_after_ms": after.get("latency_aof_write_latest_ms"),
        "latency_aof_write_latest_delta_ms": delta.get("latency_aof_write_latest_ms_delta"),
        "latency_aof_write_max_before_ms": before.get("latency_aof_write_max_ms"),
        "latency_aof_write_max_after_ms": after.get("latency_aof_write_max_ms"),
        "latency_aof_write_max_delta_ms": delta.get("latency_aof_write_max_ms_delta"),
        "latency_aof_fsync_latest_before_ms": before.get("latency_aof_fsync_latest_ms"),
        "latency_aof_fsync_latest_after_ms": after.get("latency_aof_fsync_latest_ms"),
        "latency_aof_fsync_latest_delta_ms": delta.get("latency_aof_fsync_latest_ms_delta"),
        "latency_aof_fsync_max_before_ms": before.get("latency_aof_fsync_max_ms"),
        "latency_aof_fsync_max_after_ms": after.get("latency_aof_fsync_max_ms"),
        "latency_aof_fsync_max_delta_ms": delta.get("latency_aof_fsync_max_ms_delta"),
        "latency_aof_pending_fsync_latest_before_ms": before.get("latency_aof_pending_fsync_latest_ms"),
        "latency_aof_pending_fsync_latest_after_ms": after.get("latency_aof_pending_fsync_latest_ms"),
        "latency_aof_pending_fsync_latest_delta_ms": delta.get("latency_aof_pending_fsync_latest_ms_delta"),
        "latency_aof_pending_fsync_max_before_ms": before.get("latency_aof_pending_fsync_max_ms"),
        "latency_aof_pending_fsync_max_after_ms": after.get("latency_aof_pending_fsync_max_ms"),
        "latency_aof_pending_fsync_max_delta_ms": delta.get("latency_aof_pending_fsync_max_ms_delta"),
        "host_telemetry_supported": host_telemetry.get("supported"),
        "host_telemetry_samples_path": host_telemetry.get("samples_path"),
        "host_telemetry_summary_path": host_telemetry.get("summary_path"),
        "host_telemetry_sample_count": telemetry_summary.get("sample_count"),
        "host_telemetry_duration_seconds": telemetry_summary.get("duration_seconds"),
        "system_cpu_utilization_avg_pct": telemetry_summary.get("system_cpu_utilization_avg_pct"),
        "system_cpu_utilization_peak_pct": telemetry_summary.get("system_cpu_utilization_peak_pct"),
        "per_cpu_utilization_peak_pct": telemetry_summary.get("per_cpu_utilization_peak_pct"),
        "per_cpu_utilization_floor_pct": telemetry_summary.get("per_cpu_utilization_floor_pct"),
        "system_cpu_iowait_avg_pct": telemetry_summary.get("system_cpu_iowait_avg_pct"),
        "system_procs_running_peak": telemetry_summary.get("system_procs_running_peak"),
        "system_procs_blocked_peak": telemetry_summary.get("system_procs_blocked_peak"),
        "system_loadavg_1_peak": telemetry_summary.get("system_loadavg_1_peak"),
        "system_mem_available_min_bytes": telemetry_summary.get("system_mem_available_min_bytes"),
        "system_mem_dirty_peak_bytes": telemetry_summary.get("system_mem_dirty_peak_bytes"),
        "system_mem_writeback_peak_bytes": telemetry_summary.get("system_mem_writeback_peak_bytes"),
        "system_mem_active_file_peak_bytes": telemetry_summary.get("system_mem_active_file_peak_bytes"),
        "system_mem_inactive_file_peak_bytes": telemetry_summary.get("system_mem_inactive_file_peak_bytes"),
        "system_mem_slab_reclaimable_peak_bytes": telemetry_summary.get("system_mem_slab_reclaimable_peak_bytes"),
        "system_swap_free_min_bytes": telemetry_summary.get("system_swap_free_min_bytes"),
        "system_vm_page_faults_delta": telemetry_summary.get("system_vm_page_faults_delta"),
        "system_vm_major_page_faults_delta": telemetry_summary.get("system_vm_major_page_faults_delta"),
        "system_vm_page_scan_kswapd_delta": telemetry_summary.get("system_vm_page_scan_kswapd_delta"),
        "system_vm_page_scan_direct_delta": telemetry_summary.get("system_vm_page_scan_direct_delta"),
        "system_vm_page_steal_kswapd_delta": telemetry_summary.get("system_vm_page_steal_kswapd_delta"),
        "system_vm_page_steal_direct_delta": telemetry_summary.get("system_vm_page_steal_direct_delta"),
        "system_vm_swap_in_delta": telemetry_summary.get("system_vm_swap_in_delta"),
        "system_vm_swap_out_delta": telemetry_summary.get("system_vm_swap_out_delta"),
        "system_vm_allocstall_delta": telemetry_summary.get("system_vm_allocstall_delta"),
        "system_vm_page_reclaim_delta": telemetry_summary.get("system_vm_page_reclaim_delta"),
        "process_cpu_utilization_avg_pct": telemetry_summary.get("process_cpu_utilization_avg_pct"),
        "process_cpu_utilization_peak_pct": telemetry_summary.get("process_cpu_utilization_peak_pct"),
        "process_rss_peak_bytes": telemetry_summary.get("process_rss_peak_bytes"),
        "process_minor_faults_delta": telemetry_summary.get("process_minor_faults_delta"),
        "process_major_faults_delta": telemetry_summary.get("process_major_faults_delta"),
        "process_read_bytes_delta": telemetry_summary.get("process_read_bytes_delta"),
        "process_write_bytes_delta": telemetry_summary.get("process_write_bytes_delta"),
        "process_cancelled_write_bytes_delta": telemetry_summary.get("process_cancelled_write_bytes_delta"),
        "process_syscr_delta": telemetry_summary.get("process_syscr_delta"),
        "process_syscw_delta": telemetry_summary.get("process_syscw_delta"),
        "process_voluntary_ctx_switches_delta": telemetry_summary.get("process_voluntary_ctx_switches_delta"),
        "process_nonvoluntary_ctx_switches_delta": telemetry_summary.get("process_nonvoluntary_ctx_switches_delta"),
        "network_total_rx_bytes_delta": telemetry_summary.get("network_total_rx_bytes_delta"),
        "network_total_tx_bytes_delta": telemetry_summary.get("network_total_tx_bytes_delta"),
        "network_total_rx_errors_delta": telemetry_summary.get("network_total_rx_errors_delta"),
        "network_total_tx_errors_delta": telemetry_summary.get("network_total_tx_errors_delta"),
        "network_loopback_rx_bytes_delta": telemetry_summary.get("network_loopback_rx_bytes_delta"),
        "network_loopback_tx_bytes_delta": telemetry_summary.get("network_loopback_tx_bytes_delta"),
        "tcp_retrans_segs_delta": telemetry_summary.get("tcp_retrans_segs_delta"),
        "tcp_listen_overflows_delta": telemetry_summary.get("tcp_listen_overflows_delta"),
        "tcp_listen_drops_delta": telemetry_summary.get("tcp_listen_drops_delta"),
        "tcp_syn_retrans_delta": telemetry_summary.get("tcp_syn_retrans_delta"),
        "socket_recv_q_peak": telemetry_summary.get("socket_recv_q_peak"),
        "socket_send_q_peak": telemetry_summary.get("socket_send_q_peak"),
        "disk_read_bytes_delta": telemetry_summary.get("disk_read_bytes_delta"),
        "disk_write_bytes_delta": telemetry_summary.get("disk_write_bytes_delta"),
        "disk_io_time_delta_ms": telemetry_summary.get("disk_io_time_delta_ms"),
        "disk_io_in_progress_peak": telemetry_summary.get("disk_io_in_progress_peak"),
        "runtime_reactor_slots": telemetry_summary.get("runtime_reactor_slots"),
        "eviction_admissions_host_delta": telemetry_summary.get("eviction_admissions_delta"),
        "eviction_shards_scanned_host_delta": telemetry_summary.get("eviction_shards_scanned_delta"),
        "eviction_slots_sampled_host_delta": telemetry_summary.get("eviction_slots_sampled_delta"),
        "eviction_bytes_freed_host_delta": telemetry_summary.get("eviction_bytes_freed_delta"),
        "eviction_oom_after_scan_host_delta": telemetry_summary.get("eviction_oom_after_scan_delta"),
    }


def _detect_report_aggregates_multiple_replicates(rows: list[dict[str, Any]]) -> bool:
    seen: dict[tuple[Any, ...], set[str]] = {}
    for row in rows:
        key = (
            row.get("database"),
            row.get("backend"),
            row.get("series_kind"),
            row.get("series_label"),
            row.get("thread_count"),
            row.get("configured_aof_enabled"),
            row.get("configured_aof_fsync"),
            row.get("configured_maxmemory"),
            row.get("configured_eviction_policy"),
        )
        replicate_source = str(
            row.get("replicate_run_id")
            or row.get("run_id")
            or row.get("source_summary")
            or ""
        )
        seen.setdefault(key, set()).add(replicate_source)
    return any(len(replicate_sources) > 1 for replicate_sources in seen.values())


def _normalize_redis_rows(
    result: dict[str, Any],
    record: dict[str, Any],
    summary: dict[str, Any],
    request: dict[str, Any],
    service: dict[str, Any],
    summary_path: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    record_items = record.get("items") or []
    for index, item in enumerate(result.get("items", [])):
        record_item = record_items[index] if index < len(record_items) else {}
        metrics = item.get("metrics") or {}
        rows.append(
            {
                "run_id": (_replicate_fields(record, record_item).get("replicate_run_id") or summary.get("run_id")),
                **_replicate_fields(record, record_item),
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
                "p95_latency_ms": _coerce_float(metrics.get("p95_latency_ms")),
                "p99_latency_ms": _coerce_float(metrics.get("p99_latency_ms")),
                "p99_9_latency_ms": _coerce_float(metrics.get("p99_9_latency_ms")),
                "p99_99_latency_ms": _coerce_float(metrics.get("p99_99_latency_ms")),
                "p99_999_latency_ms": _coerce_float(metrics.get("p99_999_latency_ms")),
                "workload_details": None,
                "host_os": (request.get("host_metadata") or {}).get("os"),
                "host_architecture": (request.get("host_metadata") or {}).get("architecture"),
                "source_summary": str(summary_path),
                "backend_stdout_path": item.get("stdout_path"),
                "backend_stderr_path": item.get("stderr_path"),
                "backend_csv_path": item.get("stdout_path"),
                **_runtime_fields(service),
                **_io_backend_fields(service),
                **_workload_contract_fields(result.get("backend"), item, request),
                **_observability_fields(item),
            }
        )
    return rows


def _normalize_memtier_rows(
    result: dict[str, Any],
    record: dict[str, Any],
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
    record_items = record.get("items") or []
    for index, item in enumerate(result.get("items", [])):
        record_item = record_items[index] if index < len(record_items) else {}
        totals = ((item.get("metrics") or {}).get("totals") or {})
        workload = item.get("workload")
        rows.append(
            {
                "run_id": (_replicate_fields(record, record_item).get("replicate_run_id") or summary.get("run_id")),
                **_replicate_fields(record, record_item),
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
                "p95_latency_ms": _coerce_float(totals.get("p95_latency_ms")),
                "p99_latency_ms": _coerce_float(totals.get("p99_latency_ms")),
                "p99_9_latency_ms": _coerce_float(totals.get("p99_9_latency_ms")),
                "p99_99_latency_ms": _coerce_float(totals.get("p99_99_latency_ms")),
                "p99_999_latency_ms": _coerce_float(totals.get("p99_999_latency_ms")),
                "workload_details": definitions.get(workload),
                "host_os": (request.get("host_metadata") or {}).get("os"),
                "host_architecture": (request.get("host_metadata") or {}).get("architecture"),
                "source_summary": str(summary_path),
                "backend_stdout_path": item.get("stdout_path"),
                "backend_stderr_path": item.get("stderr_path"),
                "backend_json_path": item.get("json_path"),
                "backend_hdr_files": item.get("hdr_files") or [],
                **_runtime_fields(service),
                **_io_backend_fields(service),
                **_workload_contract_fields(result.get("backend"), item, request),
                **_observability_fields(item),
            }
        )
    return rows


def _normalize_custom_rows(
    result: dict[str, Any],
    record: dict[str, Any],
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
    record_items = record.get("items") or []
    for index, item in enumerate(result.get("items", [])):
        record_item = record_items[index] if index < len(record_items) else {}
        metrics = item.get("metrics") or {}
        workload = item.get("workload")
        rows.append(
            {
                "run_id": (_replicate_fields(record, record_item).get("replicate_run_id") or summary.get("run_id")),
                **_replicate_fields(record, record_item),
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
                "p95_latency_ms": _coerce_float(metrics.get("p95_ns")) / 1_000_000.0 if metrics.get("p95_ns") is not None else None,
                "p99_latency_ms": _coerce_float(metrics.get("p99_ns")) / 1_000_000.0 if metrics.get("p99_ns") is not None else None,
                "p99_9_latency_ms": _coerce_float(metrics.get("p99_9_ns")) / 1_000_000.0 if metrics.get("p99_9_ns") is not None else None,
                "p99_99_latency_ms": None,
                "p99_999_latency_ms": _coerce_float(metrics.get("p99_999_ns")) / 1_000_000.0 if metrics.get("p99_999_ns") is not None else None,
                "workload_details": definitions.get(workload),
                "host_os": (request.get("host_metadata") or {}).get("os"),
                "host_architecture": (request.get("host_metadata") or {}).get("architecture"),
                "source_summary": str(summary_path),
                "backend_stdout_path": item.get("stdout_path"),
                "backend_stderr_path": item.get("stderr_path"),
                "backend_json_path": item.get("json_path"),
                **_runtime_fields(service),
                **_io_backend_fields(service),
                **_workload_contract_fields(result.get("backend"), item, request),
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
            rows.extend(_normalize_redis_rows(backend_result, record, summary, request, service, summary_path))
        elif backend == "memtier_benchmark":
            rows.extend(_normalize_memtier_rows(backend_result, record, summary, request, service, summary_path))
        elif backend == "custom-rust":
            rows.extend(_normalize_custom_rows(backend_result, record, summary, request, service, summary_path))
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


def _row_identity(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "database": row.get("database"),
        "backend": row.get("backend"),
        "series_kind": row.get("series_kind"),
        "series_label": row.get("series_label"),
        "thread_count": row.get("thread_count"),
        "configured_aof_enabled": row.get("configured_aof_enabled"),
        "configured_aof_fsync": row.get("configured_aof_fsync"),
        "configured_maxmemory": row.get("configured_maxmemory"),
        "configured_eviction_policy": row.get("configured_eviction_policy"),
        "source_summary": row.get("source_summary"),
    }


def _max_numeric(rows: list[dict[str, Any]], key: str) -> Optional[float]:
    values = [
        float(value)
        for value in (_coerce_float(row.get(key)) for row in rows)
        if value is not None
    ]
    if not values:
        return None
    return max(values)


def _memory_comparison_signature(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("series_kind"),
        row.get("series_label"),
        row.get("thread_count"),
        row.get("configured_aof_enabled"),
        row.get("configured_aof_fsync"),
        row.get("configured_maxmemory"),
        row.get("configured_eviction_policy"),
    )


def _rss_bytes_for_memory_claim(row: dict[str, Any]) -> Optional[float]:
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


def _engine_memory_claim(value: Optional[float]) -> tuple[str, str]:
    if value is None or value <= 0:
        return "Rejected", "missing engine bytes/live-key"
    if value <= ENGINE_BYTES_PER_KEY_ALLOWED:
        return (
            "Allowed",
            f"engine bytes/live-key {value:.2f} <= {ENGINE_BYTES_PER_KEY_ALLOWED:.2f}",
        )
    if value <= ENGINE_BYTES_PER_KEY_NARROWED:
        return (
            "Narrowed",
            "engine bytes/live-key "
            f"{value:.2f} <= {ENGINE_BYTES_PER_KEY_NARROWED:.2f}; "
            "claim must name workload and shard count",
        )
    return (
        "Rejected",
        f"engine bytes/live-key {value:.2f} > {ENGINE_BYTES_PER_KEY_NARROWED:.2f}",
    )


def _full_server_memory_claim(ratio: Optional[float]) -> tuple[str, str]:
    if ratio is None or ratio <= 0:
        return "Rejected", "missing comparable Vortex and Redis full-server RSS"
    if ratio <= FULL_SERVER_RSS_ALLOWED_RATIO:
        return (
            "Allowed",
            f"full-server RSS/Redis ratio {ratio:.2f} <= {FULL_SERVER_RSS_ALLOWED_RATIO:.2f}",
        )
    if ratio <= FULL_SERVER_RSS_NARROWED_RATIO:
        return (
            "Narrowed",
            "full-server RSS/Redis ratio "
            f"{ratio:.2f} <= {FULL_SERVER_RSS_NARROWED_RATIO:.2f}; "
            "claim must name workload and server config",
        )
    return (
        "Rejected",
        f"full-server RSS/Redis ratio {ratio:.2f} > {FULL_SERVER_RSS_NARROWED_RATIO:.2f}",
    )


def _annotate_memory_claim_decisions(rows: list[dict[str, Any]]) -> None:
    redis_rss_by_signature: dict[tuple[Any, ...], float] = {}
    for row in rows:
        if str(row.get("database") or "").lower() != "redis":
            continue
        rss = _rss_bytes_for_memory_claim(row)
        if rss is None:
            continue
        signature = _memory_comparison_signature(row)
        previous = redis_rss_by_signature.get(signature)
        redis_rss_by_signature[signature] = rss if previous is None else max(previous, rss)

    for row in rows:
        database = str(row.get("database") or "").lower()
        if database != "vortex":
            row["engine_memory_claim_decision"] = "Rejected"
            row["engine_memory_claim_reason"] = "not a Vortex engine row"
            row["full_server_rss_claim_decision"] = "Rejected"
            row["full_server_rss_claim_reason"] = "Redis/baseline row is not a Vortex claim"
            row["full_server_rss_vs_redis_ratio"] = None
            continue

        engine_decision, engine_reason = _engine_memory_claim(
            _coerce_float(row.get("engine_bytes_per_live_key_after"))
        )
        row["engine_memory_claim_decision"] = engine_decision
        row["engine_memory_claim_reason"] = engine_reason

        vortex_rss = _rss_bytes_for_memory_claim(row)
        redis_rss = redis_rss_by_signature.get(_memory_comparison_signature(row))
        ratio = None
        if vortex_rss is not None and redis_rss is not None and redis_rss > 0:
            ratio = vortex_rss / redis_rss
        full_decision, full_reason = _full_server_memory_claim(ratio)
        row["full_server_rss_vs_redis_ratio"] = ratio
        row["full_server_rss_claim_decision"] = full_decision
        row["full_server_rss_claim_reason"] = full_reason


def _build_memory_diagnostics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for row in rows:
        diagnostics.append(
            {
                **_row_identity(row),
                "throughput_ops_sec": row.get("throughput_ops_sec"),
                "memory_attribution_engine_scope": row.get(
                    "memory_attribution_engine_scope"
                ),
                "memory_attribution_full_server_scope": row.get(
                    "memory_attribution_full_server_scope"
                ),
                "engine_memory_claim_decision": row.get(
                    "engine_memory_claim_decision"
                ),
                "engine_memory_claim_reason": row.get("engine_memory_claim_reason"),
                "full_server_rss_vs_redis_ratio": row.get(
                    "full_server_rss_vs_redis_ratio"
                ),
                "full_server_rss_claim_decision": row.get(
                    "full_server_rss_claim_decision"
                ),
                "full_server_rss_claim_reason": row.get(
                    "full_server_rss_claim_reason"
                ),
                "engine_live_keys_after": row.get("engine_live_keys_after"),
                "engine_logical_dataset_after_bytes": row.get(
                    "engine_logical_dataset_after_bytes"
                ),
                "engine_table_allocated_after_bytes": row.get(
                    "engine_table_allocated_after_bytes"
                ),
                "engine_table_total_slots_after": row.get(
                    "engine_table_total_slots_after"
                ),
                "engine_capacity_slack_slots_after": row.get(
                    "engine_capacity_slack_slots_after"
                ),
                "engine_tombstone_slots_after": row.get(
                    "engine_tombstone_slots_after"
                ),
                "engine_load_factor_after": row.get("engine_load_factor_after"),
                "engine_bytes_per_live_key_after": row.get(
                    "engine_bytes_per_live_key_after"
                ),
                "engine_shard_count_after": row.get("engine_shard_count_after"),
                "io_fixed_buffer_reserved_after_bytes": row.get(
                    "io_fixed_buffer_reserved_after_bytes"
                ),
                "io_fixed_buffer_committed_after_bytes": row.get(
                    "io_fixed_buffer_committed_after_bytes"
                ),
                "io_fixed_buffer_active_after_bytes": row.get(
                    "io_fixed_buffer_active_after_bytes"
                ),
                "io_fixed_buffer_count_after": row.get("io_fixed_buffer_count_after"),
                "io_fixed_buffer_size_after": row.get("io_fixed_buffer_size_after"),
                "per_connection_state_after_bytes": row.get(
                    "per_connection_state_after_bytes"
                ),
                "connection_capacity_after": row.get("connection_capacity_after"),
                "full_server_process_rss_after_bytes": row.get(
                    "full_server_process_rss_after_bytes"
                ),
                "process_rss_peak_bytes": row.get("process_rss_peak_bytes"),
                "used_memory_rss_after_bytes": row.get("used_memory_rss_after_bytes"),
                "used_memory_dataset_after_bytes": row.get("used_memory_dataset_after_bytes"),
                "used_memory_overhead_after_bytes": row.get("used_memory_overhead_after_bytes"),
                "allocator_allocated_after_bytes": row.get("allocator_allocated_after_bytes"),
                "allocator_active_after_bytes": row.get("allocator_active_after_bytes"),
                "allocator_resident_after_bytes": row.get("allocator_resident_after_bytes"),
                "allocator_mapped_after_bytes": row.get("allocator_mapped_after_bytes"),
                "allocator_retained_after_bytes": row.get(
                    "allocator_retained_after_bytes"
                ),
                "allocator_frag_ratio_after": row.get("allocator_frag_ratio_after"),
                "mem_fragmentation_ratio_after": row.get("mem_fragmentation_ratio_after"),
                "process_minor_faults_delta": row.get("process_minor_faults_delta"),
                "process_major_faults_delta": row.get("process_major_faults_delta"),
                "system_mem_dirty_peak_bytes": row.get("system_mem_dirty_peak_bytes"),
                "system_mem_writeback_peak_bytes": row.get("system_mem_writeback_peak_bytes"),
                "system_vm_page_scan_kswapd_delta": row.get("system_vm_page_scan_kswapd_delta"),
                "system_vm_page_scan_direct_delta": row.get("system_vm_page_scan_direct_delta"),
                "system_vm_allocstall_delta": row.get("system_vm_allocstall_delta"),
                "system_vm_swap_out_delta": row.get("system_vm_swap_out_delta"),
            }
        )
    return diagnostics


def _build_aof_diagnostics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for row in rows:
        aof_enabled = row.get("configured_aof_enabled") is True or row.get("aof_enabled_after") is True
        has_aof_signal = any(
            row.get(key) is not None
            for key in (
                "aof_current_size_delta_bytes",
                "aof_delayed_fsync_delta",
                "aof_pending_bio_fsync_after",
                "latency_aof_fsync_max_after_ms",
                "latency_aof_write_max_after_ms",
            )
        )
        if not aof_enabled and not has_aof_signal:
            continue

        diagnostics.append(
            {
                **_row_identity(row),
                "aof_enabled_after": row.get("aof_enabled_after"),
                "aof_current_size_delta_bytes": row.get("aof_current_size_delta_bytes"),
                "aof_buffer_length_after_bytes": row.get("aof_buffer_length_after_bytes"),
                "aof_pending_bio_fsync_after": row.get("aof_pending_bio_fsync_after"),
                "aof_delayed_fsync_delta": row.get("aof_delayed_fsync_delta"),
                "aof_last_write_status_after": row.get("aof_last_write_status_after"),
                "process_write_bytes_delta": row.get("process_write_bytes_delta"),
                "process_cancelled_write_bytes_delta": row.get("process_cancelled_write_bytes_delta"),
                "process_syscw_delta": row.get("process_syscw_delta"),
                "disk_write_bytes_delta": row.get("disk_write_bytes_delta"),
                "disk_io_time_delta_ms": row.get("disk_io_time_delta_ms"),
                "system_mem_writeback_peak_bytes": row.get("system_mem_writeback_peak_bytes"),
                "latency_aof_write_max_after_ms": row.get("latency_aof_write_max_after_ms"),
                "latency_aof_fsync_max_after_ms": row.get("latency_aof_fsync_max_after_ms"),
                "latency_aof_pending_fsync_max_after_ms": row.get("latency_aof_pending_fsync_max_after_ms"),
            }
        )
    return diagnostics


def _build_diagnostic_summary(
    rows: list[dict[str, Any]],
    memory_rows: list[dict[str, Any]],
    aof_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    reclaim_rows = [
        row
        for row in memory_rows
        if any(
            (_coerce_float(row.get(key)) or 0.0) > 0.0
            for key in (
                "system_mem_writeback_peak_bytes",
                "system_vm_page_scan_kswapd_delta",
                "system_vm_page_scan_direct_delta",
                "system_vm_allocstall_delta",
                "system_vm_swap_out_delta",
            )
        )
    ]
    aof_pressure_rows = [
        row
        for row in aof_rows
        if any(
            (_coerce_float(row.get(key)) or 0.0) > 0.0
            for key in (
                "aof_delayed_fsync_delta",
                "aof_pending_bio_fsync_after",
                "latency_aof_fsync_max_after_ms",
                "latency_aof_pending_fsync_max_after_ms",
            )
        )
    ]
    return {
        "raw_row_count": len(rows),
        "normalized_row_count": len(rows),
        "rows_with_host_telemetry": sum(1 for row in rows if row.get("host_telemetry_supported")),
        "rows_with_memory_diagnostics": len(memory_rows),
        "rows_with_reclaim_or_writeback_pressure": len(reclaim_rows),
        "rows_with_aof_diagnostics": len(aof_rows),
        "rows_with_aof_fsync_pressure": len(aof_pressure_rows),
        "max_process_rss_peak_bytes": _max_numeric(memory_rows, "process_rss_peak_bytes"),
        "max_engine_bytes_per_live_key": _max_numeric(
            memory_rows, "engine_bytes_per_live_key_after"
        ),
        "max_full_server_rss_vs_redis_ratio": _max_numeric(
            memory_rows, "full_server_rss_vs_redis_ratio"
        ),
        "engine_memory_claims_allowed": sum(
            1
            for row in memory_rows
            if row.get("engine_memory_claim_decision") == "Allowed"
        ),
        "engine_memory_claims_narrowed": sum(
            1
            for row in memory_rows
            if row.get("engine_memory_claim_decision") == "Narrowed"
        ),
        "engine_memory_claims_rejected": sum(
            1
            for row in memory_rows
            if row.get("engine_memory_claim_decision") == "Rejected"
        ),
        "full_server_rss_claims_allowed": sum(
            1
            for row in memory_rows
            if row.get("full_server_rss_claim_decision") == "Allowed"
        ),
        "full_server_rss_claims_narrowed": sum(
            1
            for row in memory_rows
            if row.get("full_server_rss_claim_decision") == "Narrowed"
        ),
        "full_server_rss_claims_rejected": sum(
            1
            for row in memory_rows
            if row.get("full_server_rss_claim_decision") == "Rejected"
        ),
        "max_allocator_resident_after_bytes": _max_numeric(memory_rows, "allocator_resident_after_bytes"),
        "max_system_mem_dirty_peak_bytes": _max_numeric(memory_rows, "system_mem_dirty_peak_bytes"),
        "max_system_mem_writeback_peak_bytes": _max_numeric(memory_rows, "system_mem_writeback_peak_bytes"),
        "max_disk_write_bytes_delta": _max_numeric(aof_rows, "disk_write_bytes_delta"),
        "max_aof_fsync_latency_ms": _max_numeric(aof_rows, "latency_aof_fsync_max_after_ms"),
    }


def build_report_payload(summary_paths: list[Path], title: Optional[str] = None) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    host_metadata: dict[str, Any] | None = None
    database_records: dict[str, dict[str, Any]] = {}
    workloads: dict[str, dict[str, Any]] = {}
    source_runs: list[dict[str, Any]] = []
    report_validity: dict[str, Any] | None = None

    for path in summary_paths:
        summary = _read_json(path)
        request = _read_json(Path(summary["request_path"]))
        state_path = Path(request["state_file"]).expanduser().resolve()
        state = _read_json(state_path) if state_path.exists() else {}

        if host_metadata is None:
            host_metadata = request.get("host_metadata") or capture_host_metadata()

        for service in state.get("services", []):
            io_backend_requested, io_backend_effective = _infer_vortex_backend(service)
            database_records[service.get("database", "unknown")] = {
                "database": service.get("database"),
                "mode": service.get("mode"),
                "version": (service.get("metadata") or {}).get("version"),
                "bind": (service.get("metadata") or {}).get("bind"),
                "runtime_config": service.get("runtime_config") or {},
                "io_backend_requested": io_backend_requested,
                "io_backend_effective": io_backend_effective,
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
                "validity": summary.get("validity") or request.get("validity") or {},
            }
        )
        rows.extend(_normalize_rows(path, summary, request, state))
        if report_validity is None:
            report_validity = dict(summary.get("validity") or request.get("validity") or {})

    rows.sort(
        key=lambda row: (
            row.get("database") or "",
            row.get("backend") or "",
            row.get("series_label") or "",
            row.get("thread_count") or 0,
        )
    )

    normalized_validity = dict(report_validity or {})
    normalized_validity["report_aggregates_multiple_source_runs"] = len(source_runs) > 1
    normalized_validity["report_aggregates_multiple_replicates"] = _detect_report_aggregates_multiple_replicates(rows)
    _annotate_memory_claim_decisions(rows)
    aof_overhead = _build_aof_overhead(rows)
    eviction_rows = [row for row in rows if _has_eviction_activity(row)]
    memory_rows = _build_memory_diagnostics(rows)
    aof_rows = _build_aof_diagnostics(rows)
    timeseries_summaries = build_timeseries_summaries(rows)
    diagnostics = {
        "summary": _build_diagnostic_summary(rows, memory_rows, aof_rows),
        "memory_rows": memory_rows,
        "aof_rows": aof_rows,
        "aof_overhead": aof_overhead,
        "eviction_rows": eviction_rows,
        "timeseries_summaries": timeseries_summaries,
    }

    return {
        "schema_version": 1,
        "title": title or "Vortex Benchmark Report",
        "source_runs": source_runs,
        "host_metadata": host_metadata or capture_host_metadata(),
        "validity": normalized_validity,
        "databases": [database_records[key] for key in sorted(database_records)],
        "workloads": [workloads[key] for key in sorted(workloads)],
        "benchmark": {
            "rows": rows,
            "analysis": {},
        },
        "diagnostics": diagnostics,
        "rows": rows,
        "aof_overhead": aof_overhead,
        "eviction_rows": eviction_rows,
    }


def _has_eviction_activity(row: dict[str, Any]) -> bool:
    for field in (
        "evicted_keys_delta",
        "eviction_admissions_delta",
        "eviction_bytes_freed_delta",
        "eviction_oom_after_scan_delta",
    ):
        value = row.get(field)
        if isinstance(value, (int, float)) and value != 0:
            return True
    return False


# ---------------------------------------------------------------------------
# Time-series telemetry summaries
# ---------------------------------------------------------------------------


def build_timeseries_summaries(
    rows: list[dict[str, Any]],
    warmup_seconds: float = 0.0,
) -> list[dict[str, Any]]:
    """Load JSONL host telemetry and compute per-interval rate summaries.

    When warmup_seconds > 0, the first N seconds of samples are excluded
    from the summary statistics (standard benchmarking practice to avoid
    cold-start contamination).
    """
    summaries: list[dict[str, Any]] = []
    for row in rows:
        samples_path = row.get("host_telemetry_samples_path")
        if not samples_path:
            continue
        path = Path(str(samples_path))
        if not path.exists():
            continue

        samples = _load_jsonl(path)
        if len(samples) < 2:
            continue

        # Apply warmup exclusion
        if warmup_seconds > 0 and samples:
            first_ts = _parse_timestamp(samples[0].get("captured_at"))
            if first_ts is not None:
                cutoff = first_ts + warmup_seconds
                samples = [
                    s for s in samples
                    if (_parse_timestamp(s.get("captured_at")) or 0) >= cutoff
                ]
        if len(samples) < 2:
            continue

        summary = _compute_timeseries_summary(samples)
        summary["database"] = row.get("database")
        summary["backend"] = row.get("backend")
        summary["series_label"] = row.get("series_label")
        summary["thread_count"] = row.get("thread_count")
        summary["sample_count"] = len(samples)
        summaries.append(summary)

    return summaries


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    """Load JSONL file into a list of dicts."""
    import json
    samples: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    samples.append(json.loads(line))
    except (OSError, json.JSONDecodeError):
        pass
    return samples


def _parse_timestamp(ts: Any) -> Optional[float]:
    """Parse ISO 8601 timestamp to epoch seconds."""
    if ts is None:
        return None
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return dt.timestamp()
    except (ValueError, TypeError):
        return None


def _stat_summary(values: list[float]) -> dict[str, Optional[float]]:
    """Compute min, avg, p95, max for a list of values."""
    if not values:
        return {"min": None, "avg": None, "p95": None, "max": None}
    values_sorted = sorted(values)
    n = len(values_sorted)
    p95_idx = min(int(n * 0.95), n - 1)
    return {
        "min": values_sorted[0],
        "avg": sum(values) / n,
        "p95": values_sorted[p95_idx],
        "max": values_sorted[-1],
    }


def _compute_timeseries_summary(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute per-interval rates and summary statistics from telemetry samples."""

    # Direct value metrics (already per-interval)
    direct_metrics = {
        "system_cpu_pct": "system_cpu_utilization_pct",
        "process_cpu_pct": "process_cpu_utilization_pct",
        "process_rss_bytes": "process_rss_bytes",
        "mem_dirty_bytes": "mem_dirty_bytes",
        "loadavg_1": "loadavg_1",
    }

    # Delta metrics (need interval rate computation)
    delta_metrics = {
        "ctx_switches_per_sec": "system_context_switches",
        "vol_ctx_per_sec": "process_voluntary_ctx_switches",
        "nonvol_ctx_per_sec": "process_nonvoluntary_ctx_switches",
        "net_lo_rx_bytes_per_sec": "network_loopback_rx_bytes",
        "net_lo_tx_bytes_per_sec": "network_loopback_tx_bytes",
        "disk_write_bytes_per_sec": "disk_write_bytes",
        "disk_read_bytes_per_sec": "disk_read_bytes",
    }

    result: dict[str, Any] = {}

    # Direct value summaries
    for name, field in direct_metrics.items():
        values = [
            float(s[field]) for s in samples
            if s.get(field) is not None
        ]
        result[name] = _stat_summary(values)

    # Delta-rate summaries (per-second rates computed from consecutive samples)
    timestamps = [_parse_timestamp(s.get("captured_at")) for s in samples]

    for name, field in delta_metrics.items():
        rates: list[float] = []
        for i in range(1, len(samples)):
            t0, t1 = timestamps[i - 1], timestamps[i]
            v0 = samples[i - 1].get(field)
            v1 = samples[i].get(field)
            if t0 is None or t1 is None or v0 is None or v1 is None:
                continue
            dt = t1 - t0
            if dt <= 0:
                continue
            rate = (float(v1) - float(v0)) / dt
            if rate >= 0:  # ignore counter wraps
                rates.append(rate)
        result[name] = _stat_summary(rates)

    return result
