from __future__ import annotations

import csv
import json
from pathlib import Path

from vortex_benchmark.env import build_layout
from vortex_benchmark.models import timestamp_slug, utc_now
from vortex_benchmark.reporting import (
    build_analysis,
    build_publication_artifacts,
    build_report_payload,
    publish_report_artifacts,
    render_markdown_report,
    render_report_charts,
)
from vortex_benchmark.reporting.collector import resolve_summary_paths


def _resolve_output_root(summary_paths: list[Path], results_dir: str | None, output_dir: str | None) -> str | None:
    if output_dir:
        return output_dir
    if results_dir:
        return str(Path(results_dir).expanduser().resolve().parent)
    return str(summary_paths[0].resolve().parent.parent)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    base_columns = [
        "run_id",
        "suite_run_id",
        "replicate_run_id",
        "replicate_id",
        "replicate_index",
        "replicate_count",
        "environment_id",
        "benchmark_date_time",
        "database",
        "database_mode",
        "database_version",
        "backend",
        "series_kind",
        "series_label",
        "thread_count",
        "configured_aof_enabled",
        "configured_aof_fsync",
        "configured_maxmemory",
        "configured_eviction_policy",
        "throughput_ops_sec",
        "average_latency_ms",
        "p50_latency_ms",
        "p99_latency_ms",
        "p99_9_latency_ms",
        "p99_999_latency_ms",
        "used_memory_before_bytes",
        "used_memory_after_bytes",
        "used_memory_delta_bytes",
        "used_memory_rss_before_bytes",
        "used_memory_rss_after_bytes",
        "used_memory_rss_delta_bytes",
        "used_memory_dataset_before_bytes",
        "used_memory_dataset_after_bytes",
        "used_memory_dataset_delta_bytes",
        "used_memory_overhead_before_bytes",
        "used_memory_overhead_after_bytes",
        "used_memory_overhead_delta_bytes",
        "process_memory_before_bytes",
        "process_memory_after_bytes",
        "process_memory_delta_bytes",
        "allocator_allocated_before_bytes",
        "allocator_allocated_after_bytes",
        "allocator_allocated_delta_bytes",
        "allocator_active_before_bytes",
        "allocator_active_after_bytes",
        "allocator_active_delta_bytes",
        "allocator_resident_before_bytes",
        "allocator_resident_after_bytes",
        "allocator_resident_delta_bytes",
        "allocator_frag_ratio_before",
        "allocator_frag_ratio_after",
        "allocator_rss_before_bytes",
        "allocator_rss_after_bytes",
        "allocator_rss_delta_bytes",
        "mem_fragmentation_ratio_before",
        "mem_fragmentation_ratio_after",
        "mem_fragmentation_bytes_before",
        "mem_fragmentation_bytes_after",
        "mem_fragmentation_bytes_delta",
        "keyspace_hits_delta",
        "keyspace_misses_delta",
        "cache_hit_rate",
        "cache_efficiency",
        "evicted_keys_delta",
        "expired_keys_delta",
        "aof_enabled_before",
        "aof_enabled_after",
        "aof_current_size_before_bytes",
        "aof_current_size_after_bytes",
        "aof_current_size_delta_bytes",
        "aof_buffer_length_before_bytes",
        "aof_buffer_length_after_bytes",
        "aof_buffer_length_delta_bytes",
        "aof_pending_bio_fsync_before",
        "aof_pending_bio_fsync_after",
        "aof_pending_bio_fsync_delta",
        "aof_delayed_fsync_before",
        "aof_delayed_fsync_after",
        "aof_delayed_fsync_delta",
        "aof_pending_rewrite_before",
        "aof_pending_rewrite_after",
        "aof_pending_rewrite_delta",
        "aof_last_write_status_before",
        "aof_last_write_status_after",
        "latency_aof_write_latest_before_ms",
        "latency_aof_write_latest_after_ms",
        "latency_aof_write_latest_delta_ms",
        "latency_aof_write_max_before_ms",
        "latency_aof_write_max_after_ms",
        "latency_aof_write_max_delta_ms",
        "latency_aof_fsync_latest_before_ms",
        "latency_aof_fsync_latest_after_ms",
        "latency_aof_fsync_latest_delta_ms",
        "latency_aof_fsync_max_before_ms",
        "latency_aof_fsync_max_after_ms",
        "latency_aof_fsync_max_delta_ms",
        "latency_aof_pending_fsync_latest_before_ms",
        "latency_aof_pending_fsync_latest_after_ms",
        "latency_aof_pending_fsync_latest_delta_ms",
        "latency_aof_pending_fsync_max_before_ms",
        "latency_aof_pending_fsync_max_after_ms",
        "latency_aof_pending_fsync_max_delta_ms",
        "host_telemetry_supported",
        "host_telemetry_samples_path",
        "host_telemetry_summary_path",
        "host_telemetry_sample_count",
        "host_telemetry_duration_seconds",
        "system_cpu_utilization_avg_pct",
        "system_cpu_utilization_peak_pct",
        "system_cpu_iowait_avg_pct",
        "system_procs_running_peak",
        "system_procs_blocked_peak",
        "system_loadavg_1_peak",
        "system_mem_available_min_bytes",
        "system_mem_dirty_peak_bytes",
        "system_mem_writeback_peak_bytes",
        "system_mem_active_file_peak_bytes",
        "system_mem_inactive_file_peak_bytes",
        "system_mem_slab_reclaimable_peak_bytes",
        "system_swap_free_min_bytes",
        "system_vm_page_faults_delta",
        "system_vm_major_page_faults_delta",
        "system_vm_page_scan_kswapd_delta",
        "system_vm_page_scan_direct_delta",
        "system_vm_page_steal_kswapd_delta",
        "system_vm_page_steal_direct_delta",
        "system_vm_swap_in_delta",
        "system_vm_swap_out_delta",
        "system_vm_allocstall_delta",
        "system_vm_page_reclaim_delta",
        "process_cpu_utilization_avg_pct",
        "process_cpu_utilization_peak_pct",
        "process_rss_peak_bytes",
        "process_minor_faults_delta",
        "process_major_faults_delta",
        "process_read_bytes_delta",
        "process_write_bytes_delta",
        "process_cancelled_write_bytes_delta",
        "process_syscr_delta",
        "process_syscw_delta",
        "process_voluntary_ctx_switches_delta",
        "process_nonvoluntary_ctx_switches_delta",
        "network_total_rx_bytes_delta",
        "network_total_tx_bytes_delta",
        "network_total_rx_errors_delta",
        "network_total_tx_errors_delta",
        "network_loopback_rx_bytes_delta",
        "network_loopback_tx_bytes_delta",
        "disk_read_bytes_delta",
        "disk_write_bytes_delta",
        "disk_io_time_delta_ms",
        "disk_io_in_progress_peak",
        "source_summary",
    ]
    extra_columns = sorted({key for row in rows for key in row.keys()} - set(base_columns))
    columns = base_columns + extra_columns

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def execute_report(args) -> Path:
    summary_paths = resolve_summary_paths(args.summary_files, args.results_dir)
    output_root = _resolve_output_root(summary_paths, args.results_dir, args.output_dir)
    layout = build_layout(output_root)

    payload = build_report_payload(summary_paths, title=getattr(args, "title", None))
    db_names = [d.get("database", "unknown") for d in payload.get("databases", [])]
    payload["analysis"] = build_analysis(payload.get("rows", []), db_names)
    benchmark = payload.get("benchmark") or {}
    benchmark["analysis"] = payload["analysis"]
    benchmark["rows"] = payload["analysis"].get("aggregated_rows") or payload.get("rows", [])
    payload["benchmark"] = benchmark
    stamp = timestamp_slug()
    stem = f"{stamp}_report"

    assets_dir = layout.reports_dir / f"{stamp}_assets"
    chart_paths = render_report_charts(payload, assets_dir)

    json_path = layout.reports_dir / f"{stem}.json"
    csv_path = layout.reports_dir / f"{stem}.csv"
    markdown_path = layout.reports_dir / f"{stem}.md"

    payload["generated_at"] = utc_now()
    payload["artifacts"] = {
        "json": str(json_path),
        "csv": str(csv_path),
        "markdown": str(markdown_path),
        "charts": {name: str(path) for name, path in chart_paths.items()},
        **build_publication_artifacts(layout.reports_dir, chart_paths),
    }

    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _write_csv(csv_path, payload["rows"])
    markdown_path.write_text(
        render_markdown_report(payload, markdown_path, chart_paths),
        encoding="utf-8",
    )
    index_path = publish_report_artifacts(payload, json_path, csv_path, chart_paths)

    print(f"report json written: {json_path}")
    print(f"report csv written: {csv_path}")
    print(f"report markdown written: {markdown_path}")
    print(f"report latest json written: {payload['artifacts']['latest']['json']}")
    print(f"report latest csv written: {payload['artifacts']['latest']['csv']}")
    print(f"report latest markdown written: {payload['artifacts']['latest']['markdown']}")
    print(f"report history index written: {index_path}")
    for name, path in chart_paths.items():
        print(f"chart {name}: {path}")
    return markdown_path