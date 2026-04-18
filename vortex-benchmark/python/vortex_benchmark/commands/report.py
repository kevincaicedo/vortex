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
        "process_memory_before_bytes",
        "process_memory_after_bytes",
        "process_memory_delta_bytes",
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