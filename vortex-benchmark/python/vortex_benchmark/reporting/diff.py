from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path
from typing import Any

from vortex_benchmark.env import build_layout
from vortex_benchmark.models import timestamp_slug, utc_now
from vortex_benchmark.reporting.analysis import build_analysis
from vortex_benchmark.reporting.collector import build_report_payload, resolve_summary_paths


SIGNATURE_FIELDS = (
    "database",
    "database_mode",
    "backend",
    "series_kind",
    "series_label",
    "thread_count",
    "configured_aof_enabled",
    "configured_aof_fsync",
    "configured_maxmemory",
    "configured_eviction_policy",
    "configured_io_backend",
    "configured_telemetry_mode",
    "workload_client_count",
    "workload_pipeline",
    "workload_value_size",
    "workload_key_count",
    "load_threads",
)

METRICS = (
    ("throughput_ops_sec", "higher"),
    ("average_latency_ms", "lower"),
    ("p50_latency_ms", "lower"),
    ("p95_latency_ms", "lower"),
    ("p99_latency_ms", "lower"),
    ("p99_9_latency_ms", "lower"),
    ("p99_999_latency_ms", "lower"),
    ("system_cpu_utilization_avg_pct", "neutral"),
    ("process_cpu_utilization_avg_pct", "neutral"),
    ("process_rss_peak_bytes", "lower"),
    ("allocator_allocated_after_bytes", "lower"),
    ("allocator_resident_after_bytes", "lower"),
    ("process_syscr_delta", "lower"),
    ("process_syscw_delta", "lower"),
    ("process_voluntary_ctx_switches_delta", "lower"),
    ("process_nonvoluntary_ctx_switches_delta", "lower"),
    ("network_total_rx_errors_delta", "lower"),
    ("network_total_tx_errors_delta", "lower"),
)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _signature(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(field) for field in SIGNATURE_FIELDS)


def _signature_label(row: dict[str, Any]) -> str:
    return " / ".join(
        str(row.get(field))
        for field in ("database", "backend", "series_label", "thread_count")
        if row.get(field) is not None
    )


def _load_rows(summary_files: list[str], results_dir: str | None) -> tuple[list[Path], list[dict[str, Any]]]:
    paths = resolve_summary_paths(summary_files, results_dir)
    payload = build_report_payload(paths, title="Benchmark Diff Source")
    db_names = [d.get("database", "unknown") for d in payload.get("databases", [])]
    analysis = build_analysis(payload.get("rows", []), db_names)
    return paths, analysis.get("aggregated_rows") or payload.get("rows", [])


def _index_rows(rows: list[dict[str, Any]]) -> dict[tuple[Any, ...], dict[str, Any]]:
    indexed: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        indexed.setdefault(_signature(row), row)
    return indexed


def _metric_delta(record: dict[str, Any], metric: str, direction: str, baseline: dict[str, Any], candidate: dict[str, Any]) -> None:
    before = _coerce_float(baseline.get(metric))
    after = _coerce_float(candidate.get(metric))
    record[f"{metric}_baseline"] = before
    record[f"{metric}_candidate"] = after
    if before is None or after is None:
        record[f"{metric}_delta"] = None
        record[f"{metric}_percent_delta"] = None
        return

    delta = after - before
    record[f"{metric}_delta"] = delta
    record[f"{metric}_percent_delta"] = (delta / before * 100.0) if before else None
    if direction == "higher":
        record[f"{metric}_movement"] = "better" if delta > 0 else "worse" if delta < 0 else "flat"
    elif direction == "lower":
        record[f"{metric}_movement"] = "better" if delta < 0 else "worse" if delta > 0 else "flat"
    else:
        record[f"{metric}_movement"] = "changed" if delta else "flat"


def _row_verdict(record: dict[str, Any]) -> str:
    movements = [
        record.get(f"{metric}_movement")
        for metric, direction in METRICS
        if direction != "neutral" and record.get(f"{metric}_movement") is not None
    ]
    if not movements:
        return "matched-no-comparable-metrics"
    better = movements.count("better")
    worse = movements.count("worse")
    if better and not worse:
        return "improved"
    if worse and not better:
        return "regressed"
    if better or worse:
        return "mixed"
    return "flat"


def build_diff_payload(
    *,
    baseline_summary_files: list[str],
    baseline_results_dir: str | None,
    candidate_summary_files: list[str],
    candidate_results_dir: str | None,
    title: str | None = None,
) -> dict[str, Any]:
    baseline_paths, baseline_rows = _load_rows(baseline_summary_files, baseline_results_dir)
    candidate_paths, candidate_rows = _load_rows(candidate_summary_files, candidate_results_dir)
    baseline_index = _index_rows(baseline_rows)
    candidate_index = _index_rows(candidate_rows)
    all_signatures = sorted(set(baseline_index) | set(candidate_index), key=lambda item: tuple(str(part) for part in item))

    diff_rows: list[dict[str, Any]] = []
    for signature in all_signatures:
        baseline = baseline_index.get(signature)
        candidate = candidate_index.get(signature)
        source = candidate or baseline or {}
        record = {field: source.get(field) for field in SIGNATURE_FIELDS}
        record["signature"] = _signature_label(source)
        if baseline is None:
            record["comparison_status"] = "candidate-only"
            diff_rows.append(record)
            continue
        if candidate is None:
            record["comparison_status"] = "baseline-only"
            diff_rows.append(record)
            continue

        record["comparison_status"] = "matched"
        for metric, direction in METRICS:
            _metric_delta(record, metric, direction, baseline, candidate)
        record["verdict"] = _row_verdict(record)
        diff_rows.append(record)

    matched = [row for row in diff_rows if row.get("comparison_status") == "matched"]
    return {
        "schema_version": 1,
        "generated_at": utc_now(),
        "title": title or "Benchmark Diff Report",
        "baseline_sources": [str(path) for path in baseline_paths],
        "candidate_sources": [str(path) for path in candidate_paths],
        "summary": {
            "baseline_row_count": len(baseline_rows),
            "candidate_row_count": len(candidate_rows),
            "matched_signature_count": len(matched),
            "baseline_only_count": len([row for row in diff_rows if row.get("comparison_status") == "baseline-only"]),
            "candidate_only_count": len([row for row in diff_rows if row.get("comparison_status") == "candidate-only"]),
            "improved_count": len([row for row in matched if row.get("verdict") == "improved"]),
            "regressed_count": len([row for row in matched if row.get("verdict") == "regressed"]),
            "mixed_count": len([row for row in matched if row.get("verdict") == "mixed"]),
            "flat_count": len([row for row in matched if row.get("verdict") == "flat"]),
        },
        "signature_fields": list(SIGNATURE_FIELDS),
        "metrics": [{"name": metric, "direction": direction} for metric, direction in METRICS],
        "rows": diff_rows,
    }


def _fmt_metric(row: dict[str, Any], metric: str, digits: int = 3) -> str:
    before = row.get(f"{metric}_baseline")
    after = row.get(f"{metric}_candidate")
    delta = row.get(f"{metric}_delta")
    percent = row.get(f"{metric}_percent_delta")
    if before is None or after is None:
        return "n/a"
    percent_text = "n/a" if percent is None else f"{float(percent):+.2f}%"
    return f"{float(before):.{digits}f} -> {float(after):.{digits}f} ({float(delta):+.{digits}f}, {percent_text})"


def render_diff_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        f"# {payload.get('title') or 'Benchmark Diff Report'}",
        "",
        "## Summary",
        "",
        f"- **Generated at:** {payload.get('generated_at')}",
        f"- **Matched workload signatures:** {summary.get('matched_signature_count', 0)}",
        f"- **Baseline-only signatures:** {summary.get('baseline_only_count', 0)}",
        f"- **Candidate-only signatures:** {summary.get('candidate_only_count', 0)}",
        f"- **Improved / mixed / regressed / flat:** {summary.get('improved_count', 0)} / {summary.get('mixed_count', 0)} / {summary.get('regressed_count', 0)} / {summary.get('flat_count', 0)}",
        "",
        "Matched rows use the workload signature fields recorded in the JSON payload. Unmatched rows are listed so the comparison cannot silently mix different workloads.",
        "",
        "## Matched Rows",
        "",
        "| Signature | Verdict | Throughput | p99 | p99.9 | RSS Peak | CPU Avg |",
        "|-----------|---------|------------|-----|-------|----------|---------|",
    ]
    matched = [row for row in payload.get("rows", []) if row.get("comparison_status") == "matched"]
    if not matched:
        lines.append("| n/a | no matched signatures | n/a | n/a | n/a | n/a | n/a |")
    for row in matched:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("signature") or "n/a"),
                    str(row.get("verdict") or "n/a"),
                    _fmt_metric(row, "throughput_ops_sec", 0),
                    _fmt_metric(row, "p99_latency_ms", 3),
                    _fmt_metric(row, "p99_9_latency_ms", 3),
                    _fmt_metric(row, "process_rss_peak_bytes", 0),
                    _fmt_metric(row, "process_cpu_utilization_avg_pct", 2),
                ]
            )
            + " |"
        )
    lines.append("")

    unmatched = [row for row in payload.get("rows", []) if row.get("comparison_status") != "matched"]
    if unmatched:
        lines.extend(["## Unmatched Rows", "", "| Signature | Status |", "|-----------|--------|"])
        for row in unmatched:
            lines.append(f"| {row.get('signature') or 'n/a'} | {row.get('comparison_status')} |")
        lines.append("")

    lines.extend(
        [
            "## Sources",
            "",
            "Baseline:",
            "",
            *[f"- `{path}`" for path in payload.get("baseline_sources", [])],
            "",
            "Candidate:",
            "",
            *[f"- `{path}`" for path in payload.get("candidate_sources", [])],
            "",
        ]
    )
    return "\n".join(lines)


def write_diff_artifacts(payload: dict[str, Any], output_root: str | None) -> Path:
    layout = build_layout(output_root)
    stamp = timestamp_slug()
    stem = f"{stamp}_diff-report"
    json_path = layout.reports_dir / f"{stem}.json"
    csv_path = layout.reports_dir / f"{stem}.csv"
    markdown_path = layout.reports_dir / f"{stem}.md"
    latest_dir = layout.reports_dir / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        **payload,
        "artifacts": {
            "json": str(json_path),
            "csv": str(csv_path),
            "markdown": str(markdown_path),
            "latest": {
                "json": str(latest_dir / "diff-report.json"),
                "csv": str(latest_dir / "diff-report.csv"),
                "markdown": str(latest_dir / "diff-report.md"),
            },
        },
    }
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _write_diff_csv(csv_path, payload.get("rows", []))
    markdown_path.write_text(render_diff_markdown(payload) + "\n", encoding="utf-8")
    shutil.copyfile(json_path, latest_dir / "diff-report.json")
    shutil.copyfile(csv_path, latest_dir / "diff-report.csv")
    shutil.copyfile(markdown_path, latest_dir / "diff-report.md")
    return markdown_path


def _write_diff_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
