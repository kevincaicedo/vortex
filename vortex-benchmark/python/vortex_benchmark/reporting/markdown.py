"""Rich Markdown report renderer with cross-database comparisons and analysis."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def _fmt_float(value: Any, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{digits}f}"


def _fmt_ops(value: Any) -> str:
    """Format ops/sec with comma separators."""
    if value is None:
        return "n/a"
    return f"{float(value):,.0f}"


def _fmt_ops_lat(metrics: dict[str, Any]) -> str:
    """Format 'ops/s (avg lat)' combined cell."""
    ops = metrics.get("throughput_ops_sec")
    lat = metrics.get("average_latency_ms")
    if ops is None:
        return "n/a"
    ops_str = f"{float(ops):,.0f}"
    if lat is not None:
        return f"{ops_str} ({float(lat):.2f} ms)"
    # Fallback: show p50 if avg not available
    p50 = metrics.get("p50_latency_ms")
    if p50 is not None:
        return f"{ops_str} (p50: {float(p50):.2f} ms)"
    return ops_str


def _fmt_bytes(value: Any) -> str:
    if value is None:
        return "n/a"
    size = float(value)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    index = 0
    while size >= 1024.0 and index < len(units) - 1:
        size /= 1024.0
        index += 1
    return f"{size:.2f} {units[index]}"


def _relative(markdown_path: Path, asset_path: Path) -> str:
    return asset_path.relative_to(markdown_path.parent).as_posix()


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------


def render_markdown_report(
    report_payload: dict[str, Any],
    markdown_path: Path,
    chart_paths: dict[str, Path],
) -> str:
    host = report_payload.get("host_metadata") or {}
    databases = report_payload.get("databases") or []
    workloads = report_payload.get("workloads") or []
    rows = report_payload.get("rows") or []
    source_runs = report_payload.get("source_runs") or []
    aof_overhead = report_payload.get("aof_overhead") or []
    eviction_rows = report_payload.get("eviction_rows") or []
    analysis = report_payload.get("analysis") or {}

    lines: list[str] = []

    _section_header(lines, report_payload, source_runs, rows)
    _section_environment(lines, host, source_runs)
    _section_database_targets(lines, databases)
    _section_workloads(lines, workloads)
    _section_cross_db_comparison(lines, analysis, databases)
    _section_performance_analysis(lines, analysis)
    _section_detailed_results(lines, rows, aof_overhead, eviction_rows)
    _section_key_insights(lines, analysis)
    _section_visualizations(lines, markdown_path, chart_paths)
    _section_artifacts(lines, report_payload)

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Section renderers
# ---------------------------------------------------------------------------


def _section_header(
    lines: list[str],
    payload: dict[str, Any],
    source_runs: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> None:
    lines.append(f"# {payload.get('title', 'Vortex Benchmark Report')}")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append(f"- **Generated at:** {payload.get('generated_at', 'n/a')}")
    lines.append(f"- **Source runs:** {len(source_runs)}")
    lines.append(f"- **Normalized rows:** {len(rows)}")
    dbs = sorted({r.get("database", "unknown") for r in rows})
    backends = sorted({r.get("backend", "unknown") for r in rows})
    lines.append(f"- **Databases:** {', '.join(dbs) or 'n/a'}")
    lines.append(f"- **Backends:** {', '.join(backends) or 'n/a'}")
    lines.append("")


def _section_environment(
    lines: list[str],
    host: dict[str, Any],
    source_runs: list[dict[str, Any]],
) -> None:
    lines.append("## Environment")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("|-------|-------|")
    lines.append(
        f"| Benchmark date/time | "
        f"{', '.join(r.get('generated_at', 'n/a') for r in source_runs) or 'n/a'} |"
    )
    lines.append(f"| Operating system | {host.get('os') or 'n/a'} |")
    lines.append(f"| Architecture | {host.get('architecture') or 'n/a'} |")
    lines.append(f"| CPU model | {host.get('cpu_model') or 'n/a'} |")
    lines.append(f"| Logical CPUs | {host.get('logical_cpus') or 'n/a'} |")
    lines.append(
        f"| Total memory | {_fmt_bytes(host.get('total_memory_bytes'))} |"
    )
    lines.append("")


def _section_database_targets(
    lines: list[str], databases: list[dict[str, Any]]
) -> None:
    lines.append("## Database Targets")
    lines.append("")
    lines.append(
        "| Database | Mode | Version | Bind "
        "| AOF | Fsync | Maxmemory | Eviction |"
    )
    lines.append(
        "|----------|------|---------|------"
        "|-----|-------|-----------|----------|"
    )
    for db in databases:
        rt = db.get("runtime_config") or {}
        lines.append(
            f"| {db.get('database') or 'n/a'} "
            f"| {db.get('mode') or 'n/a'} "
            f"| {db.get('version') or 'n/a'} "
            f"| {db.get('bind') or 'n/a'} "
            f"| {rt.get('aof_enabled', 'n/a')} "
            f"| {rt.get('aof_fsync', 'n/a')} "
            f"| {rt.get('maxmemory', 'n/a')} "
            f"| {rt.get('eviction_policy', 'n/a')} |"
        )
    if not databases:
        lines.append("| n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |")
    lines.append("")


def _section_workloads(
    lines: list[str], workloads: list[dict[str, Any]]
) -> None:
    lines.append("## Workload Details")
    lines.append("")
    lines.append(
        "| Workload | Distribution | Read Ratio | Write Ratio "
        "| Multi-Key | Transactional | Hot-Key |"
    )
    lines.append(
        "|----------|--------------|------------|-------------"
        "|-----------|---------------|---------|"
    )
    for wk in workloads:
        lines.append(
            f"| {wk.get('name') or 'n/a'} "
            f"| {wk.get('distribution') or 'n/a'} "
            f"| {wk.get('read_ratio') or 'n/a'} "
            f"| {wk.get('write_ratio') or 'n/a'} "
            f"| {wk.get('multi_key')!s} "
            f"| {wk.get('transactional')!s} "
            f"| {wk.get('hot_key')!s} |"
        )
    if not workloads:
        lines.append("| n/a | n/a | n/a | n/a | n/a | n/a | n/a |")
    lines.append("")


# ---------------------------------------------------------------------------
# Cross-Database Performance Comparison
# ---------------------------------------------------------------------------


def _section_cross_db_comparison(
    lines: list[str],
    analysis: dict[str, Any],
    databases: list[dict[str, Any]],
) -> None:
    comparisons = analysis.get("backend_comparisons") or {}
    if not comparisons:
        return

    db_names = [d.get("database", "unknown") for d in databases]

    lines.append("---")
    lines.append("")
    lines.append("## Cross-Database Performance Comparison")
    lines.append("")

    for backend, comp in sorted(comparisons.items()):
        if comp.get("type") == "command":
            _render_command_comparison(lines, backend, comp, db_names)
        elif comp.get("type") == "thread_workload":
            _render_thread_workload_comparison(
                lines, backend, comp, db_names
            )


def _render_command_comparison(
    lines: list[str],
    backend: str,
    comp: dict[str, Any],
    db_names: list[str],
) -> None:
    lines.append(f"### {backend}")
    lines.append("")

    # --- combined ops/s + latency table ---
    header = ["Command"]
    for db in db_names:
        header.append(f"{db} ops/s (avg lat)")
    lines.append("| " + " | ".join(header) + " |")

    sep = ["---------"]
    for db in db_names:
        sep.append("-" * max(20, len(db) + 17) + ":")
    lines.append("|" + "|".join(sep) + "|")

    for row in comp.get("rows", []):
        cells = [f"**{row.get('series', 'n/a')}**"]
        for db in db_names:
            cells.append(_fmt_ops_lat(row.get(db, {})))
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")

    # --- latency detail table ---
    if len(db_names) >= 2:
        lines.append(f"**{backend} — Latency Detail**")
        lines.append("")
        header2 = ["Command"]
        for db in db_names:
            header2.extend([f"{db} P50", f"{db} P99", f"{db} P99.9"])
        lines.append("| " + " | ".join(header2) + " |")
        sep2 = ["---------"]
        for _ in db_names:
            sep2.extend(["------:", "------:", "--------:"])
        lines.append("|" + "|".join(sep2) + "|")

        for row in comp.get("rows", []):
            cells2 = [f"**{row.get('series', 'n/a')}**"]
            for db in db_names:
                m = row.get(db, {})
                cells2.append(_fmt_float(m.get("p50_latency_ms"), 2))
                cells2.append(_fmt_float(m.get("p99_latency_ms"), 2))
                cells2.append(_fmt_float(m.get("p99_9_latency_ms"), 2))
            lines.append("| " + " | ".join(cells2) + " |")
        lines.append("")


def _render_thread_workload_comparison(
    lines: list[str],
    backend: str,
    comp: dict[str, Any],
    db_names: list[str],
) -> None:
    for workload, wk_data in sorted(
        (comp.get("workloads") or {}).items()
    ):
        lines.append(f"### {backend}: {workload}")
        lines.append("")

        # --- throughput + latency table ---
        header = ["Threads"]
        for db in db_names:
            header.append(f"{db} ops/s (avg lat)")
        lines.append("| " + " | ".join(header) + " |")

        sep = ["--------:"]
        for db in db_names:
            sep.append("-" * max(20, len(db) + 17) + ":")
        lines.append("|" + "|".join(sep) + "|")

        for row in wk_data.get("rows", []):
            threads = row.get("threads")
            cells = [
                f"**{threads}**" if threads is not None else "n/a"
            ]
            for db in db_names:
                cells.append(_fmt_ops_lat(row.get(db, {})))
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")

        # --- latency detail table ---
        if len(db_names) >= 2:
            lines.append(
                f"**{backend}: {workload} — Latency Detail**"
            )
            lines.append("")
            header2 = ["Threads"]
            for db in db_names:
                header2.extend(
                    [f"{db} P50", f"{db} P99", f"{db} P99.9"]
                )
            lines.append("| " + " | ".join(header2) + " |")
            sep2 = ["--------:"]
            for _ in db_names:
                sep2.extend(["------:", "------:", "--------:"])
            lines.append("|" + "|".join(sep2) + "|")

            for row in wk_data.get("rows", []):
                threads = row.get("threads")
                cells2 = [
                    f"**{threads}**"
                    if threads is not None
                    else "n/a"
                ]
                for db in db_names:
                    m = row.get(db, {})
                    cells2.append(
                        _fmt_float(m.get("p50_latency_ms"), 2)
                    )
                    cells2.append(
                        _fmt_float(m.get("p99_latency_ms"), 2)
                    )
                    cells2.append(
                        _fmt_float(m.get("p99_9_latency_ms"), 2)
                    )
                lines.append("| " + " | ".join(cells2) + " |")
            lines.append("")


# ---------------------------------------------------------------------------
# Performance Analysis Summary
# ---------------------------------------------------------------------------


def _section_performance_analysis(
    lines: list[str], analysis: dict[str, Any]
) -> None:
    scalability = analysis.get("scalability") or []
    breakdown = analysis.get("latency_breakdown") or []
    winners = analysis.get("winners") or {}
    if not scalability and not breakdown and not winners:
        return

    lines.append("---")
    lines.append("")
    lines.append("## Performance Analysis Summary")
    lines.append("")

    # Scalability
    if scalability:
        lines.append("### Scalability")
        lines.append("")
        lines.append(
            "| Database | Backend | Workload "
            "| Min Threads | Min ops/s "
            "| Max Threads | Max ops/s | Uplift |"
        )
        lines.append(
            "|----------|---------|----------"
            "|------------:|----------:"
            "|------------:|----------:|-------:|"
        )
        for s in scalability:
            uplift = (
                f"{s['uplift_pct']:+.0f}%"
                if s.get("uplift_pct") is not None
                else "n/a"
            )
            lines.append(
                f"| {s.get('database') or 'n/a'} "
                f"| {s.get('backend') or 'n/a'} "
                f"| {s.get('workload') or 'n/a'} "
                f"| {s.get('min_threads') or 'n/a'} "
                f"| {_fmt_ops(s.get('min_ops'))} "
                f"| {s.get('max_threads') or 'n/a'} "
                f"| {_fmt_ops(s.get('max_ops'))} "
                f"| {uplift} |"
            )
        lines.append("")

    # Latency Breakdown
    if breakdown:
        lines.append("### Latency Breakdown (Peak Thread Count)")
        lines.append("")
        lines.append(
            "| Database | Backend | Series | Threads "
            "| Throughput ops/s | Avg ms | P50 ms "
            "| P99 ms | P99.9 ms |"
        )
        lines.append(
            "|----------|---------|--------|--------:"
            "|-----------------:|-------:|-------:"
            "|-------:|---------:|"
        )
        for r in breakdown:
            lines.append(
                f"| {r.get('database') or 'n/a'} "
                f"| {r.get('backend') or 'n/a'} "
                f"| {r.get('series_label') or 'n/a'} "
                f"| {r.get('thread_count') or 'n/a'} "
                f"| {_fmt_ops(r.get('throughput_ops_sec'))} "
                f"| {_fmt_float(r.get('average_latency_ms'), 2)} "
                f"| {_fmt_float(r.get('p50_latency_ms'), 2)} "
                f"| {_fmt_float(r.get('p99_latency_ms'), 2)} "
                f"| {_fmt_float(r.get('p99_9_latency_ms'), 2)} |"
            )
        lines.append("")

    # Winner by Category
    if winners:
        _render_winners(lines, winners)


def _render_winners(
    lines: list[str], winners: dict[str, dict[str, Any]]
) -> None:
    lines.append("### Winner by Category")
    lines.append("")

    friendly: dict[str, str] = {
        "peak_throughput": "Peak Throughput",
        "lowest_avg_latency": "Lowest Avg Latency",
        "lowest_p99_latency": "Lowest P99 Latency",
        "lowest_p99_9_latency": "Lowest P99.9 Latency",
        "best_scalability": "Best Scalability",
    }

    for key, win in winners.items():
        label = friendly.get(key, key.replace("_", " ").title())
        db = win.get("database", "n/a")
        value = win.get("value")
        context = win.get("context", "")

        if "latency" in key.lower():
            val_str = (
                f"{float(value):.2f} ms"
                if value is not None
                else "n/a"
            )
        elif "scalability" in key.lower():
            val_str = (
                f"{float(value):+.0f}%"
                if value is not None
                else "n/a"
            )
        else:
            val_str = _fmt_ops(value)

        lines.append(f"- **{label}:** {db} — {val_str} ({context})")
    lines.append("")


# ---------------------------------------------------------------------------
# Detailed Results
# ---------------------------------------------------------------------------


def _section_detailed_results(
    lines: list[str],
    rows: list[dict[str, Any]],
    aof_overhead: list[dict[str, Any]],
    eviction_rows: list[dict[str, Any]],
) -> None:
    lines.append("---")
    lines.append("")
    lines.append("## Detailed Results")
    lines.append("")

    # Full latency & throughput
    lines.append("### Latency And Throughput")
    lines.append("")
    lines.append(
        "| Database | Backend | Series | Threads "
        "| Throughput ops/s | Avg ms | P50 ms "
        "| P99 ms | P99.9 ms | P99.999 ms |"
    )
    lines.append(
        "|----------|---------|--------|--------:"
        "|-----------------:|-------:|-------:"
        "|-------:|---------:|-----------:|"
    )
    for row in rows:
        lines.append(
            f"| {row.get('database') or 'n/a'} "
            f"| {row.get('backend') or 'n/a'} "
            f"| {row.get('series_label') or 'n/a'} "
            f"| {row.get('thread_count') or 'n/a'} "
            f"| {_fmt_ops(row.get('throughput_ops_sec'))} "
            f"| {_fmt_float(row.get('average_latency_ms'), 2)} "
            f"| {_fmt_float(row.get('p50_latency_ms'))} "
            f"| {_fmt_float(row.get('p99_latency_ms'))} "
            f"| {_fmt_float(row.get('p99_9_latency_ms'))} "
            f"| {_fmt_float(row.get('p99_999_latency_ms'))} |"
        )
    if not rows:
        lines.append(
            "| n/a | n/a | n/a | n/a "
            "| n/a | n/a | n/a | n/a | n/a | n/a |"
        )
    lines.append("")

    # Memory, Cache, And Eviction
    lines.append("### Memory, Cache, And Eviction")
    lines.append("")
    lines.append(
        "| Database | Backend | Series | Maxmemory | Eviction "
        "| Memory Delta | RSS Delta | Hit Rate | Evicted Keys |"
    )
    lines.append(
        "|----------|---------|--------|-----------|----------"
        "|-------------:|----------:|---------:|-------------:|"
    )
    for row in rows:
        lines.append(
            f"| {row.get('database') or 'n/a'} "
            f"| {row.get('backend') or 'n/a'} "
            f"| {row.get('series_label') or 'n/a'} "
            f"| {row.get('configured_maxmemory') or 'n/a'} "
            f"| {row.get('configured_eviction_policy') or 'n/a'} "
            f"| {_fmt_bytes(row.get('used_memory_delta_bytes'))} "
            f"| {_fmt_bytes(row.get('process_memory_delta_bytes'))} "
            f"| {_fmt_float(row.get('cache_hit_rate'))} "
            f"| {_fmt_float(row.get('evicted_keys_delta'), 0)} |"
        )
    if not rows:
        lines.append(
            "| n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |"
        )
    lines.append("")

    # AOF Overhead
    lines.append("### AOF Overhead")
    lines.append("")
    if aof_overhead:
        lines.append(
            "| Database | Backend | Series | Threads "
            "| Throughput Overhead % | P99 Overhead % |"
        )
        lines.append(
            "|----------|---------|--------|---------|"
            "---------------------:|---------------:|"
        )
        for row in aof_overhead:
            lines.append(
                f"| {row.get('database') or 'n/a'} "
                f"| {row.get('backend') or 'n/a'} "
                f"| {row.get('series_label') or 'n/a'} "
                f"| {row.get('thread_count') or 'n/a'} "
                f"| {_fmt_float(row.get('throughput_overhead_pct'), 2)} "
                f"| {_fmt_float(row.get('p99_overhead_pct'), 2)} |"
            )
    else:
        lines.append(
            "No comparable baseline and AOF-enabled runs were present "
            "in the source summaries."
        )
    lines.append("")

    # Eviction Observations
    lines.append("### Eviction Observations")
    lines.append("")
    if eviction_rows:
        lines.append(
            "| Database | Backend | Series | Threads "
            "| Evicted Keys | Throughput ops/s |"
        )
        lines.append(
            "|----------|---------|--------|---------|"
            "-------------:|-------------------:|"
        )
        for row in eviction_rows:
            lines.append(
                f"| {row.get('database') or 'n/a'} "
                f"| {row.get('backend') or 'n/a'} "
                f"| {row.get('series_label') or 'n/a'} "
                f"| {row.get('thread_count') or 'n/a'} "
                f"| {_fmt_float(row.get('evicted_keys_delta'), 0)} "
                f"| {_fmt_ops(row.get('throughput_ops_sec'))} |"
            )
    else:
        lines.append(
            "No eviction events were observed in the source summaries."
        )
    lines.append("")


# ---------------------------------------------------------------------------
# Key Insights
# ---------------------------------------------------------------------------


def _section_key_insights(
    lines: list[str], analysis: dict[str, Any]
) -> None:
    insights = analysis.get("key_insights") or []
    if not insights:
        return

    lines.append("---")
    lines.append("")
    lines.append("## Key Insights")
    lines.append("")
    for insight in insights:
        lines.append(f"- {insight}")
    lines.append("")


# ---------------------------------------------------------------------------
# Visualizations
# ---------------------------------------------------------------------------


def _section_visualizations(
    lines: list[str],
    markdown_path: Path,
    chart_paths: dict[str, Path],
) -> None:
    lines.append("---")
    lines.append("")
    lines.append("## Visualizations")
    lines.append("")

    # Ordered chart keys with friendly titles
    chart_order = [
        ("throughput_comparison", "Throughput Comparison"),
        ("latency_comparison", "Latency Comparison (Avg + P99)"),
        ("heatmap_dashboard", "Performance Heatmap Dashboard"),
        ("radar_profile", "Radar Profile"),
        ("latency_heatmap", "Latency Heatmap"),
        ("scaling_sweep", "Scaling Sweep"),
    ]
    ordered_keys = {k for k, _ in chart_order}

    for key, title in chart_order:
        if key in chart_paths:
            lines.append(f"### {title}")
            lines.append("")
            lines.append(
                f"![{title}]"
                f"({_relative(markdown_path, chart_paths[key])})"
            )
            lines.append("")

    # Any extra charts not in the predefined list
    for key in sorted(chart_paths):
        if key not in ordered_keys:
            title = key.replace("_", " ").title()
            lines.append(f"### {title}")
            lines.append("")
            lines.append(
                f"![{title}]"
                f"({_relative(markdown_path, chart_paths[key])})"
            )
            lines.append("")


# ---------------------------------------------------------------------------
# Artifact Index
# ---------------------------------------------------------------------------


def _section_artifacts(
    lines: list[str], payload: dict[str, Any]
) -> None:
    lines.append("---")
    lines.append("")
    lines.append("## Artifact Index")
    lines.append("")
    artifacts = payload.get("artifacts") or {}
    lines.append(
        f"- JSON: {Path(artifacts.get('json', '')).name or 'n/a'}"
    )
    lines.append(
        f"- CSV: {Path(artifacts.get('csv', '')).name or 'n/a'}"
    )
    lines.append(
        f"- Markdown: {Path(artifacts.get('markdown', '')).name or 'n/a'}"
    )
    latest = artifacts.get("latest") or {}
    if latest:
        lines.append(
            f"- Latest JSON: "
            f"{Path(latest.get('json', '')).as_posix() or 'n/a'}"
        )
        lines.append(
            f"- Latest CSV: "
            f"{Path(latest.get('csv', '')).as_posix() or 'n/a'}"
        )
        lines.append(
            f"- Latest Markdown: "
            f"{Path(latest.get('markdown', '')).as_posix() or 'n/a'}"
        )
    if artifacts.get("index"):
        lines.append(
            f"- History Index: "
            f"{Path(artifacts.get('index', '')).as_posix()}"
        )