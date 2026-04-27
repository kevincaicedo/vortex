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
    validity = report_payload.get("validity") or {}
    databases = report_payload.get("databases") or []
    workloads = report_payload.get("workloads") or []
    benchmark = report_payload.get("benchmark") or {}
    diagnostics = report_payload.get("diagnostics") or {}
    rows = benchmark.get("rows") or report_payload.get("rows") or []
    source_runs = report_payload.get("source_runs") or []
    aof_overhead = diagnostics.get("aof_overhead") or report_payload.get("aof_overhead") or []
    eviction_rows = diagnostics.get("eviction_rows") or report_payload.get("eviction_rows") or []
    memory_rows = diagnostics.get("memory_rows") or []
    aof_rows = diagnostics.get("aof_rows") or []
    diagnostic_summary = diagnostics.get("summary") or {}
    analysis = benchmark.get("analysis") or report_payload.get("analysis") or {}

    lines: list[str] = []

    _section_header(lines, report_payload, source_runs, rows)
    _section_environment(lines, host, source_runs, validity)
    _section_validity_warnings(lines, host, validity)
    _section_database_targets(lines, databases)
    _section_workloads(lines, workloads)
    _section_cross_db_comparison(lines, analysis, databases)
    _section_workload_rankings(lines, analysis, databases)
    _section_database_scorecard(lines, analysis)
    _section_performance_analysis(lines, analysis)
    _section_diagnostics(
        lines,
        diagnostic_summary,
        memory_rows,
        aof_rows,
        aof_overhead,
        eviction_rows,
    )
    timeseries_summaries = diagnostics.get("timeseries_summaries") or []
    _section_host_telemetry_timeseries(lines, timeseries_summaries)
    _section_detailed_results(lines, rows)
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
    lines.append(f"- **Benchmark series:** {len(rows)}")
    dbs = sorted({r.get("database", "unknown") for r in rows})
    backends = sorted({r.get("backend", "unknown") for r in rows})
    lines.append(f"- **Databases:** {', '.join(dbs) or 'n/a'}")
    lines.append(f"- **Backends:** {', '.join(backends) or 'n/a'}")
    lines.append("")


def _section_environment(
    lines: list[str],
    host: dict[str, Any],
    source_runs: list[dict[str, Any]],
    validity: dict[str, Any],
) -> None:
    host_validity = validity.get("host") or {}
    git_validity = validity.get("git") or {}
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
    lines.append(f"| Evidence tier | {validity.get('evidence_tier') or 'n/a'} |")
    lines.append(f"| Requested repeat count | {validity.get('requested_repeat_count') or 'n/a'} |")
    lines.append(
        f"| Aggregates multiple source runs | {validity.get('report_aggregates_multiple_source_runs', 'n/a')} |"
    )
    lines.append(
        f"| Aggregates multiple replicates | {validity.get('report_aggregates_multiple_replicates', 'n/a')} |"
    )
    lines.append(f"| CPU governor | {host_validity.get('cpu_governor') or 'n/a'} |")
    lines.append(
        f"| Energy preference | {host_validity.get('energy_performance_preference') or 'n/a'} |"
    )
    lines.append(f"| Power profile | {host_validity.get('power_profile') or 'n/a'} |")
    lines.append(f"| Thermal degraded | {host_validity.get('thermal_degraded')!s} |")
    lines.append(
        f"| perf_event_paranoid | {host_validity.get('perf_event_paranoid') or 'n/a'} |"
    )
    lines.append(f"| PMU available | {host_validity.get('pmu_available')!s} |")
    lines.append(f"| Git revision | {git_validity.get('revision') or 'n/a'} |")
    lines.append(f"| Git dirty | {git_validity.get('dirty')!s} |")
    lines.append("")


def _section_validity_warnings(
    lines: list[str],
    host: dict[str, Any],
    validity: dict[str, Any],
) -> None:
    """Emit visible warnings for conditions that may invalidate results."""
    host_validity = validity.get("host") or {}
    warnings: list[str] = []

    if host_validity.get("thermal_degraded") is True:
        warnings.append(
            "⚠️ **Thermal degraded** — Host is thermally throttled. "
            "Results may understate peak performance."
        )
    governor = host_validity.get("cpu_governor")
    if governor and governor not in {"performance", None}:
        warnings.append(
            f"⚠️ **CPU governor is '{governor}'** — Frequency scaling may "
            f"add variance. Set to 'performance' for citation-grade runs."
        )
    if host_validity.get("pmu_available") is False:
        warnings.append(
            "⚠️ **PMU unavailable** — perf_event_paranoid blocks hardware "
            "counters. IPC and cache-miss data will not be available."
        )
    if validity.get("requested_repeat_count") in {1, None}:
        warnings.append(
            "ℹ️ **Single replicate** — No repeated runs. Spread and outlier "
            "statistics are unavailable. Use `repeat: N` for engineering-grade evidence."
        )
    git = validity.get("git") or {}
    if git.get("dirty") is True:
        warnings.append(
            "ℹ️ **Git tree is dirty** — Results may not be reproducible "
            "from committed source."
        )

    if not warnings:
        return

    lines.append("## Run Validity Warnings")
    lines.append("")
    for warning in warnings:
        lines.append(f"- {warning}")
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
    advisory = analysis.get("advisory_backend_comparisons") or {}
    is_advisory = False
    if not comparisons and advisory:
        comparisons = advisory
        is_advisory = True
    if not comparisons:
        return

    db_names = [d.get("database", "unknown") for d in databases]

    lines.append("---")
    lines.append("")
    lines.append("## Cross-Database Performance Comparison")
    lines.append("")
    if is_advisory:
        lines.append(
            "> ⚠️ **Advisory comparison** — Runtime configurations differ between "
            "databases (see Invalid Comparisons below). Use these tables for "
            "directional guidance only, not citation-grade claims."
        )
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
# Workload Rankings (databases as columns)
# ---------------------------------------------------------------------------


def _section_workload_rankings(
    lines: list[str],
    analysis: dict[str, Any],
    databases: list[dict[str, Any]],
) -> None:
    rankings = analysis.get("workload_rankings") or {}
    if not rankings:
        return

    db_names = [d.get("database", "unknown") for d in databases]
    has_advisory = not analysis.get("backend_comparisons")

    lines.append("---")
    lines.append("")
    lines.append("## Workload Ranking Matrix")
    lines.append("")
    if has_advisory:
        lines.append(
            "> ⚠️ **Advisory rankings** — Runtime configurations differ "
            "between databases. Rankings are for directional guidance only."
        )
        lines.append("")

    for backend, ranking_data in sorted(rankings.items()):
        if ranking_data.get("type") == "command":
            _render_command_ranking_table(lines, backend, ranking_data, db_names)
        elif ranking_data.get("type") == "thread_workload":
            _render_thread_ranking_tables(lines, backend, ranking_data, db_names)


def _render_command_ranking_table(
    lines: list[str],
    backend: str,
    ranking_data: dict[str, Any],
    db_names: list[str],
) -> None:
    rows = ranking_data.get("rows") or []
    if not rows:
        return

    lines.append(f"### {backend}: Throughput Ranking")
    lines.append("")

    header = ["Command"]
    for db in db_names:
        header.extend([f"{db} ops/s", f"{db} Rank"])
    header.extend(["Δ%", "Winner"])
    lines.append("| " + " | ".join(header) + " |")

    sep = ["---------"]
    for _ in db_names:
        sep.extend(["----------:", "-----:"])
    sep.extend(["----:", "--------"])
    lines.append("|" + "|".join(sep) + "|")

    for row in rows:
        cells = [f"**{row.get('series', 'n/a')}**"]
        ranks = row.get("ranks") or {}
        deltas = row.get("delta_pct") or {}
        for db in db_names:
            metrics = row.get(db, {})
            ops = metrics.get("throughput_ops_sec")
            cells.append(_fmt_ops(ops))
            rank = ranks.get(db)
            cells.append(f"#{rank}" if rank is not None else "n/a")
        # Show delta for the non-leader (the biggest gap)
        non_leader_deltas = [
            v for db, v in deltas.items()
            if v is not None and v != 0.0
        ]
        if non_leader_deltas:
            delta_val = max(non_leader_deltas, key=abs)
            cells.append(f"{delta_val:+.1f}%")
        else:
            cells.append("—")
        winner = row.get("winner")
        cells.append(f"**{winner}**" if winner else "—")
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")


def _render_thread_ranking_tables(
    lines: list[str],
    backend: str,
    ranking_data: dict[str, Any],
    db_names: list[str],
) -> None:
    workloads = ranking_data.get("workloads") or {}
    for workload, wk_data in sorted(workloads.items()):
        wk_rows = wk_data.get("rows") or []
        if not wk_rows:
            continue

        lines.append(f"### {backend}: {workload}")
        lines.append("")

        header = ["Threads"]
        for db in db_names:
            header.extend([f"{db} ops/s", f"{db} Rank"])
        header.extend(["Δ%", "Winner"])
        lines.append("| " + " | ".join(header) + " |")

        sep = ["--------:"]
        for _ in db_names:
            sep.extend(["----------:", "-----:"])
        sep.extend(["----:", "--------"])
        lines.append("|" + "|".join(sep) + "|")

        for row in wk_rows:
            threads = row.get("threads")
            cells = [f"**{threads}**" if threads is not None else "n/a"]
            ranks = row.get("ranks") or {}
            deltas = row.get("delta_pct") or {}
            for db in db_names:
                metrics = row.get(db, {})
                ops = metrics.get("throughput_ops_sec")
                cells.append(_fmt_ops(ops))
                rank = ranks.get(db)
                cells.append(f"#{rank}" if rank is not None else "n/a")
            non_leader_deltas = [
                v for db, v in deltas.items()
                if v is not None and v != 0.0
            ]
            if non_leader_deltas:
                delta_val = max(non_leader_deltas, key=abs)
                cells.append(f"{delta_val:+.1f}%")
            else:
                cells.append("—")
            winner = row.get("winner")
            cells.append(f"**{winner}**" if winner else "—")
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")


# ---------------------------------------------------------------------------
# Database Scorecard
# ---------------------------------------------------------------------------


def _section_database_scorecard(
    lines: list[str], analysis: dict[str, Any]
) -> None:
    scorecard = analysis.get("database_scorecard") or []
    if not scorecard:
        return

    has_advisory = not analysis.get("backend_comparisons")

    lines.append("### Overall Database Scorecard")
    lines.append("")
    if has_advisory:
        lines.append(
            "> ⚠️ **Advisory scorecard** — Computed from advisory comparisons. "
            "Use for directional guidance only."
        )
        lines.append("")

    lines.append(
        "| Database | Workload Wins | Losses | Avg Rank | "
        "Best Throughput | Best P99 ms | Scenarios |"
    )
    lines.append(
        "|----------|-------------:|-------:|---------:|"
        "---------------:|-----------:|----------:|"
    )
    for row in scorecard:
        best_tp = row.get("best_throughput")
        best_p99 = row.get("best_p99")
        lines.append(
            f"| **{row.get('database', 'n/a')}** "
            f"| {row.get('wins', 0)} "
            f"| {row.get('losses', 0)} "
            f"| {_fmt_float(row.get('avg_rank'), 2)} "
            f"| {_fmt_ops(best_tp)} "
            f"| {_fmt_float(best_p99, 2)} "
            f"| {row.get('scenario_count', 0)} |"
        )
    lines.append("")


# ---------------------------------------------------------------------------
# Performance Analysis Summary
# ---------------------------------------------------------------------------


def _section_performance_analysis(
    lines: list[str], analysis: dict[str, Any]
) -> None:
    scalability = analysis.get("scalability") or []
    breakdown = analysis.get("latency_breakdown") or []
    series_statistics = analysis.get("series_statistics") or []
    comparison_validity = analysis.get("comparison_validity") or []
    winners = analysis.get("winners") or {}
    if not scalability and not breakdown and not series_statistics and not comparison_validity and not winners:
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

    if series_statistics:
        lines.append("### Replicate Variance")
        lines.append("")
        lines.append(
            "| Database | Backend | Series | Threads | Reps | Throughput Median | Tp Spread % | Tp CV % | Throughput Outliers | P99 Median ms | P99 Spread % | P99 CV % | P99 Outliers |"
        )
        lines.append(
            "|----------|---------|--------|--------:|-----:|------------------:|------------:|--------:|-------------------:|--------------:|-------------:|---------:|-------------:|"
        )
        for row in series_statistics:
            lines.append(
                f"| {row.get('database') or 'n/a'} "
                f"| {row.get('backend') or 'n/a'} "
                f"| {row.get('series_label') or 'n/a'} "
                f"| {row.get('thread_count') or 'n/a'} "
                f"| {row.get('replicate_count') or 0} "
                f"| {_fmt_ops(row.get('throughput_median'))} "
                f"| {_fmt_float(row.get('throughput_spread_pct'), 2)} "
                f"| {_fmt_float(row.get('throughput_cv_pct'), 2)} "
                f"| {row.get('throughput_outlier_replicates') or 0} "
                f"| {_fmt_float(row.get('p99_latency_median'), 2)} "
                f"| {_fmt_float(row.get('p99_latency_spread_pct'), 2)} "
                f"| {_fmt_float(row.get('p99_latency_cv_pct'), 2)} "
                f"| {row.get('p99_latency_outlier_replicates') or 0} |"
            )
        lines.append("")

    if comparison_validity:
        lines.append("### Invalid Comparisons")
        lines.append("")
        lines.append(
            "| Backend | Series | Threads | Reason | Signatures |"
        )
        lines.append(
            "|---------|--------|--------:|--------|------------|"
        )
        for row in comparison_validity:
            signatures = "; ".join(
                ", ".join(
                    filter(
                        None,
                        [
                            f"dbs={','.join(signature.get('databases') or [])}",
                            f"svc_threads={signature.get('configured_service_threads')}",
                            f"aof={signature.get('configured_aof_enabled')}",
                            f"fsync={signature.get('configured_aof_fsync')}",
                            f"maxmemory={signature.get('configured_maxmemory')}",
                            f"eviction={signature.get('configured_eviction_policy')}",
                            f"mode={signature.get('database_mode')}",
                        ],
                    )
                )
                for signature in row.get('signatures') or []
            )
            lines.append(
                f"| {row.get('backend') or 'n/a'} "
                f"| {row.get('series_label') or 'n/a'} "
                f"| {row.get('thread_count') or 'n/a'} "
                f"| {row.get('reason') or 'n/a'} "
                f"| {signatures or 'n/a'} |"
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
# Host Telemetry Time-Series
# ---------------------------------------------------------------------------


def _section_host_telemetry_timeseries(
    lines: list[str],
    timeseries_summaries: list[dict[str, Any]],
) -> None:
    if not timeseries_summaries:
        return

    lines.append("---")
    lines.append("")
    lines.append("## Host Telemetry Time-Series")
    lines.append("")

    # CPU & load table
    lines.append("### CPU & Load")
    lines.append("")
    lines.append(
        "| Database | Backend | Series | Threads | Samples | "
        "Sys CPU min | Sys CPU avg | Sys CPU p95 | Sys CPU max | "
        "Proc CPU min | Proc CPU avg | Proc CPU p95 | Proc CPU max | "
        "Load1 avg |"
    )
    lines.append(
        "|----------|---------|--------|--------:|--------:|"
        "-----------:|-----------:|-----------:|-----------:|"
        "------------:|------------:|------------:|------------:|"
        "----------:|"
    )
    for ts in timeseries_summaries:
        sys_cpu = ts.get("system_cpu_pct") or {}
        proc_cpu = ts.get("process_cpu_pct") or {}
        load1 = ts.get("loadavg_1") or {}
        lines.append(
            f"| {ts.get('database') or 'n/a'} "
            f"| {ts.get('backend') or 'n/a'} "
            f"| {ts.get('series_label') or 'n/a'} "
            f"| {ts.get('thread_count') or 'n/a'} "
            f"| {ts.get('sample_count') or 0} "
            f"| {_fmt_float(sys_cpu.get('min'), 1)} "
            f"| {_fmt_float(sys_cpu.get('avg'), 1)} "
            f"| {_fmt_float(sys_cpu.get('p95'), 1)} "
            f"| {_fmt_float(sys_cpu.get('max'), 1)} "
            f"| {_fmt_float(proc_cpu.get('min'), 1)} "
            f"| {_fmt_float(proc_cpu.get('avg'), 1)} "
            f"| {_fmt_float(proc_cpu.get('p95'), 1)} "
            f"| {_fmt_float(proc_cpu.get('max'), 1)} "
            f"| {_fmt_float(load1.get('avg'), 2)} |"
        )
    lines.append("")

    # Memory table
    lines.append("### Memory")
    lines.append("")
    lines.append(
        "| Database | Backend | Series | Threads | "
        "RSS min | RSS avg | RSS p95 | RSS max | "
        "Dirty avg | Dirty max |"
    )
    lines.append(
        "|----------|---------|--------|--------:|"
        "--------:|--------:|--------:|--------:|"
        "----------:|----------:|"
    )
    for ts in timeseries_summaries:
        rss = ts.get("process_rss_bytes") or {}
        dirty = ts.get("mem_dirty_bytes") or {}
        lines.append(
            f"| {ts.get('database') or 'n/a'} "
            f"| {ts.get('backend') or 'n/a'} "
            f"| {ts.get('series_label') or 'n/a'} "
            f"| {ts.get('thread_count') or 'n/a'} "
            f"| {_fmt_bytes(rss.get('min'))} "
            f"| {_fmt_bytes(rss.get('avg'))} "
            f"| {_fmt_bytes(rss.get('p95'))} "
            f"| {_fmt_bytes(rss.get('max'))} "
            f"| {_fmt_bytes(dirty.get('avg'))} "
            f"| {_fmt_bytes(dirty.get('max'))} |"
        )
    lines.append("")

    # Network & Disk I/O rates table
    lines.append("### Network & Disk I/O Rates")
    lines.append("")
    lines.append(
        "| Database | Backend | Series | Threads | "
        "Net RX avg/s | Net TX avg/s | "
        "Disk Read avg/s | Disk Write avg/s | "
        "CtxSw avg/s |"
    )
    lines.append(
        "|----------|---------|--------|--------:|"
        "-------------:|-------------:|"
        "----------------:|-----------------:|"
        "------------:|"
    )
    for ts in timeseries_summaries:
        net_rx = ts.get("net_lo_rx_bytes_per_sec") or {}
        net_tx = ts.get("net_lo_tx_bytes_per_sec") or {}
        disk_r = ts.get("disk_read_bytes_per_sec") or {}
        disk_w = ts.get("disk_write_bytes_per_sec") or {}
        ctx = ts.get("ctx_switches_per_sec") or {}
        lines.append(
            f"| {ts.get('database') or 'n/a'} "
            f"| {ts.get('backend') or 'n/a'} "
            f"| {ts.get('series_label') or 'n/a'} "
            f"| {ts.get('thread_count') or 'n/a'} "
            f"| {_fmt_bytes(net_rx.get('avg'))}/s "
            f"| {_fmt_bytes(net_tx.get('avg'))}/s "
            f"| {_fmt_bytes(disk_r.get('avg'))}/s "
            f"| {_fmt_bytes(disk_w.get('avg'))}/s "
            f"| {_fmt_rate(ctx.get('avg'))} |"
        )
    lines.append("")


def _fmt_rate(value: Optional[float]) -> str:
    """Format a per-second rate value."""
    if value is None:
        return "n/a"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:.0f}"


# ---------------------------------------------------------------------------
# Detailed Results
# ---------------------------------------------------------------------------


def _section_detailed_results(
    lines: list[str],
    rows: list[dict[str, Any]],
) -> None:
    lines.append("---")
    lines.append("")
    lines.append("## Benchmark Results")
    lines.append("")

    # Full latency & throughput
    lines.append("### Latency And Throughput")
    lines.append("")
    lines.append(
        "| Database | Backend | Series | Threads | Reps "
        "| Throughput Median ops/s | Tp Spread % | Avg ms | P50 ms "
        "| P99 ms | P99 Spread % | P99.9 ms |"
    )
    lines.append(
        "|----------|---------|--------|--------:|-----:"
        "|------------------------:|-----------:|-------:|-------:"
        "|-------:|-------------:|---------:|"
    )
    for row in rows:
        lines.append(
            f"| {row.get('database') or 'n/a'} "
            f"| {row.get('backend') or 'n/a'} "
            f"| {row.get('series_label') or 'n/a'} "
            f"| {row.get('thread_count') or 'n/a'} "
            f"| {row.get('replicate_count') or 1} "
            f"| {_fmt_ops(row.get('throughput_ops_sec'))} "
            f"| {_fmt_float(row.get('throughput_ops_sec_spread_pct'), 2)} "
            f"| {_fmt_float(row.get('average_latency_ms'), 2)} "
            f"| {_fmt_float(row.get('p50_latency_ms'))} "
            f"| {_fmt_float(row.get('p99_latency_ms'))} "
            f"| {_fmt_float(row.get('p99_latency_ms_spread_pct'), 2)} "
            f"| {_fmt_float(row.get('p99_9_latency_ms'))} |"
        )
    if not rows:
        lines.append(
            "| n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |"
        )
    lines.append("")

    lines.append("### Cache And Keyspace")
    lines.append("")
    lines.append(
        "| Database | Backend | Series | Threads | Hit Rate | Evicted Keys | Expired Keys |"
    )
    lines.append(
        "|----------|---------|--------|--------:|---------:|-------------:|-------------:|"
    )
    for row in rows:
        lines.append(
            f"| {row.get('database') or 'n/a'} "
            f"| {row.get('backend') or 'n/a'} "
            f"| {row.get('series_label') or 'n/a'} "
            f"| {row.get('thread_count') or 'n/a'} "
            f"| {_fmt_float(row.get('cache_hit_rate'))} "
            f"| {_fmt_float(row.get('evicted_keys_delta'), 0)} "
            f"| {_fmt_float(row.get('expired_keys_delta'), 0)} |"
        )
    if not rows:
        lines.append("| n/a | n/a | n/a | n/a | n/a | n/a | n/a |")
    lines.append("")


def _section_diagnostics(
    lines: list[str],
    diagnostic_summary: dict[str, Any],
    memory_rows: list[dict[str, Any]],
    aof_rows: list[dict[str, Any]],
    aof_overhead: list[dict[str, Any]],
    eviction_rows: list[dict[str, Any]],
) -> None:
    if not any((diagnostic_summary, memory_rows, aof_rows, aof_overhead, eviction_rows)):
        return

    lines.append("---")
    lines.append("")
    lines.append("## Diagnostic Summary")
    lines.append("")

    if diagnostic_summary:
        lines.append("| Field | Value |")
        lines.append("|-------|-------|")
        lines.append(
            f"| Raw diagnostic rows | {diagnostic_summary.get('raw_row_count') or diagnostic_summary.get('normalized_row_count') or 0} |"
        )
        lines.append(
            f"| Rows with host telemetry | {diagnostic_summary.get('rows_with_host_telemetry') or 0} |"
        )
        lines.append(
            f"| Rows with memory diagnostics | {diagnostic_summary.get('rows_with_memory_diagnostics') or 0} |"
        )
        lines.append(
            f"| Rows with reclaim or writeback pressure | {diagnostic_summary.get('rows_with_reclaim_or_writeback_pressure') or 0} |"
        )
        lines.append(
            f"| Rows with AOF diagnostics | {diagnostic_summary.get('rows_with_aof_diagnostics') or 0} |"
        )
        lines.append(
            f"| Rows with AOF fsync pressure | {diagnostic_summary.get('rows_with_aof_fsync_pressure') or 0} |"
        )
        lines.append(
            f"| Max process RSS peak | {_fmt_bytes(diagnostic_summary.get('max_process_rss_peak_bytes'))} |"
        )
        lines.append(
            f"| Max allocator resident after | {_fmt_bytes(diagnostic_summary.get('max_allocator_resident_after_bytes'))} |"
        )
        lines.append(
            f"| Max dirty memory peak | {_fmt_bytes(diagnostic_summary.get('max_system_mem_dirty_peak_bytes'))} |"
        )
        lines.append(
            f"| Max writeback memory peak | {_fmt_bytes(diagnostic_summary.get('max_system_mem_writeback_peak_bytes'))} |"
        )
        lines.append(
            f"| Max disk write delta | {_fmt_bytes(diagnostic_summary.get('max_disk_write_bytes_delta'))} |"
        )
        lines.append(
            f"| Max AOF fsync latency | {_fmt_float(diagnostic_summary.get('max_aof_fsync_latency_ms'), 2)} ms |"
        )
        lines.append("")

    if memory_rows:
        lines.append("### Memory And Reclaim")
        lines.append("")
        lines.append(
            "| Database | Backend | Series | Threads | RSS Peak | Dataset After | Allocator Resident After | Frag Ratio | Dirty Peak | Writeback Peak | Direct Scan | Allocstall |"
        )
        lines.append(
            "|----------|---------|--------|--------:|---------:|--------------:|------------------------:|-----------:|-----------:|---------------:|------------:|-----------:|"
        )
        for row in memory_rows:
            lines.append(
                f"| {row.get('database') or 'n/a'} "
                f"| {row.get('backend') or 'n/a'} "
                f"| {row.get('series_label') or 'n/a'} "
                f"| {row.get('thread_count') or 'n/a'} "
                f"| {_fmt_bytes(row.get('process_rss_peak_bytes'))} "
                f"| {_fmt_bytes(row.get('used_memory_dataset_after_bytes'))} "
                f"| {_fmt_bytes(row.get('allocator_resident_after_bytes'))} "
                f"| {_fmt_float(row.get('mem_fragmentation_ratio_after'), 2)} "
                f"| {_fmt_bytes(row.get('system_mem_dirty_peak_bytes'))} "
                f"| {_fmt_bytes(row.get('system_mem_writeback_peak_bytes'))} "
                f"| {_fmt_float(row.get('system_vm_page_scan_direct_delta'), 0)} "
                f"| {_fmt_float(row.get('system_vm_allocstall_delta'), 0)} |"
            )
        lines.append("")

    if aof_rows:
        lines.append("### AOF And Disk")
        lines.append("")
        lines.append(
            "| Database | Backend | Series | Threads | AOF Size Delta | Process Writes | Disk Writes | Delayed Fsyncs | AOF Fsync Max ms | Pending Fsync Max ms | Writeback Peak |"
        )
        lines.append(
            "|----------|---------|--------|--------:|---------------:|---------------:|------------:|----------------:|------------------:|---------------------:|---------------:|"
        )
        for row in aof_rows:
            lines.append(
                f"| {row.get('database') or 'n/a'} "
                f"| {row.get('backend') or 'n/a'} "
                f"| {row.get('series_label') or 'n/a'} "
                f"| {row.get('thread_count') or 'n/a'} "
                f"| {_fmt_bytes(row.get('aof_current_size_delta_bytes'))} "
                f"| {_fmt_bytes(row.get('process_write_bytes_delta'))} "
                f"| {_fmt_bytes(row.get('disk_write_bytes_delta'))} "
                f"| {_fmt_float(row.get('aof_delayed_fsync_delta'), 0)} "
                f"| {_fmt_float(row.get('latency_aof_fsync_max_after_ms'), 2)} "
                f"| {_fmt_float(row.get('latency_aof_pending_fsync_max_after_ms'), 2)} "
                f"| {_fmt_bytes(row.get('system_mem_writeback_peak_bytes'))} |"
            )
        lines.append("")

    lines.append("### AOF Overhead")
    lines.append("")
    if aof_overhead:
        lines.append(
            "| Database | Backend | Series | Threads | Throughput Overhead % | P99 Overhead % |"
        )
        lines.append(
            "|----------|---------|--------|---------|---------------------:|---------------:|"
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
            "No comparable baseline and AOF-enabled runs were present in the source summaries."
        )
    lines.append("")

    lines.append("### Eviction Observations")
    lines.append("")
    if eviction_rows:
        lines.append(
            "| Database | Backend | Series | Threads | Evicted Keys | Throughput ops/s |"
        )
        lines.append(
            "|----------|---------|--------|---------|-------------:|-------------------:|"
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
        lines.append("No eviction events were observed in the source summaries.")
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