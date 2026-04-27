"""Derived analysis computed from normalized benchmark rows."""

from __future__ import annotations

from collections import defaultdict
from statistics import mean, median, quantiles, stdev
from typing import Any, Optional


_METRIC_FIELDS = (
    "throughput_ops_sec",
    "average_latency_ms",
    "p50_latency_ms",
    "p99_latency_ms",
    "p99_9_latency_ms",
    "p99_999_latency_ms",
)

_COMPARISON_SIGNATURE_FIELDS = (
    "configured_service_threads",
    "configured_aof_enabled",
    "configured_aof_fsync",
    "configured_maxmemory",
    "configured_eviction_policy",
    "database_mode",
)


def _fmt_ops(value: Optional[float]) -> str:
    """Format ops/sec for display."""
    if value is None:
        return "n/a"
    v = float(value)
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v / 1_000:,.0f}K"
    return f"{v:,.0f}"


def _replicate_group_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("database"),
        row.get("backend"),
        row.get("series_kind"),
        row.get("series_label"),
        row.get("thread_count"),
        row.get("configured_service_threads"),
        row.get("configured_aof_enabled"),
        row.get("configured_aof_fsync"),
        row.get("configured_maxmemory"),
        row.get("configured_eviction_policy"),
        row.get("database_mode"),
        row.get("database_version"),
    )


def _comparison_group_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("backend"),
        row.get("series_kind"),
        row.get("series_label"),
        row.get("thread_count"),
    )


def _comparison_signature(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(field) for field in _COMPARISON_SIGNATURE_FIELDS)


def _metric_values(rows: list[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(key)
        if value is None:
            continue
        values.append(float(value))
    return values


def _spread_pct(values: list[float]) -> Optional[float]:
    if not values:
        return None
    med = median(values)
    if med <= 0:
        return None
    return ((max(values) - min(values)) / med) * 100.0


def _outlier_count(values: list[float]) -> int:
    if len(values) < 4:
        return 0
    ordered = sorted(values)
    q1, _, q3 = quantiles(ordered, n=4, method="inclusive")
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return sum(1 for value in ordered if value < lower or value > upper)


def _build_series_statistics(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_replicate_group_key(row)].append(row)

    aggregated_rows: list[dict[str, Any]] = []
    series_statistics: list[dict[str, Any]] = []

    for key in sorted(grouped):
        group = sorted(
            grouped[key],
            key=lambda row: (
                row.get("replicate_index") or 0,
                str(row.get("source_summary") or ""),
                str(row.get("run_id") or ""),
            ),
        )
        base = dict(group[0])
        base["suite_run_id"] = base.get("suite_run_id") or base.get("run_id")
        base["replicate_count"] = len(group)
        base["replicate_indexes"] = [row.get("replicate_index") for row in group]
        base["replicate_run_ids"] = [row.get("replicate_run_id") for row in group]
        base["source_summary_count"] = len(
            {str(row.get("source_summary") or "") for row in group}
        )
        if len(group) > 1:
            base["run_id"] = base.get("suite_run_id")
            base["replicate_run_id"] = None
            base["replicate_index"] = None
            base["replicate_id"] = None

        stats_entry: dict[str, Any] = {
            "database": base.get("database"),
            "backend": base.get("backend"),
            "series_kind": base.get("series_kind"),
            "series_label": base.get("series_label"),
            "thread_count": base.get("thread_count"),
            "replicate_count": len(group),
        }

        for metric in _METRIC_FIELDS:
            values = _metric_values(group, metric)
            if not values:
                continue
            med = median(values)
            base[metric] = med
            base[f"{metric}_min"] = min(values)
            base[f"{metric}_max"] = max(values)
            base[f"{metric}_mean"] = mean(values)
            base[f"{metric}_stdev"] = stdev(values) if len(values) > 1 else None
            base[f"{metric}_spread_pct"] = _spread_pct(values)

            stats_key = metric.removesuffix("_ops_sec").removesuffix("_ms")
            stats_entry[f"{stats_key}_median"] = med
            stats_entry[f"{stats_key}_spread_pct"] = _spread_pct(values)

        throughput_values = _metric_values(group, "throughput_ops_sec")
        p99_values = _metric_values(group, "p99_latency_ms")
        base["throughput_outlier_replicates"] = _outlier_count(throughput_values)
        base["p99_latency_outlier_replicates"] = _outlier_count(p99_values)
        stats_entry["throughput_outlier_replicates"] = base[
            "throughput_outlier_replicates"
        ]
        stats_entry["p99_latency_outlier_replicates"] = base[
            "p99_latency_outlier_replicates"
        ]

        aggregated_rows.append(base)
        series_statistics.append(stats_entry)

    return aggregated_rows, series_statistics


def _build_comparison_validity(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_comparison_group_key(row)].append(row)

    issues: list[dict[str, Any]] = []
    for key in sorted(grouped):
        group = grouped[key]
        databases = {row.get("database") for row in group if row.get("database")}
        if len(databases) < 2:
            continue

        signature_map: dict[tuple[Any, ...], list[str]] = defaultdict(list)
        for row in group:
            signature_map[_comparison_signature(row)].append(
                str(row.get("database") or "unknown")
            )

        duplicate_databases = len(group) != len(databases)
        if len(signature_map) <= 1 and not duplicate_databases:
            continue

        signatures = []
        for signature, signature_databases in signature_map.items():
            signatures.append(
                {
                    field: signature[index]
                    for index, field in enumerate(_COMPARISON_SIGNATURE_FIELDS)
                }
                | {"databases": sorted(set(signature_databases))}
            )

        issues.append(
            {
                "backend": key[0],
                "series_kind": key[1],
                "series_label": key[2],
                "thread_count": key[3],
                "reason": "mismatched runtime or duplicate scenario rows across compared databases",
                "signatures": signatures,
            }
        )

    return issues


def build_analysis(
    rows: list[dict[str, Any]], databases: list[str]
) -> dict[str, Any]:
    """Build all derived analysis structures from normalized rows."""
    if not rows:
        return {
            "aggregated_rows": [],
            "series_statistics": [],
            "comparison_validity": [],
            "backend_comparisons": {},
            "scalability": [],
            "latency_breakdown": [],
            "winners": {},
            "key_insights": [],
        }

    aggregated_rows, series_statistics = _build_series_statistics(rows)
    comparison_validity = _build_comparison_validity(aggregated_rows)
    invalid_keys = {
        (
            entry.get("backend"),
            entry.get("series_kind"),
            entry.get("series_label"),
            entry.get("thread_count"),
        )
        for entry in comparison_validity
    }
    for row in aggregated_rows:
        comparison_key = _comparison_group_key(row)
        row["comparison_invalid"] = comparison_key in invalid_keys

    analysis_rows = [row for row in aggregated_rows if not row.get("comparison_invalid")]

    db_names = databases or sorted(
        {r.get("database", "unknown") for r in aggregated_rows}
    )

    key_insights = _build_key_insights(analysis_rows, db_names)
    if comparison_validity:
        key_insights.insert(
            0,
            f"**Invalid comparisons excluded**: {len(comparison_validity)} scenario group(s) had mismatched runtime settings or duplicate rows across databases.",
        )

    return {
        "aggregated_rows": aggregated_rows,
        "series_statistics": series_statistics,
        "comparison_validity": comparison_validity,
        "backend_comparisons": _build_backend_comparisons(analysis_rows, db_names)
        if analysis_rows
        else {},
        "scalability": _build_scalability(analysis_rows, db_names)
        if analysis_rows
        else [],
        "latency_breakdown": _build_latency_breakdown(analysis_rows, db_names)
        if analysis_rows
        else [],
        "winners": _build_winners(analysis_rows, db_names)
        if analysis_rows
        else {},
        "key_insights": key_insights,
    }


# ---- cross-database comparison tables ----


def _build_backend_comparisons(
    rows: list[dict[str, Any]], databases: list[str]
) -> dict[str, Any]:
    by_backend: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_backend[row.get("backend", "unknown")].append(row)

    comparisons: dict[str, Any] = {}
    for backend in sorted(by_backend):
        backend_rows = by_backend[backend]
        if backend == "redis-benchmark":
            comparisons[backend] = _build_command_comparison(
                backend_rows, databases
            )
        else:
            comparisons[backend] = _build_thread_workload_comparison(
                backend_rows, databases
            )
    return comparisons


def _build_command_comparison(
    rows: list[dict[str, Any]], databases: list[str]
) -> dict[str, Any]:
    by_command: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        cmd = row.get("series_label", "unknown")
        db = row.get("database", "unknown")
        by_command[cmd][db] = {
            "throughput_ops_sec": row.get("throughput_ops_sec"),
            "average_latency_ms": row.get("average_latency_ms"),
            "p50_latency_ms": row.get("p50_latency_ms"),
            "p99_latency_ms": row.get("p99_latency_ms"),
            "p99_9_latency_ms": row.get("p99_9_latency_ms"),
        }

    table_rows = []
    for cmd in sorted(by_command):
        entry: dict[str, Any] = {"series": cmd}
        for db in databases:
            entry[db] = by_command[cmd].get(db, {})
        table_rows.append(entry)
    return {"type": "command", "rows": table_rows}


def _build_thread_workload_comparison(
    rows: list[dict[str, Any]], databases: list[str]
) -> dict[str, Any]:
    by_workload: dict[
        str, dict[Any, dict[str, dict[str, Any]]]
    ] = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        workload = row.get("series_label", "unknown")
        threads = row.get("thread_count")
        db = row.get("database", "unknown")
        by_workload[workload][threads][db] = {
            "throughput_ops_sec": row.get("throughput_ops_sec"),
            "average_latency_ms": row.get("average_latency_ms"),
            "p50_latency_ms": row.get("p50_latency_ms"),
            "p99_latency_ms": row.get("p99_latency_ms"),
            "p99_9_latency_ms": row.get("p99_9_latency_ms"),
        }

    workloads: dict[str, dict[str, Any]] = {}
    for wk in sorted(by_workload):
        thread_data = by_workload[wk]
        wk_rows = []
        for threads in sorted(thread_data, key=lambda x: x or 0):
            entry: dict[str, Any] = {"threads": threads}
            for db in databases:
                entry[db] = thread_data[threads].get(db, {})
            wk_rows.append(entry)
        workloads[wk] = {"rows": wk_rows}
    return {"type": "thread_workload", "workloads": workloads}


# ---- scalability ----


def _build_scalability(
    rows: list[dict[str, Any]], databases: list[str]
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if not isinstance(row.get("thread_count"), int):
            continue
        key = (
            row.get("database"),
            row.get("backend"),
            row.get("series_label"),
        )
        grouped[key].append(row)

    scaling: list[dict[str, Any]] = []
    for key, group in sorted(grouped.items()):
        if len(group) < 2:
            continue
        group.sort(key=lambda r: r.get("thread_count") or 0)
        first, last = group[0], group[-1]
        min_ops = first.get("throughput_ops_sec")
        max_ops = last.get("throughput_ops_sec")
        uplift = None
        if min_ops and max_ops and min_ops > 0:
            uplift = ((max_ops - min_ops) / min_ops) * 100.0
        scaling.append(
            {
                "database": key[0],
                "backend": key[1],
                "workload": key[2],
                "min_threads": first.get("thread_count"),
                "min_ops": min_ops,
                "max_threads": last.get("thread_count"),
                "max_ops": max_ops,
                "uplift_pct": uplift,
            }
        )
    return scaling


# ---- latency breakdown ----


def _build_latency_breakdown(
    rows: list[dict[str, Any]], databases: list[str]
) -> list[dict[str, Any]]:
    """Pick the highest-thread-count row for each DB/backend/series combo."""
    best: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        if not any(
            row.get(k) is not None
            for k in (
                "p50_latency_ms",
                "p99_latency_ms",
                "average_latency_ms",
            )
        ):
            continue
        key = (
            row.get("database"),
            row.get("backend"),
            row.get("series_label"),
        )
        thread = row.get("thread_count") or 0
        existing = best.get(key)
        if existing is None or (existing.get("thread_count") or 0) < thread:
            best[key] = row

    return [
        {
            "database": r.get("database"),
            "backend": r.get("backend"),
            "series_label": r.get("series_label"),
            "thread_count": r.get("thread_count"),
            "throughput_ops_sec": r.get("throughput_ops_sec"),
            "average_latency_ms": r.get("average_latency_ms"),
            "p50_latency_ms": r.get("p50_latency_ms"),
            "p99_latency_ms": r.get("p99_latency_ms"),
            "p99_9_latency_ms": r.get("p99_9_latency_ms"),
            "p99_999_latency_ms": r.get("p99_999_latency_ms"),
        }
        for r in [best[k] for k in sorted(best)]
    ]


# ---- winners ----


def _build_winners(
    rows: list[dict[str, Any]], databases: list[str]
) -> dict[str, dict[str, Any]]:
    winners: dict[str, dict[str, Any]] = {}

    def _ctx(row: dict[str, Any]) -> str:
        parts = [row.get("backend", ""), row.get("series_label", "")]
        if row.get("thread_count"):
            parts.append(f"({row['thread_count']}T)")
        if (row.get("replicate_count") or 0) > 1:
            parts.append(f"[{row['replicate_count']} reps]")
        return " ".join(p for p in parts if p)

    # Peak throughput
    best = max(rows, key=lambda r: r.get("throughput_ops_sec") or 0)
    if best.get("throughput_ops_sec"):
        winners["peak_throughput"] = {
            "database": best.get("database"),
            "value": best["throughput_ops_sec"],
            "context": _ctx(best),
        }

    # Lowest avg latency
    lat_rows = [r for r in rows if r.get("average_latency_ms") is not None]
    if lat_rows:
        best = min(lat_rows, key=lambda r: r["average_latency_ms"])
        winners["lowest_avg_latency"] = {
            "database": best.get("database"),
            "value": best["average_latency_ms"],
            "context": _ctx(best),
        }

    # Lowest p99
    p99_rows = [r for r in rows if r.get("p99_latency_ms") is not None]
    if p99_rows:
        best = min(p99_rows, key=lambda r: r["p99_latency_ms"])
        winners["lowest_p99_latency"] = {
            "database": best.get("database"),
            "value": best["p99_latency_ms"],
            "context": _ctx(best),
        }

    # Lowest p99.9
    p999_rows = [
        r for r in rows if r.get("p99_9_latency_ms") is not None
    ]
    if p999_rows:
        best = min(p999_rows, key=lambda r: r["p99_9_latency_ms"])
        winners["lowest_p99_9_latency"] = {
            "database": best.get("database"),
            "value": best["p99_9_latency_ms"],
            "context": _ctx(best),
        }

    # Best scalability
    scaling = _build_scalability(rows, databases)
    valid = [s for s in scaling if s.get("uplift_pct") is not None]
    if valid:
        best_s = max(valid, key=lambda s: s["uplift_pct"])
        winners["best_scalability"] = {
            "database": best_s.get("database"),
            "value": best_s["uplift_pct"],
            "context": (
                f"{best_s.get('backend')} {best_s.get('workload')} "
                f"({best_s.get('min_threads')}→{best_s.get('max_threads')}T)"
            ),
        }

    # Per-backend throughput winners
    by_backend: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_backend[r.get("backend", "unknown")].append(r)
    for backend, b_rows in sorted(by_backend.items()):
        best = max(b_rows, key=lambda r: r.get("throughput_ops_sec") or 0)
        if best.get("throughput_ops_sec"):
            safe = backend.replace("-", "_").replace(".", "_")
            winners[f"peak_{safe}"] = {
                "database": best.get("database"),
                "value": best["throughput_ops_sec"],
                "context": _ctx(best),
            }

    return winners


# ---- key insights ----


def _build_key_insights(
    rows: list[dict[str, Any]], databases: list[str]
) -> list[str]:
    insights: list[str] = []
    if len(databases) < 2:
        insights.append(
            "Single database run — add more databases for "
            "cross-comparison insights."
        )
        return insights

    by_backend_db: dict[
        str, dict[str, list[dict[str, Any]]]
    ] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        by_backend_db[row.get("backend", "unknown")][
            row.get("database", "unknown")
        ].append(row)

    # Throughput insights per backend
    for backend in sorted(by_backend_db):
        db_map = by_backend_db[backend]
        if len(db_map) < 2:
            continue
        avg_tp: dict[str, float] = {}
        for db, rl in db_map.items():
            vals = [
                r["throughput_ops_sec"]
                for r in rl
                if r.get("throughput_ops_sec")
            ]
            if vals:
                avg_tp[db] = sum(vals) / len(vals)
        if len(avg_tp) >= 2:
            ranked = sorted(
                avg_tp.items(), key=lambda x: x[1], reverse=True
            )
            leader, runner = ranked[0], ranked[1]
            if runner[1] > 0:
                pct = ((leader[1] - runner[1]) / runner[1]) * 100
                insights.append(
                    f"**{backend}**: {leader[0]} leads with "
                    f"{_fmt_ops(leader[1])} avg ops/sec "
                    f"({pct:+.1f}% vs {runner[0]})"
                )

    # P99 latency insights per backend
    for backend in sorted(by_backend_db):
        db_map = by_backend_db[backend]
        if len(db_map) < 2:
            continue
        avg_p99: dict[str, float] = {}
        for db, rl in db_map.items():
            vals = [
                r["p99_latency_ms"]
                for r in rl
                if r.get("p99_latency_ms") is not None
            ]
            if vals:
                avg_p99[db] = sum(vals) / len(vals)
        if len(avg_p99) >= 2:
            ranked = sorted(avg_p99.items(), key=lambda x: x[1])
            leader = ranked[0]
            insights.append(
                f"**{backend} p99 latency**: {leader[0]} has lowest "
                f"average p99 at {leader[1]:.2f} ms"
            )

    # Scalability insights
    scaling = _build_scalability(rows, databases)
    if scaling:
        valid = [s for s in scaling if s.get("uplift_pct") is not None]
        if valid:
            best = max(valid, key=lambda s: s["uplift_pct"])
            insights.append(
                f"**Best scalability**: {best['database']} on "
                f"{best['backend']} {best['workload']} scales "
                f"{best['uplift_pct']:+.0f}% from "
                f"{best['min_threads']}→{best['max_threads']} threads"
            )

    # Diagnostic insights
    reclaim_rows = [
        r
        for r in rows
        if any(
            (r.get(key) or 0) > 0
            for key in (
                "system_mem_writeback_peak_bytes",
                "system_vm_page_scan_kswapd_delta",
                "system_vm_page_scan_direct_delta",
                "system_vm_allocstall_delta",
                "system_vm_swap_out_delta",
            )
        )
    ]
    if reclaim_rows:
        insights.append(
            f"**Memory pressure observed**: {len(reclaim_rows)} row(s) recorded reclaim or writeback activity; use the diagnostic tables instead of memory-delta ranking when comparing efficiency."
        )

    aof_pressure_rows = [
        r
        for r in rows
        if any(
            (r.get(key) or 0) > 0
            for key in (
                "aof_delayed_fsync_delta",
                "aof_pending_bio_fsync_after",
                "latency_aof_fsync_max_after_ms",
                "latency_aof_pending_fsync_max_after_ms",
            )
        )
    ]
    if aof_pressure_rows:
        insights.append(
            f"**AOF pressure observed**: {len(aof_pressure_rows)} row(s) recorded fsync backlog or AOF latency signals; check writeback and fsync diagnostics before drawing persistence conclusions."
        )

    return insights
