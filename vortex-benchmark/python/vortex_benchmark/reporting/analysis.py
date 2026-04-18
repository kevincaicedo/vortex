"""Derived analysis computed from normalized benchmark rows."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional


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


def build_analysis(
    rows: list[dict[str, Any]], databases: list[str]
) -> dict[str, Any]:
    """Build all derived analysis structures from normalized rows."""
    if not rows:
        return {
            "backend_comparisons": {},
            "scalability": [],
            "latency_breakdown": [],
            "winners": {},
            "key_insights": [],
        }

    db_names = databases or sorted(
        {r.get("database", "unknown") for r in rows}
    )

    return {
        "backend_comparisons": _build_backend_comparisons(rows, db_names),
        "scalability": _build_scalability(rows, db_names),
        "latency_breakdown": _build_latency_breakdown(rows, db_names),
        "winners": _build_winners(rows, db_names),
        "key_insights": _build_key_insights(rows, db_names),
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

    # Memory efficiency insights
    mem_by_db: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        delta = r.get("used_memory_delta_bytes")
        if isinstance(delta, (int, float)):
            mem_by_db[r.get("database", "unknown")].append(float(delta))
    if len(mem_by_db) >= 2:
        avg_mem = {
            db: sum(vals) / len(vals) for db, vals in mem_by_db.items()
        }
        ranked = sorted(avg_mem.items(), key=lambda x: x[1])
        if ranked[0][1] < ranked[-1][1]:
            insights.append(
                f"**Memory efficiency**: {ranked[0][0]} uses "
                f"{abs(ranked[0][1]) / 1024:.1f} KiB avg memory delta "
                f"vs {ranked[-1][0]} at "
                f"{abs(ranked[-1][1]) / 1024:.1f} KiB"
            )

    return insights
