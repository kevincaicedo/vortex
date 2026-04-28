"""Benchmark chart generation — radar, heatmap, scaling, and rich comparisons."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import Normalize
from matplotlib.ticker import FuncFormatter

# Consistent color palette across all charts
_DB_COLORS = [
    "#4E79A7",  # blue
    "#E15759",  # red
    "#76B7B2",  # teal
    "#F28E2B",  # orange
    "#59A14F",  # green
    "#EDC948",  # yellow
    "#B07AA1",  # purple
    "#FF9DA7",  # pink
]


def _get_db_color(idx: int) -> str:
    return _DB_COLORS[idx % len(_DB_COLORS)]


def _plot_empty(path: Path, title: str, message: str) -> None:
    figure, axis = plt.subplots(figsize=(8, 4.5))
    axis.axis("off")
    axis.set_title(title)
    axis.text(0.5, 0.5, message, ha="center", va="center", fontsize=12)
    figure.tight_layout()
    figure.savefig(path, dpi=180)
    plt.close(figure)


def _safe_divide(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _compact_ops(val: float) -> str:
    """Format ops/sec for compact bar labels."""
    if val >= 1_000_000:
        return f"{val / 1_000_000:.1f}M"
    if val >= 1_000:
        return f"{val / 1_000:.0f}K"
    return f"{val:.0f}"


def _coerce_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _latency_scale_anchor(values: list[float]) -> float | None:
    finite = [
        value for value in values
        if math.isfinite(value) and value > 0
    ]
    if not finite:
        return None
    if len(finite) == 1:
        return finite[0]
    return float(np.percentile(finite, 75))


def _latency_unit(values: list[float]) -> tuple[str, float]:
    anchor_ms = _latency_scale_anchor(values)
    if anchor_ms is None or anchor_ms >= 1.0:
        return "ms", 1.0
    if anchor_ms >= 0.001:
        return "us", 1_000.0
    return "ns", 1_000_000.0


def _scale_latency_values(values: list[float], factor: float) -> list[float]:
    return [
        value * factor if math.isfinite(value) else float("nan")
        for value in values
    ]


def _normalize_series_label(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _database_colors(rows: list[dict[str, Any]]) -> dict[str, str]:
    databases = sorted(
        {str(row.get("database") or "unknown") for row in rows}
    )
    return {
        database: _get_db_color(index)
        for index, database in enumerate(databases)
    }


def _row_chart_label(row: dict[str, Any]) -> str:
    parts = [
        str(row.get("database") or "unknown"),
        str(row.get("backend") or "backend"),
        str(row.get("series_label") or "series"),
    ]
    thread_count = row.get("thread_count")
    if thread_count is not None:
        parts.append(f"{thread_count}T")
    return " / ".join(parts)


def _select_representative_rows(
    rows: list[dict[str, Any]],
    predicate,
    *,
    max_rows: int = 8,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        if not predicate(row):
            continue
        key = (
            row.get("database"),
            row.get("backend"),
            row.get("series_label"),
        )
        existing = grouped.get(key)
        existing_threads = existing.get("thread_count") if existing else None
        current_threads = row.get("thread_count")
        if existing is None:
            grouped[key] = row
            continue
        if isinstance(current_threads, int) and not isinstance(existing_threads, int):
            grouped[key] = row
            continue
        if isinstance(current_threads, int) and isinstance(existing_threads, int):
            if current_threads > existing_threads:
                grouped[key] = row
                continue
            if current_threads < existing_threads:
                continue
        current_ops = _coerce_float(row.get("throughput_ops_sec")) or 0.0
        existing_ops = _coerce_float(existing.get("throughput_ops_sec")) or 0.0
        if current_ops > existing_ops:
            grouped[key] = row

    selected = list(grouped.values())
    selected.sort(
        key=lambda row: (
            -(int(row.get("thread_count") or 0)),
            -(_coerce_float(row.get("throughput_ops_sec")) or 0.0),
            str(row.get("database") or ""),
            str(row.get("series_label") or ""),
        )
    )
    return selected[:max_rows]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                samples.append(json.loads(line))
    except (OSError, json.JSONDecodeError):
        return []
    return samples


def _parse_timestamp(value: Any) -> float | None:
    if value is None:
        return None
    from datetime import datetime

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _read_memtier_cdf_points(path: Path) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("Value") or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) < 2:
                    continue
                latency_ms = _coerce_float(parts[0])
                percentile = _coerce_float(parts[1])
                if latency_ms is None or percentile is None:
                    continue
                if not 0.0 <= percentile <= 1.0:
                    continue
                points.append((latency_ms, percentile * 100.0))
    except OSError:
        return []
    return points


def _select_telemetry_scenarios(
    rows: list[dict[str, Any]],
) -> list[tuple[str, list[dict[str, Any]]]]:
    scenario_specs = [
        ("custom-rust", "multi_key_only", "Custom Rust / Multi-Key Only"),
        ("custom-rust", "uniform-read_heavy", "Custom Rust / Uniform Read Heavy"),
        (
            "memtier_benchmark",
            "uniform-read_heavy",
            "Memtier Benchmark / Uniform Read Heavy",
        ),
    ]

    scenarios: list[tuple[str, list[dict[str, Any]]]] = []
    for backend, series_label, title in scenario_specs:
        normalized_series = _normalize_series_label(series_label)
        candidates = [
            row for row in rows
            if row.get("backend") == backend
            and _normalize_series_label(row.get("series_label")) == normalized_series
            and row.get("host_telemetry_samples_path")
        ]
        thread_counts = [
            int(row["thread_count"])
            for row in candidates
            if isinstance(row.get("thread_count"), int)
        ]
        if not thread_counts:
            continue
        max_threads = max(thread_counts)

        selected_by_database: dict[str, dict[str, Any]] = {}
        for row in candidates:
            if row.get("thread_count") != max_threads:
                continue
            database = str(row.get("database") or "unknown")
            existing = selected_by_database.get(database)
            if existing is None:
                selected_by_database[database] = row
                continue
            current_ops = _coerce_float(row.get("throughput_ops_sec")) or 0.0
            existing_ops = _coerce_float(existing.get("throughput_ops_sec")) or 0.0
            if current_ops > existing_ops:
                selected_by_database[database] = row

        selected = sorted(
            selected_by_database.values(),
            key=lambda row: str(row.get("database") or ""),
        )
        if selected:
            scenarios.append((f"{title} ({max_threads}T)", selected))

    return scenarios


def _extract_memtier_latency_series(
    path: Path,
) -> tuple[str | None, list[dict[str, float]]]:
    try:
        payload = _read_json(path)
    except (OSError, json.JSONDecodeError):
        return None, []

    all_stats = payload.get("ALL STATS") or {}
    for section_name in ("Totals", "Sets", "Gets"):
        section = all_stats.get(section_name) or {}
        time_series = section.get("Time-Serie") or {}
        if not isinstance(time_series, dict) or not time_series:
            continue

        series: list[dict[str, float]] = []
        for second_key, bucket in sorted(
            time_series.items(),
            key=lambda item: int(item[0]) if str(item[0]).isdigit() else str(item[0]),
        ):
            if not isinstance(bucket, dict):
                continue
            second = _coerce_float(second_key)
            if second is None:
                continue
            series.append(
                {
                    "second": second,
                    "avg": _coerce_float(bucket.get("Average Latency")) or float("nan"),
                    "p95": _coerce_float(bucket.get("p95.00")) or float("nan"),
                    "p99": _coerce_float(bucket.get("p99.00")) or float("nan"),
                }
            )
        if series:
            return section_name, series

    return None, []


def _extract_telemetry_series(path: Path) -> dict[str, list[tuple[float, float]]]:
    samples = _load_jsonl(path)
    if len(samples) < 2:
        return {}

    base_ts = _parse_timestamp(samples[0].get("captured_at"))
    if base_ts is None:
        return {}

    cpu_points: list[tuple[float, float]] = []
    rss_points: list[tuple[float, float]] = []
    net_points: list[tuple[float, float]] = []

    prev_ts = None
    prev_net = None
    for sample in samples:
        ts = _parse_timestamp(sample.get("captured_at"))
        if ts is None:
            continue
        elapsed = ts - base_ts

        cpu = _coerce_float(sample.get("system_cpu_utilization_pct"))
        if cpu is not None:
            cpu_points.append((elapsed, cpu))

        rss = _coerce_float(sample.get("process_rss_bytes"))
        if rss is not None:
            rss_points.append((elapsed, rss / (1024.0 * 1024.0)))

        current_net = _coerce_float(sample.get("network_loopback_tx_bytes"))
        if prev_ts is not None and prev_net is not None and current_net is not None:
            dt = ts - prev_ts
            if dt > 0 and current_net >= prev_net:
                net_points.append((elapsed, (current_net - prev_net) / dt / (1024.0 * 1024.0)))
        prev_ts = ts
        prev_net = current_net

    return {
        "cpu": cpu_points,
        "rss_mib": rss_points,
        "net_tx_mib_per_sec": net_points,
    }


# ---------------------------------------------------------------------------
# NEW: Throughput Comparison (grouped bar chart per backend)
# ---------------------------------------------------------------------------


def _render_throughput_comparison(
    rows: list[dict[str, Any]], path: Path
) -> None:
    """Grouped bar chart: one subplot per backend category, bars per DB."""
    databases = sorted(
        {r.get("database") for r in rows if r.get("database")}
    )
    if not databases or not rows:
        _plot_empty(path, "Throughput Comparison", "No data available.")
        return

    # Build groups: (title, [(category_label, {db: ops_val})...])
    groups: list[tuple[str, list[tuple[str, dict[str, float]]]]] = []
    by_backend: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_backend[r.get("backend", "unknown")].append(r)

    for backend in sorted(by_backend):
        b_rows = by_backend[backend]
        if backend == "redis-benchmark":
            by_cmd: dict[str, dict[str, float]] = defaultdict(dict)
            for r in b_rows:
                cmd = r.get("series_label", "?")
                db = r.get("database", "?")
                by_cmd[cmd][db] = float(r.get("throughput_ops_sec") or 0)
            cats = [(cmd, by_cmd[cmd]) for cmd in sorted(by_cmd)]
            groups.append((backend, cats))
        else:
            by_wk: dict[
                str, dict[Any, dict[str, float]]
            ] = defaultdict(lambda: defaultdict(dict))
            for r in b_rows:
                wk = r.get("series_label", "?")
                tc = r.get("thread_count")
                db = r.get("database", "?")
                by_wk[wk][tc][db] = float(
                    r.get("throughput_ops_sec") or 0
                )
            for wk in sorted(by_wk):
                cats = []
                for tc in sorted(by_wk[wk], key=lambda x: x or 0):
                    label = f"{tc}T" if tc is not None else "n/a"
                    cats.append((label, by_wk[wk][tc]))
                groups.append((f"{backend}: {wk}", cats))

    n_groups = len(groups)
    if n_groups == 0:
        _plot_empty(path, "Throughput Comparison", "No data available.")
        return

    cols = min(n_groups, 3)
    nrows = math.ceil(n_groups / cols)
    fig, axes = plt.subplots(
        nrows, cols,
        figsize=(6.5 * cols, 5.5 * nrows),
        squeeze=False,
    )
    db_colors = {db: _get_db_color(i) for i, db in enumerate(databases)}

    for g_idx, (title, cats) in enumerate(groups):
        r_idx, c_idx = divmod(g_idx, cols)
        ax = axes[r_idx][c_idx]

        n_cats = len(cats)
        n_dbs = len(databases)
        width = 0.8 / max(n_dbs, 1)
        x = np.arange(n_cats, dtype=float)

        for db_idx, db in enumerate(databases):
            vals = [cat_data.get(db, 0) for _, cat_data in cats]
            offset = (db_idx - n_dbs / 2 + 0.5) * width
            bars = ax.bar(
                x + offset, vals, width,
                label=db, color=db_colors[db],
            )
            for bar_obj, val in zip(bars, vals):
                if val > 0:
                    ax.text(
                        bar_obj.get_x() + bar_obj.get_width() / 2,
                        bar_obj.get_height(),
                        _compact_ops(val),
                        ha="center", va="bottom", fontsize=7,
                    )

        ax.set_xticks(x)
        ax.set_xticklabels(
            [lbl for lbl, _ in cats], fontsize=9, rotation=30, ha="right"
        )
        ax.set_title(title, fontweight="bold", fontsize=10)
        ax.set_ylabel("ops/sec")
        ax.grid(axis="y", alpha=0.3)
        ax.legend(fontsize=8)

    # Hide unused subplots
    for g_idx in range(n_groups, nrows * cols):
        r_idx, c_idx = divmod(g_idx, cols)
        axes[r_idx][c_idx].set_visible(False)

    fig.suptitle(
        "Throughput Comparison by Backend",
        fontsize=14, fontweight="bold", y=1.02,
    )
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# NEW: Latency Comparison (dual-panel: avg + p99 line chart)
# ---------------------------------------------------------------------------


def _render_latency_comparison(
    rows: list[dict[str, Any]], path: Path
) -> None:
    """Dual-panel line chart: avg latency + p99 latency by thread count."""
    threaded = [
        r for r in rows
        if isinstance(r.get("thread_count"), int)
        and (
            r.get("average_latency_ms") is not None
            or r.get("p99_latency_ms") is not None
        )
    ]
    if not threaded:
        _plot_empty(
            path, "Latency Comparison",
            "No thread-sweep latency data available.",
        )
        return

    databases = sorted(
        {r.get("database") for r in threaded if r.get("database")}
    )
    db_colors = {db: _get_db_color(i) for i, db in enumerate(databases)}

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in threaded:
        key = (
            f"{r['database']}/{r.get('backend', '?')}"
            f"/{r.get('series_label', '?')}"
        )
        grouped[key].append(r)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    markers = ["o", "s", "^", "D", "v", "<", ">", "p"]
    line_styles = ["-", "--", "-.", ":"]

    for idx, (label, group) in enumerate(sorted(grouped.items())):
        group.sort(key=lambda r: r.get("thread_count") or 0)
        threads = [r["thread_count"] for r in group]
        db = group[0].get("database", "?")
        color = db_colors.get(db, "#333333")
        marker = markers[idx % len(markers)]
        ls = line_styles[(idx // len(markers)) % len(line_styles)]

        # Average latency
        avg_lat = [r.get("average_latency_ms") for r in group]
        if any(v is not None for v in avg_lat):
            vals = [
                v if v is not None else float("nan") for v in avg_lat
            ]
            ax1.plot(
                threads, vals, marker=marker, linestyle=ls,
                color=color, linewidth=2, label=label,
            )
            for t, v in zip(threads, vals):
                if not math.isnan(v):
                    ax1.annotate(
                        f"{v:.2f}", (t, v),
                        textcoords="offset points",
                        xytext=(0, 8), ha="center", fontsize=7,
                    )

        # P99 latency
        p99_lat = [r.get("p99_latency_ms") for r in group]
        if any(v is not None for v in p99_lat):
            vals = [
                v if v is not None else float("nan") for v in p99_lat
            ]
            ax2.plot(
                threads, vals, marker=marker, linestyle=ls,
                color=color, linewidth=2, label=label,
            )
            for t, v in zip(threads, vals):
                if not math.isnan(v):
                    ax2.annotate(
                        f"{v:.2f}", (t, v),
                        textcoords="offset points",
                        xytext=(0, 8), ha="center", fontsize=7,
                    )

    all_threads = sorted(
        {r["thread_count"] for r in threaded}
    )
    for ax, title in [
        (ax1, "Average Latency"),
        (ax2, "P99 Latency"),
    ]:
        ax.set_title(title, fontweight="bold")
        ax.set_xlabel("Thread Count")
        ax.set_ylabel("Latency (ms)")
        ax.set_xticks(all_threads)
        ax.grid(alpha=0.3)
        ax.legend(fontsize=7, loc="best")

    fig.suptitle(
        "Latency Comparison", fontsize=14, fontweight="bold",
    )
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# NEW: Heatmap Dashboard (4-panel: ops, avg, p99, p99.9)
# ---------------------------------------------------------------------------


def _render_heatmap_dashboard(
    rows: list[dict[str, Any]], path: Path
) -> None:
    """2×2 heatmap: ops/sec, avg latency, p99, p99.9.
    Rows = databases, columns = series labels.
    """
    if not rows:
        _plot_empty(
            path, "Performance Heatmap Dashboard",
            "No data available.",
        )
        return

    databases = sorted(
        {r.get("database") for r in rows if r.get("database")}
    )

    def _label(value: Any, fallback: str = "?") -> str:
        if value is None:
            return fallback
        text = str(value).strip()
        return text or fallback

    def _series_key(r: dict[str, Any]) -> str:
        parts = [_label(r.get("backend")), _label(r.get("series_label"), "unlabeled")]
        if r.get("thread_count") is not None:
            parts.append(f"{r['thread_count']}T")
        return "/".join(parts)

    series_labels = sorted({_series_key(r) for r in rows})
    if len(series_labels) > 16:
        series_labels = series_labels[:16]

    # Lookup table
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        key = (r.get("database", "?"), _series_key(r))
        if key[1] in series_labels:
            lookup[key] = r

    metrics_spec = [
        ("throughput_ops_sec", "Operations/sec", "RdYlGn", True),
        ("average_latency_ms", "Avg Latency (ms)", "RdYlGn_r", False),
        ("p99_latency_ms", "P99 Latency (ms)", "RdYlGn_r", False),
        ("p99_9_latency_ms", "P99.9 Latency (ms)", "RdYlGn_r", False),
    ]

    fig_w = max(12, len(series_labels) * 1.2 + 4)
    fig_h = max(7, len(databases) * 1.2 + 4)
    fig, axes = plt.subplots(2, 2, figsize=(fig_w, fig_h))

    for ax_idx, (metric_key, metric_title, cmap, _higher_better) in enumerate(
        metrics_spec
    ):
        r_idx, c_idx = divmod(ax_idx, 2)
        ax = axes[r_idx][c_idx]

        matrix = []
        for db in databases:
            row_vals = []
            for series in series_labels:
                r = lookup.get((db, series), {})
                val = r.get(metric_key)
                row_vals.append(
                    float(val)
                    if isinstance(val, (int, float))
                    else float("nan")
                )
            matrix.append(row_vals)

        data = np.array(matrix)
        masked = np.ma.masked_invalid(data)

        if masked.count() == 0:
            ax.set_title(metric_title, fontweight="bold", fontsize=10)
            ax.text(
                0.5, 0.5, "No data",
                ha="center", va="center", transform=ax.transAxes,
            )
            ax.set_xticks([])
            ax.set_yticks([])
            continue

        im = ax.imshow(masked, aspect="auto", cmap=cmap)
        ax.set_xticks(range(len(series_labels)))
        ax.set_xticklabels(
            series_labels, rotation=55, ha="right", fontsize=6,
        )
        ax.set_yticks(range(len(databases)))
        ax.set_yticklabels(databases, fontsize=9)
        ax.set_title(metric_title, fontweight="bold", fontsize=10)

        # Annotate cells
        for i in range(len(databases)):
            for j in range(len(series_labels)):
                val = data[i][j]
                if math.isnan(val):
                    text = "n/a"
                elif metric_key == "throughput_ops_sec":
                    text = _compact_ops(val)
                else:
                    text = f"{val:.2f}"
                # Choose text color for contrast
                tc = "white"
                ax.text(
                    j, i, text,
                    ha="center", va="center",
                    color=tc, fontsize=6, fontweight="bold",
                )

        fig.colorbar(im, ax=ax, shrink=0.8)

    fig.suptitle(
        "Performance Heatmap Dashboard",
        fontsize=14, fontweight="bold",
    )
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Existing: Radar Profile
# ---------------------------------------------------------------------------


def _database_profiles(
    rows: list[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    grouped: dict[str, dict[str, float]] = {}
    for row in rows:
        database = row.get("database") or "unknown"
        profile = grouped.setdefault(
            database,
            {
                "throughput": 0.0,
                "p50_latency_ms": math.inf,
                "p99_latency_ms": math.inf,
                "cache_hit_rate": 0.0,
                "memory_bytes": math.inf,
            },
        )
        throughput = row.get("throughput_ops_sec")
        if isinstance(throughput, (int, float)):
            profile["throughput"] = max(
                profile["throughput"], float(throughput)
            )
        p50 = row.get("p50_latency_ms")
        if isinstance(p50, (int, float)):
            profile["p50_latency_ms"] = min(
                profile["p50_latency_ms"], float(p50)
            )
        p99 = row.get("p99_latency_ms")
        if isinstance(p99, (int, float)):
            profile["p99_latency_ms"] = min(
                profile["p99_latency_ms"], float(p99)
            )
        hit_rate = row.get("cache_hit_rate")
        if isinstance(hit_rate, (int, float)):
            profile["cache_hit_rate"] = max(
                profile["cache_hit_rate"], float(hit_rate)
            )
        memory = (
            row.get("process_memory_after_bytes")
            or row.get("used_memory_rss_after_bytes")
            or row.get("used_memory_after_bytes")
        )
        if isinstance(memory, (int, float)):
            profile["memory_bytes"] = min(
                profile["memory_bytes"], float(memory)
            )
    return grouped


def _render_radar_chart(
    rows: list[dict[str, Any]], path: Path
) -> None:
    profiles = _database_profiles(rows)
    if not profiles:
        _plot_empty(
            path, "Radar Profile",
            "No throughput or latency data was available.",
        )
        return

    categories = [
        "Throughput", "P50 Latency", "P99 Latency",
        "Hit Rate", "Memory",
    ]
    values = list(profiles.values())
    max_throughput = max(p["throughput"] for p in values) or 1.0
    min_p50 = (
        min(
            p["p50_latency_ms"]
            for p in values
            if math.isfinite(p["p50_latency_ms"])
        )
        if any(math.isfinite(p["p50_latency_ms"]) for p in values)
        else 1.0
    )
    min_p99 = (
        min(
            p["p99_latency_ms"]
            for p in values
            if math.isfinite(p["p99_latency_ms"])
        )
        if any(math.isfinite(p["p99_latency_ms"]) for p in values)
        else 1.0
    )
    max_hit = max(p["cache_hit_rate"] for p in values) or 1.0
    min_memory = (
        min(
            p["memory_bytes"]
            for p in values
            if math.isfinite(p["memory_bytes"])
        )
        if any(math.isfinite(p["memory_bytes"]) for p in values)
        else 1.0
    )

    figure, axis = plt.subplots(
        figsize=(7, 7), subplot_kw={"projection": "polar"},
    )
    angles = [
        index / float(len(categories)) * 2 * math.pi
        for index in range(len(categories))
    ]
    angles += angles[:1]

    db_list = sorted(profiles.keys())
    for db_idx, database in enumerate(db_list):
        profile = profiles[database]
        scores = [
            _safe_divide(profile["throughput"], max_throughput),
            (
                _safe_divide(min_p50, profile["p50_latency_ms"])
                if math.isfinite(profile["p50_latency_ms"])
                else 0.0
            ),
            (
                _safe_divide(min_p99, profile["p99_latency_ms"])
                if math.isfinite(profile["p99_latency_ms"])
                else 0.0
            ),
            _safe_divide(profile["cache_hit_rate"], max_hit),
            (
                _safe_divide(min_memory, profile["memory_bytes"])
                if math.isfinite(profile["memory_bytes"])
                else 0.0
            ),
        ]
        scores += scores[:1]
        color = _get_db_color(db_idx)
        axis.plot(
            angles, scores, linewidth=2, label=database, color=color,
        )
        axis.fill(angles, scores, alpha=0.12, color=color)

    axis.set_xticks(angles[:-1])
    axis.set_xticklabels(categories)
    axis.set_yticks([0.25, 0.5, 0.75, 1.0])
    axis.set_yticklabels(["0.25", "0.50", "0.75", "1.00"])
    axis.set_title("Radar Profile")
    axis.legend(loc="upper right", bbox_to_anchor=(1.25, 1.1))
    figure.tight_layout()
    figure.savefig(path, dpi=180)
    plt.close(figure)


# ---------------------------------------------------------------------------
# Existing: Latency Heatmap
# ---------------------------------------------------------------------------


def _render_latency_heatmap(
    rows: list[dict[str, Any]], path: Path
) -> None:
    latency_rows = [
        row
        for row in rows
        if any(
            row.get(key) is not None
            for key in (
                "p50_latency_ms",
                "p99_latency_ms",
                "p99_9_latency_ms",
                "p99_999_latency_ms",
            )
        )
    ]
    if not latency_rows:
        _plot_empty(
            path, "Latency Heatmap",
            "No latency percentiles were available.",
        )
        return

    latency_rows = latency_rows[:16]
    columns = [
        "p50_latency_ms",
        "p99_latency_ms",
        "p99_9_latency_ms",
        "p99_999_latency_ms",
    ]
    labels = [
        f"{row['database']}/{row['backend']}/{row['series_label']}"
        + (
            f"/{row['thread_count']}t"
            if row.get("thread_count")
            else ""
        )
        for row in latency_rows
    ]
    matrix = []
    for row in latency_rows:
        matrix.append(
            [
                (
                    float(row[column])
                    if isinstance(row.get(column), (int, float))
                    else math.nan
                )
                for column in columns
            ]
        )

    figure_height = max(4.5, 0.45 * len(latency_rows) + 2.0)
    figure, axis = plt.subplots(figsize=(9, figure_height))
    image = axis.imshow(
        matrix, aspect="auto", cmap="viridis", norm=Normalize(vmin=0),
    )
    axis.set_xticks(range(len(columns)))
    axis.set_xticklabels(["P50", "P99", "P99.9", "P99.999"])
    axis.set_yticks(range(len(labels)))
    axis.set_yticklabels(labels)
    axis.set_title("Latency Heatmap (ms)")
    for row_index, values in enumerate(matrix):
        for column_index, value in enumerate(values):
            text = "n/a" if math.isnan(value) else f"{value:.3f}"
            axis.text(
                column_index, row_index, text,
                ha="center", va="center", color="white", fontsize=8,
            )
    figure.colorbar(image, ax=axis, label="milliseconds")
    figure.tight_layout()
    figure.savefig(path, dpi=180)
    plt.close(figure)


# ---------------------------------------------------------------------------
# Existing: Scaling Sweep
# ---------------------------------------------------------------------------


def _render_scaling_chart(
    rows: list[dict[str, Any]], path: Path
) -> None:
    scaling_rows = [
        row
        for row in rows
        if isinstance(row.get("thread_count"), int)
        and isinstance(row.get("throughput_ops_sec"), (int, float))
    ]
    if not scaling_rows:
        _plot_empty(
            path, "Scaling Sweep",
            "No thread sweep data was available.",
        )
        return

    databases = sorted(
        {r.get("database") for r in scaling_rows if r.get("database")}
    )
    db_colors = {db: _get_db_color(i) for i, db in enumerate(databases)}

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in scaling_rows:
        key = (
            f"{row['database']}/{row['backend']}"
            f"/{row['series_label']}"
        )
        grouped.setdefault(key, []).append(row)

    figure, axis = plt.subplots(figsize=(9, 5.5))
    for label, group_rows in sorted(grouped.items()):
        group_rows.sort(key=lambda row: row.get("thread_count") or 0)
        threads = [int(row["thread_count"]) for row in group_rows]
        throughput = [
            float(row["throughput_ops_sec"]) for row in group_rows
        ]
        db = group_rows[0].get("database", "?")
        color = db_colors.get(db, "#333333")
        axis.plot(
            threads, throughput,
            marker="o", linewidth=2, label=label, color=color,
        )
    axis.set_title("Scaling Sweep")
    axis.set_xlabel("Thread Count")
    axis.set_ylabel("Throughput (ops/sec)")
    axis.set_xticks(
        sorted(
            {int(row["thread_count"]) for row in scaling_rows}
        )
    )
    axis.grid(alpha=0.25)
    axis.legend(loc="best", fontsize=8)
    figure.tight_layout()
    figure.savefig(path, dpi=180)
    plt.close(figure)


def _render_latency_cdf(rows: list[dict[str, Any]], path: Path) -> None:
    selected = _select_representative_rows(
        rows,
        lambda row: row.get("backend") == "memtier_benchmark" and bool(row.get("backend_hdr_files")),
        max_rows=8,
    )
    if not selected:
        _plot_empty(path, "Latency CDF", "No memtier HDR percentile data available.")
        return

    figure, axis = plt.subplots(figsize=(10, 6))
    db_colors = _database_colors(selected)
    all_latencies_ms: list[float] = []
    plotted = 0
    for index, row in enumerate(selected):
        hdr_files = row.get("backend_hdr_files") or []
        hdr_path = None
        for raw_path in hdr_files:
            candidate = Path(str(raw_path))
            if candidate.name.endswith("FULL_RUN_1.txt") or (
                candidate.name.endswith(".txt") and "FULL_RUN" in candidate.name
            ):
                hdr_path = candidate
                break
        if hdr_path is None or not hdr_path.exists():
            continue
        points = _read_memtier_cdf_points(hdr_path)
        if not points:
            continue
        latencies = [point[0] for point in points if point[0] > 0]
        percentiles = [point[1] for point in points if point[0] > 0]
        if not latencies:
            continue
        all_latencies_ms.extend(latencies)
        database = str(row.get("database") or "unknown")
        color = db_colors.get(database, _get_db_color(index))
        axis.plot(
            percentiles,
            latencies,
            linewidth=2,
            label=_row_chart_label(row),
            color=color,
        )
        plotted += 1

    if plotted == 0:
        plt.close(figure)
        _plot_empty(path, "Latency CDF", "No readable memtier HDR percentile data available.")
        return

    unit, factor = _latency_unit(all_latencies_ms)
    for line in axis.lines:
        y_data = np.asarray(line.get_ydata(), dtype=float)
        line.set_ydata(y_data * factor)

    axis.set_title("Latency CDF")
    axis.set_xlabel("Cumulative Percentile")
    axis.set_ylabel(f"Latency ({unit})")
    axis.set_xlim(0, 100)
    axis.set_yscale("log")
    axis.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: f"{value:.0f}%"))
    axis.grid(alpha=0.3, which="both")
    axis.legend(fontsize=8, loc="upper left")
    figure.tight_layout()
    figure.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(figure)


def _render_latency_over_time(rows: list[dict[str, Any]], path: Path) -> None:
    selected = _select_representative_rows(
        rows,
        lambda row: row.get("backend") == "memtier_benchmark" and bool(row.get("backend_json_path")),
        max_rows=6,
    )
    if not selected:
        _plot_empty(path, "Latency Over Time", "No memtier latency time series available.")
        return

    loaded_series: list[tuple[dict[str, Any], list[dict[str, float]]]] = []
    avg_max_values: list[float] = []
    p99_max_values: list[float] = []
    for row in selected:
        json_path = Path(str(row.get("backend_json_path")))
        if not json_path.exists():
            continue
        _, series = _extract_memtier_latency_series(json_path)
        if not series:
            continue
        loaded_series.append((row, series))
        avg_max_values.extend(point["avg"] for point in series)
        p99_max_values.extend(point["p99"] for point in series)

    if not loaded_series:
        _plot_empty(path, "Latency Over Time", "No readable memtier latency time series available.")
        return

    avg_unit, avg_factor = _latency_unit(avg_max_values)
    p99_unit, p99_factor = _latency_unit(p99_max_values)
    db_colors = _database_colors([row for row, _series in loaded_series])

    figure, axes = plt.subplots(2, 1, figsize=(11, 8), sharex=True)
    avg_axis, p99_axis = axes
    plotted = 0
    for row, series in loaded_series:
        seconds = [point["second"] for point in series]
        avg_values = _scale_latency_values(
            [point["avg"] for point in series],
            avg_factor,
        )
        p99_values = _scale_latency_values(
            [point["p99"] for point in series],
            p99_factor,
        )
        label = _row_chart_label(row)
        database = str(row.get("database") or "unknown")
        color = db_colors.get(database, "#333333")
        avg_axis.plot(seconds, avg_values, linewidth=2, label=label, color=color)
        p99_axis.plot(seconds, p99_values, linewidth=2, label=label, color=color)
        plotted += 1

    if plotted == 0:
        plt.close(figure)
        _plot_empty(path, "Latency Over Time", "No readable memtier latency time series available.")
        return

    avg_axis.set_title("Average Latency Over Time")
    avg_axis.set_ylabel(f"Average Latency ({avg_unit})")
    avg_axis.grid(alpha=0.3)
    avg_axis.legend(fontsize=8, loc="best")

    p99_axis.set_title("P99 Latency Over Time")
    p99_axis.set_xlabel("Elapsed Time (s)")
    p99_axis.set_ylabel(f"P99 Latency ({p99_unit})")
    p99_axis.grid(alpha=0.3)
    p99_axis.legend(fontsize=8, loc="best")

    figure.tight_layout()
    figure.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(figure)


def _render_telemetry_metric(
    rows: list[dict[str, Any]],
    path: Path,
    *,
    metric_key: str,
    title: str,
    y_label: str,
) -> None:
    scenarios = _select_telemetry_scenarios(rows)
    if not scenarios:
        _plot_empty(path, title, "No host telemetry samples available for the selected workloads.")
        return

    figure, axes = plt.subplots(
        len(scenarios),
        1,
        figsize=(11, 3.6 * len(scenarios)),
        sharex=False,
    )
    axes = np.atleast_1d(axes)
    db_colors = _database_colors(
        [row for _scenario_title, scenario_rows in scenarios for row in scenario_rows]
    )
    plotted = 0

    for axis, (scenario_title, scenario_rows) in zip(axes, scenarios):
        scenario_plotted = 0
        for row in scenario_rows:
            samples_path = Path(str(row.get("host_telemetry_samples_path")))
            if not samples_path.exists():
                continue
            series = _extract_telemetry_series(samples_path)
            metric_points = series.get(metric_key) or []
            if not metric_points:
                continue
            database = str(row.get("database") or "unknown")
            axis.plot(
                [point[0] for point in metric_points],
                [point[1] for point in metric_points],
                linewidth=2,
                label=database,
                color=db_colors.get(database, "#333333"),
            )
            scenario_plotted += 1
            plotted += 1

        axis.set_title(scenario_title)
        axis.set_ylabel(y_label)
        axis.grid(alpha=0.3)
        if scenario_plotted:
            axis.legend(fontsize=8, loc="best", ncols=min(3, scenario_plotted))
        else:
            axis.text(
                0.5,
                0.5,
                "No readable samples available.",
                ha="center",
                va="center",
                transform=axis.transAxes,
            )

    if plotted == 0:
        plt.close(figure)
        _plot_empty(path, title, "No readable host telemetry samples available for the selected workloads.")
        return

    axes[-1].set_xlabel("Elapsed Time (s)")
    figure.suptitle(title)
    figure.tight_layout(rect=(0, 0, 1, 0.98))
    figure.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(figure)


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------


def render_report_charts(
    report_payload: dict[str, Any], assets_dir: Path
) -> dict[str, Path]:
    assets_dir.mkdir(parents=True, exist_ok=True)
    rows = report_payload.get("rows", [])
    paths: dict[str, Path] = {}

    # New rich charts
    tp_path = assets_dir / "throughput-comparison.png"
    _render_throughput_comparison(rows, tp_path)
    paths["throughput_comparison"] = tp_path

    lat_path = assets_dir / "latency-comparison.png"
    _render_latency_comparison(rows, lat_path)
    paths["latency_comparison"] = lat_path

    hm_path = assets_dir / "heatmap-dashboard.png"
    _render_heatmap_dashboard(rows, hm_path)
    paths["heatmap_dashboard"] = hm_path

    cdf_path = assets_dir / "latency-cdf.png"
    _render_latency_cdf(rows, cdf_path)
    paths["latency_cdf"] = cdf_path

    lot_path = assets_dir / "latency-over-time.png"
    _render_latency_over_time(rows, lot_path)
    paths["latency_over_time"] = lot_path

    cpu_path = assets_dir / "cpu-timeseries.png"
    _render_telemetry_metric(
        rows,
        cpu_path,
        metric_key="cpu",
        title="CPU Usage Over Time",
        y_label="System CPU (%)",
    )
    paths["cpu_timeseries"] = cpu_path

    rss_path = assets_dir / "rss-timeseries.png"
    _render_telemetry_metric(
        rows,
        rss_path,
        metric_key="rss_mib",
        title="Process RSS Over Time",
        y_label="RSS (MiB)",
    )
    paths["rss_timeseries"] = rss_path

    net_path = assets_dir / "network-timeseries.png"
    _render_telemetry_metric(
        rows,
        net_path,
        metric_key="net_tx_mib_per_sec",
        title="Loopback TX Throughput Over Time",
        y_label="TX Throughput (MiB/s)",
    )
    paths["network_timeseries"] = net_path

    # Existing charts
    radar_path = assets_dir / "radar-profile.png"
    _render_radar_chart(rows, radar_path)
    paths["radar_profile"] = radar_path

    heatmap_path = assets_dir / "latency-heatmap.png"
    _render_latency_heatmap(rows, heatmap_path)
    paths["latency_heatmap"] = heatmap_path

    scaling_path = assets_dir / "scaling-sweep.png"
    _render_scaling_chart(rows, scaling_path)
    paths["scaling_sweep"] = scaling_path

    return paths