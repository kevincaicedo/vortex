"""Benchmark chart generation — radar, heatmap, scaling, and rich comparisons."""

from __future__ import annotations

import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import Normalize

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

    def _series_key(r: dict[str, Any]) -> str:
        parts = [r.get("backend", "?"), r.get("series_label", "?")]
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