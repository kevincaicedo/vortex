#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PERF_STAT_KEYS = (
    "task-clock",
    "context-switches",
    "cpu-migrations",
    "page-faults",
    "instructions",
    "cycles",
    "branch-misses",
    "L1-dcache-load-misses",
    "LLC-load-misses",
)
HOST_COMPARE_KEYS = (
    "system_cpu_utilization_avg_pct",
    "system_cpu_utilization_peak_pct",
    "system_cpu_iowait_avg_pct",
    "system_procs_running_peak",
    "system_procs_blocked_peak",
    "system_mem_available_min_bytes",
    "network_total_rx_bytes_delta",
    "network_total_tx_bytes_delta",
    "disk_write_bytes_delta",
    "disk_io_time_delta_ms",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a concise machine-readable profiler summary.")
    parser.add_argument("--session-dir", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--compare-to")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def find_first(session_dir: Path, pattern: str) -> Path | None:
    matches = sorted(session_dir.glob(pattern))
    return matches[0] if matches else None


def parse_perf_report(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []

    hotspots: list[dict[str, Any]] = []
    seen: set[str] = set()
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)%\s+(.*)$", line)
        if not match:
            continue
        percent = float(match.group(1))
        tail = match.group(2).strip()
        if not tail or tail.startswith("Children") or tail.startswith("Samples"):
            continue
        parts = tail.split()
        symbol = " ".join(parts[2:]) if len(parts) > 2 else tail
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        hotspots.append({"source": "perf-report", "symbol": symbol, "percent": percent})
        if len(hotspots) >= 10:
            break
    return hotspots


def parse_flamegraph(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []

    pattern = re.compile(r"<title>(.+?) \(([0-9,]+) samples, ([0-9.]+)%\)</title>")
    rows: list[tuple[float, str]] = []
    for symbol, _samples, percent in pattern.findall(path.read_text(encoding="utf-8", errors="replace")):
        if symbol == "all":
            continue
        rows.append((float(percent), symbol))
    rows.sort(reverse=True)

    hotspots: list[dict[str, Any]] = []
    seen: set[str] = set()
    for percent, symbol in rows:
        if symbol in seen:
            continue
        seen.add(symbol)
        hotspots.append({"source": "flamegraph", "symbol": symbol, "percent": percent})
        if len(hotspots) >= 10:
            break
    return hotspots


def parse_perf_stat(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []

    counters: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("<not counted>"):
            continue
        for key in PERF_STAT_KEYS:
            if key not in stripped:
                continue
            parts = stripped.split("#", 1)[0].split()
            if len(parts) < 2:
                continue
            counters.append({"metric": parts[1], "value": parts[0], "raw": stripped})
            break
    return counters


def parse_criterion(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []

    benchmarks: list[dict[str, Any]] = []
    regex = re.compile(r"^([A-Za-z0-9_:-]+)\s+time:\s+\[([^\]]+)\]$")
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = regex.match(line.strip())
        if not match:
            continue
        benchmarks.append({"name": match.group(1), "interval": match.group(2)})
    return benchmarks


def benchmark_artifacts(session_dir: Path) -> dict[str, list[str]]:
    bench_dir = session_dir / "bench"
    if not bench_dir.exists():
        return {"requests": [], "results": [], "reports": []}

    def collect(relative: str) -> list[str]:
        base = bench_dir / relative
        if not base.exists():
            return []
        return sorted(str(path.resolve()) for path in base.rglob("*") if path.is_file())

    return {
        "requests": collect("requests"),
        "results": collect("results"),
        "reports": collect("reports"),
    }


def build_summary_payload(session_dir: Path) -> dict[str, Any]:
    session = load_json(session_dir / "session.json")

    host_summary_path = find_first(session_dir, "host/*host-telemetry-summary.json")
    perf_report_path = find_first(session_dir, "perf-report.txt")
    perf_stat_path = find_first(session_dir, "perf-stat.txt")
    flamegraph_path = find_first(session_dir, "flamegraph.svg")
    criterion_path = find_first(session_dir, "criterion.log")

    top_hotspots = parse_perf_report(perf_report_path)
    if not top_hotspots:
        top_hotspots = parse_flamegraph(flamegraph_path)

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "session_id": session.get("session_id"),
        "session_dir": str(session_dir),
        "mode": session.get("mode"),
        "tool": session.get("tool"),
        "tools_requested": session.get("tools_requested") or [],
        "tools_executed": session.get("tools_executed") or [],
        "cargo_profile": session.get("cargo_profile"),
        "workload": session.get("workload") or {},
        "comparison_key": comparison_key(session),
        "top_hotspots": top_hotspots,
        "counter_highlights": parse_perf_stat(perf_stat_path),
        "criterion_benchmarks": parse_criterion(criterion_path),
        "host_context_summary": load_json(host_summary_path) if host_summary_path else {},
        "benchmark_artifacts": benchmark_artifacts(session_dir),
        "notes_path": session.get("notes_path"),
    }


def resolve_compare_input(path: Path) -> dict[str, Any] | None:
    if path.is_dir():
        summary_path = path / "summary.json"
        if summary_path.exists():
            return load_json(summary_path)
        session_path = path / "session.json"
        if session_path.exists():
            return build_summary_payload(path)
        return None

    if path.name == "summary.json" and path.exists():
        return load_json(path)

    if path.name == "session.json" and path.exists():
        return build_summary_payload(path.parent)

    return None


def comparison_key(session: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "mode": session.get("mode"),
        "tool": session.get("tool"),
        "tools_requested": session.get("tools_requested") or [],
        "cargo_profile": session.get("cargo_profile"),
        "workload_source": (session.get("workload") or {}).get("source"),
        "workload_description": (session.get("workload") or {}).get("description"),
    }
    payload["token"] = hashlib.sha1(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()[:12]
    return payload


def build_comparison(current: dict[str, Any], previous_payload: dict[str, Any]) -> dict[str, Any]:
    previous = previous_payload.get("session") or previous_payload
    previous_host = previous.get("host_context_summary") or {}
    current_host = current.get("host_context_summary") or {}

    current_hotspots = [row["symbol"] for row in current.get("top_hotspots") or []][:5]
    previous_hotspots = [row["symbol"] for row in previous.get("top_hotspots") or []][:5]

    host_deltas: dict[str, float] = {}
    for key in HOST_COMPARE_KEYS:
        curr_value = current_host.get(key)
        prev_value = previous_host.get(key)
        if isinstance(curr_value, (int, float)) and isinstance(prev_value, (int, float)):
            host_deltas[key] = float(curr_value) - float(prev_value)

    return {
        "compared_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "same_workload": current.get("comparison_key", {}).get("token") == previous.get("comparison_key", {}).get("token"),
        "previous_session": previous.get("session_id") or (previous.get("session") or {}).get("session_id"),
        "current_hotspots": current_hotspots,
        "previous_hotspots": previous_hotspots,
        "overlap_hotspots": [name for name in current_hotspots if name in previous_hotspots],
        "new_hotspots": [name for name in current_hotspots if name not in previous_hotspots],
        "resolved_hotspots": [name for name in previous_hotspots if name not in current_hotspots],
        "host_context_deltas": host_deltas,
    }


def main() -> int:
    args = parse_args()
    session_dir = Path(args.session_dir).expanduser().resolve()
    output_path = Path(args.output_path).expanduser().resolve()
    payload = build_summary_payload(session_dir)

    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    if args.compare_to:
        compare_payload = resolve_compare_input(Path(args.compare_to).expanduser().resolve())
        if compare_payload is not None:
            compare_output = session_dir / "summary-compare.json"
            compare_output.write_text(
                json.dumps(build_comparison(payload, compare_payload), indent=2) + "\n",
                encoding="utf-8",
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())