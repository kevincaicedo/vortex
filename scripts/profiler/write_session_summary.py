#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
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
    "branches",
    "branch-misses",
    "L1-dcache-loads",
    "L1-dcache-load-misses",
    "LLC-loads",
    "LLC-load-misses",
    "dTLB-loads",
    "dTLB-load-misses",
)
PERF_STAT_EVENT_NAMES = {
    "task-clock": "task_clock",
    "context-switches": "context_switches",
    "cpu-migrations": "cpu_migrations",
    "page-faults": "page_faults",
    "instructions": "instructions",
    "cycles": "cycles",
    "branches": "branches",
    "branch-misses": "branch_misses",
    "L1-dcache-loads": "l1d_loads",
    "L1-dcache-load-misses": "l1d_load_misses",
    "LLC-loads": "llc_loads",
    "LLC-load-misses": "llc_load_misses",
    "dTLB-loads": "dtlb_loads",
    "dTLB-load-misses": "dtlb_load_misses",
    "raw_syscalls:sys_enter": "syscalls",
    "uncore_imc_free_running/data_read": "memory_read_mib",
    "uncore_imc_free_running/data_write": "memory_write_mib",
    "uncore_imc_free_running/data_total": "memory_total_mib",
}
PERF_STAT_HIGHLIGHT_ORDER = (
    ("ipc", "ipc", "insn/cycle"),
    ("instructions_per_operation", "instructions_per_op", "insn/op"),
    ("cycles_per_operation", "cycles_per_op", "cycles/op"),
    ("branch_miss_rate_pct", "branch_miss_rate_pct", "%"),
    ("l1d_miss_rate_pct", "l1d_miss_rate_pct", "%"),
    ("llc_miss_rate_pct", "llc_miss_rate_pct", "%"),
    ("dtlb_miss_rate_pct", "dtlb_miss_rate_pct", "%"),
    ("frontend_bound_pct", "frontend_bound_pct", "%"),
    ("backend_bound_pct", "backend_bound_pct", "%"),
    ("retiring_pct", "retiring_pct", "%"),
    ("memory_total_mib_per_sec", "memory_bandwidth_mib_per_sec", "MiB/s"),
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


def resolve_path(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    return str(path.resolve())


def parse_text_preview(path: Path | None, *, max_lines: int = 20) -> list[str]:
    if path is None or not path.exists():
        return []

    lines: list[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.rstrip()
        if not stripped:
            continue
        lines.append(stripped)
        if len(lines) >= max_lines:
            break
    return lines


def parse_number(value: Any) -> float | None:
    if value is None:
        return None

    text = str(value).strip().replace(",", "")
    if not text or text == "<not counted>":
        return None

    try:
        return float(text)
    except ValueError:
        return None


def parse_key_value_text(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}

    payload: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        payload[key.strip()] = value.strip()
    return payload


def normalize_perf_event_name(event: str) -> str:
    name = event.strip().rstrip("/")
    for prefix in ("cpu_core/", "cpu_atom/"):
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    return PERF_STAT_EVENT_NAMES.get(name, name)


def normalize_perf_metric_name(description: str) -> tuple[str, str] | None:
    desc = description.strip()
    if not desc:
        return None

    if desc.startswith("%"):
        parts = desc.split()
        if len(parts) >= 2:
            return parts[-1], "%"

    alias_map = {
        "insn per cycle": ("ipc", "insn/cycle"),
        "of all branches": ("branch_miss_rate_pct", "%"),
        "of all L1-dcache accesses": ("l1d_miss_rate_pct", "%"),
        "of all LL-cache accesses": ("llc_miss_rate_pct", "%"),
        "of all dTLB cache accesses": ("dtlb_miss_rate_pct", "%"),
    }
    return alias_map.get(desc)


def parse_perf_stat(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {
            "format": None,
            "counters": {},
            "metrics": {},
            "not_counted_events": [],
            "running_pct_values": [],
        }

    text = path.read_text(encoding="utf-8", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return {
            "format": None,
            "counters": {},
            "metrics": {},
            "not_counted_events": [],
            "running_pct_values": [],
        }

    if any(line.count(",") >= 4 for line in lines):
        counters: dict[str, dict[str, Any]] = {}
        metrics: dict[str, dict[str, Any]] = {}
        not_counted_events: list[str] = []
        running_pct_values: list[float] = []

        for row in csv.reader(lines):
            if not row:
                continue
            padded = [cell.strip() for cell in row[:7]] + [""] * max(0, 7 - len(row))
            value_text, unit, event, _runtime_ms, running_pct_text, metric_value_text, metric_desc = padded[:7]

            if event:
                event_name = normalize_perf_event_name(event)
                value = parse_number(value_text)
                running_pct = parse_number(running_pct_text)
                if value is None:
                    if value_text.strip() == "<not counted>":
                        not_counted_events.append(event_name)
                else:
                    entry = counters.setdefault(
                        event_name,
                        {"value": 0.0, "unit": unit or None, "events": [], "running_pct_values": []},
                    )
                    entry["value"] += value
                    if unit and not entry.get("unit"):
                        entry["unit"] = unit
                    entry["events"].append(event)
                    if running_pct is not None:
                        entry["running_pct_values"].append(running_pct)
                        running_pct_values.append(running_pct)

            if metric_desc and metric_value_text:
                resolved = normalize_perf_metric_name(metric_desc)
                metric_value = parse_number(metric_value_text)
                if resolved is None or metric_value is None:
                    continue
                metric_name, metric_unit = resolved
                metrics[metric_name] = {
                    "value": metric_value,
                    "unit": metric_unit,
                    "raw": metric_desc,
                }

        return {
            "format": "csv",
            "counters": counters,
            "metrics": metrics,
            "not_counted_events": sorted(set(not_counted_events)),
            "running_pct_values": running_pct_values,
        }

    counters: dict[str, dict[str, Any]] = {}
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("<not counted>"):
            continue
        for key in PERF_STAT_KEYS:
            if key not in stripped:
                continue
            parts = stripped.split("#", 1)[0].split()
            if len(parts) < 2:
                continue
            value = parse_number(parts[0])
            if value is None:
                continue
            event_name = normalize_perf_event_name(parts[1])
            entry = counters.setdefault(event_name, {"value": 0.0, "unit": None, "events": [], "running_pct_values": []})
            entry["value"] += value
            entry["events"].append(parts[1])
            break

    return {
        "format": "legacy",
        "counters": counters,
        "metrics": {},
        "not_counted_events": [],
        "running_pct_values": [],
    }


def parse_engine_probe_summary(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}

    text = path.read_text(encoding="utf-8", errors="replace")
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}

    try:
        payload = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def parse_redis_benchmark_summary(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}

    text = path.read_text(encoding="utf-8", errors="replace")
    request_matches = re.findall(r"([0-9][0-9,]*) requests completed in ([0-9]+(?:\.[0-9]+)?) seconds", text)
    throughput_matches = re.findall(r"throughput summary:\s+([0-9]+(?:\.[0-9]+)?) requests per second", text)

    completed_rows = [int(count.replace(",", "")) for count, _ in request_matches]
    elapsed_rows = [float(seconds) for _, seconds in request_matches]
    throughput_rows = [float(value) for value in throughput_matches]

    return {
        "summary_count": max(len(completed_rows), len(throughput_rows)),
        "requests_completed": completed_rows[0] if completed_rows else None,
        "elapsed_seconds": elapsed_rows[0] if elapsed_rows else None,
        "throughput_ops_sec": throughput_rows[0] if throughput_rows else None,
        "all_completed_rows": completed_rows,
        "all_elapsed_rows": elapsed_rows,
        "all_throughput_rows": throughput_rows,
    }


def find_first_numeric_value(payload: Any, preferred_keys: tuple[str, ...]) -> float | None:
    if isinstance(payload, dict):
        for key in preferred_keys:
            value = payload.get(key)
            numeric = parse_number(value)
            if numeric is not None:
                return numeric
        for value in payload.values():
            numeric = find_first_numeric_value(value, preferred_keys)
            if numeric is not None:
                return numeric
        return None

    if isinstance(payload, list):
        for value in payload:
            numeric = find_first_numeric_value(value, preferred_keys)
            if numeric is not None:
                return numeric

    return None


def extract_workload_window(session_dir: Path, session: dict[str, Any]) -> dict[str, Any]:
    workload = session.get("workload") or {}
    capture_duration = parse_number(workload.get("duration_seconds"))

    engine_summary_path = find_first(session_dir, "engine-target-summary.json") or find_first(session_dir, "perf-stat.log")
    engine_summary = parse_engine_probe_summary(engine_summary_path)
    if engine_summary:
        return {
            "source": "engine-probe",
            "operations_completed": parse_number(engine_summary.get("operations")),
            "throughput_ops_sec": parse_number(engine_summary.get("throughput_ops_per_second")),
            "window_seconds": parse_number(engine_summary.get("duration_seconds")),
            "estimated": False,
            "note": None,
        }

    load_summary = parse_redis_benchmark_summary(find_first(session_dir, "load-perf-stat.log"))
    if load_summary:
        summary_count = int(load_summary.get("summary_count") or 0)
        throughput = parse_number(load_summary.get("throughput_ops_sec"))
        completed = parse_number(load_summary.get("requests_completed"))
        elapsed = parse_number(load_summary.get("elapsed_seconds"))
        note_parts: list[str] = []
        estimated = False
        operations = completed
        window_seconds = elapsed

        if capture_duration is not None:
            window_seconds = capture_duration
            if summary_count > 1:
                estimated = True
                note_parts.append(
                    "redis-benchmark selected multiple command sections; op-normalized metrics are estimated from the first summary over the capture window"
                )
                operations = throughput * capture_duration if throughput is not None else None
            elif elapsed is not None and elapsed > capture_duration * 1.10 and throughput is not None:
                estimated = True
                note_parts.append(
                    "redis-benchmark outlived the requested capture window; operations are estimated from the average throughput over the capture duration"
                )
                operations = throughput * capture_duration
            elif completed is None and throughput is not None:
                estimated = True
                note_parts.append("redis-benchmark summary omitted completed requests; operations are estimated from throughput")
                operations = throughput * capture_duration

        return {
            "source": "redis-benchmark",
            "operations_completed": operations,
            "throughput_ops_sec": throughput,
            "window_seconds": window_seconds,
            "estimated": estimated,
            "note": "; ".join(note_parts) or None,
        }

    results_dir = session_dir / "bench" / "results"
    if results_dir.exists():
        for path in sorted(results_dir.glob("*.json")):
            try:
                payload = load_json(path)
            except json.JSONDecodeError:
                continue
            throughput = find_first_numeric_value(payload, ("throughput_ops_sec", "ops_sec"))
            if throughput is None:
                continue
            operations = throughput * capture_duration if capture_duration is not None else None
            return {
                "source": "benchmark-results",
                "operations_completed": operations,
                "throughput_ops_sec": throughput,
                "window_seconds": capture_duration,
                "estimated": operations is not None,
                "note": f"derived from {path.name}",
            }

    return {
        "source": None,
        "operations_completed": None,
        "throughput_ops_sec": None,
        "window_seconds": capture_duration,
        "estimated": False,
        "note": None,
    }


def lookup_counter_value(counters: dict[str, dict[str, Any]], key: str) -> float | None:
    entry = counters.get(key)
    if entry is None:
        return None
    return parse_number(entry.get("value"))


def detect_thermal_degraded(context: dict[str, str]) -> bool | None:
    listing = (context.get("power_profile_listing") or "").lower()
    if not listing:
        return None
    match = re.search(r"degraded:\s*(yes|no)", listing)
    if match is None:
        return None
    return match.group(1) == "yes"


def build_perf_stat_reliability(
    parsed: dict[str, Any],
    context: dict[str, str],
    host_summary: dict[str, Any],
) -> dict[str, Any]:
    running_pct_values = [
        value for value in parsed.get("running_pct_values") or [] if value is not None and value > 0.0
    ]
    minimum_running_pct = min(running_pct_values) if running_pct_values else None
    perf_event_paranoid = parse_number(context.get("perf_event_paranoid"))
    cpu_count = parse_number(context.get("cpu_count")) or parse_number(host_summary.get("runtime_reactor_slots"))
    virtualization = context.get("virtualization") or "unknown"
    thermal_degraded = detect_thermal_degraded(context)
    loadavg_peak = parse_number(host_summary.get("system_loadavg_1_peak"))
    procs_running_peak = parse_number(host_summary.get("system_procs_running_peak"))

    warnings: list[str] = []
    permissions_limited = False
    multiplexed = minimum_running_pct is not None and minimum_running_pct < 100.0
    noisy_neighbor_pressure = False

    if multiplexed:
        warnings.append(
            f"PMU multiplexing detected: the least-scheduled counter ran for {minimum_running_pct:.2f}% of the capture window"
        )

    syscall_status = context.get("syscall_counter_status")
    syscall_note = context.get("syscall_counter_note")
    if syscall_status and syscall_status != "supported":
        permissions_limited = True
        warnings.append(f"Syscall count unavailable on this host: {syscall_note or 'no usable syscall tracepoint'}")

    memory_status = context.get("memory_bandwidth_status")
    memory_note = context.get("memory_bandwidth_note")
    if memory_status in {"unsupported", "unavailable"}:
        warnings.append(f"Memory bandwidth counters unavailable: {memory_note or 'host PMU does not expose a usable IMC event'}")

    if perf_event_paranoid is not None and perf_event_paranoid > 2:
        permissions_limited = True
        warnings.append(
            f"perf_event_paranoid={perf_event_paranoid:.0f} can hide kernel or per-process PMU detail"
        )

    if virtualization not in {"", "none", "unknown"}:
        warnings.append(f"Virtualization detected ({virtualization}); PMU counter fidelity may be reduced")

    if thermal_degraded is True:
        warnings.append("powerprofilesctl reports thermal degradation; PMU ratios may understate peak behavior")

    if cpu_count is not None:
        if loadavg_peak is not None and loadavg_peak > cpu_count * 1.25:
            noisy_neighbor_pressure = True
        if procs_running_peak is not None and procs_running_peak > cpu_count * 1.50:
            noisy_neighbor_pressure = True
    if noisy_neighbor_pressure:
        warnings.append(
            "host run-queue pressure exceeded local CPU capacity; noisy neighbors or scheduler contention may distort the PMU snapshot"
        )

    return {
        "warnings": warnings,
        "multiplexed": multiplexed,
        "minimum_running_pct": minimum_running_pct,
        "permissions_limited": permissions_limited,
        "virtualization": virtualization,
        "thermal_degraded": thermal_degraded,
        "noisy_neighbor_pressure": noisy_neighbor_pressure,
    }


def build_perf_stat_report(
    session_dir: Path,
    session: dict[str, Any],
    host_summary: dict[str, Any],
) -> dict[str, Any] | None:
    perf_stat_path = find_first(session_dir, "perf-stat.txt")
    counter_path = find_first(session_dir, "perf-stat-counters.txt")
    if perf_stat_path is None and counter_path is None:
        return None

    context_path = find_first(session_dir, "perf-stat-context.txt")
    counter_parsed = parse_perf_stat(counter_path or perf_stat_path)
    topdown_parsed = parse_perf_stat(perf_stat_path or counter_path)
    context = parse_key_value_text(context_path)
    workload_window = extract_workload_window(session_dir, session)
    counters = dict((counter_parsed.get("counters") or {}))
    metrics = dict((counter_parsed.get("metrics") or {}))
    for name, entry in (topdown_parsed.get("counters") or {}).items():
        counters.setdefault(name, entry)
    metrics.update(topdown_parsed.get("metrics") or {})

    instructions = lookup_counter_value(counters, "instructions")
    cycles = lookup_counter_value(counters, "cycles")
    branches = lookup_counter_value(counters, "branches")
    branch_misses = lookup_counter_value(counters, "branch_misses")
    l1d_loads = lookup_counter_value(counters, "l1d_loads")
    l1d_load_misses = lookup_counter_value(counters, "l1d_load_misses")
    llc_loads = lookup_counter_value(counters, "llc_loads")
    llc_load_misses = lookup_counter_value(counters, "llc_load_misses")
    dtlb_loads = lookup_counter_value(counters, "dtlb_loads")
    dtlb_load_misses = lookup_counter_value(counters, "dtlb_load_misses")
    operations_completed = parse_number(workload_window.get("operations_completed"))
    window_seconds = parse_number(workload_window.get("window_seconds"))
    memory_read_mib = lookup_counter_value(counters, "memory_read_mib")
    memory_write_mib = lookup_counter_value(counters, "memory_write_mib")
    memory_total_mib = lookup_counter_value(counters, "memory_total_mib")

    def ratio_pct(numerator: float | None, denominator: float | None) -> float | None:
        if numerator is None or denominator in {None, 0.0}:
            return None
        return numerator / denominator * 100.0

    def ratio_value(numerator: float | None, denominator: float | None) -> float | None:
        if numerator is None or denominator in {None, 0.0}:
            return None
        return numerator / denominator

    derived = {
        "ipc": ratio_value(instructions, cycles) or parse_number((metrics.get("ipc") or {}).get("value")),
        "cycles_per_operation": ratio_value(cycles, operations_completed),
        "instructions_per_operation": ratio_value(instructions, operations_completed),
        "branch_miss_rate_pct": ratio_pct(branch_misses, branches)
        or parse_number((metrics.get("branch_miss_rate_pct") or {}).get("value")),
        "l1d_miss_rate_pct": ratio_pct(l1d_load_misses, l1d_loads)
        or parse_number((metrics.get("l1d_miss_rate_pct") or {}).get("value")),
        "llc_miss_rate_pct": ratio_pct(llc_load_misses, llc_loads)
        or parse_number((metrics.get("llc_miss_rate_pct") or {}).get("value")),
        "dtlb_miss_rate_pct": ratio_pct(dtlb_load_misses, dtlb_loads)
        or parse_number((metrics.get("dtlb_miss_rate_pct") or {}).get("value")),
        "memory_read_mib_per_sec": ratio_value(memory_read_mib, window_seconds),
        "memory_write_mib_per_sec": ratio_value(memory_write_mib, window_seconds),
        "memory_total_mib_per_sec": ratio_value(memory_total_mib, window_seconds),
    }

    topdown = {
        "backend_bound_pct": parse_number((metrics.get("tma_backend_bound") or {}).get("value")),
        "frontend_bound_pct": parse_number((metrics.get("tma_frontend_bound") or {}).get("value")),
        "bad_speculation_pct": parse_number((metrics.get("tma_bad_speculation") or {}).get("value")),
        "retiring_pct": parse_number((metrics.get("tma_retiring") or {}).get("value")),
    }

    running_pct_values = (counter_parsed.get("running_pct_values") or []) + (topdown_parsed.get("running_pct_values") or [])
    not_counted_events = sorted(
        set((counter_parsed.get("not_counted_events") or []) + (topdown_parsed.get("not_counted_events") or []))
    )
    reliability = build_perf_stat_reliability(
        {
            "running_pct_values": running_pct_values,
            "not_counted_events": not_counted_events,
        },
        context,
        host_summary,
    )
    if (
        context.get("memory_bandwidth_status") == "supported"
        and memory_read_mib is None
        and memory_write_mib is None
        and memory_total_mib is None
    ):
        reliability.setdefault("warnings", []).append(
            "Memory bandwidth counters were available in the host probe but were not emitted for this attached session; perf could not attribute uncore IMC events to the target capture mode"
        )

    normalized_counters: dict[str, dict[str, Any]] = {}
    for name, entry in counters.items():
        normalized_counters[name] = {
            "value": parse_number(entry.get("value")),
            "unit": entry.get("unit"),
            "event_count": len(entry.get("events") or []),
        }

    report = {
        "raw_path": resolve_path(perf_stat_path),
        "counter_path": resolve_path(counter_path),
        "context_path": resolve_path(context_path),
        "capture_format": topdown_parsed.get("format") or counter_parsed.get("format"),
        "workload_window": workload_window,
        "counters": normalized_counters,
        "topdown": topdown,
        "derived": derived,
        "reliability": reliability,
        "context": {
            "perf_event_paranoid": context.get("perf_event_paranoid"),
            "kptr_restrict": context.get("kptr_restrict"),
            "cpu_governor": context.get("cpu_governor"),
            "cpu_scaling_driver": context.get("cpu_scaling_driver"),
            "energy_performance_preference": context.get("energy_performance_preference"),
            "power_profile": context.get("power_profile"),
            "virtualization": context.get("virtualization"),
            "syscall_counter_status": context.get("syscall_counter_status"),
            "memory_bandwidth_status": context.get("memory_bandwidth_status"),
        },
    }

    report_lines = format_perf_stat_report(report)
    report_path = session_dir / "perf-stat-report.txt"
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    report["report_path"] = str(report_path.resolve())
    report["report_preview"] = report_lines[:20]
    return report


def format_perf_stat_value(value: float | None, unit: str | None) -> str:
    if value is None:
        return "n/a"
    if unit == "%":
        return f"{value:.2f}%"
    if unit in {"MiB", "MiB/s", "insn/cycle", "insn/op", "cycles/op"}:
        return f"{value:.2f} {unit}"
    if float(value).is_integer():
        return f"{value:,.0f}" + (f" {unit}" if unit else "")
    return f"{value:.2f}" + (f" {unit}" if unit else "")


def format_perf_stat_report(report: dict[str, Any]) -> list[str]:
    workload = report.get("workload_window") or {}
    derived = report.get("derived") or {}
    topdown = report.get("topdown") or {}
    reliability = report.get("reliability") or {}
    counters = report.get("counters") or {}

    lines = [
        "PMU And Locality Summary",
        f"capture_format: {report.get('capture_format') or 'n/a'}",
        f"workload_source: {workload.get('source') or 'n/a'}",
    ]

    operations_completed = parse_number(workload.get("operations_completed"))
    operations_label = format_perf_stat_value(operations_completed, "ops")
    if workload.get("estimated"):
        operations_label = f"{operations_label} (estimated)"
    lines.append(f"operations_in_window: {operations_label}")
    lines.append(
        f"throughput_ops_sec: {format_perf_stat_value(parse_number(workload.get('throughput_ops_sec')), 'ops/s')}"
    )
    lines.append(f"window_seconds: {format_perf_stat_value(parse_number(workload.get('window_seconds')), 's')}")
    if workload.get("note"):
        lines.append(f"workload_note: {workload.get('note')}")

    lines.append(f"ipc: {format_perf_stat_value(derived.get('ipc'), 'insn/cycle')}")
    lines.append(
        f"instructions_per_op: {format_perf_stat_value(derived.get('instructions_per_operation'), 'insn/op')}"
    )
    lines.append(f"cycles_per_op: {format_perf_stat_value(derived.get('cycles_per_operation'), 'cycles/op')}")
    lines.append(f"branch_miss_rate: {format_perf_stat_value(derived.get('branch_miss_rate_pct'), '%')}")
    lines.append(f"l1d_miss_rate: {format_perf_stat_value(derived.get('l1d_miss_rate_pct'), '%')}")
    lines.append(f"llc_miss_rate: {format_perf_stat_value(derived.get('llc_miss_rate_pct'), '%')}")
    lines.append(f"dtlb_miss_rate: {format_perf_stat_value(derived.get('dtlb_miss_rate_pct'), '%')}")
    lines.append(f"frontend_bound: {format_perf_stat_value(topdown.get('frontend_bound_pct'), '%')}")
    lines.append(f"backend_bound: {format_perf_stat_value(topdown.get('backend_bound_pct'), '%')}")
    lines.append(f"bad_speculation: {format_perf_stat_value(topdown.get('bad_speculation_pct'), '%')}")
    lines.append(f"retiring: {format_perf_stat_value(topdown.get('retiring_pct'), '%')}")
    lines.append(
        f"memory_bandwidth_total: {format_perf_stat_value(derived.get('memory_total_mib_per_sec'), 'MiB/s')}"
    )
    lines.append(
        f"memory_bandwidth_read: {format_perf_stat_value(derived.get('memory_read_mib_per_sec'), 'MiB/s')}"
    )
    lines.append(
        f"memory_bandwidth_write: {format_perf_stat_value(derived.get('memory_write_mib_per_sec'), 'MiB/s')}"
    )
    lines.append(
        f"syscalls: {format_perf_stat_value(parse_number((counters.get('syscalls') or {}).get('value')), None)}"
    )

    warnings = reliability.get("warnings") or []
    if warnings:
        lines.append("reliability_warnings:")
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("reliability_warnings:")
        lines.append("- none")

    return lines


def build_counter_highlights(report: dict[str, Any] | None) -> list[dict[str, Any]]:
    if report is None:
        return []

    derived = report.get("derived") or {}
    topdown = report.get("topdown") or {}
    value_map = {
        "ipc": derived.get("ipc"),
        "instructions_per_operation": derived.get("instructions_per_operation"),
        "cycles_per_operation": derived.get("cycles_per_operation"),
        "branch_miss_rate_pct": derived.get("branch_miss_rate_pct"),
        "l1d_miss_rate_pct": derived.get("l1d_miss_rate_pct"),
        "llc_miss_rate_pct": derived.get("llc_miss_rate_pct"),
        "dtlb_miss_rate_pct": derived.get("dtlb_miss_rate_pct"),
        "frontend_bound_pct": topdown.get("frontend_bound_pct"),
        "backend_bound_pct": topdown.get("backend_bound_pct"),
        "retiring_pct": topdown.get("retiring_pct"),
        "memory_total_mib_per_sec": derived.get("memory_total_mib_per_sec"),
    }

    highlights: list[dict[str, Any]] = []
    for source_key, metric_name, unit in PERF_STAT_HIGHLIGHT_ORDER:
        value = value_map.get(source_key)
        if value is None:
            continue
        highlights.append(
            {
                "metric": metric_name,
                "value": round(float(value), 4),
                "unit": unit,
            }
        )
    return highlights


def collect_pmu_profiles(
    session_dir: Path,
    session: dict[str, Any],
    host_summary: dict[str, Any],
) -> dict[str, Any]:
    perf_stat_path = find_first(session_dir, "perf-stat.txt")
    perf_stat_counter_path = find_first(session_dir, "perf-stat-counters.txt")
    context_path = find_first(session_dir, "perf-stat-context.txt")
    report = build_perf_stat_report(session_dir, session, host_summary)

    return {
        "perf_stat_path": resolve_path(perf_stat_path),
        "perf_stat_preview": parse_text_preview(perf_stat_path, max_lines=40),
        "perf_stat_counter_path": resolve_path(perf_stat_counter_path),
        "perf_stat_counter_preview": parse_text_preview(perf_stat_counter_path, max_lines=40),
        "context_path": resolve_path(context_path),
        "context_preview": parse_text_preview(context_path, max_lines=40),
        "report_path": report.get("report_path") if report else None,
        "report_preview": report.get("report_preview") if report else [],
        "report": report,
        "counter_highlights": build_counter_highlights(report),
    }


def collect_memory_profiles(session_dir: Path) -> dict[str, Any]:
    heaptrack_path = find_first(session_dir, "heaptrack*.zst") or find_first(session_dir, "heaptrack*.gz")
    heaptrack_summary_path = find_first(session_dir, "heaptrack-summary.txt")
    massif_path = find_first(session_dir, "massif.out")
    massif_summary_path = find_first(session_dir, "massif-summary.txt")

    return {
        "heaptrack_data_path": resolve_path(heaptrack_path),
        "heaptrack_summary_path": resolve_path(heaptrack_summary_path),
        "heaptrack_summary_preview": parse_text_preview(heaptrack_summary_path),
        "massif_data_path": resolve_path(massif_path),
        "massif_summary_path": resolve_path(massif_summary_path),
        "massif_summary_preview": parse_text_preview(massif_summary_path),
    }


def collect_cache_profiles(session_dir: Path) -> dict[str, Any]:
    cachegrind_path = find_first(session_dir, "cachegrind.out")
    cachegrind_summary_path = find_first(session_dir, "cachegrind-summary.txt")
    callgrind_path = find_first(session_dir, "callgrind.out")
    callgrind_summary_path = find_first(session_dir, "callgrind-summary.txt")

    return {
        "cachegrind_data_path": resolve_path(cachegrind_path),
        "cachegrind_summary_path": resolve_path(cachegrind_summary_path),
        "cachegrind_summary_preview": parse_text_preview(cachegrind_summary_path),
        "callgrind_data_path": resolve_path(callgrind_path),
        "callgrind_summary_path": resolve_path(callgrind_summary_path),
        "callgrind_summary_preview": parse_text_preview(callgrind_summary_path),
    }


def collect_c2c_profiles(session_dir: Path) -> dict[str, Any]:
    data_path = find_first(session_dir, "perf-c2c.data")
    report_path = find_first(session_dir, "perf-c2c-report.txt")
    stats_path = find_first(session_dir, "perf-c2c-stats.txt")
    double_cl_path = find_first(session_dir, "perf-c2c-double-cl.txt")
    buildids_path = find_first(session_dir, "perf-buildids.txt")
    tool_check_path = find_first(session_dir, "c2c-tool-check.txt")
    context_path = find_first(session_dir, "c2c-context.txt")
    symbolization_path = find_first(session_dir, "c2c-symbolization.txt")
    layout_dir = session_dir / "layout-correlation"

    layout_paths: list[str] = []
    if layout_dir.exists():
        layout_paths = sorted(str(path.resolve()) for path in layout_dir.rglob("*.txt") if path.is_file())

    return {
        "perf_c2c_data_path": resolve_path(data_path),
        "perf_c2c_report_path": resolve_path(report_path),
        "perf_c2c_report_preview": parse_text_preview(report_path, max_lines=40),
        "perf_c2c_stats_path": resolve_path(stats_path),
        "perf_c2c_stats_preview": parse_text_preview(stats_path, max_lines=40),
        "perf_c2c_double_cl_path": resolve_path(double_cl_path),
        "perf_c2c_double_cl_preview": parse_text_preview(double_cl_path, max_lines=40),
        "perf_buildids_path": resolve_path(buildids_path),
        "perf_buildids_preview": parse_text_preview(buildids_path),
        "tool_check_path": resolve_path(tool_check_path),
        "tool_check_preview": parse_text_preview(tool_check_path),
        "context_path": resolve_path(context_path),
        "context_preview": parse_text_preview(context_path),
        "symbolization_path": resolve_path(symbolization_path),
        "symbolization_preview": parse_text_preview(symbolization_path),
        "layout_correlation_paths": layout_paths,
    }


def collect_lock_offcpu_profiles(session_dir: Path) -> dict[str, Any]:
    tool_check_path = find_first(session_dir, "lock-offcpu-tool-check.txt")
    context_path = find_first(session_dir, "lock-offcpu-context.txt")
    classification_path = find_first(session_dir, "lock-offcpu-classification.txt")
    platform_note_path = find_first(session_dir, "lock-offcpu-platform-note.txt")
    runqlat_path = find_first(session_dir, "bpf-runqlat.txt")
    biolatency_path = find_first(session_dir, "bpf-biolatency.txt")
    offcputime_path = find_first(session_dir, "bpf-offcputime.txt")
    offwaketime_path = find_first(session_dir, "bpf-offwaketime.txt")
    futex_path = find_first(session_dir, "bpf-futex.txt")
    sync_path = find_first(session_dir, "bpf-sync-syscalls.txt")
    perf_sched_data_path = find_first(session_dir, "perf-sched.data")
    perf_sched_latency_path = find_first(session_dir, "perf-sched-latency.txt")
    perf_sched_timehist_path = find_first(session_dir, "perf-sched-timehist.txt")

    return {
        "tool_check_path": resolve_path(tool_check_path),
        "tool_check_preview": parse_text_preview(tool_check_path),
        "context_path": resolve_path(context_path),
        "context_preview": parse_text_preview(context_path),
        "classification_path": resolve_path(classification_path),
        "classification_preview": parse_text_preview(classification_path),
        "platform_note_path": resolve_path(platform_note_path),
        "platform_note_preview": parse_text_preview(platform_note_path),
        "runqlat_path": resolve_path(runqlat_path),
        "runqlat_preview": parse_text_preview(runqlat_path, max_lines=40),
        "biolatency_path": resolve_path(biolatency_path),
        "biolatency_preview": parse_text_preview(biolatency_path, max_lines=40),
        "offcputime_path": resolve_path(offcputime_path),
        "offcputime_preview": parse_text_preview(offcputime_path, max_lines=40),
        "offwaketime_path": resolve_path(offwaketime_path),
        "offwaketime_preview": parse_text_preview(offwaketime_path, max_lines=40),
        "futex_path": resolve_path(futex_path),
        "futex_preview": parse_text_preview(futex_path, max_lines=40),
        "sync_syscalls_path": resolve_path(sync_path),
        "sync_syscalls_preview": parse_text_preview(sync_path, max_lines=40),
        "perf_sched_data_path": resolve_path(perf_sched_data_path),
        "perf_sched_latency_path": resolve_path(perf_sched_latency_path),
        "perf_sched_latency_preview": parse_text_preview(perf_sched_latency_path, max_lines=40),
        "perf_sched_timehist_path": resolve_path(perf_sched_timehist_path),
        "perf_sched_timehist_preview": parse_text_preview(perf_sched_timehist_path, max_lines=40),
    }


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
    flamegraph_path = find_first(session_dir, "flamegraph.svg")
    criterion_path = find_first(session_dir, "criterion.log")
    host_context_summary = load_json(host_summary_path) if host_summary_path else {}
    pmu_profiles = collect_pmu_profiles(session_dir, session, host_context_summary)

    top_hotspots = parse_perf_report(perf_report_path)
    if not top_hotspots:
        top_hotspots = parse_flamegraph(flamegraph_path)

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "session_id": session.get("session_id"),
        "session_dir": str(session_dir),
        "status": session.get("status"),
        "exit_code": session.get("exit_code"),
        "mode": session.get("mode"),
        "tool": session.get("tool"),
        "tool_role": session.get("tool_role"),
        "tools_requested": session.get("tools_requested") or [],
        "tools_executed": session.get("tools_executed") or [],
        "cargo_profile": session.get("cargo_profile"),
        "target_mode": session.get("target_mode"),
        "target": session.get("target") or {},
        "workload": session.get("workload") or {},
        "comparison_key": comparison_key(session),
        "top_hotspots": top_hotspots,
        "counter_highlights": pmu_profiles.get("counter_highlights") or [],
        "criterion_benchmarks": parse_criterion(criterion_path),
        "host_context_summary": host_context_summary,
        "memory_profiles": collect_memory_profiles(session_dir),
        "cache_profiles": collect_cache_profiles(session_dir),
        "pmu_profiles": pmu_profiles,
        "c2c_profiles": collect_c2c_profiles(session_dir),
        "lock_offcpu_profiles": collect_lock_offcpu_profiles(session_dir),
        "benchmark_artifacts": benchmark_artifacts(session_dir),
        "notes_path": session.get("notes_path"),
        "artifact_paths": session.get("artifact_paths") or [],
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
