from __future__ import annotations

import os
import platform
import re
import shlex
import socket
import subprocess
from pathlib import Path
from typing import Any, Optional

from vortex_benchmark.models import ServiceState, utc_now


def _run_output(command: list[str]) -> Optional[str]:
    try:
        result = subprocess.run(
            command,
            check=False,
            text=True,
            capture_output=True,
        )
    except FileNotFoundError:
        return None

    if result.returncode != 0:
        return None
    output = (result.stdout or result.stderr or "").strip()
    return output or None


def _parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _parse_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "yes", "true", "on"}:
        return True
    if text in {"0", "no", "false", "off"}:
        return False
    return None


def _detect_total_memory_bytes() -> Optional[int]:
    if platform.system() == "Darwin":
        output = _run_output(["sysctl", "-n", "hw.memsize"])
        return _parse_int(output)

    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if not line.startswith("MemTotal:"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                kib = _parse_int(parts[1])
                if kib is not None:
                    return kib * 1024
    return None


def _detect_cpu_model() -> Optional[str]:
    if platform.system() == "Darwin":
        return _run_output(["sysctl", "-n", "machdep.cpu.brand_string"])

    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.startswith("model name"):
                continue
            _, value = line.split(":", 1)
            return value.strip()

    return platform.processor() or None


def capture_host_metadata() -> dict[str, Any]:
    return {
        "captured_at": utc_now(),
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "os": platform.system(),
        "os_release": platform.release(),
        "architecture": platform.machine(),
        "python_version": platform.python_version(),
        "logical_cpus": os.cpu_count(),
        "cpu_model": _detect_cpu_model(),
        "total_memory_bytes": _detect_total_memory_bytes(),
    }


def _redis_cli(service: ServiceState, args: list[str]) -> Optional[str]:
    command = ["redis-cli", "-h", service.host, "-p", str(service.port), *args]
    try:
        result = subprocess.run(
            command,
            check=False,
            text=True,
            capture_output=True,
        )
    except FileNotFoundError:
        return None

    if result.returncode != 0:
        return None
    return result.stdout


def _parse_info_response(payload: str) -> dict[str, str]:
    info: dict[str, str] = {}
    for line in payload.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        info[key] = value
    return info


_MEMORY_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([kmgt]?i?b?)\s*$", re.IGNORECASE)


def _parse_memory_text(value: str) -> Optional[int]:
    match = _MEMORY_RE.match(value)
    if not match:
        return None

    amount = float(match.group(1))
    unit = match.group(2).lower()
    multipliers = {
        "b": 1,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "tb": 1000**4,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
        "": 1,
    }
    multiplier = multipliers.get(unit)
    if multiplier is None:
        return None
    return int(amount * multiplier)


def _capture_process_memory_bytes(service: ServiceState) -> Optional[int]:
    if service.pid:
        output = _run_output(["ps", "-o", "rss=", "-p", str(service.pid)])
        kib = _parse_int(output)
        if kib is not None:
            return kib * 1024

    if service.container_id:
        format_arg = "{{json .}}"
        output = _run_output(
            ["docker", "stats", "--no-stream", "--format", format_arg, service.container_id]
        )
        if output:
            match = re.search(r'"MemUsage":"([^"]+)"', output)
            if match:
                used = match.group(1).split("/")[0].strip()
                return _parse_memory_text(used)
    return None


def capture_service_snapshot(service: ServiceState) -> dict[str, Any]:
    raw_info = _redis_cli(service, ["INFO"])
    info = _parse_info_response(raw_info or "")
    return {
        "captured_at": utc_now(),
        "server_version": info.get("redis_version") or service.metadata.get("version"),
        "redis_mode": info.get("redis_mode"),
        "os": info.get("os"),
        "arch_bits": _parse_int(info.get("arch_bits")),
        "used_memory_bytes": _parse_int(info.get("used_memory")),
        "used_memory_rss_bytes": _parse_int(info.get("used_memory_rss")),
        "used_memory_peak_bytes": _parse_int(info.get("used_memory_peak")),
        "total_system_memory_bytes": _parse_int(info.get("total_system_memory")),
        "process_memory_bytes": _capture_process_memory_bytes(service),
        "keyspace_hits": _parse_int(info.get("keyspace_hits")),
        "keyspace_misses": _parse_int(info.get("keyspace_misses")),
        "evicted_keys": _parse_int(info.get("evicted_keys")),
        "expired_keys": _parse_int(info.get("expired_keys")),
        "total_commands_processed": _parse_int(info.get("total_commands_processed")),
        "instantaneous_ops_per_sec": _parse_float(info.get("instantaneous_ops_per_sec")),
        "aof_enabled": _parse_bool(info.get("aof_enabled")),
        "aof_current_size_bytes": _parse_int(info.get("aof_current_size")),
        "aof_base_size_bytes": _parse_int(info.get("aof_base_size")),
    }


def diff_service_snapshots(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    delta_fields = [
        "keyspace_hits",
        "keyspace_misses",
        "evicted_keys",
        "expired_keys",
        "total_commands_processed",
        "used_memory_bytes",
        "used_memory_rss_bytes",
        "used_memory_peak_bytes",
        "process_memory_bytes",
        "aof_current_size_bytes",
    ]
    delta: dict[str, Any] = {}
    for field in delta_fields:
        before_value = _parse_float(before.get(field))
        after_value = _parse_float(after.get(field))
        key = f"{field}_delta"
        if before_value is None or after_value is None:
            delta[key] = None
            continue
        value = after_value - before_value
        delta[key] = int(value) if value.is_integer() else value

    hits = _parse_float(delta.get("keyspace_hits_delta")) or 0.0
    misses = _parse_float(delta.get("keyspace_misses_delta")) or 0.0
    total = hits + misses
    if total > 0:
        delta["cache_hit_rate"] = hits / total
        delta["cache_efficiency"] = hits / total
        delta["cache_miss_rate"] = misses / total
    else:
        delta["cache_hit_rate"] = None
        delta["cache_efficiency"] = None
        delta["cache_miss_rate"] = None

    delta["aof_enabled_before"] = before.get("aof_enabled")
    delta["aof_enabled_after"] = after.get("aof_enabled")
    return delta