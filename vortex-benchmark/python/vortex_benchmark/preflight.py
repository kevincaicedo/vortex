from __future__ import annotations

import json
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Any

from vortex_benchmark.models import utc_now


LOW_OVERHEAD_LINUX_TOOLS = (
    "vmstat",
    "mpstat",
    "pidstat",
    "iostat",
    "sar",
    "ss",
    "nstat",
)
DEEP_LINUX_TOOLS = (
    "perf",
    "trace-cmd",
    "bpftrace",
    "runqlat",
    "biolatency",
    "offcputime",
)
MACOS_TOOLS = ("vm_stat", "iostat", "netstat", "sysctl", "xcrun", "samply")
BENCHMARK_TOOLS = ("redis-benchmark", "memtier_benchmark", "docker", "ssh", "scp")


def _tool_status(name: str) -> dict[str, Any]:
    path = shutil.which(name)
    return {
        "tool": name,
        "status": "available" if path else "missing",
        "path": path,
        "requires_root": name in {"perf", "trace-cmd", "bpftrace", "runqlat", "biolatency", "offcputime"},
    }


def _command_text(argv: list[str]) -> str | None:
    try:
        return subprocess.run(
            argv,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=2.0,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def build_preflight_summary(*, target_mode: str, profiler: bool = False) -> dict[str, Any]:
    system = platform.system().lower()
    tool_names = list(BENCHMARK_TOOLS)
    if profiler:
        tool_names.extend(DEEP_LINUX_TOOLS if system == "linux" else MACOS_TOOLS)
    else:
        tool_names.extend(LOW_OVERHEAD_LINUX_TOOLS if system == "linux" else MACOS_TOOLS)

    tools = [_tool_status(name) for name in dict.fromkeys(tool_names)]
    return {
        "schema_version": 1,
        "generated_at": utc_now(),
        "target_mode": target_mode,
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
        "tools": tools,
        "perf_event_paranoid": _read_text(Path("/proc/sys/kernel/perf_event_paranoid")),
        "ssh_version": _command_text(["ssh", "-V"]),
        "docker_version": _command_text(["docker", "--version"]),
    }


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def write_preflight(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path
