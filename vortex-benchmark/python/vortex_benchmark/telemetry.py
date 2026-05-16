from __future__ import annotations

import json
import os
import platform
import re
import shlex
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Optional

from vortex_benchmark.models import ServiceState, sanitize_identifier, utc_now


def _run_output(command: list[str], *, cwd: Optional[Path] = None) -> Optional[str]:
    try:
        result = subprocess.run(
            command,
            check=False,
            text=True,
            capture_output=True,
            cwd=cwd,
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


def _read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return None


def _detect_cpu_governor() -> Optional[str]:
    return _read_text(Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"))


def _detect_cpu_scaling_driver() -> Optional[str]:
    return _read_text(Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_driver"))


def _detect_energy_performance_preference() -> Optional[str]:
    return _read_text(
        Path("/sys/devices/system/cpu/cpu0/cpufreq/energy_performance_preference")
    )


def _detect_perf_event_paranoid() -> Optional[int]:
    return _parse_int(_read_text(Path("/proc/sys/kernel/perf_event_paranoid")))


def _detect_pmu_available(perf_event_paranoid: Optional[int]) -> Optional[bool]:
    if perf_event_paranoid is None:
        return None
    return perf_event_paranoid <= 2


def _detect_power_profile() -> dict[str, Any]:
    profile = _run_output(["powerprofilesctl", "get"])
    listing = _run_output(["powerprofilesctl", "list"])
    degraded: Optional[bool] = None

    if listing:
        for line in listing.splitlines():
            if "degraded:" not in line.lower():
                continue
            _, value = line.split(":", 1)
            text = value.strip().lower()
            if text.startswith("yes"):
                degraded = True
            elif text.startswith("no"):
                degraded = False
            break

    return {
        "profile": profile,
        "degraded": degraded,
        "source": "powerprofilesctl" if profile or listing else None,
    }


def _normalize_power_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower().replace("_", "-")
    return text or None


def _infer_effective_cpu_power_mode(
    *,
    governor: Optional[str],
    scaling_driver: Optional[str],
    energy_preference: Optional[str],
    power_profile: Optional[str],
) -> Optional[str]:
    gov = _normalize_power_value(governor)
    driver = _normalize_power_value(scaling_driver)
    epp = _normalize_power_value(energy_preference)
    profile = _normalize_power_value(power_profile)

    if profile == "performance" or epp == "performance" or gov == "performance":
        return "performance"
    if profile in {"power-saver", "power-save"}:
        return "power-saver"
    if profile == "balanced":
        return "balanced"
    if epp in {"balance-performance", "balanced-performance"}:
        return "balanced-performance"
    if epp in {"balance-power", "balanced-power", "power"}:
        return "power-saver"
    if gov == "powersave":
        return "intel-pstate-powersave" if driver == "intel-pstate" else "powersave"
    return gov or profile or epp


def _detect_git_revision(repo_root: Optional[Path]) -> dict[str, Any]:
    if repo_root is None:
        return {"revision": None, "dirty": None}

    revision = _run_output(["git", "rev-parse", "HEAD"], cwd=repo_root)
    dirty_output = _run_output(["git", "status", "--porcelain"], cwd=repo_root)
    dirty = None if dirty_output is None else bool(dirty_output.strip())
    return {"revision": revision, "dirty": dirty}


def _service_validity_record(service: ServiceState) -> dict[str, Any]:
    runtime = dict(service.runtime_config or {})
    metadata = dict(service.metadata or {})
    io_backend_requested = runtime.get("io_backend")
    io_backend_effective = _infer_vortex_effective_backend(service, io_backend_requested)
    return {
        "database": service.database,
        "mode": service.mode,
        "external": service.external,
        "pid": service.pid,
        "container_id": service.container_id,
        "threads": (service.resource_config or {}).get("threads"),
        "service_cpus": (service.resource_config or {}).get("service_cpus"),
        "load_cpus": (service.resource_config or {}).get("load_cpus"),
        "bind": metadata.get("bind"),
        "binary": metadata.get("binary"),
        "version": metadata.get("version"),
        "command": metadata.get("command"),
        "io_backend": runtime.get("io_backend"),
        "io_backend_requested": io_backend_requested,
        "io_backend_effective": io_backend_effective,
        "telemetry_mode": runtime.get("telemetry_mode"),
        "ring_size": runtime.get("ring_size"),
        "fixed_buffers": runtime.get("fixed_buffers"),
        "fixed_buffer_registration": runtime.get("fixed_buffer_registration"),
        "sqpoll_idle_ms": runtime.get("sqpoll_idle_ms"),
        "shard_count": runtime.get("shard_count") or metadata.get("shard_count"),
    }


def _infer_vortex_effective_backend(
    service: ServiceState, requested: Any
) -> Optional[str]:
    if service.database != "vortex":
        return None

    requested_text = str(requested or "auto")
    command = str((service.metadata or {}).get("command") or "")
    log_text = _read_text(Path(service.log_path)) or ""

    if requested_text in {"polling", "uring"}:
        return requested_text
    if "backend_effective=\"polling\"" in log_text or "backend_effective=polling" in log_text:
        return "polling"
    if "backend_effective=\"io_uring\"" in log_text or "backend_effective=io_uring" in log_text:
        return "io_uring"
    if "--io-backend polling" in command or "io_backend=polling" in log_text:
        return "polling"
    if "--io-backend uring" in command or "io_backend=uring" in log_text:
        return "uring"
    if "falling back to polling" in log_text:
        return "polling"
    if "io_backend=auto" in log_text and "reactor pool started" in log_text:
        return "uring"
    return "unknown"


def capture_run_validity(
    *,
    services: list[ServiceState],
    repo_root: Optional[Path],
    evidence_tier: str = "engineering",
    requested_repeat_count: int = 1,
    aggregates_multiple_replicates: bool = False,
) -> dict[str, Any]:
    power = _detect_power_profile()
    cpu_governor = _detect_cpu_governor()
    cpu_scaling_driver = _detect_cpu_scaling_driver()
    energy_preference = _detect_energy_performance_preference()
    power_profile = power.get("profile")
    perf_event_paranoid = _detect_perf_event_paranoid()
    return {
        "captured_at": utc_now(),
        "evidence_tier": evidence_tier,
        "requested_repeat_count": requested_repeat_count,
        "aggregates_multiple_replicates": aggregates_multiple_replicates,
        "host": {
            "cpu_governor": cpu_governor,
            "cpu_scaling_driver": cpu_scaling_driver,
            "energy_performance_preference": energy_preference,
            "power_profile": power_profile,
            "effective_cpu_power_mode": _infer_effective_cpu_power_mode(
                governor=cpu_governor,
                scaling_driver=cpu_scaling_driver,
                energy_preference=energy_preference,
                power_profile=power_profile,
            ),
            "thermal_degraded": power.get("degraded"),
            "thermal_source": power.get("source"),
            "perf_event_paranoid": perf_event_paranoid,
            "pmu_available": _detect_pmu_available(perf_event_paranoid),
        },
        "git": _detect_git_revision(repo_root),
        "services": [_service_validity_record(service) for service in services],
    }


def _read_linux_proc_stat() -> Optional[dict[str, Any]]:
    stat_path = Path("/proc/stat")
    if not stat_path.exists():
        return None

    cpu_fields: list[int] | None = None
    per_cpu_fields: list[list[int]] = []
    payload: dict[str, Any] = {}
    text = _read_text(stat_path)
    if text is None:
        return None

    for line in text.splitlines():
        if line.startswith("cpu "):
            parts = line.split()[1:]
            cpu_fields = [int(part) for part in parts]
        elif re.match(r"^cpu\d+ ", line):
            parts = line.split()[1:]
            per_cpu_fields.append([int(part) for part in parts])
        elif line.startswith("ctxt "):
            payload["context_switches"] = _parse_int(line.split()[1])
        elif line.startswith("procs_running "):
            payload["procs_running"] = _parse_int(line.split()[1])
        elif line.startswith("procs_blocked "):
            payload["procs_blocked"] = _parse_int(line.split()[1])

    if cpu_fields is None:
        return None
    payload["cpu_fields"] = cpu_fields
    payload["per_cpu_fields"] = per_cpu_fields
    return payload


def _read_linux_meminfo() -> dict[str, Optional[int]]:
    meminfo_path = Path("/proc/meminfo")
    fields = {
        "MemAvailable": None,
        "MemFree": None,
        "Dirty": None,
        "Writeback": None,
        "Slab": None,
        "SReclaimable": None,
        "SUnreclaim": None,
        "Active(file)": None,
        "Inactive(file)": None,
        "SwapFree": None,
        "SwapTotal": None,
    }
    text = _read_text(meminfo_path)
    if text is None:
        return {key.lower(): None for key in fields}

    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        if key not in fields:
            continue
        parts = value.strip().split()
        kib = _parse_int(parts[0]) if parts else None
        fields[key] = None if kib is None else kib * 1024

    return {
        "mem_available_bytes": fields["MemAvailable"],
        "mem_free_bytes": fields["MemFree"],
        "mem_dirty_bytes": fields["Dirty"],
        "mem_writeback_bytes": fields["Writeback"],
        "mem_slab_bytes": fields["Slab"],
        "mem_slab_reclaimable_bytes": fields["SReclaimable"],
        "mem_slab_unreclaimable_bytes": fields["SUnreclaim"],
        "mem_active_file_bytes": fields["Active(file)"],
        "mem_inactive_file_bytes": fields["Inactive(file)"],
        "swap_free_bytes": fields["SwapFree"],
        "swap_total_bytes": fields["SwapTotal"],
    }


def _read_linux_vmstat() -> dict[str, Optional[int]]:
    text = _read_text(Path("/proc/vmstat"))
    fields = {
        "pgfault": None,
        "pgmajfault": None,
        "pgscan_kswapd": None,
        "pgscan_direct": None,
        "pgsteal_kswapd": None,
        "pgsteal_direct": None,
        "pswpin": None,
        "pswpout": None,
        "allocstall": None,
        "pgreclaim": None,
        "pgactivate": None,
        "pgdeactivate": None,
    }
    if text is None:
        return {
            "vmstat_page_faults": None,
            "vmstat_major_page_faults": None,
            "vmstat_page_scan_kswapd": None,
            "vmstat_page_scan_direct": None,
            "vmstat_page_steal_kswapd": None,
            "vmstat_page_steal_direct": None,
            "vmstat_swap_in": None,
            "vmstat_swap_out": None,
            "vmstat_allocstall": None,
            "vmstat_page_reclaim": None,
            "vmstat_page_activate": None,
            "vmstat_page_deactivate": None,
        }

    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        key, raw_value = parts
        if key not in fields:
            continue
        fields[key] = _parse_int(raw_value)

    return {
        "vmstat_page_faults": fields["pgfault"],
        "vmstat_major_page_faults": fields["pgmajfault"],
        "vmstat_page_scan_kswapd": fields["pgscan_kswapd"],
        "vmstat_page_scan_direct": fields["pgscan_direct"],
        "vmstat_page_steal_kswapd": fields["pgsteal_kswapd"],
        "vmstat_page_steal_direct": fields["pgsteal_direct"],
        "vmstat_swap_in": fields["pswpin"],
        "vmstat_swap_out": fields["pswpout"],
        "vmstat_allocstall": fields["allocstall"],
        "vmstat_page_reclaim": fields["pgreclaim"],
        "vmstat_page_activate": fields["pgactivate"],
        "vmstat_page_deactivate": fields["pgdeactivate"],
    }


def _read_linux_loadavg() -> dict[str, Any]:
    text = _read_text(Path("/proc/loadavg"))
    if text is None:
        return {
            "loadavg_1": None,
            "loadavg_5": None,
            "loadavg_15": None,
            "runnable_entities": None,
            "total_entities": None,
        }

    parts = text.split()
    runnable = None
    total = None
    if len(parts) >= 4 and "/" in parts[3]:
        left, right = parts[3].split("/", 1)
        runnable = _parse_int(left)
        total = _parse_int(right)

    return {
        "loadavg_1": _parse_float(parts[0]) if len(parts) > 0 else None,
        "loadavg_5": _parse_float(parts[1]) if len(parts) > 1 else None,
        "loadavg_15": _parse_float(parts[2]) if len(parts) > 2 else None,
        "runnable_entities": runnable,
        "total_entities": total,
    }


def _read_linux_net_dev() -> dict[str, Optional[int]]:
    text = _read_text(Path("/proc/net/dev"))
    if text is None:
        return {
            "network_total_rx_bytes": None,
            "network_total_tx_bytes": None,
            "network_total_rx_errors": None,
            "network_total_tx_errors": None,
            "network_loopback_rx_bytes": None,
            "network_loopback_tx_bytes": None,
        }

    total_rx_bytes = 0
    total_tx_bytes = 0
    total_rx_errors = 0
    total_tx_errors = 0
    loopback_rx_bytes = 0
    loopback_tx_bytes = 0
    saw_interface = False

    for line in text.splitlines()[2:]:
        if ":" not in line:
            continue
        name, data = line.split(":", 1)
        iface = name.strip()
        fields = data.split()
        if len(fields) < 16:
            continue
        saw_interface = True
        rx_bytes = _parse_int(fields[0]) or 0
        rx_errors = _parse_int(fields[2]) or 0
        tx_bytes = _parse_int(fields[8]) or 0
        tx_errors = _parse_int(fields[10]) or 0
        total_rx_bytes += rx_bytes
        total_tx_bytes += tx_bytes
        total_rx_errors += rx_errors
        total_tx_errors += tx_errors
        if iface == "lo":
            loopback_rx_bytes += rx_bytes
            loopback_tx_bytes += tx_bytes

    if not saw_interface:
        return {
            "network_total_rx_bytes": None,
            "network_total_tx_bytes": None,
            "network_total_rx_errors": None,
            "network_total_tx_errors": None,
            "network_loopback_rx_bytes": None,
            "network_loopback_tx_bytes": None,
        }

    return {
        "network_total_rx_bytes": total_rx_bytes,
        "network_total_tx_bytes": total_tx_bytes,
        "network_total_rx_errors": total_rx_errors,
        "network_total_tx_errors": total_tx_errors,
        "network_loopback_rx_bytes": loopback_rx_bytes,
        "network_loopback_tx_bytes": loopback_tx_bytes,
    }


def _discover_block_devices() -> set[str]:
    sys_block = Path("/sys/block")
    if not sys_block.exists():
        return set()

    ignored_prefixes = ("loop", "ram", "zram", "fd", "dm-", "md")
    return {
        entry.name
        for entry in sys_block.iterdir()
        if entry.is_dir() and not entry.name.startswith(ignored_prefixes)
    }


def _read_linux_diskstats() -> dict[str, Optional[int]]:
    text = _read_text(Path("/proc/diskstats"))
    block_devices = _discover_block_devices()
    if text is None or not block_devices:
        return {
            "disk_read_bytes": None,
            "disk_write_bytes": None,
            "disk_io_time_ms": None,
            "disk_io_in_progress": None,
        }

    read_sectors = 0
    write_sectors = 0
    io_time_ms = 0
    io_in_progress = 0
    saw_device = False

    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 14:
            continue
        name = parts[2]
        if name not in block_devices:
            continue
        saw_device = True
        read_sectors += _parse_int(parts[5]) or 0
        write_sectors += _parse_int(parts[9]) or 0
        io_in_progress += _parse_int(parts[11]) or 0
        io_time_ms += _parse_int(parts[12]) or 0

    if not saw_device:
        return {
            "disk_read_bytes": None,
            "disk_write_bytes": None,
            "disk_io_time_ms": None,
            "disk_io_in_progress": None,
        }

    sector_size = 512
    return {
        "disk_read_bytes": read_sectors * sector_size,
        "disk_write_bytes": write_sectors * sector_size,
        "disk_io_time_ms": io_time_ms,
        "disk_io_in_progress": io_in_progress,
    }


def _read_linux_tcp_stats() -> dict[str, Optional[int]]:
    """Read TCP retransmit and listen overflow counters from /proc/net/snmp
    and /proc/net/netstat.

    Key metrics (Systems Performance ch. 10):
    - RetransSegs: total TCP retransmits (indicates congestion/packet loss)
    - ListenOverflows: SYN queue overflows (server too slow to accept())
    - ListenDrops: connections dropped due to full accept queue
    - TCPSynRetrans: SYN retransmits (connection establishment failures)
    """
    result: dict[str, Optional[int]] = {
        "tcp_retrans_segs": None,
        "tcp_in_segs": None,
        "tcp_out_segs": None,
        "tcp_active_opens": None,
        "tcp_passive_opens": None,
        "tcp_listen_overflows": None,
        "tcp_listen_drops": None,
        "tcp_syn_retrans": None,
    }

    # /proc/net/snmp — TCP retransmits and segment counts
    snmp_text = _read_text(Path("/proc/net/snmp"))
    if snmp_text is not None:
        lines = snmp_text.splitlines()
        for i in range(0, len(lines) - 1, 2):
            if not lines[i].startswith("Tcp:"):
                continue
            headers = lines[i].split()
            values = lines[i + 1].split()
            if len(headers) != len(values):
                continue
            snmp_map = dict(zip(headers[1:], values[1:]))
            result["tcp_retrans_segs"] = _parse_int(snmp_map.get("RetransSegs"))
            result["tcp_in_segs"] = _parse_int(snmp_map.get("InSegs"))
            result["tcp_out_segs"] = _parse_int(snmp_map.get("OutSegs"))
            result["tcp_active_opens"] = _parse_int(snmp_map.get("ActiveOpens"))
            result["tcp_passive_opens"] = _parse_int(snmp_map.get("PassiveOpens"))
            break

    # /proc/net/netstat — ListenOverflows, ListenDrops, TCPSynRetrans
    netstat_text = _read_text(Path("/proc/net/netstat"))
    if netstat_text is not None:
        lines = netstat_text.splitlines()
        for i in range(0, len(lines) - 1, 2):
            if not lines[i].startswith("TcpExt:"):
                continue
            headers = lines[i].split()
            values = lines[i + 1].split()
            if len(headers) != len(values):
                continue
            netstat_map = dict(zip(headers[1:], values[1:]))
            result["tcp_listen_overflows"] = _parse_int(netstat_map.get("ListenOverflows"))
            result["tcp_listen_drops"] = _parse_int(netstat_map.get("ListenDrops"))
            result["tcp_syn_retrans"] = _parse_int(netstat_map.get("TCPSynRetrans"))
            break

    return result


def _read_linux_socket_queue(port: int) -> dict[str, Optional[int]]:
    """Read socket queue depths from /proc/net/tcp for a specific port.

    Returns recv-Q and send-Q depths for the listening socket, which are
    key indicators of accept() backpressure (Systems Performance ch. 10).
    """
    result: dict[str, Optional[int]] = {
        "socket_recv_q": None,
        "socket_send_q": None,
    }
    if port <= 0:
        return result

    hex_port = f"{port:04X}"
    tcp_text = _read_text(Path("/proc/net/tcp"))
    if tcp_text is None:
        return result

    for line in tcp_text.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 5:
            continue
        local_addr = fields[1]
        if not local_addr.endswith(f":{hex_port}"):
            continue
        # State 0A = LISTEN
        state = fields[3]
        if state != "0A":
            continue
        queue_fields = fields[4]
        if ":" in queue_fields:
            tx_q, rx_q = queue_fields.split(":", 1)
            result["socket_recv_q"] = int(rx_q, 16)
            result["socket_send_q"] = int(tx_q, 16)
        break

    return result

def _read_linux_process_stat(pid: int) -> dict[str, Optional[int]]:
    text = _read_text(Path(f"/proc/{pid}/stat"))
    if text is None:
        return {
            "process_minor_faults": None,
            "process_major_faults": None,
            "process_utime_ticks": None,
            "process_stime_ticks": None,
            "process_rss_bytes": None,
        }

    right_paren = text.rfind(")")
    if right_paren == -1:
        return {
            "process_minor_faults": None,
            "process_major_faults": None,
            "process_utime_ticks": None,
            "process_stime_ticks": None,
            "process_rss_bytes": None,
        }

    fields = text[right_paren + 2 :].split()
    if len(fields) < 22:
        return {
            "process_minor_faults": None,
            "process_major_faults": None,
            "process_utime_ticks": None,
            "process_stime_ticks": None,
            "process_rss_bytes": None,
        }

    page_size = os.sysconf("SC_PAGE_SIZE")
    rss_pages = _parse_int(fields[21])
    return {
        "process_minor_faults": _parse_int(fields[7]),
        "process_major_faults": _parse_int(fields[9]),
        "process_utime_ticks": _parse_int(fields[11]),
        "process_stime_ticks": _parse_int(fields[12]),
        "process_rss_bytes": None if rss_pages is None else rss_pages * page_size,
    }


def _read_linux_process_status(pid: int) -> dict[str, Optional[int]]:
    text = _read_text(Path(f"/proc/{pid}/status"))
    if text is None:
        return {
            "process_voluntary_ctx_switches": None,
            "process_nonvoluntary_ctx_switches": None,
        }

    values = {
        "voluntary_ctxt_switches": None,
        "nonvoluntary_ctxt_switches": None,
    }
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        if key not in values:
            continue
        values[key] = _parse_int(value.strip())

    return {
        "process_voluntary_ctx_switches": values["voluntary_ctxt_switches"],
        "process_nonvoluntary_ctx_switches": values["nonvoluntary_ctxt_switches"],
    }


def _read_linux_process_io(pid: int) -> dict[str, Optional[int]]:
    text = _read_text(Path(f"/proc/{pid}/io"))
    if text is None:
        return {
            "process_read_bytes": None,
            "process_write_bytes": None,
            "process_cancelled_write_bytes": None,
            "process_syscr": None,
            "process_syscw": None,
        }

    values = {
        "read_bytes": None,
        "write_bytes": None,
        "cancelled_write_bytes": None,
        "syscr": None,
        "syscw": None,
    }
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        if key not in values:
            continue
        values[key] = _parse_int(value.strip())

    return {
        "process_read_bytes": values["read_bytes"],
        "process_write_bytes": values["write_bytes"],
        "process_cancelled_write_bytes": values["cancelled_write_bytes"],
        "process_syscr": values["syscr"],
        "process_syscw": values["syscw"],
    }


def _read_latency_latest(service: ServiceState) -> dict[str, dict[str, Optional[int]]]:
    payload = _redis_cli(service, ["--raw", "LATENCY", "LATEST"])
    if payload is None:
        return {}

    lines = [line.strip() for line in payload.splitlines() if line.strip()]
    if len(lines) < 4:
        return {}

    events: dict[str, dict[str, Optional[int]]] = {}
    for index in range(0, len(lines) - 3, 4):
        name = lines[index]
        if not name:
            continue
        events[name] = {
            "latest_timestamp": _parse_int(lines[index + 1]),
            "latest_latency_ms": _parse_int(lines[index + 2]),
            "max_latency_ms": _parse_int(lines[index + 3]),
        }
    return events


class HostTelemetryCollector:
    def __init__(
        self,
        output_dir: Path,
        *,
        label: str,
        service: Optional[ServiceState] = None,
        interval_seconds: float = 1.0,
    ) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.label = sanitize_identifier(label)
        self.service = service
        self.interval_seconds = interval_seconds
        self.samples_path = self.output_dir / f"{self.label}-host-telemetry.jsonl"
        self.summary_path = self.output_dir / f"{self.label}-host-telemetry-summary.json"
        self._samples: list[dict[str, Any]] = []
        self._prev_cpu_fields: Optional[list[int]] = None
        self._prev_per_cpu_fields: Optional[list[list[int]]] = None
        self._prev_process_cpu: Optional[tuple[float, int]] = None
        self._started_monotonic: Optional[float] = None
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._runtime_snapshot_captured = False
        self._runtime_snapshot_successes = 0
        self._last_runtime_snapshot_index = -1
        self._runtime_snapshot_interval_samples = max(1, int(round(5.0 / interval_seconds)))

    @property
    def supported(self) -> bool:
        return platform.system() == "Linux"

    def start(self) -> "HostTelemetryCollector":
        self._started_monotonic = time.monotonic()
        self.samples_path.write_text("", encoding="utf-8")
        if not self.supported:
            return self

        self._append_sample()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> dict[str, Any]:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(2.0, self.interval_seconds * 2))
        if self.supported:
            self._append_sample()

        summary = self._build_summary()
        self.summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        return {
            "supported": self.supported,
            "samples_path": str(self.samples_path),
            "summary_path": str(self.summary_path),
            "summary": summary,
        }

    def _run_loop(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            self._append_sample()

    def _append_sample(self) -> None:
        sample = self._capture_sample()
        self._samples.append(sample)
        with self.samples_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(sample) + "\n")

    def _capture_sample(self) -> dict[str, Any]:
        sample: dict[str, Any] = {
            "captured_at": utc_now(),
            "system_cpu_utilization_pct": None,
            "system_cpu_user_pct": None,
            "system_cpu_system_pct": None,
            "system_cpu_iowait_pct": None,
            "process_cpu_utilization_pct": None,
        }

        proc_stat = _read_linux_proc_stat() if self.supported else None
        if proc_stat is not None:
            cpu_fields = proc_stat.get("cpu_fields")
            per_cpu_fields = proc_stat.get("per_cpu_fields") or []
            if cpu_fields and self._prev_cpu_fields is not None:
                total_delta = sum(cpu_fields) - sum(self._prev_cpu_fields)
                idle_delta = (cpu_fields[3] + cpu_fields[4]) - (
                    self._prev_cpu_fields[3] + self._prev_cpu_fields[4]
                )
                if total_delta > 0:
                    sample["system_cpu_utilization_pct"] = (
                        (total_delta - idle_delta) / total_delta
                    ) * 100.0
                    sample["system_cpu_user_pct"] = (
                        (cpu_fields[0] - self._prev_cpu_fields[0]) / total_delta
                    ) * 100.0
                    sample["system_cpu_system_pct"] = (
                        (cpu_fields[2] - self._prev_cpu_fields[2]) / total_delta
                    ) * 100.0
                    sample["system_cpu_iowait_pct"] = (
                        (cpu_fields[4] - self._prev_cpu_fields[4]) / total_delta
                    ) * 100.0

            # Per-CPU utilization — compute max and list of per-core percentages
            if per_cpu_fields and self._prev_per_cpu_fields is not None:
                per_cpu_pcts: list[float] = []
                for idx, cur in enumerate(per_cpu_fields):
                    if idx >= len(self._prev_per_cpu_fields):
                        break
                    prev = self._prev_per_cpu_fields[idx]
                    cpu_total = sum(cur) - sum(prev)
                    cpu_idle = (cur[3] + cur[4]) - (prev[3] + prev[4])
                    if cpu_total > 0:
                        per_cpu_pcts.append(
                            ((cpu_total - cpu_idle) / cpu_total) * 100.0
                        )
                if per_cpu_pcts:
                    sample["per_cpu_max_pct"] = max(per_cpu_pcts)
                    sample["per_cpu_min_pct"] = min(per_cpu_pcts)
                    sample["per_cpu_count"] = len(per_cpu_pcts)

            self._prev_cpu_fields = cpu_fields
            self._prev_per_cpu_fields = per_cpu_fields
            sample["system_context_switches"] = proc_stat.get("context_switches")
            sample["system_procs_running"] = proc_stat.get("procs_running")
            sample["system_procs_blocked"] = proc_stat.get("procs_blocked")

        sample.update(_read_linux_meminfo() if self.supported else {})
        sample.update(_read_linux_vmstat() if self.supported else {})
        sample.update(_read_linux_loadavg() if self.supported else {})
        sample.update(_read_linux_net_dev() if self.supported else {})
        sample.update(_read_linux_diskstats() if self.supported else {})

        # TCP retransmit, listen-drop, and socket-queue telemetry
        if self.supported:
            sample.update(_read_linux_tcp_stats())
            port = 0
            if self.service is not None and self.service.port:
                port = self.service.port
            if port > 0:
                sample.update(_read_linux_socket_queue(port))

        # Service-side runtime counters are sampled once the server becomes
        # reachable and once more at shutdown. This keeps INFO traffic off the
        # steady-state hot path while avoiding the start-up race where the
        # sampler begins before the server is ready.
        if self.service is not None:
            should_try_runtime = self._runtime_snapshot_successes < 2 or self._stop_event.is_set()
            if (
                not should_try_runtime
                and self._last_runtime_snapshot_index >= 0
                and len(self._samples) - self._last_runtime_snapshot_index
                >= self._runtime_snapshot_interval_samples
            ):
                should_try_runtime = True
            if should_try_runtime:
                runtime_snapshot = _capture_runtime_snapshot(self.service)
                sample.update(runtime_snapshot)
                if _runtime_snapshot_available(runtime_snapshot):
                    self._runtime_snapshot_captured = True
                    self._runtime_snapshot_successes += 1
                    self._last_runtime_snapshot_index = len(self._samples)

        if self.service is not None and self.service.pid and self.supported:
            proc_sample = {}
            proc_sample.update(_read_linux_process_stat(self.service.pid))
            proc_sample.update(_read_linux_process_status(self.service.pid))
            proc_sample.update(_read_linux_process_io(self.service.pid))
            sample.update(proc_sample)

            utime_ticks = proc_sample.get("process_utime_ticks")
            stime_ticks = proc_sample.get("process_stime_ticks")
            if utime_ticks is not None and stime_ticks is not None:
                total_ticks = utime_ticks + stime_ticks
                now_monotonic = time.monotonic()
                if self._prev_process_cpu is not None:
                    elapsed = now_monotonic - self._prev_process_cpu[0]
                    tick_delta = total_ticks - self._prev_process_cpu[1]
                    if elapsed > 0 and tick_delta >= 0:
                        hz = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
                        sample["process_cpu_utilization_pct"] = (
                            (tick_delta / hz) / elapsed
                        ) * 100.0
                self._prev_process_cpu = (now_monotonic, total_ticks)

        return sample

    def _build_summary(self) -> dict[str, Any]:
        duration_seconds = None
        if self._started_monotonic is not None:
            duration_seconds = time.monotonic() - self._started_monotonic

        if not self.supported:
            return {
                "supported": False,
                "platform": platform.system(),
                "reason": "host telemetry collector currently supports Linux procfs only",
                "sample_interval_seconds": self.interval_seconds,
                "sample_count": 0,
                "duration_seconds": duration_seconds,
            }

        def values(key: str) -> list[float]:
            collected: list[float] = []
            for sample in self._samples:
                value = sample.get(key)
                if isinstance(value, (int, float)):
                    collected.append(float(value))
            return collected

        invalid_delta_fields: list[str] = []

        def delta(key: str) -> Optional[float]:
            first = next(
                (sample.get(key) for sample in self._samples if sample.get(key) is not None),
                None,
            )
            last = next(
                (sample.get(key) for sample in reversed(self._samples) if sample.get(key) is not None),
                None,
            )
            return _snapshot_delta(key, first, last, invalid_delta_fields)

        def avg(key: str) -> Optional[float]:
            series = values(key)
            if not series:
                return None
            return sum(series) / len(series)

        def peak(key: str) -> Optional[float]:
            series = values(key)
            if not series:
                return None
            return max(series)

        def minimum(key: str) -> Optional[float]:
            series = values(key)
            if not series:
                return None
            return min(series)

        runtime_summary: dict[str, Any] = {
            "runtime_reactor_slots": peak("runtime_reactor_slots")
        }
        for field in _RUNTIME_DELTA_FIELDS:
            runtime_summary[f"{field}_delta"] = delta(field)
            runtime_summary[f"{field}_peak"] = peak(field)
        for field in _RUNTIME_FLOAT_FIELDS:
            runtime_summary[f"{field}_peak"] = peak(field)

        return {
            "supported": True,
            "platform": "Linux",
            "sample_interval_seconds": self.interval_seconds,
            "sample_count": len(self._samples),
            "duration_seconds": duration_seconds,
            "system_cpu_utilization_avg_pct": avg("system_cpu_utilization_pct"),
            "system_cpu_utilization_peak_pct": peak("system_cpu_utilization_pct"),
            "per_cpu_utilization_peak_pct": peak("per_cpu_max_pct"),
            "per_cpu_utilization_floor_pct": minimum("per_cpu_min_pct"),
            "system_cpu_iowait_avg_pct": avg("system_cpu_iowait_pct"),
            "system_procs_running_peak": peak("system_procs_running"),
            "system_procs_blocked_peak": peak("system_procs_blocked"),
            "system_loadavg_1_peak": peak("loadavg_1"),
            "system_mem_available_min_bytes": minimum("mem_available_bytes"),
            "system_mem_dirty_peak_bytes": peak("mem_dirty_bytes"),
            "system_mem_writeback_peak_bytes": peak("mem_writeback_bytes"),
            "system_mem_active_file_peak_bytes": peak("mem_active_file_bytes"),
            "system_mem_inactive_file_peak_bytes": peak("mem_inactive_file_bytes"),
            "system_mem_slab_reclaimable_peak_bytes": peak("mem_slab_reclaimable_bytes"),
            "system_swap_free_min_bytes": minimum("swap_free_bytes"),
            "system_vm_page_faults_delta": delta("vmstat_page_faults"),
            "system_vm_major_page_faults_delta": delta("vmstat_major_page_faults"),
            "system_vm_page_scan_kswapd_delta": delta("vmstat_page_scan_kswapd"),
            "system_vm_page_scan_direct_delta": delta("vmstat_page_scan_direct"),
            "system_vm_page_steal_kswapd_delta": delta("vmstat_page_steal_kswapd"),
            "system_vm_page_steal_direct_delta": delta("vmstat_page_steal_direct"),
            "system_vm_swap_in_delta": delta("vmstat_swap_in"),
            "system_vm_swap_out_delta": delta("vmstat_swap_out"),
            "system_vm_allocstall_delta": delta("vmstat_allocstall"),
            "system_vm_page_reclaim_delta": delta("vmstat_page_reclaim"),
            "process_cpu_utilization_avg_pct": avg("process_cpu_utilization_pct"),
            "process_cpu_utilization_peak_pct": peak("process_cpu_utilization_pct"),
            "process_rss_peak_bytes": peak("process_rss_bytes"),
            "process_minor_faults_delta": delta("process_minor_faults"),
            "process_major_faults_delta": delta("process_major_faults"),
            "process_read_bytes_delta": delta("process_read_bytes"),
            "process_write_bytes_delta": delta("process_write_bytes"),
            "process_cancelled_write_bytes_delta": delta("process_cancelled_write_bytes"),
            "process_syscr_delta": delta("process_syscr"),
            "process_syscw_delta": delta("process_syscw"),
            "process_voluntary_ctx_switches_delta": delta("process_voluntary_ctx_switches"),
            "process_nonvoluntary_ctx_switches_delta": delta("process_nonvoluntary_ctx_switches"),
            "network_total_rx_bytes_delta": delta("network_total_rx_bytes"),
            "network_total_tx_bytes_delta": delta("network_total_tx_bytes"),
            "network_total_rx_errors_delta": delta("network_total_rx_errors"),
            "network_total_tx_errors_delta": delta("network_total_tx_errors"),
            "network_loopback_rx_bytes_delta": delta("network_loopback_rx_bytes"),
            "network_loopback_tx_bytes_delta": delta("network_loopback_tx_bytes"),
            "tcp_retrans_segs_delta": delta("tcp_retrans_segs"),
            "tcp_listen_overflows_delta": delta("tcp_listen_overflows"),
            "tcp_listen_drops_delta": delta("tcp_listen_drops"),
            "tcp_syn_retrans_delta": delta("tcp_syn_retrans"),
            "socket_recv_q_peak": peak("socket_recv_q"),
            "socket_send_q_peak": peak("socket_send_q"),
            "disk_read_bytes_delta": delta("disk_read_bytes"),
            "disk_write_bytes_delta": delta("disk_write_bytes"),
            "disk_io_time_delta_ms": delta("disk_io_time_ms"),
            "disk_io_in_progress_peak": peak("disk_io_in_progress"),
            "invalid_delta_fields": sorted(set(invalid_delta_fields)),
            **runtime_summary,
        }


def start_host_telemetry_capture(
    output_dir: Path,
    *,
    label: str,
    service: Optional[ServiceState] = None,
    interval_seconds: float = 1.0,
) -> HostTelemetryCollector:
    return HostTelemetryCollector(
        output_dir,
        label=label,
        service=service,
        interval_seconds=interval_seconds,
    ).start()


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


_RUNTIME_INT_FIELDS = [
    "runtime_profile_timers_available",
    "runtime_local_flush_metrics_available",
    "backend_plan_mixed",
    "backend_fixed_buffers_capable",
    "backend_fixed_buffers_registered",
    "backend_sqpoll",
    "backend_multishot_accept",
    "backend_accept4",
    "backend_close_opcode",
    "backend_cancel_support",
    "backend_nonblocking_drain",
    "backend_requested_ring_size",
    "backend_effective_ring_size",
    "backend_submit_syscalls",
    "backend_sq_occupancy_max",
    "backend_sq_capacity",
    "backend_cq_occupancy_max",
    "backend_cq_capacity",
    "backend_cq_overflows",
    "backend_completions_per_submit_syscall_x1000",
    "backend_sq_pressure_events",
    "backend_cq_pressure_events",
    "runtime_reactor_slots",
    "reactor_loop_iterations",
    "reactor_accept_eagain_rearms",
    "reactor_accept_drain_runs",
    "reactor_accept_drain_accepted",
    "reactor_accept_drain_accepted_max",
    "reactor_submit_sq_full_retries",
    "reactor_submit_failures",
    "reactor_completion_budget_exhaustions",
    "reactor_command_budget_exhaustions",
    "reactor_accept_budget_exhaustions",
    "reactor_writev_budget_exhaustions",
    "reactor_maintenance_budget_exhaustions",
    "reactor_yielded_connections",
    "reactor_parser_resumes",
    "reactor_completion_batches",
    "reactor_completion_batch_total",
    "reactor_completion_batch_max",
    "reactor_completion_nanos_total",
    "reactor_completion_nanos_max",
    "reactor_command_batches",
    "reactor_command_batch_total",
    "reactor_command_batch_max",
    "reactor_writev_chunks",
    "reactor_writev_iovecs_total",
    "reactor_writev_iovecs_max",
    "reactor_queued_response_bytes_total",
    "reactor_queued_response_bytes_max",
    "reactor_client_retained_bytes",
    "reactor_client_retained_bytes_max",
    "reactor_client_retained_bytes_peak",
    "reactor_request_cap_exceeded",
    "reactor_response_cap_exceeded",
    "reactor_multi_queue_command_cap_exceeded",
    "reactor_multi_queue_bytes_cap_exceeded",
    "reactor_watch_cap_exceeded",
    "reactor_writev_chunk_cap_exceeded",
    "reactor_overload_accept_throttled",
    "reactor_overload_accept_resumed",
    "reactor_overload_read_disabled",
    "reactor_overload_read_resumed",
    "reactor_overload_command_deferred",
    "reactor_overload_command_resumed",
    "reactor_overload_connections_dropped",
    "reactor_overload_pending_response_bytes",
    "reactor_overload_pending_response_bytes_peak",
    "reactor_overload_parser_accumulator_bytes",
    "reactor_overload_parser_accumulator_bytes_peak",
    "reactor_overload_writev_backlog_bytes",
    "reactor_overload_writev_backlog_bytes_peak",
    "reactor_overload_aof_pending_bytes",
    "reactor_overload_aof_pending_bytes_peak",
    "reactor_overload_maintenance_debt",
    "reactor_overload_maintenance_debt_peak",
    "reactor_overload_read_disabled_connections",
    "reactor_overload_read_disabled_connections_peak",
    "reactor_overload_deferred_commands",
    "reactor_overload_deferred_commands_peak",
    "reactor_close_drain_nanos_total",
    "reactor_close_drain_nanos_max",
    "reactor_active_expiry_runs",
    "reactor_active_expiry_sampled",
    "reactor_active_expiry_expired",
    "reactor_active_expiry_nanos_total",
    "reactor_active_expiry_nanos_max",
    "reactor_aof_append_nanos_total",
    "reactor_aof_append_nanos_max",
    "reactor_aof_fsync_nanos_total",
    "reactor_aof_fsync_nanos_max",
    "reactor_aof_pending_bytes",
    "reactor_aof_pending_bytes_max",
    "reactor_aof_pending_writes",
    "reactor_aof_pending_writes_max",
    "reactor_aof_fsync_requested",
    "reactor_aof_fsync_completed",
    "reactor_aof_fsync_failed",
    "reactor_aof_fsync_worker_saturation",
    "reactor_aof_backpressure_events",
    "reactor_aof_backpressure_nanos_total",
    "reactor_aof_backpressure_nanos_max",
    "reactor_aof_last_appended_lsn",
    "reactor_aof_last_durable_lsn",
    "reactor_aof_durable_lsn_lag",
    "reactor_aof_fsync_latency_nanos_total",
    "reactor_aof_fsync_latency_nanos_max",
    "reactor_aof_fsync_latency_le_100us",
    "reactor_aof_fsync_latency_le_500us",
    "reactor_aof_fsync_latency_le_1ms",
    "reactor_aof_fsync_latency_le_5ms",
    "reactor_aof_fsync_latency_le_10ms",
    "reactor_aof_fsync_latency_le_50ms",
    "reactor_aof_fsync_latency_le_100ms",
    "reactor_aof_fsync_latency_gt_100ms",
    "reactor_maintenance_nanos_total",
    "reactor_maintenance_nanos_max",
    "reactor_metrics_flush_nanos_total",
    "reactor_metrics_flush_nanos_max",
    "eviction_admissions",
    "eviction_shards_scanned",
    "eviction_slots_sampled",
    "eviction_bytes_freed",
    "eviction_oom_after_scan",
    "eviction_nanos_total",
    "eviction_nanos_max",
    "shared_nothing_accepted_remote",
    "shared_nothing_remote_backpressure",
    "shared_nothing_reply_backpressure",
    "shared_nothing_deferred_replies",
    "shared_nothing_wakeups_sent",
    "shared_nothing_wakeup_failures",
    "shared_nothing_aggregates_accepted",
    "shared_nothing_aggregate_width_max",
    "shared_nothing_txns_accepted",
    "shared_nothing_txn_prepare_messages",
    "shared_nothing_txn_commit_messages",
    "shared_nothing_txn_abort_messages",
    "shared_nothing_txn_condition_aborts",
    "shared_nothing_txn_conflict_aborts",
    "shared_nothing_prepared_key_waits",
    "shared_nothing_prepared_key_retries",
    "shared_nothing_prepared_key_wait_nanos_total",
    "shared_nothing_prepared_key_wait_nanos_max",
]

_RUNTIME_FLOAT_FIELDS = [
    "reactor_completion_batch_avg",
    "reactor_command_batch_avg",
]

_RUNTIME_STRING_FIELDS = [
    "runtime_telemetry_mode",
    "backend_requested",
    "backend_effective",
]

_RUNTIME_DELTA_FIELDS = [
    field
    for field in _RUNTIME_INT_FIELDS
    if field
    not in {
        "runtime_reactor_slots",
        "runtime_profile_timers_available",
        "runtime_local_flush_metrics_available",
    }
]

_MONOTONIC_DELTA_FIELDS = {
    "keyspace_hits",
    "keyspace_misses",
    "evicted_keys",
    "expired_keys",
    "total_commands_processed",
    "backend_submit_syscalls",
    "backend_cq_overflows",
    "backend_sq_pressure_events",
    "backend_cq_pressure_events",
    "reactor_loop_iterations",
    "reactor_accept_eagain_rearms",
    "reactor_accept_drain_runs",
    "reactor_accept_drain_accepted",
    "reactor_submit_sq_full_retries",
    "reactor_submit_failures",
    "reactor_completion_budget_exhaustions",
    "reactor_command_budget_exhaustions",
    "reactor_accept_budget_exhaustions",
    "reactor_writev_budget_exhaustions",
    "reactor_maintenance_budget_exhaustions",
    "reactor_yielded_connections",
    "reactor_parser_resumes",
    "reactor_completion_batches",
    "reactor_completion_batch_total",
    "reactor_completion_nanos_total",
    "reactor_command_batches",
    "reactor_command_batch_total",
    "reactor_writev_chunks",
    "reactor_writev_iovecs_total",
    "reactor_queued_response_bytes_total",
    "reactor_request_cap_exceeded",
    "reactor_response_cap_exceeded",
    "reactor_multi_queue_command_cap_exceeded",
    "reactor_multi_queue_bytes_cap_exceeded",
    "reactor_watch_cap_exceeded",
    "reactor_writev_chunk_cap_exceeded",
    "reactor_overload_accept_throttled",
    "reactor_overload_accept_resumed",
    "reactor_overload_read_disabled",
    "reactor_overload_read_resumed",
    "reactor_overload_command_deferred",
    "reactor_overload_command_resumed",
    "reactor_overload_connections_dropped",
    "reactor_close_drain_nanos_total",
    "reactor_active_expiry_runs",
    "reactor_active_expiry_sampled",
    "reactor_active_expiry_expired",
    "reactor_active_expiry_nanos_total",
    "reactor_aof_append_nanos_total",
    "reactor_aof_fsync_nanos_total",
    "reactor_aof_fsync_requested",
    "reactor_aof_fsync_completed",
    "reactor_aof_fsync_failed",
    "reactor_aof_fsync_worker_saturation",
    "reactor_aof_backpressure_events",
    "reactor_aof_backpressure_nanos_total",
    "reactor_aof_fsync_latency_nanos_total",
    "reactor_maintenance_nanos_total",
    "reactor_metrics_flush_nanos_total",
    "eviction_admissions",
    "eviction_shards_scanned",
    "eviction_slots_sampled",
    "eviction_bytes_freed",
    "eviction_oom_after_scan",
    "eviction_nanos_total",
    "vmstat_page_faults",
    "vmstat_major_page_faults",
    "vmstat_page_scan_kswapd",
    "vmstat_page_scan_direct",
    "vmstat_page_steal_kswapd",
    "vmstat_page_steal_direct",
    "vmstat_swap_in",
    "vmstat_swap_out",
    "vmstat_allocstall",
    "vmstat_page_reclaim",
    "process_minor_faults",
    "process_major_faults",
    "process_read_bytes",
    "process_write_bytes",
    "process_cancelled_write_bytes",
    "process_syscr",
    "process_syscw",
    "process_voluntary_ctx_switches",
    "process_nonvoluntary_ctx_switches",
    "network_total_rx_bytes",
    "network_total_tx_bytes",
    "network_total_rx_errors",
    "network_total_tx_errors",
    "network_loopback_rx_bytes",
    "network_loopback_tx_bytes",
    "tcp_retrans_segs",
    "tcp_listen_overflows",
    "tcp_listen_drops",
    "tcp_syn_retrans",
    "disk_read_bytes",
    "disk_write_bytes",
    "disk_io_time_ms",
}


def _snapshot_delta(
    key: str,
    first: Any,
    last: Any,
    invalid_fields: list[str],
) -> Optional[float]:
    before = _parse_float(first)
    after = _parse_float(last)
    if before is None or after is None:
        return None
    value = after - before
    if key in _MONOTONIC_DELTA_FIELDS and value < 0:
        invalid_fields.append(key)
        return None
    return value


def _capture_runtime_snapshot(service: ServiceState) -> dict[str, Any]:
    raw_info = _redis_cli(service, ["INFO", "runtime"])
    info = _parse_info_response(raw_info or "")
    snapshot = {field: _parse_int(info.get(field)) for field in _RUNTIME_INT_FIELDS}
    snapshot.update(
        {field: _parse_float(info.get(field)) for field in _RUNTIME_FLOAT_FIELDS}
    )
    snapshot.update({field: info.get(field) for field in _RUNTIME_STRING_FIELDS})
    return snapshot


def _runtime_snapshot_available(snapshot: dict[str, Any]) -> bool:
    return any(value is not None for value in snapshot.values())


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
    runtime = _capture_runtime_snapshot(service)
    latency_latest = _read_latency_latest(service)

    def latency_field(name: str, key: str) -> Optional[int]:
        event = latency_latest.get(name) or {}
        return _parse_int(event.get(key))

    return {
        "captured_at": utc_now(),
        "server_version": info.get("redis_version") or service.metadata.get("version"),
        "redis_mode": info.get("redis_mode"),
        "os": info.get("os"),
        "arch_bits": _parse_int(info.get("arch_bits")),
        "used_memory_bytes": _parse_int(info.get("used_memory")),
        "used_memory_rss_bytes": _parse_int(info.get("used_memory_rss")),
        "used_memory_peak_bytes": _parse_int(info.get("used_memory_peak")),
        "used_memory_dataset_bytes": _parse_int(info.get("used_memory_dataset")),
        "used_memory_lua_bytes": _parse_int(info.get("used_memory_lua")),
        "used_memory_overhead_bytes": _parse_int(info.get("used_memory_overhead")),
        "used_memory_startup_bytes": _parse_int(info.get("used_memory_startup")),
        "used_memory_scripts_bytes": _parse_int(info.get("used_memory_scripts")),
        "memory_attribution_engine_scope": info.get("memory_attribution_engine_scope"),
        "memory_attribution_full_server_scope": info.get(
            "memory_attribution_full_server_scope"
        ),
        "engine_live_keys": _parse_int(info.get("engine_live_keys")),
        "engine_logical_dataset_bytes": _parse_int(
            info.get("engine_logical_dataset_bytes")
        ),
        "engine_table_allocated_bytes": _parse_int(
            info.get("engine_table_allocated_bytes")
        ),
        "engine_table_total_slots": _parse_int(info.get("engine_table_total_slots")),
        "engine_capacity_slack_slots": _parse_int(
            info.get("engine_capacity_slack_slots")
        ),
        "engine_tombstone_slots": _parse_int(info.get("engine_tombstone_slots")),
        "engine_load_factor": _parse_float(info.get("engine_load_factor")),
        "engine_bytes_per_live_key": _parse_float(
            info.get("engine_bytes_per_live_key")
        ),
        "engine_shard_count": _parse_int(info.get("engine_shard_count")),
        "io_fixed_buffer_reserved_bytes": _parse_int(
            info.get("io_fixed_buffer_reserved_bytes")
        ),
        "io_fixed_buffer_committed_bytes": _parse_int(
            info.get("io_fixed_buffer_committed_bytes")
        ),
        "io_fixed_buffer_active_bytes": _parse_int(
            info.get("io_fixed_buffer_active_bytes")
        ),
        "io_fixed_buffer_count": _parse_int(info.get("io_fixed_buffer_count")),
        "io_fixed_buffer_size": _parse_int(info.get("io_fixed_buffer_size")),
        "per_connection_state_bytes": _parse_int(
            info.get("per_connection_state_bytes")
        ),
        "client_retained_bytes": _parse_int(info.get("client_retained_bytes")),
        "client_retained_bytes_max": _parse_int(
            info.get("client_retained_bytes_max")
        ),
        "client_retained_bytes_peak": _parse_int(
            info.get("client_retained_bytes_peak")
        ),
        "connection_capacity": _parse_int(info.get("connection_capacity")),
        "full_server_process_rss_bytes": _parse_int(
            info.get("full_server_process_rss_bytes")
        ),
        "used_memory_vm_eval_bytes": _parse_int(info.get("used_memory_vm_eval")),
        "used_memory_vm_functions_bytes": _parse_int(info.get("used_memory_vm_functions")),
        "allocator_allocated_bytes": _parse_int(info.get("allocator_allocated")),
        "allocator_active_bytes": _parse_int(info.get("allocator_active")),
        "allocator_resident_bytes": _parse_int(info.get("allocator_resident")),
        "allocator_mapped_bytes": _parse_int(info.get("allocator_mapped")),
        "allocator_retained_bytes": _parse_int(info.get("allocator_retained")),
        "allocator_frag_ratio": _parse_float(info.get("allocator_frag_ratio")),
        "allocator_frag_bytes": _parse_int(info.get("allocator_frag_bytes")),
        "allocator_rss_ratio": _parse_float(info.get("allocator_rss_ratio")),
        "allocator_rss_bytes": _parse_int(info.get("allocator_rss_bytes")),
        "mem_fragmentation_ratio": _parse_float(info.get("mem_fragmentation_ratio")),
        "mem_fragmentation_bytes": _parse_int(info.get("mem_fragmentation_bytes")),
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
        "aof_buffer_length": _parse_int(info.get("aof_buffer_length")),
        "aof_pending_bio_fsync": _parse_int(info.get("aof_pending_bio_fsync")),
        "aof_delayed_fsync": _parse_int(info.get("aof_delayed_fsync")),
        "aof_pending_rewrite": _parse_int(info.get("aof_pending_rewrite")),
        "aof_rewrite_in_progress": _parse_bool(info.get("aof_rewrite_in_progress")),
        "aof_last_write_status": info.get("aof_last_write_status"),
        "aof_last_bgrewrite_status": info.get("aof_last_bgrewrite_status"),
        "aof_current_rewrite_time_sec": _parse_int(info.get("aof_current_rewrite_time_sec")),
        "aof_last_cow_size_bytes": _parse_int(info.get("aof_last_cow_size")),
        "persistence_current_cow_peak_bytes": _parse_int(info.get("current_cow_peak")),
        "persistence_current_cow_size_bytes": _parse_int(info.get("current_cow_size")),
        "persistence_rdb_bgsave_in_progress": _parse_bool(info.get("rdb_bgsave_in_progress")),
        "latency_events": latency_latest,
        "latency_aof_write_latest_ms": latency_field("aof-write", "latest_latency_ms"),
        "latency_aof_write_max_ms": latency_field("aof-write", "max_latency_ms"),
        "latency_aof_fsync_latest_ms": latency_field("aof-fsync-always", "latest_latency_ms"),
        "latency_aof_fsync_max_ms": latency_field("aof-fsync-always", "max_latency_ms"),
        "latency_aof_pending_fsync_latest_ms": latency_field(
            "aof-write-pending-fsync", "latest_latency_ms"
        ),
        "latency_aof_pending_fsync_max_ms": latency_field(
            "aof-write-pending-fsync", "max_latency_ms"
        ),
        **runtime,
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
        "used_memory_dataset_bytes",
        "used_memory_overhead_bytes",
        "engine_live_keys",
        "engine_logical_dataset_bytes",
        "engine_table_allocated_bytes",
        "engine_table_total_slots",
        "engine_capacity_slack_slots",
        "engine_tombstone_slots",
        "engine_load_factor",
        "engine_bytes_per_live_key",
        "engine_shard_count",
        "io_fixed_buffer_reserved_bytes",
        "io_fixed_buffer_committed_bytes",
        "io_fixed_buffer_active_bytes",
        "io_fixed_buffer_count",
        "io_fixed_buffer_size",
        "per_connection_state_bytes",
        "client_retained_bytes",
        "client_retained_bytes_max",
        "client_retained_bytes_peak",
        "connection_capacity",
        "full_server_process_rss_bytes",
        "allocator_allocated_bytes",
        "allocator_active_bytes",
        "allocator_resident_bytes",
        "allocator_mapped_bytes",
        "allocator_retained_bytes",
        "allocator_frag_bytes",
        "allocator_rss_bytes",
        "mem_fragmentation_bytes",
        "process_memory_bytes",
        "aof_current_size_bytes",
        "aof_buffer_length",
        "aof_pending_bio_fsync",
        "aof_delayed_fsync",
        "aof_pending_rewrite",
        "aof_current_rewrite_time_sec",
        "aof_last_cow_size_bytes",
        "persistence_current_cow_peak_bytes",
        "persistence_current_cow_size_bytes",
        "latency_aof_write_latest_ms",
        "latency_aof_write_max_ms",
        "latency_aof_fsync_latest_ms",
        "latency_aof_fsync_max_ms",
        "latency_aof_pending_fsync_latest_ms",
        "latency_aof_pending_fsync_max_ms",
        *_RUNTIME_DELTA_FIELDS,
    ]
    delta: dict[str, Any] = {}
    invalid_delta_fields: list[str] = []
    for field in delta_fields:
        key = f"{field}_delta"
        value = _snapshot_delta(
            field,
            before.get(field),
            after.get(field),
            invalid_delta_fields,
        )
        if value is None:
            delta[key] = None
            continue
        delta[key] = int(value) if value.is_integer() else value
    delta["invalid_delta_fields"] = sorted(set(invalid_delta_fields))

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
