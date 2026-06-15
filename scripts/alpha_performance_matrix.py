#!/usr/bin/env python3
"""Generate and optionally run the VAL-ALPHA-002 performance matrix.

The full alpha matrix is intentionally large. This runner makes the contract
repeatable without hiding evidence quality: generated-only rows are a plan,
single-replicate runs are exploratory, and publication-grade rows need 3-5
clean repeats plus explicit report artifacts.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / ".artifacts" / "validation" / "alpha-performance"

DEFAULT_SERVICE_THREADS = [1, 4, 8, 16]
EXTENDED_SERVICE_THREADS = [1, 2, 4, 8, 16]
CORE_DATABASES = ["vortex", "redis"]
ALL_DATABASES = ["vortex", "redis", "dragonfly", "valkey"]
TIER1_SHARDS = [4096, 16_384, 65_536]
EXTENDED_SHARDS = [1024, 4096, 16_384, 65_536]
CORE_COMMANDS = ["PING", "GET", "SET", "INCR"]
POINT_COMMANDS = ["PING", "GET", "SET", "INCR", "DEL", "TTL", "EXPIRE", "MGET", "MSET"]
PRESSURE_PIPELINES = [1, 16, 256, 4096]
LARGE_VALUE_SIZES = [16 * 1024, 64 * 1024, 1024 * 1024]


@dataclass(frozen=True)
class MatrixRow:
    name: str
    tier: str
    surface: str
    manifest: dict[str, Any]
    evidence_tier: str
    notes: str


@dataclass
class RunResult:
    row: str
    status: str
    report_path: str | None
    stdout_path: str
    stderr_path: str
    returncode: int


def timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def parse_csv_ints(value: str) -> list[int]:
    values: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        values.append(int(part))
    if not values:
        raise argparse.ArgumentTypeError("expected at least one integer")
    return values


def runtime_config(
    *,
    shard_count: int = 4096,
    io_backend: str = "uring",
    aof_enabled: bool = False,
    aof_fsync: str = "everysec",
    eviction_policy: str = "noeviction",
    maxmemory: str = "1800mb",
    telemetry_mode: str = "minimal",
    fixed_buffers: int = 1024,
    fixed_buffer_registration: str = "auto",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "aof_enabled": aof_enabled,
        "aof_fsync": aof_fsync,
        "eviction_policy": eviction_policy,
        "telemetry_mode": telemetry_mode,
        "io_backend": io_backend,
        "shard_count": shard_count,
        "fixed_buffers": fixed_buffers,
        "fixed_buffer_registration": fixed_buffer_registration,
        "sqpoll_idle_ms": 0,
    }
    if maxmemory:
        payload["maxmemory"] = maxmemory
    return payload


def resource_memory_for(*, environment_mode: str, service_threads: int) -> str:
    if environment_mode != "container":
        return "2g"
    if service_threads >= 16:
        return "6g"
    if service_threads >= 8:
        return "4g"
    return "2g"


def runtime_maxmemory_for(*, environment_mode: str, service_threads: int) -> str:
    if environment_mode != "container":
        return "1800mb"
    if service_threads >= 16:
        return "6000mb"
    if service_threads >= 8:
        return "3000mb"
    return "1800mb"


def base_manifest(
    *,
    name: str,
    description: str,
    databases: list[str],
    backends: list[str],
    commands: list[str] | None = None,
    workloads: list[str] | None = None,
    repeat: int,
    duration: str,
    service_threads: int,
    shard_count: int = 4096,
    pipeline: int = 1,
    io_backend: str = "uring",
    aof_enabled: bool = False,
    aof_fsync: str = "everysec",
    eviction_policy: str = "noeviction",
    maxmemory: str = "1800mb",
    keyspace_size: int = 100_000,
    data_size: int = 64,
    load_thread_sweep: list[int] | None = None,
    custom_ops_per_thread: int = 6000,
    custom_value_size: int | None = None,
    environment_mode: str = "native",
    port_base: int = 22679,
) -> dict[str, Any]:
    load_thread_sweep = load_thread_sweep or [max(1, min(service_threads, 8))]
    resource_config: dict[str, Any] = {
        "cpus": max(1, service_threads),
        "memory": resource_memory_for(
            environment_mode=environment_mode,
            service_threads=service_threads,
        ),
        "threads": service_threads,
    }
    settings: dict[str, Any] = {
        "redis-benchmark": {
            "requests": 30_000,
            "clients": 100,
            "pipeline": pipeline,
            "keyspace_size": keyspace_size,
        },
        "memtier_benchmark": {
            "clients": 100,
            "pipeline": pipeline,
            "keyspace_size": keyspace_size,
            "data_size": data_size,
            "thread_sweep": load_thread_sweep,
        },
        "custom-rust": {
            "profile": "release",
            "thread_sweep": load_thread_sweep,
            "keyspace_size": min(keyspace_size, 100_000),
            "ops_per_thread": custom_ops_per_thread,
            "warmup_ops": 500,
            "value_size": custom_value_size if custom_value_size is not None else data_size,
        },
    }
    return {
        "schema_version": 1,
        "name": name,
        "description": description,
        "databases": databases,
        "backends": backends,
        "commands": commands or [],
        "workloads": workloads or [],
        "repeat": repeat,
        "duration": duration,
        "environment": {
            "mode": environment_mode,
            "port_base": port_base,
        },
        "resource_config": resource_config,
        "runtime_config": runtime_config(
            shard_count=shard_count,
            io_backend=io_backend,
            aof_enabled=aof_enabled,
            aof_fsync=aof_fsync,
            eviction_policy=eviction_policy,
            maxmemory=maxmemory,
            fixed_buffers=max(1024, service_threads * 256),
        ),
        "settings": settings,
    }


def make_core_row(
    *,
    service_threads: int,
    shard_count: int,
    pipeline: int,
    io_backend: str,
    aof_enabled: bool,
    repeat: int,
    duration: str,
) -> MatrixRow:
    aof_label = "aof-everysec" if aof_enabled else "aof-off"
    name = (
        f"val-alpha-002-tier1-core-svc{service_threads}-shards{shard_count}"
        f"-{io_backend}-{aof_label}-p{pipeline}"
    )
    return MatrixRow(
        name=name,
        tier="tier1",
        surface="core point commands",
        evidence_tier="citation-grade" if repeat >= 3 else "engineering",
        notes="redis-benchmark row; p99.9 is unavailable, so use memtier/custom rows when p99.9 is required.",
        manifest=base_manifest(
            name=name,
            description="VAL-ALPHA-002 Tier 1 core point-command comparison.",
            databases=["vortex", "redis"],
            backends=["redis-benchmark"],
            commands=POINT_COMMANDS,
            repeat=repeat,
            duration=duration,
            service_threads=service_threads,
            shard_count=shard_count,
            pipeline=pipeline,
            io_backend=io_backend,
            aof_enabled=aof_enabled,
        ),
    )


def make_mixed_row(
    *,
    workload: str,
    service_threads: int,
    shard_count: int,
    io_backend: str,
    aof_enabled: bool,
    repeat: int,
    duration: str,
    data_size: int = 64,
    keyspace_size: int = 100_000,
) -> MatrixRow:
    aof_label = "aof-everysec" if aof_enabled else "aof-off"
    name = (
        f"val-alpha-002-tier1-{workload.replace('_', '-')}-svc{service_threads}"
        f"-shards{shard_count}-{io_backend}-{aof_label}-p1"
    )
    backend = "custom-rust" if "multi_key" in workload or "tx" in workload else "memtier_benchmark"
    return MatrixRow(
        name=name,
        tier="tier1",
        surface="mixed workload with p99.9",
        evidence_tier="citation-grade" if repeat >= 3 else "engineering",
        notes="p99.9-capable row for latency and throughput analysis.",
        manifest=base_manifest(
            name=name,
            description="VAL-ALPHA-002 Tier 1 mixed workload comparison.",
            databases=["vortex", "redis"],
            backends=[backend],
            workloads=[workload],
            repeat=repeat,
            duration=duration,
            service_threads=service_threads,
            shard_count=shard_count,
            io_backend=io_backend,
            aof_enabled=aof_enabled,
            data_size=data_size,
            keyspace_size=keyspace_size,
        ),
    )


def make_pressure_row(
    *,
    workload: str,
    service_threads: int,
    shard_count: int,
    pipeline: int,
    repeat: int,
    duration: str,
    aof_enabled: bool = False,
    aof_fsync: str = "everysec",
    eviction_policy: str = "noeviction",
    maxmemory: str = "1800mb",
    value_size: int = 64,
    keyspace_size: int = 100_000,
    tier: str = "tier1",
) -> MatrixRow:
    name = (
        f"val-alpha-002-{tier}-{workload.replace('_', '-')}-svc{service_threads}"
        f"-shards{shard_count}-p{pipeline}-v{value_size}"
    )
    return MatrixRow(
        name=name,
        tier=tier,
        surface="pressure/fairness workload",
        evidence_tier="citation-grade" if repeat >= 3 else "engineering",
        notes="custom-rust row with p99.9/p99.999 latency-sensitive client reporting.",
        manifest=base_manifest(
            name=name,
            description=f"VAL-ALPHA-002 {tier} pressure row for {workload}.",
            databases=["vortex", "redis"],
            backends=["custom-rust"],
            workloads=[workload],
            repeat=repeat,
            duration=duration,
            service_threads=service_threads,
            shard_count=shard_count,
            pipeline=pipeline,
            aof_enabled=aof_enabled,
            aof_fsync=aof_fsync,
            eviction_policy=eviction_policy,
            maxmemory=maxmemory,
            data_size=value_size,
            keyspace_size=keyspace_size,
            custom_value_size=value_size,
            custom_ops_per_thread=1500,
        ),
    )


def scaling_rows(
    *,
    threads: list[int],
    repeat: int,
    duration: str,
    databases: list[str] | None = None,
    environment_mode: str = "native",
    port_base: int = 22679,
) -> list[MatrixRow]:
    rows: list[MatrixRow] = []
    databases = databases or CORE_DATABASES
    profile_label = "docker-all-db" if environment_mode == "container" else "native"
    for service_threads in threads:
        name = (
            f"val-alpha-002-scaling-{profile_label}-svc{service_threads}"
            "-shards4096-uring-aof-off-p1"
        )
        notes = (
            "Database-side thread sweep. Vortex receives --threads=N; Redis receives "
            "--io-threads=N with --io-threads-do-reads=yes; Valkey uses the same Redis-style "
            "IO-thread knob; Dragonfly receives --proactor_threads=N. The load generator is "
            "fixed at 4 memtier threads so this row does not measure load-thread scaling."
        )
        rows.append(
            MatrixRow(
                name=name,
                tier="scaling",
                surface="database service-thread scaling smoke",
                evidence_tier="engineering" if repeat >= 3 else "exploratory",
                notes=notes,
                manifest=base_manifest(
                    name=name,
                    description=(
                        "VAL-ALPHA-002 exploratory database-side thread scaling row. "
                        "Vortex uses service/reactor threads; Redis/Valkey use networking IO "
                        "threads; Dragonfly uses proactor threads."
                    ),
                    databases=databases,
                    backends=["redis-benchmark", "memtier_benchmark"],
                    commands=CORE_COMMANDS,
                    workloads=["uniform-read_heavy"],
                    repeat=repeat,
                    duration=duration,
                    service_threads=service_threads,
                    shard_count=4096,
                    pipeline=1,
                    keyspace_size=100_000,
                    data_size=64,
                    load_thread_sweep=[4],
                    environment_mode=environment_mode,
                    port_base=port_base,
                    maxmemory=runtime_maxmemory_for(
                        environment_mode=environment_mode,
                        service_threads=service_threads,
                    ),
                ),
            )
        )
    return rows


def smoke_rows(*, repeat: int, duration: str) -> list[MatrixRow]:
    return [
        make_core_row(
            service_threads=4,
            shard_count=4096,
            pipeline=1,
            io_backend="uring",
            aof_enabled=False,
            repeat=repeat,
            duration=duration,
        ),
        make_mixed_row(
            workload="uniform-read_heavy",
            service_threads=4,
            shard_count=4096,
            io_backend="uring",
            aof_enabled=False,
            repeat=repeat,
            duration=duration,
            data_size=64,
            keyspace_size=10_000,
        ),
        make_pressure_row(
            workload="pressure-large_bulk",
            service_threads=4,
            shard_count=4096,
            pipeline=1,
            repeat=repeat,
            duration=duration,
            value_size=16 * 1024,
            keyspace_size=1000,
            tier="tier2-smoke",
        ),
        make_pressure_row(
            workload="pressure-eviction_pressure",
            service_threads=4,
            shard_count=4096,
            pipeline=1,
            repeat=repeat,
            duration=duration,
            eviction_policy="allkeys-lru",
            maxmemory="64mb",
            value_size=1024,
            keyspace_size=10_000,
            tier="tier2-smoke",
        ),
    ]


def tier1_rows(*, repeat: int, duration: str) -> list[MatrixRow]:
    rows: list[MatrixRow] = []
    for service_threads in DEFAULT_SERVICE_THREADS:
        for shard_count in TIER1_SHARDS:
            for io_backend in ["uring", "polling"]:
                for aof_enabled in [False, True]:
                    for pipeline in [1, 16]:
                        rows.append(
                            make_core_row(
                                service_threads=service_threads,
                                shard_count=shard_count,
                                pipeline=pipeline,
                                io_backend=io_backend,
                                aof_enabled=aof_enabled,
                                repeat=repeat,
                                duration=duration,
                            )
                        )
    for service_threads in DEFAULT_SERVICE_THREADS:
        for shard_count in TIER1_SHARDS:
            rows.append(
                make_mixed_row(
                    workload="uniform-read_heavy",
                    service_threads=service_threads,
                    shard_count=shard_count,
                    io_backend="uring",
                    aof_enabled=False,
                    repeat=repeat,
                    duration=duration,
                )
            )
            rows.append(
                make_mixed_row(
                    workload="uniform-write_heavy",
                    service_threads=service_threads,
                    shard_count=shard_count,
                    io_backend="uring",
                    aof_enabled=True,
                    repeat=repeat,
                    duration=duration,
                )
            )
            rows.append(
                make_mixed_row(
                    workload="zipfian-read_heavy",
                    service_threads=service_threads,
                    shard_count=shard_count,
                    io_backend="uring",
                    aof_enabled=False,
                    repeat=repeat,
                    duration=duration,
                )
            )
            rows.append(
                make_mixed_row(
                    workload="multi_key_only",
                    service_threads=service_threads,
                    shard_count=shard_count,
                    io_backend="uring",
                    aof_enabled=False,
                    repeat=repeat,
                    duration=duration,
                )
            )
            rows.append(
                make_mixed_row(
                    workload="single_key_tx_mixed",
                    service_threads=service_threads,
                    shard_count=shard_count,
                    io_backend="uring",
                    aof_enabled=False,
                    repeat=repeat,
                    duration=duration,
                )
            )
    for pipeline in PRESSURE_PIPELINES:
        rows.append(
            make_pressure_row(
                workload="pressure-deep_pipeline",
                service_threads=4,
                shard_count=4096,
                pipeline=pipeline,
                repeat=repeat,
                duration=duration,
            )
        )
        rows.append(
            make_pressure_row(
                workload="pressure-slow_reader",
                service_threads=4,
                shard_count=4096,
                pipeline=pipeline,
                repeat=repeat,
                duration=duration,
            )
        )
    rows.append(
        make_pressure_row(
            workload="pressure-ttl_expiry",
            service_threads=4,
            shard_count=4096,
            pipeline=1,
            repeat=repeat,
            duration=duration,
        )
    )
    rows.append(
        make_pressure_row(
            workload="pressure-eviction_pressure",
            service_threads=4,
            shard_count=4096,
            pipeline=1,
            repeat=repeat,
            duration=duration,
            eviction_policy="allkeys-lru",
            maxmemory="128mb",
            value_size=1024,
        )
    )
    return rows


def tier2_rows(*, repeat: int, duration: str) -> list[MatrixRow]:
    rows = [
        make_mixed_row(
            workload="uniform-write_heavy",
            service_threads=4,
            shard_count=4096,
            io_backend="uring",
            aof_enabled=True,
            repeat=repeat,
            duration=duration,
        ),
        make_pressure_row(
            workload="pressure-aof_backlog",
            service_threads=4,
            shard_count=4096,
            pipeline=1,
            repeat=repeat,
            duration=duration,
            aof_enabled=True,
            aof_fsync="always",
            tier="tier2",
        ),
        make_pressure_row(
            workload="pressure-connection_storm",
            service_threads=4,
            shard_count=4096,
            pipeline=1,
            repeat=repeat,
            duration=duration,
            tier="tier2",
        ),
        make_pressure_row(
            workload="pressure-close_storm",
            service_threads=4,
            shard_count=4096,
            pipeline=1,
            repeat=repeat,
            duration=duration,
            tier="tier2",
        ),
    ]
    for size in LARGE_VALUE_SIZES:
        keyspace_size = max(128, min(4096, (256 * 1024 * 1024) // size))
        rows.append(
            make_pressure_row(
                workload="pressure-large_bulk",
                service_threads=4,
                shard_count=4096,
                pipeline=1,
                repeat=repeat,
                duration=duration,
                value_size=size,
                keyspace_size=keyspace_size,
                tier="tier2",
            )
        )
    for command in ["APPEND", "SETRANGE", "INCRBYFLOAT"]:
        name = f"val-alpha-002-tier2-{command.lower()}-svc4-shards4096-p1"
        rows.append(
            MatrixRow(
                name=name,
                tier="tier2",
                surface="value-dependent mutation",
                evidence_tier="citation-grade" if repeat >= 3 else "engineering",
                notes="redis-benchmark point row for mutation-path overhead; pair with profiler if it regresses.",
                manifest=base_manifest(
                    name=name,
                    description=f"VAL-ALPHA-002 Tier 2 {command} mutation row.",
                    databases=["vortex", "redis"],
                    backends=["redis-benchmark"],
                    commands=[command],
                    repeat=repeat,
                    duration=duration,
                    service_threads=4,
                    shard_count=4096,
                    pipeline=1,
                ),
            )
        )
    return rows


def tier3_rows(*, repeat: int, duration: str) -> list[MatrixRow]:
    rows: list[MatrixRow] = []
    for service_threads in EXTENDED_SERVICE_THREADS:
        for shard_count in EXTENDED_SHARDS:
            rows.append(
                make_mixed_row(
                    workload="uniform-read_heavy",
                    service_threads=service_threads,
                    shard_count=shard_count,
                    io_backend="uring",
                    aof_enabled=False,
                    repeat=repeat,
                    duration=duration,
                    data_size=256,
                )
            )
    return rows


def select_rows(profile: str, *, threads: list[int], repeat: int, duration: str) -> list[MatrixRow]:
    if profile == "smoke":
        return smoke_rows(repeat=repeat, duration=duration)
    if profile == "scaling":
        return scaling_rows(threads=threads, repeat=repeat, duration=duration)
    if profile == "docker-scaling":
        return scaling_rows(
            threads=threads,
            repeat=repeat,
            duration=duration,
            databases=ALL_DATABASES,
            environment_mode="container",
            port_base=26379,
        )
    if profile == "tier1":
        return tier1_rows(repeat=repeat, duration=duration)
    if profile == "tier2":
        return tier2_rows(repeat=repeat, duration=duration)
    if profile == "tier3":
        return tier3_rows(repeat=repeat, duration=duration)
    if profile == "all":
        return (
            tier1_rows(repeat=repeat, duration=duration)
            + tier2_rows(repeat=repeat, duration=duration)
            + tier3_rows(repeat=repeat, duration=duration)
        )
    raise ValueError(f"unsupported profile: {profile}")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_manifests(rows: list[MatrixRow], output_dir: Path) -> dict[str, Path]:
    manifest_dir = output_dir / "manifests"
    paths: dict[str, Path] = {}
    for row in rows:
        path = manifest_dir / f"{row.name}.json"
        write_json(path, row.manifest)
        paths[row.name] = path
    return paths


def report_path_from_stdout(stdout: str) -> str | None:
    prefix = "latest report markdown:"
    for line in stdout.splitlines():
        if line.startswith(prefix):
            return line[len(prefix) :].strip()
    return None


def run_row(row: MatrixRow, manifest_path: Path, output_dir: Path) -> RunResult:
    run_dir = output_dir / "runs" / row.name
    stdout_path = output_dir / "logs" / f"{row.name}.stdout.log"
    stderr_path = output_dir / "logs" / f"{row.name}.stderr.log"
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        "bash",
        "vortex-benchmark/bin/vortex_bench_local",
        "--manifest",
        str(manifest_path),
        "--output-dir",
        str(run_dir),
        "--label",
        row.name,
        "--report-title",
        f"VAL-ALPHA-002 {row.name}",
        "--evidence-tier",
        row.evidence_tier,
    ]
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    stdout_path.write_text(completed.stdout, encoding="utf-8")
    stderr_path.write_text(completed.stderr, encoding="utf-8")
    return RunResult(
        row=row.name,
        status="passed" if completed.returncode == 0 else "failed",
        report_path=report_path_from_stdout(completed.stdout),
        stdout_path=str(stdout_path),
        stderr_path=str(stderr_path),
        returncode=completed.returncode,
    )


def write_report(
    *,
    output_dir: Path,
    profile: str,
    rows: list[MatrixRow],
    manifest_paths: dict[str, Path],
    run_results: list[RunResult],
    run_enabled: bool,
) -> Path:
    report = output_dir / "report.md"
    lines = [
        "# VAL-ALPHA-002 Performance Matrix",
        "",
        f"- Generated at: `{datetime.now(timezone.utc).isoformat(timespec='seconds')}`",
        f"- Profile: `{profile}`",
        f"- Rows: `{len(rows)}`",
        f"- Execution: `{'run' if run_enabled else 'generate-only'}`",
        "",
        "Evidence policy:",
        "",
        "- Single-replicate rows are exploratory only.",
        "- Publication-grade rows require 3-5 repeats, clean host validity, and artifact paths.",
        "- `redis-benchmark` rows do not expose p99.9; use `memtier_benchmark` or `custom-rust` rows when p99.9 is required.",
        "",
        "## Rows",
        "",
        "| Row | Tier | Surface | Repeat | Manifest | Notes |",
        "|-----|------|---------|--------|----------|-------|",
    ]
    for row in rows:
        manifest_path = manifest_paths[row.name]
        repeat = row.manifest.get("repeat")
        lines.append(
            f"| `{row.name}` | `{row.tier}` | {row.surface} | `{repeat}` | `{manifest_path}` | {row.notes} |"
        )

    lines.extend(["", "## Run Results", ""])
    if not run_results:
        lines.append("No rows were executed in this invocation.")
    else:
        lines.extend(["| Row | Status | Report | Logs |", "|-----|--------|--------|------|"])
        for result in run_results:
            report_path = result.report_path or "n/a"
            lines.append(
                f"| `{result.row}` | `{result.status}` ({result.returncode}) | `{report_path}` | `{result.stdout_path}`, `{result.stderr_path}` |"
            )

    lines.extend(
        [
            "",
            "## Manual/Release Gates Not Automated Here",
            "",
            "- CPU governor, power profile, thermal state, affinity, socket queues, retransmits, and client saturation must be checked in the generated benchmark reports before using a row for publication.",
            "- Full-server memory comparison at 50k, 100k, and 1M keys is owned by the memory attribution tooling and must be linked when memory efficiency is being analyzed.",
            "- Tier 3 DragonflyDB and Valkey comparisons are scheduled after the core comparison rows stabilize.",
        ]
    )
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        choices=["smoke", "scaling", "docker-scaling", "tier1", "tier2", "tier3", "all"],
        default="smoke",
        help="Matrix profile to generate.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory. Defaults under .artifacts/validation/alpha-performance.",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Execute generated rows with vortex_bench_local after writing manifests.",
    )
    parser.add_argument(
        "--allow-large-run",
        action="store_true",
        help="Allow --run for profiles with more rows than --max-run-rows.",
    )
    parser.add_argument(
        "--max-run-rows",
        type=int,
        default=20,
        help="Safety cap for --run unless --allow-large-run is present.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Repeat count to write into generated manifests.",
    )
    parser.add_argument(
        "--duration",
        default="5s",
        help="Benchmark duration for generated rows.",
    )
    parser.add_argument(
        "--threads",
        type=parse_csv_ints,
        default=EXTENDED_SERVICE_THREADS,
        help="Comma-separated service-thread counts for the scaling profile.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.repeat <= 0:
        print("error: --repeat must be positive", file=sys.stderr)
        return 2
    if args.max_run_rows <= 0:
        print("error: --max-run-rows must be positive", file=sys.stderr)
        return 2
    output_dir = args.output_dir or DEFAULT_OUTPUT_ROOT / f"{timestamp()}-{args.profile}"
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = select_rows(
        args.profile,
        threads=args.threads,
        repeat=args.repeat,
        duration=args.duration,
    )
    manifest_paths = write_manifests(rows, output_dir)
    write_json(
        output_dir / "matrix-plan.json",
        {
            "schema_version": 1,
            "profile": args.profile,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "row_count": len(rows),
            "rows": [
                {
                    "name": row.name,
                    "tier": row.tier,
                    "surface": row.surface,
                    "evidence_tier": row.evidence_tier,
                    "manifest_path": str(manifest_paths[row.name]),
                    "notes": row.notes,
                }
                for row in rows
            ],
        },
    )

    if args.run and len(rows) > args.max_run_rows and not args.allow_large_run:
        print(
            f"error: profile generated {len(rows)} rows; rerun with --allow-large-run or increase --max-run-rows",
            file=sys.stderr,
        )
        return 2

    run_results: list[RunResult] = []
    if args.run:
        for row in rows:
            result = run_row(row, manifest_paths[row.name], output_dir)
            run_results.append(result)
            if result.status != "passed":
                break

    report = write_report(
        output_dir=output_dir,
        profile=args.profile,
        rows=rows,
        manifest_paths=manifest_paths,
        run_results=run_results,
        run_enabled=args.run,
    )
    print(f"matrix report: {report}")
    if run_results and any(result.status != "passed" for result in run_results):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
