from __future__ import annotations

import csv
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from vortex_benchmark.backends.base import (
    BackendError,
    BackendExecutionRecord,
    BackendRunContext,
    DEFAULT_REDIS_CLIENTS,
    DEFAULT_REDIS_KEYSPACE,
    DEFAULT_REDIS_PIPELINE,
    DEFAULT_REDIS_REQUESTS,
    coerce_positive_int,
    get_setting,
    quote_command,
    run_process,
    send_resp_pipeline,
    write_json,
)
from vortex_benchmark.models import sanitize_identifier, utc_now
from vortex_benchmark.telemetry import (
    capture_service_snapshot,
    diff_service_snapshots,
    start_host_telemetry_capture,
)


@dataclass(frozen=True)
class RedisBenchmarkItem:
    label: str
    mode: str
    builtin_test: Optional[str] = None
    command_args: tuple[str, ...] = ()
    benchmark_args: tuple[str, ...] = ()
    slow: bool = False


BUILTIN_ITEMS = {
    "GET": RedisBenchmarkItem(label="GET", mode="builtin", builtin_test="GET", benchmark_args=("-r", "1000")),
    "INCR": RedisBenchmarkItem(label="INCR", mode="builtin", builtin_test="INCR", benchmark_args=("-r", "1000")),
    "MSET": RedisBenchmarkItem(label="MSET", mode="builtin", builtin_test="MSET", benchmark_args=("-r", "1000")),
    "SET": RedisBenchmarkItem(label="SET", mode="builtin", builtin_test="SET", benchmark_args=("-r", "1000")),
}

RAW_ITEMS = {
    "APPEND": RedisBenchmarkItem("APPEND", "raw", command_args=("APPEND", "bench:key:__rand_int__", "x"), benchmark_args=("-r", "1000")),
    "COMMAND": RedisBenchmarkItem("COMMAND", "raw", command_args=("COMMAND", "COUNT")),
    "COPY": RedisBenchmarkItem("COPY", "raw", command_args=("COPY", "bench:key:1", "bench:key:copy:dest", "REPLACE")),
    "DBSIZE": RedisBenchmarkItem("DBSIZE", "raw", command_args=("DBSIZE",)),
    "DECR": RedisBenchmarkItem("DECR", "raw", command_args=("DECR", "bench:counter:decr")),
    "DECRBY": RedisBenchmarkItem("DECRBY", "raw", command_args=("DECRBY", "bench:counter:decrby", "1")),
    "DEL": RedisBenchmarkItem("DEL", "raw", command_args=("DEL", "bench:delete:__rand_int__"), benchmark_args=("-r", "1000")),
    "ECHO": RedisBenchmarkItem("ECHO", "raw", command_args=("ECHO", "hello")),
    "EXISTS": RedisBenchmarkItem("EXISTS", "raw", command_args=("EXISTS", "bench:key:1")),
    "EXPIRE": RedisBenchmarkItem("EXPIRE", "raw", command_args=("EXPIRE", "bench:key:1", "3600")),
    "EXPIREAT": RedisBenchmarkItem("EXPIREAT", "raw", command_args=("EXPIREAT", "bench:key:3", "2000000000")),
    "EXPIRETIME": RedisBenchmarkItem("EXPIRETIME", "raw", command_args=("EXPIRETIME", "bench:ttl:1")),
    "FLUSHALL": RedisBenchmarkItem("FLUSHALL", "raw", command_args=("FLUSHALL",), slow=True),
    "FLUSHDB": RedisBenchmarkItem("FLUSHDB", "raw", command_args=("FLUSHDB",), slow=True),
    "GETDEL": RedisBenchmarkItem("GETDEL", "raw", command_args=("GETDEL", "bench:getdel:__rand_int__"), benchmark_args=("-r", "1000")),
    "GETEX": RedisBenchmarkItem("GETEX", "raw", command_args=("GETEX", "bench:ttl:__rand_int__", "EX", "3600"), benchmark_args=("-r", "1000")),
    "GETRANGE": RedisBenchmarkItem("GETRANGE", "raw", command_args=("GETRANGE", "bench:long:__rand_int__", "0", "49"), benchmark_args=("-r", "1000")),
    "GETSET": RedisBenchmarkItem("GETSET", "raw", command_args=("GETSET", "bench:key:__rand_int__", "newvalue"), benchmark_args=("-r", "1000")),
    "INFO": RedisBenchmarkItem("INFO", "raw", command_args=("INFO",)),
    "INCRBY": RedisBenchmarkItem("INCRBY", "raw", command_args=("INCRBY", "bench:counter:incrby", "1")),
    "INCRBYFLOAT": RedisBenchmarkItem("INCRBYFLOAT", "raw", command_args=("INCRBYFLOAT", "bench:counter:float", "1.5")),
    "KEYS": RedisBenchmarkItem("KEYS", "raw", command_args=("KEYS", "bench:key:*"), slow=True),
    "MGET": RedisBenchmarkItem("MGET", "raw", command_args=("MGET", "bench:key:1", "bench:key:2", "bench:key:3")),
    "MSETNX": RedisBenchmarkItem("MSETNX", "raw", command_args=("MSETNX", "bench:msetnx:__rand_int__:a", "va", "bench:msetnx:__rand_int__:b", "vb"), benchmark_args=("-r", "1000000")),
    "PERSIST": RedisBenchmarkItem("PERSIST", "raw", command_args=("PERSIST", "bench:ttl:__rand_int__"), benchmark_args=("-r", "1000")),
    "PEXPIRE": RedisBenchmarkItem("PEXPIRE", "raw", command_args=("PEXPIRE", "bench:key:2", "3600000")),
    "PEXPIREAT": RedisBenchmarkItem("PEXPIREAT", "raw", command_args=("PEXPIREAT", "bench:key:4", "2000000000000")),
    "PEXPIRETIME": RedisBenchmarkItem("PEXPIRETIME", "raw", command_args=("PEXPIRETIME", "bench:ttl:1")),
    "PING": RedisBenchmarkItem("PING", "raw", command_args=("PING",)),
    "PSETEX": RedisBenchmarkItem("PSETEX", "raw", command_args=("PSETEX", "bench:psetex:__rand_int__", "3600000", "value"), benchmark_args=("-r", "1000")),
    "PTTL": RedisBenchmarkItem("PTTL", "raw", command_args=("PTTL", "bench:ttl:1")),
    "RANDOMKEY": RedisBenchmarkItem("RANDOMKEY", "raw", command_args=("RANDOMKEY",)),
    "RENAME": RedisBenchmarkItem("RENAME", "raw", command_args=("RENAME", "bench:rename:1", "bench:rename:1")),
    "SCAN": RedisBenchmarkItem("SCAN", "raw", command_args=("SCAN", "0", "COUNT", "100"), slow=True),
    "SELECT": RedisBenchmarkItem("SELECT", "raw", command_args=("SELECT", "0")),
    "SETEX": RedisBenchmarkItem("SETEX", "raw", command_args=("SETEX", "bench:setex:__rand_int__", "3600", "value"), benchmark_args=("-r", "1000")),
    "SETNX": RedisBenchmarkItem("SETNX", "raw", command_args=("SETNX", "bench:setnx:__rand_int__", "value"), benchmark_args=("-r", "1000")),
    "SETRANGE": RedisBenchmarkItem("SETRANGE", "raw", command_args=("SETRANGE", "bench:long:1", "10", "REPLACED")),
    "STRLEN": RedisBenchmarkItem("STRLEN", "raw", command_args=("STRLEN", "bench:key:1")),
    "TIME": RedisBenchmarkItem("TIME", "raw", command_args=("TIME",)),
    "TOUCH": RedisBenchmarkItem("TOUCH", "raw", command_args=("TOUCH", "bench:key:1")),
    "TTL": RedisBenchmarkItem("TTL", "raw", command_args=("TTL", "bench:ttl:1")),
    "TYPE": RedisBenchmarkItem("TYPE", "raw", command_args=("TYPE", "bench:key:1")),
    "UNLINK": RedisBenchmarkItem("UNLINK", "raw", command_args=("UNLINK", "bench:unlink:__rand_int__"), benchmark_args=("-r", "1000")),
}


def _redis_cli(service, args: list[str], *, input_text: Optional[str] = None) -> subprocess.CompletedProcess[str]:
    command = ["redis-cli", "-h", service.host, "-p", str(service.port), *args]
    try:
        return subprocess.run(
            command,
            check=True,
            text=True,
            input=input_text,
            capture_output=True,
        )
    except FileNotFoundError as error:
        raise BackendError("required command not found in PATH: redis-cli") from error
    except subprocess.CalledProcessError as error:
        detail = (error.stderr or error.stdout or "").strip() or f"command exited with status {error.returncode}"
        raise BackendError(f"command failed: {quote_command(command)}\n{detail}") from error


def _reset_and_seed(service, *, keyspace_size: int) -> None:
    _redis_cli(service, ["FLUSHALL"])

    long_value = "x" * 200
    commands: list[list[str]] = []
    for index in range(1, keyspace_size + 1):
        commands.append(["SET", f"bench:key:{index}", f"value_{index}"])
        commands.append(["SET", f"bench:delete:{index}", f"value_{index}"])
        commands.append(["SET", f"bench:unlink:{index}", f"value_{index}"])
        commands.append(["SET", f"bench:getdel:{index}", f"value_{index}"])
        commands.append(["SET", f"bench:ttl:{index}", f"value_{index}", "EX", "3600"])
        commands.append(["SET", f"bench:long:{index}", long_value])
    for index in range(1, 1025):
        commands.append(["SET", f"bench:rename:{index}", f"value_{index}"])

    commands.extend(
        [
            ["SET", "bench:counter:decr", "0"],
            ["SET", "bench:counter:decrby", "0"],
            ["SET", "bench:counter:incrby", "0"],
            ["SET", "bench:counter:float", "0"],
            ["SET", "bench:key:copy:dest", "value"],
        ]
    )

    send_resp_pipeline(service.host, service.port, commands)


def _resolve_items(commands: list[str]) -> tuple[list[RedisBenchmarkItem], list[str]]:
    items: list[RedisBenchmarkItem] = []
    unsupported: list[str] = []
    seen: set[str] = set()

    for command in commands:
        if command in seen:
            continue
        seen.add(command)
        if command == "RENAMENX":
            unsupported.append(command)
            continue
        if command in BUILTIN_ITEMS:
            items.append(BUILTIN_ITEMS[command])
            continue
        if command in RAW_ITEMS:
            items.append(RAW_ITEMS[command])
            continue
        unsupported.append(command)
    return items, unsupported


def _parse_csv_metrics(path: Path, *, label: str) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not row:
                continue
            raw_label = (row.get("test") or row.get('"test"') or "").strip().strip('"')
            if raw_label and raw_label != label:
                row["label"] = raw_label
            else:
                row["label"] = label
            return {
                "label": row["label"],
                "rps": row.get("rps"),
                "avg_latency_ms": row.get("avg_latency_ms"),
                "min_latency_ms": row.get("min_latency_ms"),
                "p50_latency_ms": row.get("p50_latency_ms"),
                "p95_latency_ms": row.get("p95_latency_ms"),
                "p99_latency_ms": row.get("p99_latency_ms"),
                "max_latency_ms": row.get("max_latency_ms"),
            }
    raise BackendError(f"redis-benchmark did not produce a CSV metrics row for {label}: {path}")


def run_redis_benchmark_backend(context: BackendRunContext) -> BackendExecutionRecord:
    ensure_started = utc_now()
    requests = coerce_positive_int(
        get_setting(context.spec.settings, "redis-benchmark", "requests"),
        label="redis-benchmark.requests",
        default=DEFAULT_REDIS_REQUESTS,
    )
    clients = coerce_positive_int(
        get_setting(context.spec.settings, "redis-benchmark", "clients"),
        label="redis-benchmark.clients",
        default=DEFAULT_REDIS_CLIENTS,
    )
    pipeline = coerce_positive_int(
        get_setting(context.spec.settings, "redis-benchmark", "pipeline"),
        label="redis-benchmark.pipeline",
        default=DEFAULT_REDIS_PIPELINE,
    )
    keyspace_size = coerce_positive_int(
        get_setting(context.spec.settings, "redis-benchmark", "keyspace_size"),
        label="redis-benchmark.keyspace_size",
        default=DEFAULT_REDIS_KEYSPACE,
    )

    items, unsupported = _resolve_items(context.selected_commands)
    if not items:
        unsupported_text = ", ".join(unsupported) if unsupported else "no commands"
        raise BackendError(
            f"redis-benchmark could not map the selected commands to benchmarkable cases: {unsupported_text}"
        )

    backend_dir = (
        context.layout.backend_runs_dir
        / context.run_id
        / sanitize_identifier(context.service.database)
        / "redis-benchmark"
    )
    backend_dir.mkdir(parents=True, exist_ok=True)

    result_items: list[dict[str, object]] = []
    notes: list[str] = []
    status = "succeeded"

    if unsupported:
        status = "partial"
        notes.append(
            "redis-benchmark skipped commands without a Phase 4 adapter mapping: "
            + ", ".join(unsupported)
        )

    for item in items:
        _reset_and_seed(context.service, keyspace_size=keyspace_size)

        slug = sanitize_identifier(item.label.lower())
        stdout_path = backend_dir / f"{slug}.csv"
        stderr_path = backend_dir / f"{slug}.stderr.log"

        item_requests = requests
        item_clients = clients
        item_pipeline = pipeline
        if item.slow:
            item_requests = min(requests, 10_000)
            item_clients = min(clients, 10)
            item_pipeline = 1

        command = [
            "redis-benchmark",
            "-h",
            context.service.host,
            "-p",
            str(context.service.port),
            "-n",
            str(item_requests),
            "-c",
            str(item_clients),
            "-P",
            str(item_pipeline),
            "--precision",
            "3",
            "--csv",
            *item.benchmark_args,
        ]
        if item.mode == "builtin":
            command.extend(["-t", item.builtin_test or item.label])
        else:
            command.extend(item.command_args)

        snapshot_before = capture_service_snapshot(context.service)
        host_telemetry = None
        telemetry = start_host_telemetry_capture(
            backend_dir,
            label=slug,
            service=context.service,
        )
        try:
            _, elapsed = run_process(command, stdout_path=stdout_path, stderr_path=stderr_path)
        finally:
            host_telemetry = telemetry.stop()
        snapshot_after = capture_service_snapshot(context.service)
        metrics = _parse_csv_metrics(stdout_path, label=item.label)
        result_items.append(
            {
                "label": item.label,
                "mode": item.mode,
                "command": quote_command(command),
                "duration_seconds": round(elapsed, 6),
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
                "metrics": metrics,
                "observability": {
                    "before": snapshot_before,
                    "after": snapshot_after,
                    "delta": diff_service_snapshots(snapshot_before, snapshot_after),
                    "host_telemetry": host_telemetry,
                },
            }
        )

    completed_at = utc_now()
    result_path = context.layout.results_dir / f"{context.run_id}-{context.service.database}-redis-benchmark.json"
    summary = {
        "schema_version": 1,
        "generated_at": completed_at,
        "backend": "redis-benchmark",
        "database": context.service.database,
        "environment_id": context.state.environment_id,
        "request_path": str(context.request_path),
        "selected_commands": context.selected_commands,
        "config": {
            "requests": requests,
            "clients": clients,
            "pipeline": pipeline,
            "keyspace_size": keyspace_size,
        },
        "items": result_items,
        "notes": notes,
    }
    write_json(result_path, summary)

    return BackendExecutionRecord(
        backend="redis-benchmark",
        database=context.service.database,
        status=status,
        started_at=ensure_started,
        completed_at=completed_at,
        duration_seconds=round(sum(float(item["duration_seconds"]) for item in result_items), 6),
        selection={
            "commands": context.selected_commands,
            "workloads": context.selected_workloads,
        },
        artifacts={
            "backend_dir": str(backend_dir),
            "result_json": str(result_path),
            "host_telemetry_summary_paths": [
                ((item.get("observability") or {}).get("host_telemetry") or {}).get("summary_path")
                for item in result_items
                if ((item.get("observability") or {}).get("host_telemetry") or {}).get("summary_path")
            ],
        },
        items=result_items,
        notes=notes,
    )