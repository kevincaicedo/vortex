#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
VORTEX_BENCHMARK_PYTHON = REPO_ROOT / "vortex-benchmark" / "python"
if str(VORTEX_BENCHMARK_PYTHON) not in sys.path:
    sys.path.insert(0, str(VORTEX_BENCHMARK_PYTHON))

from vortex_benchmark.manifests.loader import load_manifest  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare profiler benchmark bridge inputs.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--manifest")
    group.add_argument("--request")
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def parse_duration_seconds(raw: str | None) -> int | None:
    if not raw:
        return None

    raw = raw.strip()
    if raw.endswith("ms"):
        millis = int(raw[:-2])
        return max(1, (millis + 999) // 1000)
    if raw.endswith("s"):
        return int(raw[:-1])
    if raw.endswith("m"):
        return int(raw[:-1]) * 60
    if raw.endswith("h"):
        return int(raw[:-1]) * 3600
    return None


def shell_assign(name: str, value: str | None) -> str:
    safe = "" if value is None else value
    return f"{name}={shlex.quote(safe)}"


def manifest_description(path: Path) -> tuple[str, str | None, str | None, dict[str, object], dict[str, object]]:
    manifest = load_manifest(path)
    description = manifest.description or f"vortex_bench manifest {path.name}"
    return (
        str(path.resolve()),
        description,
        manifest.duration,
        dict(manifest.resource_config),
        dict(manifest.runtime_config),
    )


def request_to_manifest(request_path: Path, output_dir: Path) -> tuple[str, str, str | None, dict[str, object], dict[str, object]]:
    payload = json.loads(request_path.read_text(encoding="utf-8"))
    generated_manifest = {
        "schema_version": 1,
        "name": payload.get("manifest_name") or f"profiler-bridge-{request_path.stem}",
        "description": f"Generated from benchmark request {request_path.name}",
        "databases": ["vortex"],
        "workloads": payload.get("workloads") or [],
        "commands": payload.get("commands") or [],
        "command_groups": payload.get("command_groups") or [],
        "backends": payload.get("resolved_backends") or payload.get("requested_backends") or [],
        "duration": payload.get("duration"),
        "resource_config": payload.get("resource_config") or {},
        "runtime_config": payload.get("runtime_config") or {},
        "settings": payload.get("settings") or {},
    }

    generated_path = output_dir / "bridge-request-manifest.json"
    generated_path.write_text(json.dumps(generated_manifest, indent=2) + "\n", encoding="utf-8")
    return (
        str(generated_path.resolve()),
        generated_manifest["description"],
        generated_manifest.get("duration"),
        dict(generated_manifest.get("resource_config") or {}),
        dict(generated_manifest.get("runtime_config") or {}),
    )


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    effective_request_path = ""
    if args.manifest:
        manifest_path = Path(args.manifest).expanduser().resolve()
        effective_manifest, description, duration, resource_config, runtime_config = manifest_description(manifest_path)
        source = "vortex_bench-manifest"
    else:
        request_path = Path(args.request).expanduser().resolve()
        effective_request_path = str(request_path)
        effective_manifest, description, duration, resource_config, runtime_config = request_to_manifest(request_path, output_dir)
        source = "vortex_bench-request"

    duration_seconds = parse_duration_seconds(duration)
    print(shell_assign("BENCH_EFFECTIVE_MANIFEST", effective_manifest))
    print(shell_assign("BENCH_EFFECTIVE_REQUEST_PATH", effective_request_path))
    print(shell_assign("BENCH_EFFECTIVE_SOURCE", source))
    print(shell_assign("BENCH_EFFECTIVE_DESCRIPTION", description))
    print(shell_assign("BENCH_EFFECTIVE_DURATION_SECONDS", "" if duration_seconds is None else str(duration_seconds)))
    print(shell_assign("BENCH_RESOURCE_THREADS", str(resource_config.get("threads", ""))))
    print(shell_assign("BENCH_RUNTIME_AOF_ENABLED", "" if "aof_enabled" not in runtime_config else str(runtime_config.get("aof_enabled")).lower()))
    print(shell_assign("BENCH_RUNTIME_MAXMEMORY", str(runtime_config.get("maxmemory", ""))))
    print(shell_assign("BENCH_RUNTIME_EVICTION_POLICY", str(runtime_config.get("eviction_policy", ""))))
    print(shell_assign("BENCH_RUNTIME_IO_BACKEND", str(runtime_config.get("io_backend", ""))))
    print(shell_assign("BENCH_RUNTIME_RING_SIZE", str(runtime_config.get("ring_size", ""))))
    print(shell_assign("BENCH_RUNTIME_SQPOLL_IDLE_MS", str(runtime_config.get("sqpoll_idle_ms", ""))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())