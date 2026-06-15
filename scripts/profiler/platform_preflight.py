#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
VORTEX_BENCHMARK_PYTHON = REPO_ROOT / "vortex-benchmark" / "python"
if str(VORTEX_BENCHMARK_PYTHON) not in sys.path:
    sys.path.insert(0, str(VORTEX_BENCHMARK_PYTHON))

from vortex_benchmark.telemetry import capture_profiler_preflight  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report profiler platform capability boundaries.")
    parser.add_argument("--system", help="Override platform.system() for validation.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args()


def render_text(payload: dict[str, object]) -> str:
    probes = payload.get("probes") or []
    lines = [
        f"platform={payload.get('platform')}",
        f"platform_key={payload.get('platform_key')}",
        f"evidence_boundary={payload.get('evidence_boundary')}",
    ]

    linux_only = payload.get("unavailable_linux_only_probes") or []
    if linux_only:
        lines.append("unavailable_linux_only_probes=" + ",".join(str(item) for item in linux_only))

    macos_only = payload.get("unavailable_macos_only_probes") or []
    if macos_only:
        lines.append("unavailable_macos_only_probes=" + ",".join(str(item) for item in macos_only))

    lines.append("probes:")
    for probe in probes:
        if not isinstance(probe, dict):
            continue
        reason = probe.get("reason") or "ok"
        lines.append(f"- {probe.get('name')}: {probe.get('status')} ({reason})")

    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    payload = capture_profiler_preflight(system=args.system)
    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print(render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
