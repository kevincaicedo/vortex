#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
VORTEX_BENCHMARK_PYTHON = REPO_ROOT / "vortex-benchmark" / "python"
if str(VORTEX_BENCHMARK_PYTHON) not in sys.path:
    sys.path.insert(0, str(VORTEX_BENCHMARK_PYTHON))

from vortex_benchmark.models import ServiceState, utc_now  # noqa: E402
from vortex_benchmark.telemetry import start_host_telemetry_capture  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run profiler host telemetry until signaled.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--database", default="vortex")
    parser.add_argument("--mode", default="native")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--pid", type=int)
    return parser.parse_args()


def build_service(args: argparse.Namespace) -> ServiceState | None:
    if not args.host or not args.port:
        return None

    timestamp = utc_now()
    return ServiceState(
        database=args.database,
        mode=args.mode,
        host=args.host,
        port=args.port,
        ready=True,
        log_path="",
        started_at=timestamp,
        ready_at=timestamp,
        pid=args.pid,
    )


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    service = build_service(args)

    stop_event = threading.Event()

    def handle_signal(signum, _frame) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    collector = start_host_telemetry_capture(
        output_dir,
        label=args.label,
        service=service,
        interval_seconds=args.interval_seconds,
    )

    try:
        while not stop_event.wait(0.25):
            pass
    finally:
        result = collector.stop()
        (output_dir / "host-telemetry-runner.json").write_text(
            json.dumps(result, indent=2) + "\n",
            encoding="utf-8",
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())