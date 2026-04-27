from __future__ import annotations

import argparse
import sys
from typing import Optional

from vortex_benchmark.commands import (
    execute_attach,
    execute_report,
    execute_run,
    execute_setup,
    execute_teardown,
    has_run_selection,
)
from vortex_benchmark.db.base import SetupError
from vortex_benchmark.models import (
    DEFAULT_CPUS,
    DEFAULT_MEMORY,
    DEFAULT_PORT_BASE,
    SUPPORTED_AOF_FSYNC_POLICIES,
    SUPPORTED_EVICTION_POLICIES,
)

SUPPORTED_VORTEX_IO_BACKENDS = ("auto", "uring", "polling")
SUPPORTED_EVIDENCE_TIERS = ("exploratory", "engineering", "citation-grade")


def add_environment_arguments(parser: argparse.ArgumentParser, *, include_state_file: bool) -> None:
    parser.add_argument(
        "--db",
        dest="databases",
        action="append",
        default=[],
        help="Comma-separated database list. Supported: vortex, redis, dragonfly, valkey.",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--native", action="store_true", help="Use native processes where supported.")
    mode_group.add_argument("--container", action="store_true", help="Use Docker containers.")
    if include_state_file:
        parser.add_argument("--state-file", help="Path to an environment state file.")
    parser.add_argument(
        "--output-dir",
        help="Artifact root directory. Defaults to .artifacts/benchmarks under the repo root.",
    )
    parser.add_argument(
        "--port-base",
        type=int,
        default=None,
        help=f"Base port used for setup. Default: {DEFAULT_PORT_BASE}.",
    )
    parser.add_argument(
        "--cpus",
        type=int,
        default=None,
        help=f"CPU budget used for container limits and service thread defaults. Default: {DEFAULT_CPUS}.",
    )
    parser.add_argument(
        "--memory",
        default=None,
        help=f"Container memory limit to record in state files. Default: {DEFAULT_MEMORY}.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        help="Override the service thread count used for Vortex, Redis, Dragonfly, or Valkey setup.",
    )
    aof_group = parser.add_mutually_exclusive_group()
    aof_group.add_argument(
        "--aof-enabled",
        dest="aof_enabled",
        action="store_true",
        help="Enable AOF or appendonly persistence for supported databases during setup.",
    )
    aof_group.add_argument(
        "--aof-disabled",
        dest="aof_enabled",
        action="store_false",
        help="Explicitly disable AOF or appendonly persistence in the requested runtime config.",
    )
    parser.add_argument(
        "--aof-fsync",
        choices=SUPPORTED_AOF_FSYNC_POLICIES,
        help="AOF fsync policy for supported databases. Choices: always, everysec, no.",
    )
    parser.add_argument(
        "--maxmemory",
        help="Database maxmemory setting for setup scenarios, for example 4mb, 2g, or 1073741824.",
    )
    parser.add_argument(
        "--eviction-policy",
        choices=SUPPORTED_EVICTION_POLICIES,
        help="Database eviction policy for supported databases.",
    )
    parser.add_argument(
        "--io-backend",
        choices=SUPPORTED_VORTEX_IO_BACKENDS,
        help="Vortex I/O backend override for setup scenarios. Choices: auto, uring, polling.",
    )
    parser.add_argument(
        "--ring-size",
        type=int,
        help="Vortex io_uring submission queue size override for setup scenarios.",
    )
    parser.add_argument(
        "--fixed-buffers",
        type=int,
        help="Vortex fixed I/O buffer count override for setup scenarios.",
    )
    parser.add_argument(
        "--sqpoll-idle-ms",
        type=int,
        help="Vortex SQPOLL idle timeout override for setup scenarios.",
    )
    parser.add_argument(
        "--no-build-vortex",
        dest="build_vortex",
        action="store_false",
        help="Do not auto-build the Vortex binary or Docker image when missing.",
    )
    parser.set_defaults(build_vortex=None)
    parser.set_defaults(aof_enabled=None)


def add_run_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--evidence-tier",
        choices=SUPPORTED_EVIDENCE_TIERS,
        default="engineering",
        help="Validity tier for the run metadata. Default: engineering.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=None,
        help="Execute each selected scenario multiple times. Overrides manifest repeat; defaults to 1 when neither is set.",
    )
    parser.add_argument(
        "--backend",
        dest="backends",
        action="append",
        default=[],
        help="Comma-separated backend list to execute. Defaults are chosen from the selected commands and workloads.",
    )
    parser.add_argument(
        "--workload",
        dest="workloads",
        action="append",
        default=[],
        help="Comma-separated workload names.",
    )
    parser.add_argument(
        "--workload-manifest",
        help="Path to a JSON or YAML benchmark manifest.",
    )
    parser.add_argument(
        "--command",
        dest="commands",
        action="append",
        default=[],
        help="Comma-separated command names.",
    )
    parser.add_argument(
        "--command-group",
        dest="command_groups",
        action="append",
        default=[],
        help="Comma-separated higher-level command groups.",
    )
    parser.add_argument("--duration", help="Requested benchmark duration, for example 60s.")


def add_report_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--summary-file",
        dest="summary_files",
        action="append",
        default=[],
        help="Path to a benchmark summary JSON file. Repeat to aggregate multiple runs.",
    )
    parser.add_argument(
        "--results-dir",
        help="Directory containing *-summary.json files. Defaults to .artifacts/benchmarks/results when no summary file is passed.",
    )
    parser.add_argument(
        "--output-dir",
        help="Artifact root directory for generated report outputs. Defaults to the source artifact root.",
    )
    parser.add_argument(
        "--title",
        help="Optional report title override.",
    )


def add_attach_arguments(parser: argparse.ArgumentParser) -> None:
    add_environment_arguments(parser, include_state_file=True)
    parser.add_argument("--host", required=True, help="Host of the already running endpoint.")
    parser.add_argument(
        "--port",
        type=int,
        required=True,
        help="RESP port of the already running endpoint.",
    )
    parser.add_argument(
        "--pid",
        type=int,
        required=True,
        help="PID of the already running server process.",
    )
    parser.add_argument(
        "--label",
        help="Optional short label added to the attached environment identifier.",
    )
    parser.add_argument(
        "--binary-path",
        help="Optional binary path recorded in the environment metadata.",
    )
    parser.add_argument(
        "--log-path",
        help="Optional existing log file path to record in the attached state.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="vortex_bench",
        description="Unified benchmark CLI and setup subsystem for VortexDB.",
        epilog=(
            "Phase 5 status: setup, manifest resolution, backend execution, normalized JSON/CSV exports, "
            "and Markdown report generation with charts are live. Phase 7 expands CI and documentation integration."
        ),
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Run setup in unified mode before optional run-request resolution.",
    )
    parser.add_argument(
        "--teardown",
        action="store_true",
        help="Stop services tracked by --state-file in unified mode.",
    )
    add_environment_arguments(parser, include_state_file=True)
    add_run_arguments(parser)

    subparsers = parser.add_subparsers(dest="subcommand")

    setup_parser = subparsers.add_parser("setup", help="Start database services and write a state file.")
    add_environment_arguments(setup_parser, include_state_file=True)
    setup_parser.add_argument(
        "--workload-manifest",
        help="Path to a JSON or YAML benchmark manifest.",
    )

    attach_parser = subparsers.add_parser(
        "attach",
        help="Record an already running endpoint as an environment state file.",
    )
    add_attach_arguments(attach_parser)

    run_parser = subparsers.add_parser(
        "run",
        help="Execute benchmark backends from an existing state file.",
    )
    add_environment_arguments(run_parser, include_state_file=True)
    add_run_arguments(run_parser)

    teardown_parser = subparsers.add_parser(
        "teardown",
        help="Stop services tracked by an environment state file.",
    )
    teardown_parser.add_argument("--state-file", required=True, help="Path to an environment state file.")

    report_parser = subparsers.add_parser(
        "report",
        help="Aggregate benchmark summaries into JSON, CSV, Markdown, and chart artifacts.",
    )
    add_report_arguments(report_parser)

    return parser


def dispatch(args: argparse.Namespace) -> int:
    if args.subcommand == "setup":
        execute_setup(args)
        return 0

    if args.subcommand == "attach":
        execute_attach(args)
        return 0

    if args.subcommand == "run":
        execute_run(args)
        return 0

    if args.subcommand == "teardown":
        execute_teardown(args)
        return 0

    if args.subcommand == "report":
        execute_report(args)
        return 0

    if args.setup and args.teardown:
        raise ValueError("--setup and --teardown cannot be combined")

    if args.teardown:
        if not args.state_file:
            raise ValueError("--teardown requires --state-file")
        execute_teardown(args)
        return 0

    if args.setup:
        state = execute_setup(args)
        if has_run_selection(args):
            execute_run(args, preloaded_state=state)
        return 0

    if args.state_file and has_run_selection(args):
        execute_run(args)
        return 0

    raise ValueError(
        "no action requested; use --setup, a subcommand, or --state-file with run selectors"
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        return dispatch(args)
    except (SetupError, ValueError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
