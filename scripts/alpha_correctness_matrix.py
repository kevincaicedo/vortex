#!/usr/bin/env python3
"""Run and report the Vortex alpha correctness matrix."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import pathlib
import subprocess
import sys
from collections.abc import Iterable


@dataclasses.dataclass(frozen=True)
class MatrixRow:
    id: str
    title: str
    tier: str
    command: tuple[str, ...]
    covers: tuple[str, ...]
    tests: tuple[str, ...]


ROWS: tuple[MatrixRow, ...] = (
    MatrixRow(
        id="cargo-vortex-io",
        title="IO lifetime, backend, reactor, shutdown, and network correctness",
        tier="core",
        command=("cargo", "test", "-p", "vortex-io"),
        covers=(
            "io-package",
            "invalid-completion-tokens",
            "cancellation-races",
            "shutdown-inflight",
            "fixed-buffer-index-limits",
            "late-accept-drain",
            "large-bulk-across-reads",
            "iov-max-plus-one",
            "high-fd-polling",
            "ipv6-bind",
            "timer-deadline-bounds",
            "multi-reactor-aof-mode",
            "watch-multi-exec",
        ),
        tests=(
            "backend::tests::malformed_reserved_accept_bits_do_not_decode_as_accept",
            "backend::polling::tests::cancel_pending_writev_yields_ecanceled_completion",
            "reactor::tests::shutdown_drain_waits_for_terminal_cqes_before_releasing_buffers",
            "reactor::tests::fixed_buffer_policy_rejects_strict_oversized_range",
            "reactor::tests::late_accept_during_drain_closes_fd_without_allocating_connection",
            "reactor::tests::large_bulk_value_streams_across_read_buffer",
            "reactor::tests::iov_max_plus_one_pipeline_is_chunked_before_backend_submit",
            "backend::polling::tests::high_fd_registration_is_dense_not_raw_fd_indexed",
            "accept::tests::listener_binds_ipv6_loopback_when_host_supports_ipv6",
            "timer::tests::bulk_timers",
            "reactor::tests::multi_reactor_runtime_appendonly_enable_is_rejected_without_writer",
            "reactor::tests::watch_aborts_when_other_connection_modifies_key",
        ),
    ),
    MatrixRow(
        id="cargo-vortex-engine",
        title="Engine semantics, transactions, table invariants, effects, and cursor safety",
        tier="core",
        command=("cargo", "test", "-p", "vortex-engine"),
        covers=(
            "engine-package",
            "watch-multi-exec",
            "table-live-slot-safety",
            "deferred-effect-publication",
            "optimistic-prepare-revalidate-swap",
            "borrowed-multikey-duplicates",
            "fused-table-cursor-property",
            "timer-deadline-bounds",
        ),
        tests=(
            "commands::string::tests::mget_duplicate_key_returns_duplicate_values",
            "commands::generic::tests::exists_multi_with_duplicates",
            "engine::domain::tests::present_watch_exec_validation_sees_lsn_before_deferred_effects_publish",
            "engine::domain::tests::optimistic_setrange_retries_after_concurrent_delete",
            "engine::domain::tests::optimistic_incrbyfloat_retries_after_concurrent_expire",
            "keyspace::tests::eviction_effects_preserve_aof_watch_and_ttl_for_each_policy",
            "table::tests::slot_cursor_replace_reports_ttl_memory_and_stamp_status",
            "table::proptests::storage_metadata_invariants_match_model",
            "table::safe_slot_access_tests::out_of_bounds_slot_access_is_safe",
            "commands::tests::absolute_deadline_round_trips_between_unix_and_monotonic_domains",
        ),
    ),
    MatrixRow(
        id="cargo-vortex-persist",
        title="Persistence record, replay, rewrite, fsync, and failure-injection correctness",
        tier="core",
        command=("cargo", "test", "-p", "vortex-persist"),
        covers=(
            "persist-package",
            "multi-reactor-aof-mode",
            "persistence-replay-rewrite-fsync",
        ),
        tests=(
            "aof::reader::tests::kway_merge_two_reactors",
            "aof::reader::tests::kway_merge_fails_on_duplicate_lsn",
            "aof::rewrite::tests::manifest_rewrite_replays_base_and_post_swap_tail",
            "aof::writer::tests::append_with_lsn_returns_policy_commit_outcome",
            "aof::fault::tests::faulting_write_injects_append_and_flush_errors",
        ),
    ),
    MatrixRow(
        id="smoketest-build",
        title="Smoke-test harness compiles before optimization rows are accepted",
        tier="core",
        command=("cargo", "test", "-p", "vortex-smoketests", "--no-run"),
        covers=("smoketest-harness",),
        tests=("smoketests/tests/server_startup.rs", "smoketests/src/commands/*"),
    ),
    MatrixRow(
        id="smoketest-list",
        title="Smoke-test command catalog remains readable and registered",
        tier="core",
        command=("cargo", "run", "-p", "vortex-smoketests", "--", "list", "--verbose"),
        covers=("smoketest-harness", "watch-multi-exec", "borrowed-multikey-duplicates"),
        tests=("vortex-smoketests list --verbose",),
    ),
    MatrixRow(
        id="smoketest-command-compat",
        title="End-to-end command smoke against Vortex and Redis baseline",
        tier="smoke",
        command=(
            "cargo",
            "run",
            "-p",
            "vortex-smoketests",
            "--",
            "run",
            "--spawn-vortex",
            "--spawn-redis-baseline",
            "--vortex-arg=--threads",
            "--vortex-arg=4",
            "--report",
            "smoketests/.artifacts/alpha-correctness-smoke.md",
        ),
        covers=(
            "smoketest-e2e",
            "watch-multi-exec",
            "borrowed-multikey-duplicates",
            "large-bulk-across-reads",
        ),
        tests=("just smoke --vortex-arg=--threads --vortex-arg=4",),
    ),
    MatrixRow(
        id="smoketest-aof",
        title="Real-server AOF startup, replay, truncation, and backpressure smoke",
        tier="smoke",
        command=("cargo", "test", "-p", "vortex-smoketests", "--test", "server_startup", "--", "--nocapture"),
        covers=("smoketest-e2e", "multi-reactor-aof-mode", "persistence-replay-rewrite-fsync"),
        tests=(
            "real_server_replay_truncates_partial_merge_tail",
            "real_server_replay_rejects_mid_file_merge_corruption",
            "real_server_aof_always_replays_smoke_workload",
            "real_server_aof_everysec_backpressure_smoke_reports_telemetry",
        ),
    ),
)


COVERAGE: tuple[tuple[str, str], ...] = (
    ("io-package", "`cargo test -p vortex-io`"),
    ("engine-package", "`cargo test -p vortex-engine`"),
    ("persist-package", "`cargo test -p vortex-persist`"),
    ("invalid-completion-tokens", "Invalid completion tokens do not decode as accept or dispatch."),
    ("cancellation-races", "Cancel completions cannot release buffers before target terminal state."),
    ("shutdown-inflight", "Shutdown waits for terminal read/write/writev/close/cancel proof."),
    ("fixed-buffer-index-limits", "Fixed-buffer IDs validate before narrowing to kernel `u16`."),
    ("late-accept-drain", "Late accept during drain closes the fd without normal slot allocation."),
    ("large-bulk-across-reads", "Large bulk frames can span read buffers safely."),
    ("iov-max-plus-one", "`IOV_MAX + 1` pipelines are chunked before backend submit."),
    ("high-fd-polling", "Polling registry is dense and not raw-fd sized."),
    ("ipv6-bind", "Listeners bind IPv6 loopback when the host supports IPv6."),
    ("timer-deadline-bounds", "Timer and TTL deadline arithmetic stays bounded."),
    ("multi-reactor-aof-mode", "Multi-reactor AOF enable/disable/replay behavior is explicit."),
    ("watch-multi-exec", "WATCH/MULTI/EXEC conflicts and partial visibility are covered."),
    ("table-live-slot-safety", "Table live-slot/eager-entry safety and unsafe entry access invariants are covered."),
    ("deferred-effect-publication", "Deferred WATCH, TTL, AOF, eviction, and memory effects publish correctly."),
    ("optimistic-prepare-revalidate-swap", "Optimistic mutation prepare/revalidate/swap retries or falls back correctly."),
    ("borrowed-multikey-duplicates", "Borrowed multi-key duplicate semantics match Redis-visible behavior."),
    ("fused-table-cursor-property", "Fused table cursor and property tests cover TTL, LSN, raw bytes, resize, tombstones, and delete-heavy rows."),
    ("persistence-replay-rewrite-fsync", "AOF replay, rewrite, fsync policy, and failure injection remain green."),
    ("smoketest-harness", "Smoke-test harness compiles and command coverage is visible."),
    ("smoketest-e2e", "Real server smoke tests exercise command and AOF behavior through the Redis client."),
)

COVERAGE_PROOFS: dict[str, tuple[str, ...]] = {
    "io-package": ("cargo test -p vortex-io",),
    "engine-package": ("cargo test -p vortex-engine",),
    "persist-package": ("cargo test -p vortex-persist",),
    "invalid-completion-tokens": (
        "backend::tests::malformed_reserved_accept_bits_do_not_decode_as_accept",
        "reactor::tests::malformed_completion_token_is_counted_and_dropped",
    ),
    "cancellation-races": (
        "backend::polling::tests::cancel_pending_writev_yields_ecanceled_completion",
        "reactor::tests::cancel_completion_does_not_release_buffer_before_target_terminal",
        "reactor::tests::cancel_success_does_not_complete_target_operation",
    ),
    "shutdown-inflight": (
        "reactor::tests::shutdown_drain_waits_for_terminal_cqes_before_releasing_buffers",
        "reactor::tests::close_waits_for_inflight_io_before_releasing_buffers",
        "backend::polling::tests::close_purges_pending_fd_ops_and_armed_reads",
    ),
    "fixed-buffer-index-limits": (
        "backend::tests::fixed_buffer_id_rejects_overflow_before_u16_narrowing",
        "reactor::tests::fixed_buffer_policy_rejects_strict_oversized_range",
    ),
    "late-accept-drain": ("reactor::tests::late_accept_during_drain_closes_fd_without_allocating_connection",),
    "large-bulk-across-reads": (
        "reactor::tests::large_bulk_value_streams_across_read_buffer",
        "tests/engine_integration.rs::large_bulk_value_exceeds_io_buffer",
    ),
    "iov-max-plus-one": ("reactor::tests::iov_max_plus_one_pipeline_is_chunked_before_backend_submit",),
    "high-fd-polling": ("backend::polling::tests::high_fd_registration_is_dense_not_raw_fd_indexed",),
    "ipv6-bind": ("accept::tests::listener_binds_ipv6_loopback_when_host_supports_ipv6",),
    "timer-deadline-bounds": (
        "timer::tests::bulk_timers",
        "commands::tests::absolute_deadline_round_trips_between_unix_and_monotonic_domains",
    ),
    "multi-reactor-aof-mode": (
        "reactor::tests::multi_reactor_runtime_appendonly_enable_is_rejected_without_writer",
        "reactor::tests::multi_reactor_runtime_appendonly_disable_is_rejected_and_writer_remains",
        "aof::reader::tests::kway_merge_two_reactors",
        "real_server_aof_always_replays_smoke_workload",
    ),
    "watch-multi-exec": (
        "reactor::tests::watch_aborts_when_other_connection_modifies_key",
        "reactor::tests::multi_exec_handles_cross_key_and_duplicate_key_plans",
        "smoketests WATCH/MULTI/EXEC cases",
    ),
    "table-live-slot-safety": (
        "table::safe_slot_access_tests::out_of_bounds_slot_access_is_safe",
        "entry::tests::read_key_panics_for_empty_entry",
        "table::proptests::storage_metadata_invariants_match_model",
    ),
    "deferred-effect-publication": (
        "engine::domain::tests::present_watch_exec_validation_sees_lsn_before_deferred_effects_publish",
        "engine::domain::tests::absent_watch_exec_validation_sees_created_key_before_deferred_effects_publish",
        "keyspace::tests::eviction_effects_preserve_aof_watch_and_ttl_for_each_policy",
    ),
    "optimistic-prepare-revalidate-swap": (
        "engine::domain::tests::optimistic_setrange_retries_after_concurrent_delete",
        "engine::domain::tests::optimistic_incrbyfloat_retries_after_concurrent_expire",
        "engine::domain::tests::optimistic_setrange_retries_after_concurrent_copy_replace",
        "engine::domain::tests::optimistic_value_mutations_publish_watch_and_aof_lsn",
    ),
    "borrowed-multikey-duplicates": (
        "commands::generic::tests::exists_multi_with_duplicates",
        "commands::generic::tests::del_duplicate_key_counts_removed_key_once",
        "commands::string::tests::mset_duplicate_key_uses_last_value_under_tight_noeviction",
        "reactor::tests::multi_exec_handles_cross_key_and_duplicate_key_plans",
    ),
    "fused-table-cursor-property": (
        "table::tests::slot_cursor_replace_reports_ttl_memory_and_stamp_status",
        "table::tests::mutate_prehashed_raw_bytes_reports_old_ttl",
        "table::tests::resize_after_pointer_move_rewrites_heap_entry_pointers",
        "table::tests::memory_and_tombstone_ratios_remain_consistent_after_delete_heavy_workload",
        "table::proptests::storage_metadata_invariants_match_model",
    ),
    "persistence-replay-rewrite-fsync": (
        "aof::reader::tests::kway_merge_fails_on_duplicate_lsn",
        "aof::rewrite::tests::manifest_rewrite_replays_base_and_post_swap_tail",
        "aof::writer::tests::append_with_lsn_returns_policy_commit_outcome",
        "aof::fault::tests::faulting_write_injects_append_and_flush_errors",
    ),
    "smoketest-harness": (
        "cargo test -p vortex-smoketests --no-run",
        "cargo run -p vortex-smoketests -- list --verbose",
    ),
    "smoketest-e2e": (
        "cargo run -p vortex-smoketests -- run --spawn-vortex --spawn-redis-baseline",
        "cargo test -p vortex-smoketests --test server_startup",
    ),
}


PROFILE_TIERS = {
    "core": {"core"},
    "smoke": {"smoke"},
    "full": {"core", "smoke"},
}


@dataclasses.dataclass
class RowResult:
    row: MatrixRow
    status: str
    reason: str
    duration_seconds: float | None = None
    log_path: pathlib.Path | None = None


def parse_skip(raw_items: Iterable[str]) -> dict[str, str]:
    skipped: dict[str, str] = {}
    valid_ids = {row.id for row in ROWS}
    for raw in raw_items:
        if "=" not in raw:
            raise SystemExit(f"--skip requires ROW_ID=REASON, got {raw!r}")
        row_id, reason = raw.split("=", 1)
        row_id = row_id.strip()
        reason = reason.strip()
        if row_id not in valid_ids:
            raise SystemExit(f"unknown --skip row {row_id!r}")
        if not reason:
            raise SystemExit(f"--skip for {row_id} must include a non-empty reason")
        skipped[row_id] = reason
    return skipped


def shell_quote(command: Iterable[str]) -> str:
    return " ".join(sh_quote(part) for part in command)


def sh_quote(value: str) -> str:
    if value and all(ch.isalnum() or ch in "@%_+=:,./-" for ch in value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"


def run_row(row: MatrixRow, cwd: pathlib.Path, log_dir: pathlib.Path) -> RowResult:
    started = dt.datetime.now(dt.UTC)
    completed = None
    log_path = log_dir / f"{row.id}.log"
    header = [
        f"$ {shell_quote(row.command)}",
        f"cwd: {cwd}",
        f"started_at_utc: {started.isoformat(timespec='seconds')}",
        "",
    ]
    process = subprocess.run(
        row.command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    completed = dt.datetime.now(dt.UTC)
    duration = (completed - started).total_seconds()
    footer = [
        "",
        f"completed_at_utc: {completed.isoformat(timespec='seconds')}",
        f"duration_seconds: {duration:.3f}",
        f"exit_code: {process.returncode}",
    ]
    log_path.write_text("\n".join(header) + process.stdout + "\n".join(footer) + "\n", encoding="utf-8")
    status = "PASS" if process.returncode == 0 else "FAIL"
    reason = "command completed successfully" if process.returncode == 0 else f"exit code {process.returncode}"
    return RowResult(row=row, status=status, reason=reason, duration_seconds=duration, log_path=log_path)


def coverage_status(coverage_id: str, results: list[RowResult]) -> tuple[str, str, str]:
    covering = [result for result in results if coverage_id in result.row.covers]
    if not covering:
        return ("MISSING", "", "no matrix row covers this invariant")
    if any(result.status == "PASS" for result in covering):
        rows = ", ".join(result.row.id for result in covering if result.status == "PASS")
        tests = ", ".join(COVERAGE_PROOFS.get(coverage_id, ()))
        return ("PASS", rows, tests)
    if any(result.status == "FAIL" for result in covering):
        rows = ", ".join(result.row.id for result in covering if result.status == "FAIL")
        return ("FAIL", rows, "covered by failing row")
    rows = ", ".join(result.row.id for result in covering)
    reasons = "; ".join(result.reason for result in covering)
    return ("SKIPPED", rows, reasons)


def write_report(
    report_path: pathlib.Path,
    profile: str,
    cwd: pathlib.Path,
    results: list[RowResult],
    artifact_dir: pathlib.Path,
) -> None:
    passed = sum(1 for result in results if result.status == "PASS")
    failed = sum(1 for result in results if result.status == "FAIL")
    skipped = sum(1 for result in results if result.status in {"SKIPPED", "NOT_SELECTED"})
    generated = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    lines = [
        "# Vortex Alpha Correctness Matrix",
        "",
        f"- Generated: `{generated}`",
        f"- Profile: `{profile}`",
        f"- Workspace: `{cwd}`",
        f"- Artifact dir: `{artifact_dir}`",
        f"- Summary: `{passed}` passed, `{failed}` failed, `{skipped}` skipped/not-selected",
        "",
        "## Rows",
        "",
        "| Row | Status | Duration | Command | Log/Reason |",
        "| --- | --- | ---: | --- | --- |",
    ]
    for result in results:
        duration = "" if result.duration_seconds is None else f"{result.duration_seconds:.2f}s"
        command = shell_quote(result.row.command)
        log_or_reason = str(result.log_path) if result.log_path else result.reason
        lines.append(
            f"| `{result.row.id}` | `{result.status}` | {duration} | `{command}` | {log_or_reason} |"
        )

    lines.extend(
        [
            "",
            "## Coverage",
            "",
            "| Invariant | Status | Covering rows | Representative proof |",
            "| --- | --- | --- | --- |",
        ]
    )
    for coverage_id, description in COVERAGE:
        status, rows, proof = coverage_status(coverage_id, results)
        lines.append(f"| {description} | `{status}` | {rows} | {proof} |")

    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- `core` profile is the minimum optimization-loop gate for IO/engine/persist code changes.",
            "- `full` profile is the alpha release gate and includes real-server smoke rows.",
            "- Any skipped row must be passed with `--skip ROW=reason`; undocumented skips are rejected by the runner.",
            "- A performance optimization cannot be accepted if a relevant correctness row is failed, missing, or skipped without a task-specific reason.",
            "",
        ]
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")


def list_rows() -> None:
    for row in ROWS:
        print(f"{row.id}\t{row.tier}\t{shell_quote(row.command)}")
        print(f"  covers: {', '.join(row.covers)}")
        print(f"  representative tests: {', '.join(row.tests)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=sorted(PROFILE_TIERS), default="core")
    parser.add_argument("--cwd", type=pathlib.Path, default=pathlib.Path.cwd())
    parser.add_argument("--artifact-dir", type=pathlib.Path)
    parser.add_argument("--report", type=pathlib.Path)
    parser.add_argument("--skip", action="append", default=[], metavar="ROW=REASON")
    parser.add_argument("--fail-on-skip", action="store_true")
    parser.add_argument("--list", action="store_true", help="List matrix rows and exit.")
    args = parser.parse_args()

    if args.list:
        list_rows()
        return 0

    cwd = args.cwd.resolve()
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
    artifact_dir = (
        args.artifact_dir
        if args.artifact_dir is not None
        else cwd / ".artifacts" / "validation" / "alpha-correctness" / timestamp
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    log_dir = artifact_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    report_path = args.report if args.report is not None else artifact_dir / "report.md"
    skipped = parse_skip(args.skip)
    included_tiers = PROFILE_TIERS[args.profile]

    results: list[RowResult] = []
    for row in ROWS:
        if row.id in skipped:
            results.append(RowResult(row=row, status="SKIPPED", reason=skipped[row.id]))
            continue
        if row.tier not in included_tiers:
            results.append(
                RowResult(
                    row=row,
                    status="NOT_SELECTED",
                    reason=f"row tier `{row.tier}` is not part of profile `{args.profile}`",
                )
            )
            continue
        print(f"[alpha-correctness] running {row.id}: {shell_quote(row.command)}", flush=True)
        result = run_row(row, cwd, log_dir)
        print(
            f"[alpha-correctness] {row.id}: {result.status} ({result.duration_seconds:.2f}s)",
            flush=True,
        )
        results.append(result)

    write_report(report_path, args.profile, cwd, results, artifact_dir)
    print(f"[alpha-correctness] report: {report_path}")

    if any(result.status == "FAIL" for result in results):
        return 1
    if args.fail_on_skip and any(result.status in {"SKIPPED", "NOT_SELECTED"} for result in results):
        return 1
    if any(coverage_status(coverage_id, results)[0] == "MISSING" for coverage_id, _ in COVERAGE):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
