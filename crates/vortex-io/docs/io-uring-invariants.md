# io_uring Reactor Invariant Review

Status: 2026-05-28 source-level review note for `PR-IO-003A`, refreshed with
backend-contract SQ-full, fatal backend error, CQ-overflow coverage, native SQ
pressure, cancellation-race, write-pressure coverage, reactor SQ-full family
rows, reactor partial-write drain and fd-reuse coverage, typed native-status
CQ-overflow accounting, and native strict runtime/churn/shutdown-drain coverage
for `PR-IO-003B` through `PR-IO-003F`.

Scope: this note maps the current `vortex-io` submit, completion, cancel, and
fd lifetime contracts for the Linux `io_uring` backend. It is correctness
evidence only. It does not authorize an `io_uring` throughput, latency, SQPOLL,
or registered-buffer release claim.

## Backend Boundary

The backend boundary is crate-private in `crates/vortex-io/src/backend/mod.rs`.
Production code dispatches through the `Backend` enum, not a hot-path trait
object. The `BackendDriver` trait is the shared test/backend contract, while
concrete polling and `io_uring` modules keep backend-specific behavior local.

Typed submission objects carry ownership into backend calls:

| Object | Contract |
| --- | --- |
| `ListenerFd` | Reactor-owned listening fd used only for accept submissions. |
| `ConnFd` | Reactor connection fd. Close ownership remains with the active or closing slot until terminal proof. |
| `ReadLease` | Writable reactor buffer range, non-null, non-empty, result-width checked, live until the matching CQE is handled. |
| `WriteLease` | Readable reactor buffer range, non-null, non-empty, result-width checked, live until the matching CQE is handled. |
| `IovecBatch` | Non-null iovec slice with `count <= IOV_MAX`; iovec storage and response buffers stay live until writev completion. |
| `FixedBufferId` | Checked `usize -> u16` narrowing before fixed-buffer registration or fixed read/write submission. |
| `CompletionToken` | Typed wrapper over backend `user_data`; decoding is fallible and cancel tokens are distinct from target-operation tokens. |

## Submission Paths

| Path | io_uring opcode | Owner/lifetime proof | Failure contract |
| --- | --- | --- | --- |
| Accept | `Accept` with `SOCK_NONBLOCK | SOCK_CLOEXEC` | Listener fd remains owned by reactor. Accepted fds are nonblocking/CLOEXEC at creation. | SQ push failure returns `SubmitError::QueueFull`. |
| Read | `ReadFixed` when fixed buffers are registered and lease has an id; otherwise `Read` | `ReadLease` points to reactor-owned buffer memory that stays live until CQE handling. | Empty/null/oversized leases are rejected before submission; SQ push failure returns `QueueFull`. |
| Write | `WriteFixed` when fixed buffers are registered and lease has an id; otherwise `Write` | `WriteLease` points to reactor-owned response memory that stays live until CQE handling. | Empty/null/oversized leases are rejected before submission; SQ push failure returns `QueueFull`. |
| Writev | `Writev` | `IovecBatch` enforces segment count and points at stable iovec/response memory owned by `PendingWritev`. | Empty/null/oversized batches are rejected before submission; SQ push failure returns `QueueFull`. |
| Close | `Close` | The closing connection slot owns the fd until close completion, or until fallback direct close after all tracked operations are terminal. | SQ push failure is handled by close-state fallback; fd is not immediately released while operations remain in flight. |
| Cancel | `AsyncCancel::new(target.raw())` with separate `cancel_token.raw()` user data | Cancel CQE cannot decode as the original target operation. The target remains tracked until its own terminal proof or backend `ENOENT` classification. | SQ push failure returns `QueueFull`; duplicate cancel submissions are suppressed by reactor cancel-inflight state. |
| Fixed-buffer registration | `register_buffers` | Iovecs point to pinned `BufferPool` storage that outlives the backend. | Strict fixed mode fails startup; auto mode can continue without fixed buffers. |
| Non-SQPOLL flush | Deferred to `completions()` | Submit and wait share one bounded `io_uring_enter` path. | Non-`ETIME` submit errors are returned; `ETIME` is an empty drain. |
| SQPOLL flush | `submit()` | Synchronizes SQ tail and wakes the poll thread after idle. | Submit errors are surfaced as `SubmitError::Backend`. |

## Completion Dispatch

The reactor decodes every CQE through `CompletionToken::decode` before dispatch.
Invalid accept payload bits, unknown operation bits, and malformed cancel targets
are counted and dropped rather than falling through to accept or connection
handling.

Connection completions carry a slot id, 24-bit generation, and operation kind.
`live_conn_token` validates that the slot is still occupied and the generation
matches before cancellation or shutdown logic uses the token. Normal completion
dispatch also drops same-generation completions that have no matching in-flight
operation state, so duplicate or late CQEs do not mutate a reused slot.

Cancel completions are classified separately from target completions:

| Cancel result | Meaning |
| --- | --- |
| `result >= 0` | Cancel request was accepted; wait for the target operation to complete terminally. |
| `-ENOENT` with target still tracked | Backend could not find the target; keep terminal proof explicit. |
| `-ENOENT` after target terminal | Target already completed; cancel can be retired. |
| Other negative result | Backend error; the target operation is not treated as completed by the cancel CQE. |

Read and write completions validate result ranges before mutating connection
state. Partial `write` and `writev` completions resume from stable owned pending
state instead of rebuilding response iovecs from borrowed stack data.

## Shutdown And fd Lifetime

Shutdown drain and connection close use terminal-state proof before releasing
buffers or fds:

- Entering drain cancels outstanding accept and closes late accepted fds without
  allocating normal connection state.
- Closing a connection submits cancels for tracked read/write/writev operations
  and submits a close operation for the fd.
- Buffer leases are released only after the original operation is terminal, or
  after backend `ENOENT` proves the target was already terminal.
- Direct fd close fallback is delayed until tracked read/write/writev/cancel
  state is terminal, which prevents fd reuse before late CQEs are classified.
- Shutdown drain processes nonblocking CQEs until the tracked in-flight and
  accept-cancel state is empty or the caller's shutdown timeout expires.

## Focused Coverage

Existing focused tests cover the current source-level proof:

| Invariant | Coverage |
| --- | --- |
| Fallible token decode and cancel-token separation | `cargo test -p vortex-io completion_token_is_zero_cost_u64_wrapper`, `cargo test -p vortex-io cancel_success_does_not_complete_target_operation`, `cargo test -p vortex-io cancel_enoent_tracks_not_found_vs_already_terminal` |
| Stale slot reuse and duplicate same-generation CQEs | `cargo test -p vortex-io stale_slot_reuse_cqes_are_dropped_by_generation`, `cargo test -p vortex-io stale_cancel_cqe_does_not_touch_reused_slot_generation`, `cargo test -p vortex-io same_generation_completion_without_inflight_state_is_dropped` |
| Close-finalization fd reuse | `cargo test -p vortex-io --features io-uring stale_cqes_after_close_finalization_do_not_touch_reused_fd_slot -- --nocapture` |
| Shutdown waits for target terminal proof before releasing buffers | `cargo test -p vortex-io shutdown_drain_waits_for_terminal_cqes_before_releasing_buffers`, `cargo test -p vortex-io cancel_completion_does_not_release_buffer_before_target_terminal`, `cargo test -p vortex-io cancel_enoent_does_not_release_buffer_before_target_terminal` |
| SQ-full retry preserves operation ownership | `cargo test -p vortex-io submit_cancel_sq_full`, `cargo test -p vortex-io submit_read_sq_full`, `cargo test -p vortex-io submit_close_sq_full`, `cargo test -p vortex-io --features io-uring submit_cancel_sq_full`, `cargo test -p vortex-io --features io-uring submit_close_sq_full` |
| Reactor SQ-full operation families | `cargo test -p vortex-io --features io-uring submit_accept_sq_full -- --nocapture`, `cargo test -p vortex-io --features io-uring submit_writev_sq_full -- --nocapture` |
| Fatal backend errors and CQ overflow status | `cargo test -p vortex-io backend_flush_error_records_submit_failure_and_stops`, `cargo test -p vortex-io backend_completions_error_records_submit_failure_and_stops`, `cargo test -p vortex-io backend_drain_cq_error_records_submit_failure_without_blocking_shutdown`, `cargo test -p vortex-io backend_cq_overflow_status_publishes_from_cold_snapshot`, `cargo test -p vortex-io backend_queue_capacity_publishes_once_during_reactor_construction` |
| Accept drain closes to new clients | `cargo test -p vortex-io shutdown_drain_processes_accept_completion_without_connections`, `cargo test -p vortex-io shutdown_drain_does_not_duplicate_accept_cancel_after_enter_drain`, `cargo test -p vortex-io accept_cancel_enoent_keeps_accept_inflight_until_accept_cqe` |
| Read/write cursor and partial write ownership | `cargo test -p vortex-io oversized_read_completion_closes_before_extending_read_cursor`, `cargo test -p vortex-io writev_eagain_completion_resubmits_without_advancing`, `cargo test -p vortex-io pending_writev_chunks_at_backend_iov_limit` |
| Reactor slow-reader partial write drain | `cargo test -p vortex-io --features io-uring capped_pending_write_partial_resubmits_then_closes_after_drain -- --nocapture` |
| Polling fallback close/cancel handle model | `cargo test -p vortex-io close_purges_pending_fd_ops_and_armed_reads`, `cargo test -p vortex-io cancel_armed_write_yields_ecanceled_completion`, `cargo test -p vortex-io high_fd_registration_is_dense_not_raw_fd_indexed` |
| Linux feature build coverage | `cargo test -p vortex-io --features io-uring` |
| Native io_uring SQ pressure and queue status | `env VORTEX_REQUIRE_IO_URING_TESTS=1 cargo test -p vortex-io --features io-uring native_ -- --nocapture` |
| Native-status CQ overflow accounting | `cargo test -p vortex-io --features io-uring native_queue_status -- --nocapture`, `cargo test -p vortex-io --features io-uring backend_cq_overflow_status_publishes_from_cold_snapshot -- --nocapture` |
| Native io_uring cancellation race | `env VORTEX_REQUIRE_IO_URING_TESTS=1 cargo test -p vortex-io --features io-uring native_cancel_race -- --nocapture` |
| Native io_uring pressured writev completion | `env VORTEX_REQUIRE_IO_URING_TESTS=1 cargo test -p vortex-io --features io-uring native_writev_pressure -- --nocapture` |
| Native strict io_uring runtime contract and churn/shutdown drain | `env VORTEX_REQUIRE_IO_URING_TESTS=1 cargo test -p vortex-io --features io-uring --test io_uring_runtime -- --nocapture` |

## Remaining PR-IO-003 Gaps

This closes the source-level review-note slice and adds mock-backend SQ-full
retry coverage for read, cancel, and close submissions, plus fatal
flush/completion/drain error coverage, CQ-overflow publication coverage, a stale
cancel-CQE slot-reuse guard, native SQ `QueueFull`/queue-status coverage, typed
CQ-overflow delta accounting through the native status path, native
cancellation-race token-separation coverage, native pressured-writev
short/EAGAIN coverage, deterministic reactor-level slow-reader partial-write
drain coverage, deterministic close-finalization fd-reuse coverage, mock
reactor SQ-full rows for accept/read/writev/cancel/close ownership, native
strict runtime/churn/shutdown-drain coverage, and engineering-tier native
slow-reader/fd-reuse timing visibility. The release gate still requires a
reliable kernel CQ-overflow workload row or explicit acceptance waiver, plus
clean p99.9-capable pressure repeats, before backend performance experiments or
public backend claims.
