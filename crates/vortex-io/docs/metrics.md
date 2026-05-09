# Vortex IO Release Metrics

IO release metrics exist to prove backend capability, bounded reactor work,
bounded per-client memory, and overload behavior. They are deliberately not a
full profiler.

`target/release/vortex-server` supports `minimal` only. Profile timing requires
a profiling binary compiled with `--features profile-telemetry`; normal release
does not compile the profile timer reads or storage.

Supported-mode notation:

- `release:minimal` means available in the normal release binary.
- `profiling:minimal/profile` means available in the profiling binary, with
  Profile allowed only when compiled with `profile-telemetry`.

## Metric Catalog

| Metric | Group | Supported modes | Hot path? | Update path and overhead | Why it stays in release |
| --- | --- | --- | --- | --- | --- |
| `backend_requested` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup/config publication. | Fair benchmark rows must name requested backend. |
| `backend_effective` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup/effective-plan publication. | Auto/mixed fallback must be visible. |
| `backend_plan_mixed` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup/effective-plan publication. | Prevents claiming io_uring when part of the plan fell back. |
| `backend_fixed_buffers_capable` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup capability publication. | Documents whether fixed buffers can be used. |
| `backend_fixed_buffers_registered` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup registration result. | Proves fixed-buffer mode actually registered. |
| `backend_sqpoll` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup capability publication. | SQPOLL changes fairness, CPU cost, and comparison validity. |
| `backend_multishot_accept` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup capability publication. | Accept behavior affects connection-storm results. |
| `backend_accept4` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup capability publication. | Documents accept syscall path. |
| `backend_close_opcode` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup capability publication. | Close-drain correctness depends on backend close support. |
| `backend_cancel_support` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup capability publication. | Cancel semantics are part of backend safety. |
| `backend_nonblocking_drain` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup capability publication. | Explains shutdown/drain behavior. |
| `backend_requested_ring_size` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup/config publication. | Ring size is required for fair io_uring comparisons. |
| `backend_effective_ring_size` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup/effective-plan publication. | Documents clamped or failed ring sizing. |
| `backend_submit_syscalls` | Backend pressure | release:minimal; profiling:minimal/profile | Submit path | Sharded counter on backend submit/flush events. | Required to compare backend syscall pressure. |
| `backend_sq_occupancy_max` | Backend pressure | release:minimal; profiling:minimal/profile | Event-loop/backend boundary | Backend queue sample before flush/fast CQ drain; shared max only when occupancy is nonzero. | Required to diagnose SQ saturation without per-SQE instrumentation. |
| `backend_sq_capacity` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup queue sample. | Makes SQ occupancy interpretable across ring sizes. |
| `backend_cq_occupancy_max` | Backend pressure | release:minimal; profiling:minimal/profile | Event-loop/backend boundary | Backend queue sample before flush/fast CQ drain; shared max only when occupancy is nonzero. | Required to diagnose completion backlog and CQ pressure. |
| `backend_cq_capacity` | Backend contract | release:minimal; profiling:minimal/profile | No | Startup queue sample. | Makes CQ occupancy interpretable across ring sizes. |
| `backend_cq_overflows` | Backend pressure | release:minimal; profiling:minimal/profile | Event-loop/backend boundary | Reads io_uring CQ overflow counter at queue sample points and publishes deltas only when nonzero. | CQ overflow is correctness/performance critical; release reports must not hide dropped completions. |
| `backend_completions_per_submit_syscall_x1000` | Backend pressure | release:minimal; profiling:minimal/profile | No | Derived from completion batch total and submit syscall count. | Required to evaluate non-SQPOLL submit policy without adding another hot counter. |
| `backend_sq_pressure_events` | Backend pressure | release:minimal; profiling:minimal/profile | Pressure path | Alias of SQ-full retry pressure. | Support/report compatibility while backend reports stabilize. |
| `backend_cq_pressure_events` | Backend pressure | release:minimal; profiling:minimal/profile | Budget-exhaustion path | Alias of completion-budget exhaustion pressure. | Support/report compatibility while backend reports stabilize. |
| `reactor_accept_eagain_rearms` | Accept pressure | release:minimal; profiling:minimal/profile | Accept path on EAGAIN | Sharded counter only when accept returns EAGAIN. | Explains accept loop rearm behavior. |
| `reactor_submit_sq_full_retries` | Backend pressure | release:minimal; profiling:minimal/profile | SQ-full path only | Sharded counter only on retry. | Required to diagnose ring saturation. |
| `reactor_submit_failures` | Backend errors | release:minimal; profiling:minimal/profile | Error path | Sharded counter only on submit failure. | Correctness and availability signal. |
| `reactor_completion_budget_exhaustions` | Fairness budget | release:minimal; profiling:minimal/profile | Event loop when budget exhausted | Sharded counter only on yield. | Explains p99.9 cliffs caused by completion backlog. |
| `reactor_command_budget_exhaustions` | Fairness budget | release:minimal; profiling:minimal/profile | Command loop when budget exhausted | Sharded counter only on yield. | Proves pipelines are bounded by command budget. |
| `reactor_accept_budget_exhaustions` | Fairness budget | release:minimal; profiling:minimal/profile | Accept loop when budget exhausted | Sharded counter only on yield. | Required for connection-storm diagnosis. |
| `reactor_writev_budget_exhaustions` | Fairness budget | release:minimal; profiling:minimal/profile | Write loop when budget exhausted | Sharded counter only on yield. | Proves large writes cannot monopolize the reactor. |
| `reactor_maintenance_budget_exhaustions` | Fairness budget | release:minimal; profiling:minimal/profile | Maintenance path | Sharded counter only on yield. | Shows maintenance debt before tail latency moves. |
| `reactor_yielded_connections` | Fairness budget | release:minimal; profiling:minimal/profile | Budget-yield path | Sharded counter only when a connection is requeued. | Proves resumable parsing/execution is active. |
| `reactor_parser_resumes` | Parser fairness | release:minimal; profiling:minimal/profile | Resume path | Sharded counter only when parser resumes. | Detects deep-pipeline or large-request pressure. |
| `reactor_loop_iterations` | Local flush batch | release:minimal; profiling:minimal/profile | Event-loop iteration | Reactor-local plain `u64`, cold flushed. | Basic denominator for scheduler/fairness interpretation. |
| `reactor_accept_drain_runs` | Local flush batch | release:minimal; profiling:minimal/profile | Accept phase | Reactor-local plain `u64`, cold flushed. | Shows accept-drain activity without per-accept atomics. |
| `reactor_accept_drain_accepted` | Local flush batch | release:minimal; profiling:minimal/profile | Accept phase | Reactor-local plain `u64`, cold flushed. | Measures accepted connections per drain. |
| `reactor_accept_drain_accepted_max` | Local flush batch | release:minimal; profiling:minimal/profile | Accept phase | Reactor-local max, cold flushed. | Detects connection-storm bursts. |
| `reactor_completion_batches` | Local flush batch | release:minimal; profiling:minimal/profile | Completion phase | Reactor-local plain `u64`, cold flushed. | Explains CQE batch width. |
| `reactor_completion_batch_total` | Local flush batch | release:minimal; profiling:minimal/profile | Completion phase | Reactor-local plain `u64`, cold flushed. | Denominator for completion batch average. |
| `reactor_completion_batch_max` | Local flush batch | release:minimal; profiling:minimal/profile | Completion phase | Reactor-local max, cold flushed. | Detects CQE bursts. |
| `reactor_completion_batch_avg` | Derived local flush | release:minimal; profiling:minimal/profile | No | Derived in snapshot/reporting. | Human-readable batch width. |
| `reactor_command_batches` | Local flush batch | release:minimal; profiling:minimal/profile | Command phase | Reactor-local plain `u64`, cold flushed. | Explains command batch width. |
| `reactor_command_batch_total` | Local flush batch | release:minimal; profiling:minimal/profile | Command phase | Reactor-local plain `u64`, cold flushed. | Denominator for command batch average. |
| `reactor_command_batch_max` | Local flush batch | release:minimal; profiling:minimal/profile | Command phase | Reactor-local max, cold flushed. | Detects deep-pipeline bursts. |
| `reactor_command_batch_avg` | Derived local flush | release:minimal; profiling:minimal/profile | No | Derived in snapshot/reporting. | Human-readable command batch width. |
| `reactor_writev_chunks` | Local flush batch | release:minimal; profiling:minimal/profile | Write path | Reactor-local plain `u64`, cold flushed. | Explains writev segmentation. |
| `reactor_writev_iovecs_total` | Local flush batch | release:minimal; profiling:minimal/profile | Write path | Reactor-local plain `u64`, cold flushed. | Tracks writev vector pressure. |
| `reactor_writev_iovecs_max` | Local flush batch | release:minimal; profiling:minimal/profile | Write path | Reactor-local max, cold flushed. | Detects writev chunk spikes. |
| `reactor_queued_response_bytes_total` | Local flush batch | release:minimal; profiling:minimal/profile | Response queueing | Reactor-local plain `u64`, cold flushed. | Quantifies response bytes queued by workload. |
| `reactor_queued_response_bytes_max` | Local flush batch | release:minimal; profiling:minimal/profile | Response queueing | Reactor-local max, cold flushed. | Detects slow-reader response spikes. |
| `reactor_client_retained_bytes` | Per-connection memory | release:minimal; profiling:minimal/profile | No | Cold metrics-flush scan of reactor-local connection state. | Proves client memory is bounded separately from dataset memory. |
| `reactor_client_retained_bytes_max` | Per-connection memory | release:minimal; profiling:minimal/profile | No | Cold metrics-flush scan. | Identifies the worst retained connection. |
| `reactor_client_retained_bytes_peak` | Per-connection memory | release:minimal; profiling:minimal/profile | No | Cold metrics-flush max publication. | Detects historical client-memory spikes. |
| `reactor_request_cap_exceeded` | Per-connection cap | release:minimal; profiling:minimal/profile | Cap error path | Sharded counter only when request cap rejects/closes. | Required to audit malicious or oversized requests. |
| `reactor_response_cap_exceeded` | Per-connection cap | release:minimal; profiling:minimal/profile | Cap error path | Sharded counter only when pending response cap trips. | Required for slow-reader safety. |
| `reactor_multi_queue_command_cap_exceeded` | Transaction cap | release:minimal; profiling:minimal/profile | Cap error path | Sharded counter only on MULTI command-count cap. | Proves queued transactions are bounded. |
| `reactor_multi_queue_bytes_cap_exceeded` | Transaction cap | release:minimal; profiling:minimal/profile | Cap error path | Sharded counter only on MULTI byte cap. | Proves transaction memory is bounded. |
| `reactor_watch_cap_exceeded` | WATCH cap | release:minimal; profiling:minimal/profile | Cap error path | Sharded counter only on WATCH cap. | Prevents unbounded WATCH registrations. |
| `reactor_writev_chunk_cap_exceeded` | Writev cap | release:minimal; profiling:minimal/profile | Cap error path | Sharded counter only on writev chunk cap. | Proves deferred writev state is bounded. |
| `reactor_overload_accept_throttled` | Overload admission | release:minimal; profiling:minimal/profile | Admission transition path | Sharded counter only when accept throttles. | Explains deliberate connection throttling. |
| `reactor_overload_accept_resumed` | Overload admission | release:minimal; profiling:minimal/profile | Resume transition path | Sharded counter only when accept resumes. | Proves throttling clears. |
| `reactor_overload_read_disabled` | Overload admission | release:minimal; profiling:minimal/profile | State transition path | Sharded counter only when reads are disabled. | Shows Vortex applied backpressure before queues grew. |
| `reactor_overload_read_resumed` | Overload admission | release:minimal; profiling:minimal/profile | Resume transition path | Sharded counter only when reads resume. | Proves read disablement is not permanent. |
| `reactor_overload_command_deferred` | Overload admission | release:minimal; profiling:minimal/profile | Deferral path | Sharded counter only when commands defer. | Explains AOF/backlog-driven latency. |
| `reactor_overload_command_resumed` | Overload admission | release:minimal; profiling:minimal/profile | Resume path | Sharded counter only when deferred commands resume. | Proves backlog recovery. |
| `reactor_overload_connections_dropped` | Overload admission | release:minimal; profiling:minimal/profile | Drop path | Sharded counter only on deterministic overload drop. | Required for overload correctness reports. |
| `reactor_overload_pending_response_bytes` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush gauge. | Shows response pressure used for read disablement. |
| `reactor_overload_pending_response_bytes_peak` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush peak. | Captures transient response pressure. |
| `reactor_overload_parser_accumulator_bytes` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush gauge. | Shows request accumulation pressure. |
| `reactor_overload_parser_accumulator_bytes_peak` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush peak. | Captures large-bulk/deep-pipeline spikes. |
| `reactor_overload_writev_backlog_bytes` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush gauge. | Shows write backlog pressure. |
| `reactor_overload_writev_backlog_bytes_peak` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush peak. | Captures slow-writer spikes. |
| `reactor_overload_aof_pending_bytes` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush gauge. | Shows durable-work backlog used for write deferral. |
| `reactor_overload_aof_pending_bytes_peak` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush peak. | Captures AOF-stall pressure. |
| `reactor_overload_maintenance_debt` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush gauge. | Shows close/expiry/eviction debt before p99.9 moves. |
| `reactor_overload_maintenance_debt_peak` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush peak. | Captures storm debt. |
| `reactor_overload_read_disabled_connections` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush gauge. | Shows how many connections are under read backpressure. |
| `reactor_overload_read_disabled_connections_peak` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush peak. | Detects overload fanout. |
| `reactor_overload_deferred_commands` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush gauge. | Shows commands waiting for local overload relief. |
| `reactor_overload_deferred_commands_peak` | Overload gauge | release:minimal; profiling:minimal/profile | No | Cold metrics-flush peak. | Captures worst deferral backlog. |

## Hot-Path Audit

| Area | Release overhead decision |
| --- | --- |
| Backend capabilities | Startup/cold-publish only. |
| Submit counters | Count only backend submit/flush events; keep for alpha because backend fairness claims require syscall pressure. |
| Queue status | Sampled at startup and at reactor/backend boundaries; no timestamp reads, per-command atomics, or per-CQE counters are added. |
| Budget counters | Increment only on exhausted budget/yield, not on successful command completion. |
| Local-flush counters | Reactor-local plain `u64` fields; shared publication happens in the metrics-flush maintenance class. |
| Per-connection caps | Cap checks are enforcement logic; shared counters update only on violations. |
| Overload gauges | Published on cold metrics flush; overload decisions are reactor-local and happen before shard guards. |
| Profile timers | Not compiled into normal release; see `profiling.md`. |

## Release Review Candidates

| Metric | Current decision | Possible removal or demotion rule |
| --- | --- | --- |
| `backend_sq_pressure_events` | Compatibility alias. | Remove once reports use `reactor_submit_sq_full_retries` everywhere. |
| `backend_cq_pressure_events` | Compatibility alias. | Remove once reports use `reactor_completion_budget_exhaustions` everywhere. |
| `reactor_loop_iterations` | Keep for alpha scheduler interpretation. | Demote if perf/profiler evidence and batch counters are enough for release diagnosis. |
| `reactor_completion_batch_avg` | Derived convenience. | Remove from INFO and compute in reports from count/total. |
| `reactor_command_batch_avg` | Derived convenience. | Remove from INFO and compute in reports from count/total. |
| `reactor_writev_iovecs_total` | Keep while writev hardening is active. | Demote if writev chunk cap and max iovecs cover operator needs. |
| `reactor_queued_response_bytes_total` | Keep for alpha slow-reader evidence. | Demote if retained bytes, pending-response gauges, and cap counters are sufficient. |
