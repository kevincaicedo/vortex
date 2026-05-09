# Vortex Persist Release Metrics

Persistence release metrics prove durability state, fsync progress, bounded AOF
backlog, and write-stop/backpressure behavior. They are not latency profilers.

`target/release/vortex-server` supports `minimal` only. AOF latency histograms
and wait-duration timers require `--features profile-telemetry` and are
documented in `profiling.md`.

Supported-mode notation:

- `release:minimal` means available in the normal release binary.
- `profiling:minimal/profile` means available in the profiling binary, with
  Profile allowed only when compiled with `profile-telemetry`.

## Metric Catalog

| Metric | Group | Supported modes | Hot path? | Update path and overhead | Why it stays in release |
| --- | --- | --- | --- | --- | --- |
| `reactor_aof_pending_bytes` | AOF backlog | release:minimal; profiling:minimal/profile | AOF append/maintenance | Writer state updated with append and fsync progress; published on cold IO flush. | Operators must see durable-work accumulation before latency cliffs. |
| `reactor_aof_pending_bytes_max` | AOF backlog | release:minimal; profiling:minimal/profile | AOF append/maintenance | Max publication from writer state. | Captures worst unsynced byte backlog. |
| `reactor_aof_pending_writes` | AOF backlog | release:minimal; profiling:minimal/profile | AOF append/maintenance | Writer state updated with append and fsync progress; published on cold IO flush. | Shows pending write count independent of bytes. |
| `reactor_aof_pending_writes_max` | AOF backlog | release:minimal; profiling:minimal/profile | AOF append/maintenance | Max publication from writer state. | Captures write-count backlog spikes. |
| `reactor_aof_fsync_requested` | Fsync progress | release:minimal; profiling:minimal/profile | Fsync scheduling path | Writer counter increments when fsync is scheduled/requested. | Required to prove fsync policy is active. |
| `reactor_aof_fsync_completed` | Fsync progress | release:minimal; profiling:minimal/profile | Fsync completion path | Writer counter increments on successful fsync completion. | Required to prove durability progress. |
| `reactor_aof_fsync_failed` | Fsync progress | release:minimal; profiling:minimal/profile | Error path | Writer counter increments only on fsync failure. | Durability failure must be visible in production. |
| `reactor_aof_fsync_worker_saturation` | Fsync progress | release:minimal; profiling:minimal/profile | Everysec saturation path | Writer counter increments only when async fsync worker/channel is saturated. | Explains everysec backpressure and write-stop risk. |
| `reactor_aof_backpressure_events` | Backpressure | release:minimal; profiling:minimal/profile | Backpressure path | Writer counter increments only when backlog forces waiting/throttling. | Explains intentional write throttling without needing latency timers. |
| `reactor_aof_last_appended_lsn` | Durability LSN | release:minimal; profiling:minimal/profile | AOF append path | Writer state stores latest appended LSN. | Defines latest mutation handed to persistence. |
| `reactor_aof_last_durable_lsn` | Durability LSN | release:minimal; profiling:minimal/profile | Fsync completion path | Writer state stores latest durable LSN. | Defines external commit point after crash. |
| `reactor_aof_durable_lsn_lag` | Durability LSN | release:minimal; profiling:minimal/profile | No | Derived from appended minus durable LSN in snapshot. | Quantifies crash exposure under everysec/no fsync. |

## Hot-Path Audit

| Area | Release overhead decision |
| --- | --- |
| Append path | Release keeps counters/state that are part of the durability contract: pending bytes/writes and last appended LSN. No profile `Instant`/`Timestamp` calls are compiled in. |
| Fsync path | Release counts requested/completed/failed/saturation events. Latency measurement is profile-only. |
| Backpressure | Release counts backpressure events. Wait-duration measurement is profile-only. |
| Publication | IO publishes writer telemetry to engine runtime metrics on the cold metrics-flush cadence. |

## Release Review Candidates

| Metric | Current decision | Possible removal or demotion rule |
| --- | --- | --- |
| `reactor_aof_pending_writes_max` | Keep for alpha everysec tuning. | Remove if pending bytes and durable LSN lag explain backlog sufficiently. |
| `reactor_aof_fsync_worker_saturation` | Keep for everysec correctness. | Remove only if everysec async fsync is replaced by a different scheduler with stronger diagnostics. |
| `reactor_aof_durable_lsn_lag` | Keep as durability KPI. | Do not remove while Vortex documents external commit/crash exposure. |
