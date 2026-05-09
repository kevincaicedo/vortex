# Vortex IO Profiling Metrics

Profile metrics are for evidence capture and root-cause work. They are not part
of the normal release binary.

```bash
cargo build --profile profiling --features profile-telemetry --bin vortex-server
target/profiling/vortex-server --telemetry-mode profile
```

Normal `target/release/vortex-server` supports `minimal` only and rejects
`profile`. The release binary keeps zero/unavailable report fields for schema
stability, but the timestamp reads and timer storage below are not compiled.

## Metric Catalog

| Metric | Group | Supported modes | Hot path if enabled? | Cost | Use case |
| --- | --- | --- | --- | --- | --- |
| `reactor_completion_nanos_total` | Completion phase timing | profiling:profile only; release:not compiled | Event loop completion phase | `Timestamp::now()` at phase boundaries plus sharded counter. | Attribute CQE drain time when p99.9 moves. |
| `reactor_completion_nanos_max` | Completion phase timing | profiling:profile only; release:not compiled | Event loop completion phase | Timestamp delta plus max update. | Detect long completion slices. |
| `reactor_close_drain_nanos_total` | Close-drain timing | profiling:profile only; release:not compiled | Shutdown/close drain | Timestamp delta plus sharded counter. | Prove drain waits for terminal CQEs without large stalls. |
| `reactor_close_drain_nanos_max` | Close-drain timing | profiling:profile only; release:not compiled | Shutdown/close drain | Timestamp delta plus max update. | Detect close-storm tail risk. |
| `reactor_aof_append_nanos_total` | AOF handoff timing | profiling:profile only; release:not compiled | Mutation path when AOF is enabled | Timestamp delta around append handoff plus sharded counter. | Separate network/engine latency from AOF append cost. |
| `reactor_aof_append_nanos_max` | AOF handoff timing | profiling:profile only; release:not compiled | Mutation path when AOF is enabled | Timestamp delta plus max update. | Detect AOF append outliers. |
| `reactor_aof_fsync_nanos_total` | AOF maintenance timing | profiling:profile only; release:not compiled | AOF maintenance path | Timestamp delta plus sharded counter. | Attribute event-loop time spent polling/scheduling fsync. |
| `reactor_aof_fsync_nanos_max` | AOF maintenance timing | profiling:profile only; release:not compiled | AOF maintenance path | Timestamp delta plus max update. | Detect fsync maintenance stalls. |
| `reactor_maintenance_nanos_total` | Maintenance scheduler timing | profiling:profile only; release:not compiled | Maintenance phase | Timestamp delta plus sharded counter. | Prove maintenance scheduling is bounded. |
| `reactor_maintenance_nanos_max` | Maintenance scheduler timing | profiling:profile only; release:not compiled | Maintenance phase | Timestamp delta plus max update. | Detect maintenance debt tail risk. |
| `reactor_metrics_flush_nanos_total` | Metrics flush timing | profiling:profile only; release:not compiled | Cold metrics-flush phase | Timestamp delta plus sharded counter. | Prove local-to-shared metrics flushing is cold. |
| `reactor_metrics_flush_nanos_max` | Metrics flush timing | profiling:profile only; release:not compiled | Cold metrics-flush phase | Timestamp delta plus max update. | Detect telemetry flush spikes. |

## Removal Rules

| Metric family | Removal or demotion rule |
| --- | --- |
| Completion timers | Keep profile-only while backend fairness and stale-CQE work is active. Remove if perf/flamegraphs provide enough phase attribution. |
| AOF append/fsync timers | Keep profile-only for AOF p99.9 and everysec tuning. Never promote to release without no-cost A/B proof. |
| Maintenance and metrics-flush timers | Keep profile-only until Wave 3 fairness gates close. Remove if scheduler counters explain p99.9 cliffs without clock reads. |
