# Vortex Persist Profiling Metrics

Persistence profiling metrics explain fsync and backpressure cost after a
same-workload Minimal benchmark row shows a latency or throughput change. They
are not part of the normal release binary.

```bash
cargo build --profile profiling --features profile-telemetry --bin vortex-server
target/profiling/vortex-server --telemetry-mode profile
```

Normal `target/release/vortex-server` supports `minimal` only and rejects
`profile`. Release reports may include zero/unavailable schema fields for these
metrics, but their timing storage and clock reads are not compiled.

## Metric Catalog

| Metric | Group | Supported modes | Hot path if enabled? | Cost | Use case |
| --- | --- | --- | --- | --- | --- |
| `reactor_aof_backpressure_nanos_total` | AOF backpressure timing | profiling:profile only; release:not compiled | Backpressure path | `Instant::now()` around wait plus writer counter. | Quantify foreground time waiting for durable backlog relief. |
| `reactor_aof_backpressure_nanos_max` | AOF backpressure timing | profiling:profile only; release:not compiled | Backpressure path | Wait delta plus max update. | Detect worst backpressure wait. |
| `reactor_aof_fsync_latency_nanos_total` | Fsync latency timing | profiling:profile only; release:not compiled | Fsync completion path | `Instant` delta plus writer counter. | Separate disk/fsync stalls from engine or network work. |
| `reactor_aof_fsync_latency_nanos_max` | Fsync latency timing | profiling:profile only; release:not compiled | Fsync completion path | Latency delta plus max update. | Detect worst fsync completion latency. |
| `reactor_aof_fsync_latency_le_100us` | Fsync latency bucket | profiling:profile only; release:not compiled | Fsync completion path | One bucket increment. | Histogram bucket for very fast fsyncs. |
| `reactor_aof_fsync_latency_le_500us` | Fsync latency bucket | profiling:profile only; release:not compiled | Fsync completion path | One bucket increment. | Histogram bucket for sub-ms fsyncs. |
| `reactor_aof_fsync_latency_le_1ms` | Fsync latency bucket | profiling:profile only; release:not compiled | Fsync completion path | One bucket increment. | Histogram bucket for the sub-ms durability target. |
| `reactor_aof_fsync_latency_le_5ms` | Fsync latency bucket | profiling:profile only; release:not compiled | Fsync completion path | One bucket increment. | Histogram bucket for visible fsync stalls. |
| `reactor_aof_fsync_latency_le_10ms` | Fsync latency bucket | profiling:profile only; release:not compiled | Fsync completion path | One bucket increment. | Histogram bucket for slow disk events. |
| `reactor_aof_fsync_latency_le_50ms` | Fsync latency bucket | profiling:profile only; release:not compiled | Fsync completion path | One bucket increment. | Histogram bucket for severe disk pressure. |
| `reactor_aof_fsync_latency_le_100ms` | Fsync latency bucket | profiling:profile only; release:not compiled | Fsync completion path | One bucket increment. | Histogram bucket for release-gate failure analysis. |
| `reactor_aof_fsync_latency_gt_100ms` | Fsync latency bucket | profiling:profile only; release:not compiled | Fsync completion path | One bucket increment. | Captures pathological fsync stalls. |
| `reactor_aof_append_nanos_total` | IO AOF phase timing | profiling:profile only; release:not compiled | AOF-enabled mutation path | IO phase timer around append handoff. | Attribute command latency to append handoff. |
| `reactor_aof_append_nanos_max` | IO AOF phase timing | profiling:profile only; release:not compiled | AOF-enabled mutation path | IO phase timer plus max update. | Detect append outliers. |
| `reactor_aof_fsync_nanos_total` | IO AOF phase timing | profiling:profile only; release:not compiled | AOF maintenance path | IO phase timer around fsync maintenance. | Attribute reactor time spent scheduling/polling fsync. |
| `reactor_aof_fsync_nanos_max` | IO AOF phase timing | profiling:profile only; release:not compiled | AOF maintenance path | IO phase timer plus max update. | Detect fsync maintenance outliers. |

## Removal Rules

| Metric family | Removal or demotion rule |
| --- | --- |
| Fsync latency histograms | Keep profile-only while AOF everysec/always tuning is active. Never promote to release without no-cost A/B proof. |
| Backpressure wait timers | Keep profile-only until slow-disk gates are closed. Release only needs event counts and backlog/LSN gauges. |
| IO AOF phase timers | Keep in IO/persist profiling docs for correlation, but remove duplicate report fields if profiler artifacts provide better attribution. |
