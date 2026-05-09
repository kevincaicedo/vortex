# Vortex Engine Profiling Metrics

Profile telemetry is not part of the normal release binary. Build profiling
binaries explicitly:

```bash
cargo build --profile profiling --features profile-telemetry --bin vortex-server
```

Shard-lock profiling is separate and also excluded from normal release builds:

```bash
cargo build --profile profiling --features profile-telemetry,lock-profile --bin vortex-server
```

In `target/release`, `--telemetry-mode profile` is rejected. The INFO/report
schema may still contain zero/unavailable profile fields so tooling can compare
rows, but release does not compile the clock reads or storage for these fields.

## Metric Catalog

| Metric | Group | Supported modes | Hot path if enabled? | Cost | Use case |
| --- | --- | --- | --- | --- | --- |
| `reactor_active_expiry_nanos_total` | TTL phase timing | profiling:profile only; release:not compiled | Maintenance path | Timestamp delta plus sharded counter. | Explain expiry-driven p99.9 cliffs. |
| `reactor_active_expiry_nanos_max` | TTL phase timing | profiling:profile only; release:not compiled | Maintenance path | Timestamp delta plus max update. | Bound worst active-expiry slice time. |
| `eviction_nanos_total` | Eviction phase timing | profiling:profile only; release:not compiled | Eviction path | Timestamp delta plus relaxed counter. | Correlate eviction-pressure latency with scan work. |
| `eviction_nanos_max` | Eviction phase timing | profiling:profile only; release:not compiled | Eviction path | Timestamp delta plus max update. | Prove eviction slices stay bounded. |
| `lock_profile.<class>.<read/write>.wait_nanos_total` | Lock profiling | profiling:lock-profile only; release:not compiled by default | Shard lock acquisition | `Instant::now()` around lock acquisition plus atomics. | Measure lock wait by command class. |
| `lock_profile.<class>.<read/write>.wait_nanos_max` | Lock profiling | profiling:lock-profile only; release:not compiled by default | Shard lock acquisition | Max atomic update. | Identify tail lock waits. |
| `lock_profile.<class>.<read/write>.hold_nanos_total` | Lock profiling | profiling:lock-profile only; release:not compiled by default | Shard lock guard lifetime | Drop-time `Instant` delta plus atomics. | Measure critical-section time for optimization experiments. |
| `lock_profile.<class>.<read/write>.hold_nanos_max` | Lock profiling | profiling:lock-profile only; release:not compiled by default | Shard lock guard lifetime | Max atomic update. | Detect outlier shard-lock holds. |
| `lock_profile.<class>.<read/write>.wait_buckets` | Lock profiling | profiling:lock-profile only; release:not compiled by default | Shard lock acquisition | Bucket atomic increment. | Build contention histograms for p99/p99.9 analysis. |
| `lock_profile.<class>.<read/write>.hold_buckets` | Lock profiling | profiling:lock-profile only; release:not compiled by default | Shard lock guard lifetime | Bucket atomic increment. | Build critical-section histograms. |
| `lock_profile.<class>.retries` | Optimistic mutation profiling | profiling:lock-profile only; release:not compiled by default | Retry path | Relaxed counter only on retry. | Decide whether optimistic prepare/revalidate/swap earns its place. |
| `lock_profile.<class>.revalidation_failures` | Optimistic mutation profiling | profiling:lock-profile only; release:not compiled by default | Revalidation failure path | Relaxed counter only on failed validation. | Detect stale optimistic plans and retry pressure. |

## Removal Rules

| Metric family | Removal or demotion rule |
| --- | --- |
| TTL phase timers | Remove or keep profile-only if expiry workloads do not show p99.9 sensitivity. Never promote to release without A/B proof. |
| Eviction phase timers | Keep profile-only while maxmemory tuning is active. Never promote to release without showing no measurable clock/cache cost. |
| Lock profiling | Keep behind `lock-profile`; do not enable in release claim binaries. Remove classes that stop driving accepted optimization decisions. |
