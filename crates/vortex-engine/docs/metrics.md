# Vortex Engine Release Metrics

This catalog lists engine-owned metrics that may remain in normal release
builds. The release contract is intentionally strict:

- `target/release/vortex-server` is built without `profile-telemetry` and
  supports `TelemetryModeKind::Minimal` only.
- `--telemetry-mode profile` is rejected unless the binary was compiled with
  `--features profile-telemetry`.
- Cargo does not provide a reliable built-in `cfg(target = release)` switch for
  this policy. Vortex uses explicit Cargo feature gating instead.
- Profile timers keep their report fields as zero/unavailable schema stubs in
  Minimal mode, but their timestamp reads and storage are not compiled into the
  normal release binary.

Supported-mode notation:

- `release:minimal` means available in the normal release binary.
- `profiling:minimal/profile` means available in the profiling binary, with
  Profile allowed only when compiled with `profile-telemetry`.

## Metric Catalog

| Metric | Group | Supported modes | Hot path? | Update path and overhead | Why it stays in release |
| --- | --- | --- | --- | --- | --- |
| `runtime_telemetry_mode` | Telemetry contract | release:minimal; profiling:minimal/profile | No | Cold `INFO runtime` read of an atomic mode byte. | Prevents benchmark and support reports from mixing Minimal and Profile rows. |
| `runtime_profile_timers_available` | Telemetry contract | release:minimal; profiling:minimal/profile | No | Derived from compile feature and runtime mode. | Makes disabled profile timers explicit instead of hiding zeros. |
| `runtime_local_flush_metrics_available` | Telemetry contract | release:minimal; profiling:minimal/profile | No | Constant/derived field in snapshot. | Confirms high-frequency counters are published through cold flush, not per-event shared writes. |
| `runtime_reactor_slots` | Runtime topology | release:minimal; profiling:minimal/profile | No | Cold snapshot of configured reactor metric slots. | Required to interpret per-reactor aggregation and benchmark service-thread rows. |
| `memory_attribution_engine_scope` | Memory attribution | release:minimal; profiling:minimal/profile | No | Static `INFO memory` label. | States that engine numbers exclude full-server RSS and IO buffers. |
| `memory_attribution_full_server_scope` | Memory attribution | release:minimal; profiling:minimal/profile | No | Static `INFO memory` label. | Prevents unfair Redis comparisons by naming full-server accounting. |
| `engine_live_keys` | Memory attribution | release:minimal; profiling:minimal/profile | No | `INFO memory` aggregation over engine attribution state. | Needed for bytes/key and dataset-size claims. |
| `engine_logical_dataset_bytes` | Memory attribution | release:minimal; profiling:minimal/profile | Mutation accounting already exists | Reads engine memory accounting; no extra per-command probe is added for INFO. | Separates logical dataset from table capacity and allocator/RSS. |
| `engine_table_allocated_bytes` | Memory attribution | release:minimal; profiling:minimal/profile | No | `INFO memory` aggregation of table allocation. | Required to diagnose table slack and memory-efficiency regressions. |
| `engine_table_total_slots` | Memory attribution | release:minimal; profiling:minimal/profile | No | `INFO memory` aggregation. | Explains load factor, slack, and resize behavior. |
| `engine_capacity_slack_slots` | Memory attribution | release:minimal; profiling:minimal/profile | No | `INFO memory` aggregation. | Shows retained capacity that can dominate bytes/key. |
| `engine_tombstone_slots` | Memory attribution | release:minimal; profiling:minimal/profile | No | `INFO memory` aggregation. | Required for delete-heavy workload diagnosis. |
| `engine_load_factor` | Memory attribution | release:minimal; profiling:minimal/profile | No | Derived in INFO/reporting. | Helps catch resize-policy and tombstone regressions. |
| `engine_bytes_per_live_key` | Memory attribution | release:minimal; profiling:minimal/profile | No | Derived in INFO/reporting. | Primary release memory-efficiency metric. |
| `engine_shard_count` | Runtime topology | release:minimal; profiling:minimal/profile | No | Static/cold keyspace field. | Required to compare lock, memory, and throughput rows fairly. |
| `reactor_active_expiry_runs` | TTL health | release:minimal; profiling:minimal/profile | Maintenance path | Sharded counter increments only when the active-expiry maintenance class runs. | Operators need to know whether TTL cleanup is active. |
| `reactor_active_expiry_sampled` | TTL health | release:minimal; profiling:minimal/profile | Maintenance path | Sharded counter incremented during bounded expiry slices. | Explains TTL debt without enabling timers. |
| `reactor_active_expiry_expired` | TTL health | release:minimal; profiling:minimal/profile | Maintenance path | Sharded counter incremented only for expired keys removed by active expiry. | Confirms TTL progress under expiry-heavy workloads. |
| `eviction_admissions` | Eviction health | release:minimal; profiling:minimal/profile | Eviction path only | Relaxed counter increments when maxmemory admission runs eviction. | Proves Vortex is enforcing maxmemory instead of silently accepting growth. |
| `eviction_shards_scanned` | Eviction health | release:minimal; profiling:minimal/profile | Eviction path only | Relaxed counter adds bounded scan work. | Explains how much eviction work was needed before admitting writes. |
| `eviction_slots_sampled` | Eviction health | release:minimal; profiling:minimal/profile | Eviction path only | Relaxed counter adds sampled slots. | Needed to validate bounded eviction-pressure behavior. |
| `eviction_bytes_freed` | Eviction health | release:minimal; profiling:minimal/profile | Eviction path only | Relaxed counter adds freed bytes. | Required for maxmemory and Redis eviction comparison rows. |
| `eviction_oom_after_scan` | Eviction health | release:minimal; profiling:minimal/profile | Error path | Relaxed counter increments only when eviction cannot free enough memory. | Exposes correctness/capacity failures. |

## Hot-Path Audit

| Area | Release overhead decision |
| --- | --- |
| Command execution | No profile timestamp reads are compiled into normal release for OBS timers. Engine memory accounting that existed for correctness remains part of mutation accounting. |
| Expiry | Release counters update only when the maintenance expiry class runs, not on every command. Timing stays profile-only. |
| Eviction | Release counters update only on maxmemory admission/eviction work. Timing stays profile-only. |
| Memory attribution | INFO/reporting reads aggregate state. It is not command-path work. |
| Telemetry contract fields | Cold snapshot fields only. They exist to prevent invalid benchmark interpretation. |

## Release Review Candidates

These metrics are currently kept for alpha evidence, but must be re-reviewed
before the public alpha release metric set is frozen.

| Metric | Current decision | Possible removal or demotion rule |
| --- | --- | --- |
| `engine_load_factor` | Keep for alpha memory diagnosis. | Remove from INFO if reports can derive it from slots and live keys without losing operator clarity. |
| `engine_bytes_per_live_key` | Keep as release-facing memory KPI. | Keep unless release docs stop making bytes/key claims. |
| `eviction_shards_scanned` | Keep for eviction proof. | Demote to profiling if `eviction_admissions`, `eviction_bytes_freed`, and `eviction_oom_after_scan` are enough for operators. |
| `eviction_slots_sampled` | Keep for bounded-scan proof. | Demote to profiling after maxmemory policy is stable and public docs do not expose scan-budget tuning. |
| `reactor_active_expiry_sampled` | Keep while TTL behavior is alpha-gated. | Demote if `reactor_active_expiry_runs` and `reactor_active_expiry_expired` prove sufficient. |
| `runtime_local_flush_metrics_available` | Keep as interpretation guard. | Remove only if benchmark/report schema stops publishing local-flush fields. |
