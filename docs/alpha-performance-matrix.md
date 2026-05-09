# Alpha Performance Matrix

`VAL-ALPHA-002` defines the repeatable benchmark contract for alpha performance claims. The matrix is not a single benchmark command; it is a set of workload families with explicit evidence tiers.

## Evidence Rules

- Single-replicate runs are exploratory only.
- Release-candidate rows require 3-5 clean Linux repeats.
- Every claim must name backend, effective backend capabilities, service threads, load threads, shard count, AOF mode, eviction mode, dataset size, value size, pipeline depth, fixed-buffer count, buffer size, CPU governor, affinity, kernel, repeat count, RSS, CPU, and client-saturation verdict.
- Public alpha rows must use `telemetry_mode=minimal`. `profile` rows are diagnostic and cannot be used for release throughput claims.
- `redis-benchmark` does not expose p99.9. Use `memtier_benchmark` or `custom-rust` rows for p99.9 and p99.99 claims.

## Runner

Generate the smoke matrix without executing it:

```bash
just alpha-performance-matrix --profile smoke
```

Run the exploratory service-thread scaling sweep:

```bash
just alpha-performance-matrix --profile scaling --run --repeat 1 --duration 5s
```

Run the exploratory Docker all-database scaling sweep:

```bash
just alpha-performance-matrix --profile docker-scaling --run --repeat 1 --duration 5s
```

Generate the release Tier 1 manifest set:

```bash
just alpha-performance-matrix --profile tier1 --repeat 3 --duration 20s
```

Run a large profile only with an explicit safety override:

```bash
just alpha-performance-matrix --profile tier1 --run --repeat 3 --duration 20s --allow-large-run
```

The runner writes generated manifests, `matrix-plan.json`, row logs, and `report.md` under `.artifacts/validation/alpha-performance/<timestamp>-<profile>/`.

## Matrix Summary

| Tier | Purpose | Required Surfaces | Claim Policy |
|------|---------|-------------------|--------------|
| Tier 1 | Minimum alpha gate | Core commands, mixed read/write, Zipfian, multi-key, WATCH/MULTI/EXEC, TTL, eviction pressure, deep pipeline plus latency clients, slow-reader backpressure | Must pass or be explicitly narrowed before alpha publication |
| Tier 2 | Pressure wording | AOF `always`, delete/tombstone-heavy, connection and close storms, large bulk values, noeviction admission, LFU, LSN-heavy writes, full-server memory | Decides durability, pressure, and memory wording |
| Tier 3 | Research | 1/2/4/8/16 thread sweep, 1K/4K/16K/64K shard sweep, SQPOLL, registered buffers, DragonflyDB, Valkey, owner-shard research | Does not block alpha unless the claim mentions that surface |

## Scaling Rows

| Dimension | Values | Notes |
|-----------|--------|-------|
| Database-side threads | `1,2,4,8,16` | Vortex receives `--threads=N`, which creates reactor/service threads. Redis receives `--io-threads N --io-threads-do-reads yes`, which is Redis networking IO threading, not an equivalent multi-reactor command-execution model. |
| Load-generator threads | fixed at `4` for the focused scaling profile | This keeps the client contract stable while the database-side thread knob changes. A saturated-client sweep is a separate gate. |
| Docker memory limit | `2g` for <= 4 database threads, `4g` for 8, `6g` for 16 | Dragonfly reserves memory per proactor and refuses to start when the cgroup limit is below its thread-derived floor. |
| Docker high-thread `maxmemory` | `1800mb` for <= 4 database threads, `3000mb` for 8, `6000mb` for 16 | Dragonfly also enforces a thread-derived `maxmemory` floor. These rows are scaling rows, not maxmemory pressure rows; Tier 2 owns memory-pressure wording. |
| Shards | `4096` for focused scaling; `1024,4096,16384,65536` for Tier 3 | Current server default is `4096`. Tier 1 still includes `4096,16384,65536` to test memory/lock tradeoffs. |
| Pipeline | `1` for common-case round-trip scaling | Pipeline `16,256,4096` belongs to fairness rows. |
| Workloads | `redis-benchmark` PING/GET/SET/INCR and memtier uniform read-heavy | memtier gives p99.9 and p99.99. |
| Fixed buffers | `max(1024, service_threads * 256)` in generated scaling manifests | Keeps per-reactor fixed-buffer capacity constant enough for 100-client rows so the 16-thread row does not measure an artificial buffer-cap cliff. |

## Tier 1 Contract

| Family | Backend | Values |
|--------|---------|--------|
| Core point commands | `redis-benchmark` | PING, GET hit, GET miss where available, SET, INCR, DEL, TTL/EXPIRE, MGET 10, MSET 10; pipelines 1 and 16 |
| Mixed workload | `memtier_benchmark` | read-heavy, write-heavy, Zipfian hot keys; 100k and 1M keys; 8, 16, and 64 byte values |
| Multi-key and transaction | `custom-rust` | MGET/MSET/MSETNX/DEL and WATCH/MULTI/EXEC with deterministic duplicate and conflict semantics |
| Fairness and overload | `custom-rust` | one deep-pipeline or slow-reader client plus 100 latency-sensitive pipeline-1 clients |
| Runtime modes | setup manifest | Linux `uring` and polling, AOF off and everysec, noeviction and one alpha eviction policy |

## Tier 2 Contract

| Family | Backend | Values |
|--------|---------|--------|
| AOF always | `custom-rust`, `memtier_benchmark` | Storage-bound durability row labeled outside the sub-ms pressure gate |
| Large bulk | `custom-rust` | 16 KiB, 64 KiB, and 1 MiB SET/GET pressure |
| Mutation path | `redis-benchmark` plus profiler when needed | APPEND, SETRANGE, INCRBYFLOAT |
| Memory | memory attribution tooling | Full-server RSS and engine attribution at 50k, 100k, and 1M keys |
| Storms | `custom-rust` | connection storm and close storm |

## Acceptance Targets

| Gate | Target |
|------|--------|
| Core no-pressure latency | p99 < 1 ms and p99.9 < 1 ms for p99.9-capable rows |
| Pressure latency | p99 < 1 ms and p99.9 < 5 ms, or the claim must be narrowed |
| Throughput | Vortex should target 1.5x Redis for common pipeline-1 core rows and up to 2x for deeper pipeline rows, but only citation-grade evidence may claim this |
| Memory | Move scoped tiny/mixed KV full-server RSS toward <= 1.5x Redis |
| Scaling | >= 70 percent efficiency at 8 service threads; 16-thread rows must identify the limiting resource |

## Reporting Checklist

Each summarized row must include:

- median, spread, outlier count, p50, p95, p99, p99.9 and p99.99 when available
- throughput, CPU, RSS, allocator, backend capability, and phase telemetry
- load-generator CPU, affinity, socket queue/retransmit notes, and client-saturation verdict
- artifact paths for raw backend output, report JSON/CSV/Markdown, host telemetry, and profiler artifacts when used

Rows that miss these fields remain exploratory.

Large-bulk generated manifests intentionally reduce key count as value size grows so the row measures parser, writev, and response backpressure behavior before eviction/noeviction pressure. Eviction pressure is a separate row with an explicit `maxmemory` and eviction policy.
