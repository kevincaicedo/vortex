# VortexDB Benchmark Methodology

This document describes how VortexDB benchmarks are conducted, what hardware is used, and how to reproduce results independently. The benchmark program now has two complementary layers: `redis-benchmark` for point-command throughput and `memtier_benchmark` for mixed Gaussian workloads.

---

## Hardware & Environment

### Primary Development Machine (macOS)

| Component | Specification |
|-----------|---------------|
| CPU | Apple M4 Pro (12-core: 4P + 8E) |
| RAM | Unified memory, ~200 GB/s bandwidth |
| OS | macOS 15 (Sequoia) |
| I/O Backend | kqueue via `polling` crate |
| Rust | nightly-2026-03-15 |

### Linux CI / Bare-Metal

| Component | Specification |
|-----------|---------------|
| CPU | Intel i7-13700KF (24 cores: 8P + 16E) |
| RAM | 64 GB DDR5 |
| OS | Ubuntu 24.04 LTS |
| Kernel | 6.17.0 with io_uring support |
| I/O Backend | io_uring via `io-uring` crate |
| Rust | nightly-2026-03-15 |

### Docker Benchmark Environment

| Parameter | Value |
|-----------|-------|
| Docker Desktop | Apple Virtualization Framework (macOS) |
| CPU per container | 4 cores (configurable via `--cpus`) |
| Memory per container | 2 GB (configurable via `--memory`) |
| Network | Docker bridge (default) |

---

## Benchmark Tools

### redis-benchmark

The primary throughput measurement tool. Ships with Redis — measures raw ops/sec by sending pipelined commands over TCP.

| Parameter | Default | Notes |
|-----------|---------|-------|
| Requests (`-n`) | 100,000 | Total requests per test |
| Clients (`-c`) | 50 | Parallel client connections |
| Pipeline (`-P`) | 16 | Requests pipelined per batch |
| Data size (`-d`) | 3 bytes | Default value size for SET |

### memtier_benchmark

The mixed-workload companion harness. It exercises concurrent GET/SET traffic with a Gaussian key distribution, which makes hot-key behavior, hit/miss ratios, and thread scaling visible in a way `redis-benchmark` does not.

| Parameter | Default | Notes |
|-----------|---------|-------|
| Threads (`-t`) | `1,2,4,8` | Thread sweep published in the same report |
| Requests (`--requests`) | 2,000 | Per client |
| Clients (`-c`) | 50 | Per thread |
| Pipeline (`--pipeline`) | 1 | Keeps latency signal visible |
| Data size (`--data-size`) | 384 bytes | Matches the research script baseline |
| Ratio (`--ratio`) | `1:15` | Read-heavy mixed workload |
| Key pattern (`--key-pattern`) | `G:G` | Gaussian access distribution |

### Criterion (Micro-Benchmarks)

The `vortex-bench` crate contains 69 Criterion benchmarks measuring individual operation latency at the Rust function level — no network overhead.

### Custom Benchmark Scripts

`scripts/bench-commands.sh` benchmarks 37+ commands that `redis-benchmark -t` doesn't cover natively, using `redis-benchmark` with raw command syntax. `scripts/compare.sh --memtier` augments the same report with `memtier_benchmark` mixed-workload totals and latency tables.

---

## Competitive Comparison

### Databases Under Test

| Database | Version | Configuration |
|----------|---------|---------------|
| **VortexDB** | 0.1.0-alpha | Default (auto-detect threads, kqueue/io_uring) |
| **Redis** | 8.6.2-alpine | `io-threads 4`, `io-threads-do-reads yes` |
| **Dragonfly** | Latest (v1.27.1) | `--proactor_threads 4`, `--pipeline_squash 10` |
| **Valkey** | 9.1-alpine | `io-threads 4`, `io-threads-do-reads yes` |

All databases are configured with optimal threading for fair comparison. Redis and Valkey use 4 I/O threads for read/write offloading. Dragonfly uses 4 proactor threads.

### Benchmark Modes

#### Native Mode

Both VortexDB and Redis run natively on the host — no Docker overhead. Most accurate comparison for macOS.

```sh
just compare-native
```

#### Docker Mode (Fair Comparison)

All databases run in identical Docker containers with the same resource limits.

```sh
just compare-docker
```

#### Full Statistical Mode

Multiple runs with confidence intervals:

```sh
just compare-full  # 3 runs, JSON + Markdown, latency, custom commands, memtier mixed workloads
```

---

## Results — macOS (Apple M4 Pro)

### Native Mode (VortexDB native + Redis native with io-threads 4)

Both servers natively on macOS, kqueue I/O backend, Redis configured with `io-threads 4`.

These tables are **point-workload** results from `redis-benchmark`. The automated report can also include a `memtier_benchmark` mixed-workload section when run with `--memtier`.

| Command | VortexDB | Redis 8 | vs Redis | Notes |
|---------|----------|---------|----------|-------|
| SET | 1,923,076 | 1,960,784 | **1.0×** | I/O-bound on kqueue |
| GET | 2,272,727 | 1,923,076 | **1.2×** | |
| INCR | 2,325,581 | 1,960,784 | **1.2×** | |
| MSET (10 keys) | 1,470,588 | 598,802 | **2.5×** | Batch-prefetch advantage |
| MSETNX (10) | 2,000,000 | 917,431 | **2.2×** | Batch-prefetch advantage |
| INCRBYFLOAT | 2,325,581 | 1,612,903 | **1.4×** | |
| DECR | 2,564,102 | 2,040,816 | **1.3×** | |
| MGET (10) | 1,694,915 | 1,515,151 | **1.1×** | |
| DEL | 2,439,024 | 1,886,792 | **1.3×** | |
| EXISTS | 2,564,102 | 2,000,000 | **1.3×** | |
| TTL | 2,631,579 | 1,960,784 | **1.3×** | |
| SETEX | 2,173,913 | 1,176,470 | **1.8×** | |
| COPY | 2,564,102 | 1,923,076 | **1.3×** | |
| DBSIZE | 2,631,579 | 1,960,784 | **1.3×** | |
| SCAN (100K keys) | 90,090 | 71,942 | **1.3×** | |
| PING_INLINE | 1,923,076 | 2,000,000 | **1.0×** | I/O-bound, not engine |
| ECHO | 2,439,024 | 1,886,792 | **1.3×** | |

---

## Results — Linux (i7-13700KF, Docker-All, identical containers, 4 CPUs / 2 GB each)

| Command | VortexDB | Redis 8 | Dragonfly | Valkey 9 | vs Redis |
|---------|----------|---------|-----------|----------|----------|
| SET | 3,448,276 | 2,325,581 | 1,612,903 | 2,272,727 | **1.5×** |
| GET | 3,571,429 | 3,225,806 | 2,564,103 | 3,125,000 | **1.1×** |
| INCR | 3,448,276 | 3,030,303 | 2,380,952 | 2,857,143 | **1.1×** |
| MSET (10 keys) | 2,941,176 | 892,857 | 552,486 | 1,041,666 | **3.3×** |
| MSETNX (10) | 3,448,276 | 980,392 | 471,698 | 793,651 | **3.5×** |
| INCRBYFLOAT | 3,333,333 | 1,369,863 | 1,250,000 | 1,333,333 | **2.4×** |
| PING_INLINE | 1,612,903 | 3,225,806 | 3,333,333 | 2,857,143 | **0.5×** |
| PING_MBULK | 3,571,429 | 3,571,429 | 2,702,703 | 3,333,333 | **1.0×** |

### Key Observations

1. **Multi-key batch commands** (MSET, MSETNX) show the largest advantage (3–3.7×) thanks to SwissTable batch-prefetch pipeline and zero-copy RESP serializer.
2. **Single-key read commands** (GET, INCR) show 1.0–1.5× — the advantage is real but modest since Redis with io-threads is already highly optimized.
3. **PING_INLINE** is consistently 0.5× Redis — this is pure I/O round-trip latency with no engine work, indicating VortexDB's event loop has higher per-round-trip overhead than Redis's epoll loop for tiny responses.
4. **macOS kqueue** shows lower advantage than Linux io_uring because kqueue uses synchronous I/O with polling, while io_uring provides true asynchronous completions.

---

## Micro-Benchmarks (Criterion, No Network)

Measured at the Rust function level — pure computation, no TCP overhead.

| Benchmark | Latency | Target | Margin |
|-----------|---------|--------|--------|
| `swiss_table_lookup_hit` | 5.0 ns | ≤50 ns | 10× |
| `swiss_table_lookup_miss` | 3.7 ns | ≤50 ns | 13.5× |
| `swiss_table_insert` | 25.6 ns (10K) | ≤70 ns | 2.7× |
| `cmd_get_inline` | 32 ns | ≤50 ns | 1.6× |
| `cmd_set_inline` | 40 ns | ≤70 ns | 1.8× |
| `cmd_del_inline` | 34 ns | ≤40 ns | 1.2× |
| `cmd_incr` | 15 ns | ≤30 ns | 2× |
| `cmd_mget_100` | 3.47 µs | ≤5 µs | 1.4× |
| `cmd_mset_100` | 3.02 µs | ≤8 µs | 2.6× |
| `cmd_scan_10k` | 281 µs | ≤10 ms | 35× |
| `cmd_ping` | 2.47 ns | — | Zero alloc |
| `cmd_dbsize` | 3.32 ns | — | Direct field read |
| `expiry_register` | 2.93 ns | ≤10 ns | 3.4× |
| `expiry_tick_20` | 40.2 ns | ≤200 ns | 5× |
| `throughput_get_set_mix` | 12.5M ops/sec | ≥5M | 2.5× |

---

## How to Reproduce

### Prerequisites

```sh
# Install Redis CLI tools (for redis-benchmark)
# macOS:
brew install redis
brew install memtier_benchmark

# Ubuntu:
sudo apt-get install redis-tools

# memtier_benchmark on Ubuntu/Debian is available from Redis packages
sudo apt-get install memtier-benchmark

# Install Docker (for competitor databases)
# https://docs.docker.com/get-docker/

# Install Just command runner
cargo install just
```

### Run Competitive Benchmarks

```sh
cd vortex/

# Native VortexDB vs native Redis (most accurate on macOS)
just compare-native

# Quick comparison with throughput only
just compare

# Mixed workload + point-command suite
just compare-memtier

# Full comparison with latency percentiles, custom commands, and reports
just compare --latency --markdown --custom --json --memtier

# Fair Docker-based comparison (all databases containerized)
just compare-docker

# Full statistical run (3 iterations with CI95 + memtier mixed workloads)
just compare-full

# Custom parameters
bash scripts/compare.sh -n 200000 -c 100 -P 32 --native --latency --markdown --memtier
```

### Run Micro-Benchmarks

```sh
# Engine micro-benchmarks
cargo bench -p vortex-engine --bench engine

# Shared parser/reactor/support benchmarks
cargo bench -p vortex-bench

# Specific benchmark group
cargo bench -p vortex-engine --bench engine -- swiss_table
cargo bench -p vortex-engine --bench engine -- cmd_get
cargo bench -p vortex-engine --bench engine -- throughput

# Validate against Phase 3 performance targets
just bench-validate
```

### Output Formats

| Flag | Output | Location |
|------|--------|----------|
| `--json` | Machine-readable JSON | `benchmarks/results-{timestamp}.json` |
| `--markdown` | GitHub-friendly tables | `benchmarks/results-{timestamp}.md` |
| `--latency` | p50/p95/p99 percentiles | Included in JSON/Markdown output |
| `--runs N` | Statistical multi-run | Mean, stddev, CI95 per command |

---

## Known Limitations

1. **Docker Desktop on macOS** — VortexDB measures ~0.7× Redis when all databases are containerized on Docker Desktop macOS. The Apple Virtualization Framework adds overhead to VortexDB's async I/O path disproportionately. Use `--native` mode for accurate macOS comparisons. On bare-metal Linux Docker the same containerized setup shows 1.0–3.7× advantage.

2. **macOS kqueue vs Linux io_uring** — VortexDB's polling backend (kqueue) performs synchronous I/O with event notification, while the io_uring backend uses truly asynchronous completions. macOS native results show 1.0–2.5× Redis on single/batch commands; Linux shows 1.0–3.7×.

3. **PING_INLINE overhead** — VortexDB consistently shows 0.5–1.0× Redis on PING_INLINE. This is pure I/O round-trip cost with zero engine work. It reflects event-loop per-round-trip overhead, not engine performance. All engine-touching commands show higher ratios.

4. **Redis io-threads** — Redis 8.6 with `io-threads 4` is ~2× faster than single-threaded Redis. Previous benchmarks comparing VortexDB against single-threaded Redis showed inflated ratios. All current benchmarks use optimally configured Redis (io-threads 4).

5. **Point vs mixed workloads** — `redis-benchmark` and `memtier_benchmark` answer different questions. Point-command wins do not automatically predict mixed-workload wins, which is why the automated suite now publishes both.

6. **Single-core measurement** — `redis-benchmark` defaults measure aggregate throughput across pipelined connections to a single server instance. VortexDB's per-core advantage is most visible in micro-benchmarks.

7. **Pipeline depth** — Default pipeline depth of 16 favors throughput over latency. Use `-P 1` for realistic low-latency measurements. The memtier harness intentionally defaults to pipeline depth 1 so mixed-workload latency remains visible.

8. **Data size** — Default point-workload value size is 3 bytes, while the mixed-workload memtier suite uses 384-byte values. Real-world workloads with larger or smaller values will shift the balance toward memory bandwidth or parser overhead.

9. **Warm-up** — The first benchmark run after build warms CPU caches and JIT. For statistical validity, use `--runs 3` or higher.

---

## Statistical Methodology

When using `--runs N`:

1. Each command is benchmarked N times against each database
2. The first run is included (no explicit warm-up discard)
3. **Mean** and **standard deviation** are calculated across runs
4. **95% confidence interval** is computed as: `mean ± (1.96 × stddev / √N)`
5. Results are reported with the CI95 range

For single runs (default), results represent a single measurement with 100,000 requests. This is sufficient for relative comparison but not for absolute latency claims.
