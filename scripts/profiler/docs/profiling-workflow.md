# Profiling Workflow

> `just profiler` is a profiler-first workflow manager for `vortex-server`. It builds the profiling binary, starts the server, runs the selected profiler, captures host context, and can now drive load either through the built-in `redis-benchmark` path or through `vortex_bench attach` so profiling and benchmark artifacts land in one session root.

## Quick Start

```bash
# Show what tools are available on your machine
just profiler --check

# CPU flamegraph under SET,GET load
just profiler --command SET,GET

# Force the uring backend explicitly for a profiling run
just profiler --flamegraph --io-backend uring --ring-size 4096 --sqpoll-idle-ms 1000 --command SET,GET

# Full CPU suite (flamegraph + perf/samply/instruments)
just profiler --cpu --command SET,GET --duration 20

# Scheduler-focused diagnostics driven by a benchmark manifest
just profiler --scheduler --bench-manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml --duration 5

# Memory heap tracking
just profiler --memory --command SET --duration 15

# Cache miss analysis
just profiler --cache --command SET --threads 1

# AOF and disk diagnostics
just profiler --aof-disk --command SET,INCR --duration 20

# Network-focused diagnostics
just profiler --network --command SET,GET,INCR --duration 20

# Everything
just profiler --all --command SET,GET

# Criterion micro-benchmarks
just profiler --criterion --filter cmd_get_inline

# Use a profiling manifest for reproducibility
just profiler --manifest scripts/profiler/manifests/cpu-set-heavy.yaml

# Compare a new session to an earlier matching workload
just profiler --scheduler --bench-manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml --compare-to .artifacts/profiling/<previous-session>
```

## Build Configuration

Profiling always uses the `profiling` Cargo profile:

```toml
[profile.profiling]
inherits = "release"
debug = true
strip = "none"
lto = "thin"
split-debuginfo = "unpacked"
```

Workspace compiler flags (`.cargo/config.toml`):

```toml
[target.'cfg(any(target_os = "linux", target_os = "macos"))']
rustflags = [
    "-C", "force-frame-pointers=yes",
    "-C", "symbol-mangling-version=v0",
]
```

**Why:** Release optimizations make hotspot shapes real. Debug symbols make samples inspectable. Frame pointers enable reliable stack unwinding. Either half alone produces misleading traces.

## Tool Matrix

| Question | Linux Primary | macOS Primary | Cross-Platform |
|----------|---------------|---------------|----------------|
| CPU hotspot triage | `cargo flamegraph`, `perf` | `samply`, Instruments Time Profiler | `cargo flamegraph` |
| Hardware counters (IPC, cache, branches) | `perf stat -d` | Instruments System Trace | — |
| Interactive flamegraph + source view | `samply`, `cargo flamegraph` | `samply` | `samply` |
| Instruction-level deep dive | Callgrind | — | — |
| Cache locality analysis | Cachegrind | — | — |
| Heap growth / allocation churn | Heaptrack, Massif | Instruments Allocations | Heaptrack |
| Crate-level latency regression | Criterion | Criterion | Criterion |

## Mode Reference

### Question-First Modes

| Flag | What Runs | Description |
|------|-----------|-------------|
| `--cpu` | flamegraph + perf stat + perf record (Linux) or samply + instruments CPU suite [Time Profiler, System Trace] (macOS) | Full CPU analysis |
| `--scheduler` | perf stat (Linux) or Instruments/System Trace fallback (macOS) + host sampler pack | Run queue, context-switch, and scheduling pressure triage |
| `--memory` | heaptrack or massif (Linux) or heaptrack or instruments Memory suite [Allocations, Leaks] (macOS) | Heap allocation tracking |
| `--cache` | cachegrind | Cache locality L1/L2/LL miss rates |
| `--aof-disk` | perf stat (Linux) or Instruments/System Trace fallback (macOS) + host sampler pack | AOF write path and disk-pressure investigation |
| `--network` | perf stat or samply/instruments fallback + host sampler pack | Socket, loopback, and network-path diagnostics |
| `--all` | `--cpu` + `--memory` + `--cache` sequentially | Full profiling gauntlet |

### Specific Tool Flags

| Flag | Tool | Platform |
|------|------|----------|
| `--flamegraph` | `cargo flamegraph` | Linux + macOS |
| `--perf-stat` | `perf stat -d` | Linux only |
| `--samply` | `samply record` | Linux + macOS |
| `--instruments` | `xcrun xctrace record` | macOS only |
| `--heaptrack` | `heaptrack` | Linux + macOS |
| `--cachegrind` | `valgrind --tool=cachegrind` | Linux (+ macOS if valgrind available) |
| `--callgrind` | `valgrind --tool=callgrind` | Linux (+ macOS if valgrind available) |
| `--massif` | `valgrind --tool=massif` | Linux (+ macOS if valgrind available) |

### Criterion

| Flag | Description |
|------|-------------|
| `--criterion` | Run micro-benchmarks (no server, no load) |
| `--filter PATTERN` | Filter benchmark names |
| `--package NAME` | Cargo package (default: vortex-bench) |
| `--bench-target T` | Cargo bench target |

## When to Use Which Tool

**Use Criterion when:**
- The suspected regression is inside one crate or command path
- You need statistical before/after comparison on isolated code
- You want CI-friendly regression evidence

**Use `--cpu` / `--flamegraph` when:**
- You already know the workload and need to locate the hotspot
- Criterion shows a regression but not *why*

**Use `--memory` when:**
- You suspect allocation churn or heap growth
- The CPU profiler shows time in `alloc::` or `memmove` functions

**Use `--cache` when:**
- You suspect poor data locality
- Optimizing tight inner loops where L1/L2 misses dominate

## Server Configuration

Pass server settings for realistic profiling conditions:

```bash
just profiler --cpu --command SET --threads 4
just profiler --cpu --command SET --aof
just profiler --cpu --command SET --maxmemory 64mb --eviction allkeys-lru
```

| Flag | Description | Default |
|------|-------------|---------|
| `--threads N` | Server thread count | 4 |
| `--aof` | Enable AOF persistence | disabled |
| `--maxmemory SIZE` | Set max memory (e.g., 64mb) | unlimited |
| `--eviction POLICY` | Set eviction policy | noeviction |
| `--io-backend auto|uring|polling` | Explicit Vortex I/O backend | auto |
| `--ring-size N` | Override io_uring ring size | binary default |
| `--sqpoll-idle-ms N` | Override io_uring SQPOLL idle timeout (ms) | binary default |
| `--host HOST` | Bind address | 127.0.0.1 |
| `--port PORT` | Bind port | 16379 |
| `--bin PATH` | Use pre-built binary | auto-build |

## Profiling Manifests

Manifests make profiling reproducible across machines and OS. Store them under `scripts/profiler/manifests/`.

### Example Manifest

```yaml
schema_version: 1
name: cpu-set-heavy
description: Full CPU profiling under SET-heavy workload

modes:
  - cpu
  - flamegraph

server:
  threads: 4
  aof: false

workload:
  command: SET
  duration: 30
  clients: 50

profiler:
  frequency: 99
```

### Running a Manifest

```bash
just profiler --manifest scripts/profiler/manifests/cpu-set-heavy.yaml
```

CLI flags override manifest values, so you can customize on the fly:

```bash
# Use manifest but override duration
just profiler --manifest scripts/profiler/manifests/cpu-set-heavy.yaml --duration 60
```

### Included Manifests

| Manifest | Description |
|----------|-------------|
| `cpu-set-heavy.yaml` | Flamegraph + CPU profiling under SET-heavy load |
| `full-suite.yaml` | All modes under mixed SET,GET,INCR workload |
| `memory-write-pressure.yaml` | Heap tracking under write pressure with 64mb maxmemory |
| `scheduler-read-heavy.yaml` | Scheduler-focused diagnostics under read-heavy pressure |
| `aof-disk-write-pressure.yaml` | AOF and disk diagnostics under sustained writes |
| `network-mixed.yaml` | Network-focused diagnostics under mixed load |

## `vortex_bench` Bridge

When you need the profiler to reuse an already defined benchmark workload, use one of these flags instead of `--command`:

```bash
# Reuse a benchmark manifest as the profiling load source
just profiler --scheduler --bench-manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml

# Reuse a previously normalized benchmark request
just profiler --scheduler --bench-request .artifacts/benchmarks/requests/<request>.json
```

The profiler now uses `vortex_bench attach` under the hood, writes the attached state file into the current profiler session, and keeps the benchmark request/result artifacts under `bench/` so the profiling session and benchmark workload stay aligned.

## Standard Optimization Loop

Every performance-sensitive change should follow this loop:

1. **Pick** the representative workload
2. **Baseline Criterion**: `just profiler --criterion --filter <hot_path>`
3. **Baseline System**: `just profiler --cpu --command SET,GET`
4. **Identify** one bottleneck (CPU, memory, cache, allocation, I/O, locks)
5. **Make one change**
6. **Re-run** Criterion and system profiling
7. **Save** before/after artifacts and summarize what changed

## Artifact Layout

All artifacts land in a timestamped session directory:

```
.artifacts/profiling/<YYYYMMDD-HHMMSS>-profiling/
├── session.json
├── notes.md
├── summary.json
├── summary-compare.json
├── host/
│   ├── *-host-telemetry.jsonl
│   ├── *-host-telemetry-summary.json
│   ├── process-probe.log
│   └── socket-summary.log
├── bench/
│   ├── environments/
│   ├── requests/
│   ├── results/
│   └── backend-runs/
├── flamegraph.svg
├── perf.data
├── perf-stat.txt
├── perf-report.txt
├── samply-profile.json
├── *-toc.xml
├── *-data.xml
├── *.trace
├── cachegrind.out
├── callgrind.out
├── massif.out
├── heaptrack.*.gz
├── server-*.log
├── load-*.log
├── summary.log
└── criterion/
```

`session.json` records the session contract, `notes.md` is the engineer note template, `summary.json` is the concise machine-readable summary, and `summary-compare.json` appears when `--compare-to` is used.

## Tool Installation

### macOS

```bash
# Flamegraph (CPU sampling via DTrace)
cargo install flamegraph

# samply (cross-platform CPU profiler, Firefox Profiler UI)
cargo install --locked samply

# Heaptrack (memory, via Homebrew)
brew install heaptrack

# Valgrind (cache/callgraph — limited macOS support)
brew install valgrind

# redis-benchmark (load generation)
brew install redis
```

### Linux (Ubuntu/Debian)

```bash
# perf (hardware counters, sampling)
sudo apt install linux-tools-$(uname -r) linux-tools-generic

# Flamegraph
cargo install flamegraph

# samply
cargo install --locked samply

# Heaptrack (memory allocation)
sudo apt install heaptrack

# Valgrind (cachegrind, callgrind, massif)
sudo apt install valgrind

# KCachegrind (GUI for callgrind/cachegrind)
sudo apt install kcachegrind

# redis-benchmark (load generation)
sudo apt install redis-tools
```

## Script Architecture

```
scripts/
├── profiler.sh                     ← Entry point (arg parsing, dispatch)
└── profiler/
    ├── common.sh                   ← Colors, OS detection, tool helpers, PING probe
    ├── bench.sh                    ← vortex_bench attach/run bridge helpers
    ├── build.sh                    ← cargo build --profile profiling
    ├── host.sh                     ← Host telemetry runner and socket/process logs
    ├── server.sh                   ← Server lifecycle, load generation, cleanup
    ├── cpu.sh                      ← flamegraph, perf, samply, instruments (xctrace)
    ├── memory.sh                   ← heaptrack, massif, instruments allocs/leaks
    ├── cache.sh                    ← cachegrind, callgrind
    ├── criterion.sh                ← cargo bench --profile profiling
    ├── check.sh                    ← Tool availability diagnostics
    ├── parse_manifest.py           ← YAML manifest parser (Python, stateless)
    ├── prepare_benchmark_bridge.py ← Normalizes bench manifests/requests for profiling
    ├── summary.sh                  ← Summary lifecycle wrapper
    ├── write_session_summary.py    ← Machine-readable session summary + comparison
    ├── docs/
    │   └── profiling-workflow.md   ← This document
    └── manifests/
      ├── aof-disk-write-pressure.yaml
        ├── cpu-set-heavy.yaml
        ├── full-suite.yaml
      ├── memory-write-pressure.yaml
      ├── network-mixed.yaml
      └── scheduler-read-heavy.yaml
```
