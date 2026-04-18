# Profiling Workflow

> `just profiler` is a **profiling tool manager** for vortex-server. It detects the OS, checks available tools, builds the binary, starts the server, runs the selected profiler, generates load internally, and collects artifacts. It is **not** a benchmark wrapper. `vortex_bench` is the separate benchmark tool for any Redis-compatible server.

## Quick Start

```bash
# Show what tools are available on your machine
just profiler --check

# CPU flamegraph under SET,GET load
just profiler --command SET,GET

# Full CPU suite (flamegraph + perf/samply/instruments)
just profiler --cpu --command SET,GET --duration 20

# Memory heap tracking
just profiler --memory --command SET --duration 15

# Cache miss analysis
just profiler --cache --command SET --threads 1

# Everything
just profiler --all --command SET,GET

# Criterion micro-benchmarks
just profiler --criterion --filter cmd_get_inline

# Use a profiling manifest for reproducibility
just profiler --manifest scripts/profiler/manifests/cpu-set-heavy.yaml
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

### Composite Modes

| Flag | What Runs | Description |
|------|-----------|-------------|
| `--cpu` | flamegraph + perf stat + perf record (Linux) or samply + instruments CPU suite [Time Profiler, System Trace] (macOS) | Full CPU analysis |
| `--memory` | heaptrack or massif (Linux) or heaptrack or instruments Memory suite [Allocations, Leaks] (macOS) | Heap allocation tracking |
| `--cache` | cachegrind | Cache locality L1/L2/LL miss rates |
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
└── criterion/
```

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
    ├── build.sh                    ← cargo build --profile profiling
    ├── server.sh                   ← Server lifecycle, load generation, cleanup
    ├── cpu.sh                      ← flamegraph, perf, samply, instruments (xctrace)
    ├── memory.sh                   ← heaptrack, massif, instruments allocs/leaks
    ├── cache.sh                    ← cachegrind, callgrind
    ├── criterion.sh                ← cargo bench --profile profiling
    ├── check.sh                    ← Tool availability diagnostics
    ├── parse_manifest.py           ← YAML manifest parser (Python, stateless)
    ├── docs/
    │   └── profiling-workflow.md   ← This document
    └── manifests/
        ├── cpu-set-heavy.yaml
        ├── full-suite.yaml
        └── memory-write-pressure.yaml
```
