# Profiling Workflow

> `just profiler` is a profiler-first workflow manager for `vortex-server`. It builds the profiling binary, starts the server, runs the selected profiler, captures host context, and can now drive load either through the built-in `redis-benchmark` path or through `vortex_bench attach` so profiling and benchmark artifacts land in one session root.

The profiler uses the same artifact vocabulary as `just benchmark`: `--artifact-root`, `--target-mode`, `--dry-run`, `--json`, and `--no-color` are the preferred UX flags. `just profiler --help` is the operator man page for every mode and option. Reports and summaries describe measurements and tool availability without embedding product policy.

## Quick Start

```bash
# Show what tools are available on your machine
just profiler --check

# Show the full option reference, target modes, reports, and examples
just profiler --help

# Resolve a session, target, and tool preflight without starting a capture
just profiler --dry-run --cpu --command PING --artifact-root .artifacts/profiling/dry-run

# CPU flamegraph under SET,GET load
just profiler --command SET,GET

# Force the uring backend explicitly for a profiling run
just profiler --flamegraph --io-backend uring --ring-size 4096 --sqpoll-idle-ms 1000 --command SET,GET

# Full CPU suite (flamegraph + perf/samply/instruments)
just profiler --cpu --command SET,GET --duration 20

# Scheduler-focused diagnostics driven by a benchmark manifest
just profiler --scheduler --bench-manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml --duration 5

# Lock wait and off-CPU diagnostics
just profiler --lock-offcpu --command SET --duration 10

# Memory heap tracking
just profiler --memory --command SET --duration 15

# Cache miss analysis
just profiler --cache --command SET --threads 1

# AOF and disk diagnostics
just profiler --aof-disk --command SET,INCR --duration 20

# Explicit AOF always off-CPU validation
VORTEX_AOF_FSYNC=always just profiler --lock-offcpu --aof --command SET --duration 10

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

Interactive terminals use one live progress line for long-running benchmark
and profiler phases when the tool is attached to a TTY. CI and captured logs
stay line-oriented, and `--json` emits machine-readable progress/status events.
Each completed profiler session writes `summary.md` for quick review and
`summary.json` for automation.

## Target Modes

| Mode | Use |
|------|-----|
| `local` | Build/start a local profiling target and stop it after capture |
| `host-port` | Attach to an already running local endpoint; the profiler does not stop it |
| `ssh-managed` | Delegate capture to `scripts/profiler.sh` in a remote checkout after an explicit remote start command |
| `ssh-attach` | Delegate capture to `scripts/profiler.sh` in a remote checkout for an existing remote endpoint |

Examples:

```bash
just profiler --cpu --target-mode local --command SET,GET --duration 20

just profiler --perf-stat \
  --target-mode host-port \
  --host 127.0.0.1 \
  --port 16379 \
  --command PING \
  --artifact-root .artifacts/profiling/attach

just profiler --cpu \
  --target-mode ssh-attach \
  --ssh-target perfbox \
  --ssh-port 2222 \
  --ssh-identity-file ~/.ssh/perfbox_ed25519 \
  --ssh-option StrictHostKeyChecking=accept-new \
  --ssh-workdir /srv/vortex \
  --host perfbox \
  --port 16379 \
  --command PING \
  --artifact-root .artifacts/profiling/remote
```

Remote profiler sessions use SSH to run the same profiler script from the
remote checkout, then copy the remote artifact root back under
`<artifact-root>/remote/<timestamp>` when `scp` is available. Use `--ssh-port`,
`--ssh-identity-file`, `--ssh-config`, `--ssh-option`, and
`--ssh-connect-timeout` for non-default SSH transport. Root-required tools still require an explicit
operator-controlled command path; the profiler does not run remote sudo
implicitly.

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
| Hardware counters, top-down, and locality (IPC, cache, branches, TLB) | dual-pass `perf stat` PMU report | Instruments System Trace | — |
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
| `--lock-offcpu` | perf stat + host sampler pack + runqlat/biolatency + offcputime/offwaketime or perf sched + futex/sync tracing (Linux) or Instruments/Samply fallback with a platform note (macOS) | Lock wait, runnable delay, off-CPU blocking, and fsync stall triage |
| `--memory` | heaptrack or massif (Linux) or heaptrack or instruments Memory suite [Allocations, Leaks] (macOS) | Heap allocation tracking |
| `--cache` | cachegrind | Cache locality L1/L2/LL miss rates |
| `--aof-disk` | perf stat (Linux) or Instruments/System Trace fallback (macOS) + host sampler pack | AOF write path and disk-pressure investigation |
| `--network` | perf stat or samply/instruments fallback + host sampler pack | Socket, loopback, and network-path diagnostics |
| `--all` | `--cpu` + `--memory` + `--cache` sequentially | Full profiling gauntlet |

### Specific Tool Flags

| Flag | Tool | Platform |
|------|------|----------|
| `--flamegraph` | `cargo flamegraph` | Linux + macOS |
| `--perf-stat` | dual-pass `perf stat` PMU report (`perf-stat-counters.txt` + `perf-stat.txt` + `perf-stat-report.txt`) | Linux only |
| `--samply` | `samply record` | Linux + macOS |
| `--instruments` | `xcrun xctrace record` | macOS only |
| `--heaptrack` | `heaptrack` | Linux + macOS |
| `--cachegrind` | `valgrind --tool=cachegrind` | Linux (+ macOS if valgrind available) |
| `--callgrind` | `valgrind --tool=callgrind` | Linux (+ macOS if valgrind available) |
| `--massif` | `valgrind --tool=massif` | Linux (+ macOS if valgrind available) |

`just profiler --check` prints the platform evidence boundary and per-probe
availability before the ordinary tool list. `just profiler --check --dry-run`
also writes JSON preflight artifacts, including a Darwin-simulated row that
marks Linux-only PMU/BPF/perf/procfs/socket probes as unsupported on macOS.
Those artifacts document platform capability boundaries only; they do not
replace actual macOS polling runtime rows.

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

The profiler now uses `vortex_bench attach` under the hood, writes the attached state file into the current profiler session, and keeps the benchmark request/result artifacts under `bench/` so the profiling session and benchmark workload stay aligned. Benchmark-manifest load is also driven by memory and cache profiles, and AOF fsync runtime fields are forwarded into attach state so runtime validation sees the same persistence policy as the profiled server.


For Linux PMU sessions, `--perf-stat` now writes two raw counter captures plus a derived report:

- `perf-stat-counters.txt`: explicit IPC, branch, cache, TLB, scheduler, and page-fault counters
- `perf-stat.txt`: top-down and locality-oriented counter pass
- `perf-stat-context.txt`: PMU permissions, power profile, virtualization, and optional counter probe notes
- `perf-stat-report.txt`: derived IPC, `instructions/op`, `cycles/op`, top-down percentages, locality miss rates, and reliability warnings

`summary.json` exposes the same bundle under `pmu_profiles`. Engine targets derive exact operation counts from `engine-target-summary.json`. Server sessions derive `instructions/op` and `cycles/op` from the capture window and mark the values as estimated when `redis-benchmark` runs multiple sequential command sections or outlives the requested capture duration. Optional tracepoint and uncore probes retry with `sudo perf stat` only when the profiler already has `HOST_PASSWORD`, an active sudo session, or `VORTEX_PROFILER_TRY_SUDO_PERF_STAT=force`; `perf-stat-context.txt` labels those counters as `supported-sudo`.
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
├── perf-stat-counters.txt
├── perf-stat.txt
├── perf-stat-context.txt
├── perf-stat-report.txt
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

`session.json` records the session contract, `notes.md` is the engineer note template, `summary.json` is the concise machine-readable summary, and `summary-compare.json` appears when `--compare-to` is used. PMU sessions also publish `pmu_profiles` in `summary.json`, including artifact paths, previews, derived highlights, and reliability warnings.

## Runtime Counter Snapshots

Profiler host telemetry now captures boundary snapshots of Vortex `INFO runtime` when the target server is Vortex and the profiler knows the host, port, and pid.

Those counters are intentionally cheap:

- reactor loop counts and accept `EAGAIN` rearms use per-reactor relaxed atomics
- completion and command batch width use per-reactor counters plus per-slot max tracking
- active expiry and eviction scan effort reuse existing bounded-work counters

Read the rolled-up counters from `host/*-host-telemetry-summary.json`.
Read raw interval samples, including `per_cpu_*`, `tcp_*`, and `socket_*`, from `host/*-host-telemetry.jsonl`.

Important fields:

- `reactor_loop_iterations_delta`
- `reactor_accept_eagain_rearms_delta`
- `reactor_completion_batches_delta`
- `reactor_completion_batch_max_peak`
- `reactor_command_batches_delta`
- `reactor_command_batch_max_peak`
- `reactor_active_expiry_runs_delta`
- `reactor_active_expiry_sampled_delta`
- `reactor_active_expiry_expired_delta`
- `eviction_*_delta`

Current limit: true always-on shard lock contention is not exported here yet. Measuring lock wait directly would require hotter instrumentation on the lock-acquisition path and would risk perturbing the workload.

## BPF Artifacts And Session Diffs

The question-first Linux modes emit extra artifacts when the corresponding BPF tools are installed:

- `--scheduler` -> `bpf-runqlat.txt`
- `--lock-offcpu` -> `bpf-runqlat.txt`, `bpf-biolatency.txt`, `bpf-offcputime.txt`, `bpf-offwaketime.txt`, `bpf-futex.txt`, `bpf-sync-syscalls.txt`, `lock-offcpu-classification.txt`
- `--aof-disk` -> `bpf-biolatency.txt`
- `--network` -> `bpf-tcpretrans.txt`

`--compare-to` also attempts to generate `diff-flamegraph.svg` when both sessions contain `perf.data`.

Interpretation rules:

- `bpf-runqlat.txt`: check the long-tail buckets before assuming a hotspot is purely CPU-bound
- `bpf-offcputime.txt`: look for blocked stacks when the target is sleeping off-CPU
- `bpf-offwaketime.txt`: use the blocked+waker stacks to separate lock contention from generic scheduler delay
- `bpf-futex.txt`: explicit futex wait evidence for the profiled process
- `bpf-sync-syscalls.txt`: explicit `fsync`/`fdatasync`/`sync_file_range` evidence for the profiled process
- `lock-offcpu-classification.txt`: the profiler's label for runnable off-CPU, blocked lock/off-CPU, disk/fsync, or unknown
- `bpf-biolatency.txt`: rising right-tail latency points to block-layer stalls rather than pure userspace cost
- `bpf-tcpretrans.txt`: empty output is valid; it usually means no retransmits occurred in that session window
- `diff-flamegraph.svg`: red stacks grew relative to baseline, blue stacks shrank

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
