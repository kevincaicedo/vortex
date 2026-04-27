# Profiling Workflow

> Status: P1.10.1 through P1.10.4 are live. `just profiler` is a **profiling tool manager** for vortex-server — it detects the OS, checks available tools, builds the binary, starts the server, runs the selected profiler, and collects artifacts. It is **not** a benchmark wrapper. `vortex_bench` is the separate benchmark tool for any Redis-compatible server.

## Goal

Profiling in Vortex should be a repeatable engineering loop, not an ad hoc debugging trick.

The workflow has to work on both macOS and Linux and it has to combine four layers cleanly:

1. Criterion for crate-level latency regression detection.
2. `vortex_bench` for reproducible end-to-end workload generation and reporting.
3. OS-native profilers for CPU, scheduler, cache, branch, and allocation analysis.
4. Simple `just` and shell wrappers so contributors do not have to memorize per-OS profiler incantations.

## Scope

This workflow covers:

- release-like profiling builds with symbols
- function-level and crate-level benchmarking
- end-to-end server profiling under representative client load
- macOS and Linux profiler selection
- artifact capture and report linkage
- guidance for future performance-sensitive tasks

This workflow intentionally does not cover:

- `tokio-console`
- `dhat-rs`
- full normalization of every profiler trace format into one cross-tool schema in v1

## Principles

- Profile optimized binaries, not `dev` builds.
- Keep debug symbols and frame pointers so stacks are trustworthy.
- Use real workloads when profiling server behavior.
- Separate micro-benchmark questions from end-to-end workload questions.
- Change one thing at a time and keep before/after evidence.
- Persist artifacts so another engineer can inspect the same session later.

## Standard Build Configuration

The baseline profiling build should be a shared workspace profile:

```toml
[profile.profiling]
inherits = "release"
debug = true
strip = "none"
lto = "thin"
split-debuginfo = "unpacked"
```

The shared compiler flags should enforce:

```toml
[target.'cfg(any(target_os = "linux", target_os = "macos"))']
rustflags = [
  "-C", "force-frame-pointers=yes",
  "-C", "symbol-mangling-version=v0",
]
```

Why these settings matter:

- `inherits = "release"`: profiling `dev` builds is misleading because inlining, branch layout, register allocation, and lock behavior are all materially different from the binaries we actually ship and benchmark.
- `debug = true`: maps samples back to real source lines and functions.
- `strip = "none"`: preserves symbol tables.
- `split-debuginfo = "unpacked"`: especially important for macOS symbol handling.
- `force-frame-pointers=yes`: improves stack unwinding quality for `perf` and `samply`.
- `symbol-mangling-version=v0`: makes symbol names readable across tools.

Release-like optimizations plus symbols are mandatory together. Optimizations make the hotspot shape real; symbols make the samples inspectable. Either half on its own produces misleading or low-value traces.

## Tool Matrix

| Question | Linux Primary | macOS Primary | Cross-Platform Fallback | Notes |
|----------|---------------|---------------|-------------------------|-------|
| Crate-level latency regression | Criterion | Criterion | Criterion | No network overhead |
| End-to-end throughput and latency regression | `vortex_bench` | `vortex_bench` | `vortex_bench` | Benchmark context source of truth |
| CPU hotspot triage | `perf record/report`, `cargo flamegraph` | `cargo instruments -t "Time Profiler"` | `samply` | First stop for most regressions |
| Interactive flamegraph and source view | `cargo flamegraph`, `samply` | `samply`, Instruments | `samply` | Prefer `samply` over DTrace-based flamegraph on macOS |
| Counters, CPI, cache misses, branch misses | `perf stat -d`, `perf report`, `perf-focus` | Instruments Time Profiler + System Trace | none | Linux is the PMU authority |
| Instruction-accurate deep dive | Callgrind | none | none | Linux-only |
| Cache locality deep dive | Cachegrind | none | none | Linux-only |
| Heap growth / temporary allocations | Massif, Heaptrack | Instruments Allocations | none | Use after CPU hotspot identification |
| Scheduler / thread behavior | `perf`, system tools | Instruments System Trace | `samply` partial | Important for reactor work |

## When To Use Which Tool

Use Criterion when:

- the suspected regression is inside one crate or command path
- you need statistical before/after comparison on isolated code
- you want CI-friendly regression evidence

Use `vortex_bench` when:

- the issue only appears with network, parser, reactor, AOF, or mixed workload pressure
- you need a reproducible client workload
- you need report artifacts that compare databases or manifests

Use the OS profiler when:

- you already know the workload and now need to locate the hotspot
- you suspect lock contention, scheduler imbalance, cache misses, or allocation churn
- Criterion shows a regression but not why it exists

## Standard Optimization Loop

Every performance-sensitive change should follow this loop:

1. Pick the representative workload.
2. Run the relevant Criterion benchmark(s).
3. Run the relevant `vortex_bench` workload.
4. Profile the optimized binary on the current OS.
5. Identify one bottleneck category: CPU, memory/cache, allocation, I/O, or synchronization.
6. Make one targeted change.
7. Re-run Criterion and `vortex_bench`.
8. Save before/after artifacts and summarize what changed.

## Profiler Interface

`just profiler` is a profiling **tool manager** for vortex-server. It is organized by profiling concern, not by benchmark mode. The profiler always targets Vortex — it builds `target/profiling/vortex-server` automatically, starts it, runs the OS-native profiler, and collects artifacts.

### Modes

| Mode | Purpose | Default Tool (Linux) | Default Tool (macOS) |
|------|---------|---------------------|---------------------|
| `cpu` | CPU flamegraph / time profiling | cargo flamegraph | samply |
| `memory` | Heap allocation profiling | heaptrack | heaptrack |
| `cache` | Cache locality analysis | cachegrind | cachegrind |
| `callgraph` | Instruction-accurate call graph | callgrind | callgrind |
| `counters` | Hardware performance counters | perf stat | — |
| `criterion` | Criterion micro-benchmarks | criterion | criterion |
| `check` | Show OS, available tools, binary path | — | — |

### Examples

```bash
# CPU profiling (auto-detects best tool for your OS)
just profiler cpu

# Force a specific tool
just profiler cpu --tool flamegraph
just profiler cpu --tool samply
just profiler cpu --tool perf
just profiler cpu --tool instruments

# CPU profiling with automated load generation
just profiler cpu --load --command SET,GET --duration 20

# Memory profiling
just profiler memory
just profiler memory --load --command SET --duration 15

# Cache analysis (Linux, valgrind required)
just profiler cache --threads 1

# Call graph (Linux, valgrind required)
just profiler callgraph --threads 1

# Hardware counters (Linux, perf required)
just profiler counters --load --command SET,GET,INCR --duration 10

# Criterion micro-benchmarks
just profiler criterion --filter cmd_get_inline

# Check what tools are available
just profiler check
```

### Tool Detection

The profiler auto-detects the OS and selects the best available tool for each mode. Run `just profiler check` to see what is installed. You can override the auto-selection with `--tool NAME`.

### Load Generation

Load generation is **optional**. The profiler runs regardless — if you omit `--load`, it prints a hint for running load manually in another terminal. When `--load` is specified, it uses `redis-benchmark` by default (or `vortex_bench` via `--load-tool vortex_bench`) to drive traffic in the background while the profiler captures.

### Key Design Separation

- **Profiler** (`just profiler`): Vortex-only. Manages profiling tools. Always builds and starts vortex-server. Produces flamegraphs, traces, counter reports, etc.
- **Benchmark** (`vortex_bench`): Works with any Redis-compatible server. Manages workload generation, measurement, and reporting. The profiler can use it internally for load, but they serve different purposes.

## Script Set

- `scripts/profiler.sh` — The profiling tool manager. Concern-based interface (`cpu`, `memory`, `cache`, `callgraph`, `counters`, `criterion`, `check`). Detects OS, resolves tools, builds the binary, starts the server, runs the profiler, collects artifacts.
- `scripts/profile-common.sh` — Shared helpers: OS detection, binary building, server readiness probes, session directories.
- `scripts/profile-criterion.sh` — Low-level Criterion session runner (used internally by `profiler.sh criterion`).
- `scripts/profile-system.sh` — Legacy spawned-server profiling (retained for backward compatibility).
- `scripts/profile-existing.sh` — Legacy attach-to-existing-server profiling.
- `scripts/profile-report.sh` — Session metadata collation into `session.json` and `notes.md`.

## `vortex_bench` Integration

`vortex_bench` remains the owner of workload manifests, benchmark execution, normalized summaries, and reports.

The lifecycle bridge is now live through `attach`:

```bash
bash vortex-benchmark/bin/vortex_bench attach \
  --db vortex \
  --host 127.0.0.1 \
  --port 16379 \
  --pid 4242 \
  --label ttl-regression
```

The attached environment should write a normal state file so the existing flow still works:

```bash
bash vortex-benchmark/bin/vortex_bench run --state-file <external-state>.json ...
bash vortex-benchmark/bin/vortex_bench report --results-dir ...
```

That keeps one benchmark and report surface whether the target was started by `vortex_bench`, a profiler, or the simplified repo-root `just profiler` wrapper.

## Artifact Layout

Profiling artifacts should live under:

```text
.artifacts/profiling/<timestamp>/
  session.json
  notes.md
  profiler/
  criterion/
  bench/
  reports/
```

Expected contents:

- `session.json`: metadata describing tool, OS, binary, Cargo profile, manifest, pid, host, port, and artifact paths
- `notes.md`: short hotspot summary and before/after interpretation
- `profiler/`: raw profiler outputs such as `perf.data`, flamegraph SVG, `.trace`, samply capture, callgrind, cachegrind, massif, heaptrack
- `criterion/`: copied benchmark output or comparison snapshot
- `bench/`: `vortex_bench` request, summary, and raw backend outputs
- `reports/`: generated benchmark report plus profiling-aware summary

## Linux Workflow

### Setup

Recommended tools:

- `perf`
- `cargo-flamegraph`
- `samply`
- `valgrind`
- `heaptrack`
- KCachegrind or Hotspot for local analysis

Typical workflow:

1. Build with `--profile profiling`.
2. Launch the profiled binary directly.
3. Attach `vortex_bench` to the running endpoint or let a wrapper orchestrate both.
4. Drive a representative manifest.
5. Analyze with the right Linux tool:
   - `perf stat -d` for counters
   - `perf record -F 99 -g --call-graph fp` for sampled CPU stacks
   - `cargo flamegraph` for flamegraph-first investigation
   - Callgrind or Cachegrind for deep instruction/cache work
   - Massif or Heaptrack for memory growth or allocation churn

Representative commands:

```bash
perf stat -d target/profiling/vortex-server --bind 127.0.0.1:16379 --threads 4
perf record -F 99 -g --call-graph fp target/profiling/vortex-server --bind 127.0.0.1:16379 --threads 4
cargo flamegraph --profile profiling --bin vortex-server -- --bind 127.0.0.1:16379 --threads 4
samply record target/profiling/vortex-server --bind 127.0.0.1:16379 --threads 4
valgrind --tool=callgrind target/profiling/vortex-server --bind 127.0.0.1:16379 --threads 1
valgrind --tool=cachegrind target/profiling/vortex-server --bind 127.0.0.1:16379 --threads 1
valgrind --tool=massif target/profiling/vortex-server --bind 127.0.0.1:16379 --threads 1
heaptrack target/profiling/vortex-server --bind 127.0.0.1:16379 --threads 1
```

## macOS Workflow

### Setup

Recommended tools:

- Xcode Instruments
- `cargo-instruments`
- `samply`

Prefer Instruments and Samply on macOS. `cargo flamegraph` is not the default path here because the DTrace/SIP setup is more fragile.

Typical workflow:

1. Build with `--profile profiling`.
2. Launch via `cargo instruments` or `samply`.
3. Use `vortex_bench` against the profiled endpoint.
4. Inspect Time Profiler first, then System Trace or Allocations if the symptom points there.

Representative commands:

```bash
cargo instruments -t "Time Profiler" --profile profiling --bin vortex-server -- --bind 127.0.0.1:16379 --threads 4
cargo instruments -t "System Trace" --profile profiling --bin vortex-server -- --bind 127.0.0.1:16379 --threads 4
cargo instruments -t "Allocations" --profile profiling --bin vortex-server -- --bind 127.0.0.1:16379 --threads 4
samply record target/profiling/vortex-server --bind 127.0.0.1:16379 --threads 4
```

## Criterion Integration

Criterion remains mandatory for function-level regression evidence.

Representative commands:

```bash
cargo bench -p vortex-engine --bench engine --profile profiling -- cmd_get_inline
cargo bench -p vortex-engine --bench engine --profile profiling -- concurrent_cmd_get_inline
```

The point is not to replace Criterion with a system profiler. Criterion answers "did this hot path get slower in isolation?" The OS profiler answers "why?"

## Review Expectations For Future Tasks

For any performance-sensitive implementation task, the engineering note or PR should record:

- what workload was chosen and why
- which Criterion benches were run
- which `vortex_bench` manifest was used
- which profiler was used on the relevant OS
- the primary hotspot found
- what changed in the code
- before/after evidence

## Future Extensions

Likely follow-on work after P1.10:

- PGO workflow for release builds
- optional Linux eBPF-based continuous profiling
- richer Linux-side profiler summary extraction into Markdown
- deeper benchmark-to-profiler report linkage once the baseline workflow is stable