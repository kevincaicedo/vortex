# Performance Tooling Guide

This is the current operator guide for Vortex performance work. It covers when to use `vortex_bench`, when to use `just profiler`, how artifacts are organized, how manifests work, how to read profiler notes, and how to run optimization work without confusing throughput measurement with root-cause analysis.

## Tool Split

Use `vortex_bench` when the question is about externally visible behavior:

- throughput
- latency distributions
- repeatability
- database-to-database comparison
- runtime policy comparison such as AOF, eviction, or thread count

Use `just profiler` when the question is about explanation:

- where CPU time went
- whether the scheduler is the bottleneck
- whether memory allocation or reclaim is dominating
- whether AOF or filesystem latency is causing stalls
- whether network or loopback behavior is material

The standard workflow is benchmark first, profiler second. A benchmark proves that something changed. The profiler explains why.

## Default Artifact Roots

Both tools now default to the repository-level `.artifacts/` tree.

```text
vortex/.artifacts/
  benchmarks/
    environments/
    logs/
    requests/
    results/
    backend-runs/
    reports/
    runtime/
    local-dev/
  profiling/
    <timestamp>-profiling/
```

Key rule: benchmark artifacts default to `.artifacts/benchmarks/` and profiler artifacts default to `.artifacts/profiling/`. This keeps both tools under one repo-root artifact tree while preserving separate benchmark and profiling contracts.

## Benchmark Workflows

### Single-Cycle Local Loop

Use `just benchmark-local` when you want setup, run, report, and teardown as one command.

```bash
just benchmark-local
just benchmark-local --manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml
just benchmark-local --manifest vortex-benchmark/manifests/examples/local-native-full-cycle.yaml
just benchmark-local --db vortex,redis --native --backend custom-rust --workload multi_key_only
```

This writes a timestamped session under `.artifacts/benchmarks/local-dev/`.

### Explicit Five-Step Flow

Use the explicit flow when you want to keep a state file around, attach later, or aggregate multiple result summaries into one report.

```bash
bash vortex-benchmark/bin/vortex_bench setup \
  --db vortex,redis \
  --native \
  --workload-manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml

bash vortex-benchmark/bin/vortex_bench run \
  --state-file .artifacts/benchmarks/environments/<state>.json \
  --backend redis-benchmark \
  --command GET,SET,INCR

bash vortex-benchmark/bin/vortex_bench report \
  --results-dir .artifacts/benchmarks/results \
  --output-dir .artifacts/benchmarks

bash vortex-benchmark/bin/vortex_bench teardown \
  --state-file .artifacts/benchmarks/environments/<state>.json
```

### Attach Flow

Use `attach` when the target server is already running or when a profiler session should reuse benchmark infrastructure.

```bash
bash vortex-benchmark/bin/vortex_bench attach \
  --db vortex \
  --host 127.0.0.1 \
  --port 16379 \
  --pid 4242 \
  --label scheduler-pass
```

This writes an environment state file compatible with `run`, `report`, and the profiler bridge.

## Benchmark CLI Combinations

Common combinations:

- Point-command sweep: `--backend redis-benchmark --command GET,SET,INCR`
- Mixed workload sweep: `--backend memtier_benchmark --workload uniform-read_heavy`
- Multi-key or transactional load: `--backend custom-rust --workload multi_key_only` or `transaction_only`
- AOF scenario: `--aof-enabled --aof-fsync everysec`
- Eviction scenario: `--maxmemory 64mb --eviction-policy allkeys-lru`
- Repeat-aware scenario: `--repeat 3` or manifest `repeat: 3`

Key rule: when comparing databases, keep command/workload, thread count, duration, runtime policy, and environment mode identical. If you aggregate summaries that violate that rule, the report marks those scenario groups invalid instead of pretending the comparison is fair.

## Benchmark Manifests

### Top-Level Fields

Benchmark manifests support these top-level fields:

- `schema_version`
- `name`
- `description`
- `databases`
- `workloads`
- `commands`
- `command_groups`
- `backends`
- `repeat`
- `duration`
- `environment`
- `resource_config`
- `runtime_config`
- `settings`

### Example Manifest

```yaml
schema_version: 1
name: citation-read-heavy
description: Two-database repeat-aware command sweep
databases:
  - vortex
  - redis
backends:
  - redis-benchmark
commands:
  - GET
  - SET
repeat: 3
duration: 5s
environment:
  mode: native
  port_base: 17379
resource_config:
  cpus: 4
  memory: 2g
  threads: 4
runtime_config:
  aof_enabled: false
  eviction_policy: noeviction
settings:
  redis-benchmark:
    requests: 30000
    clients: 20
    pipeline: 8
```

### How To Create A New Benchmark Manifest

1. Start from the closest file under `vortex-benchmark/manifests/examples/`.
2. Choose databases first.
3. Choose the backend that actually matches the workload shape.
4. Define either explicit commands, workload names, or both.
5. Add `repeat` if the scenario should carry citation-grade or engineering-grade replicate intent in version control.
6. Pin `resource_config` and `runtime_config` so the comparison surface is stable.
7. Keep backend-specific tuning inside `settings`.

CLI wins over manifest values. That means `--repeat`, `--threads`, `--duration`, `--aof-enabled`, and similar flags override the manifest when both are present.

### Existing Benchmark Example Manifests

Important shipped examples:

- `vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml`
- `vortex-benchmark/manifests/examples/local-native-redis-benchmark-repeat.yaml`
- `vortex-benchmark/manifests/examples/local-native-memtier.yaml`
- `vortex-benchmark/manifests/examples/local-native-full-cycle.yaml`
- `vortex-benchmark/manifests/examples/aof-everysec-native.yaml`
- `vortex-benchmark/manifests/examples/eviction-allkeys-lru-native.yaml`
- `vortex-benchmark/manifests/examples/ci-regression-native.yaml`

Use the repeat-enabled manifest when you want a short repeat-aware validation pass without editing a manifest yourself.

## Reading Benchmark Reports

Important benchmark report outputs:

- `report.json`: machine-readable report payload
- `report.csv`: flattened rows for further analysis
- `report.md`: operator-facing report with findings and invalid-comparison notes
- `reports/latest/`: stable copies for dashboards and CI publishing

Repeat-aware reports aggregate replicate rows into medians and attach spread, min/max, and outlier counts. Mixed-signature comparisons are surfaced in `comparison_validity` and excluded from winner tables and cross-database comparison tables.

### Runtime Counter Artifacts

Vortex now exposes a low-overhead `INFO runtime` section for always-on diagnostic counters. The implementation uses per-reactor relaxed atomics in the shared keyspace and exports them through a dedicated `INFO runtime` section instead of piggybacking on the heavier general `INFO` path.

These counters show up in three places:

- benchmark result JSON under `observability.before`, `observability.after`, and `observability.delta`
- flattened benchmark report rows as `reactor_*` and `eviction_*` columns
- profiler and benchmark host telemetry summaries as `reactor_*_delta`, `*_peak`, and `eviction_*_delta`

Raw interval samples still live in `host/*-host-telemetry.jsonl`. That JSONL stream is where to inspect per-sample `per_cpu_max_pct`, `per_cpu_min_pct`, `tcp_*`, and `socket_*` fields instead of only their rolled-up summary values.

Important fields and how to read them:

- `reactor_loop_iterations_delta`: total event-loop turns during the measurement window
- `reactor_accept_eagain_rearms_delta`: non-blocking accept rearms; rising values usually mean idle accept wakeups or accept-backlog pressure
- `reactor_completion_batches_delta`, `reactor_completion_batch_max_peak`, `reactor_completion_batch_avg_peak`: completion-drain batching on the I/O side
- `reactor_command_batches_delta`, `reactor_command_batch_max_peak`, `reactor_command_batch_avg_peak`: RESP pipeline width per parsed command batch
- `reactor_active_expiry_runs_delta`, `reactor_active_expiry_sampled_delta`, `reactor_active_expiry_expired_delta`: active TTL cleanup work paid during the run
- `eviction_admissions_delta`, `eviction_shards_scanned_delta`, `eviction_slots_sampled_delta`, `eviction_bytes_freed_delta`: bounded eviction scan effort and resulting reclaim work

Current boundary: true always-on shard lock contention is still intentionally not exported. Accurate lock-wait instrumentation would need to touch hot lock acquisition paths or add timing probes that could perturb the workload being measured.

## Profiler Workflows

### Quick Start

```bash
just profiler --check
just profiler --criterion --filter cmd_get_inline
just profiler --cpu --command SET,GET --duration 20
just profiler --scheduler --bench-manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml --duration 5
just profiler --memory --command SET --duration 15
just profiler --aof-disk --command SET,INCR --duration 20
just profiler --network --command SET,GET,INCR --duration 20
```

### Profiler Modes

Use the question-first modes unless you already know the specific tool you need.

- `--cpu`: full CPU hotspot analysis
- `--scheduler`: run queue, context-switch, and scheduling pressure diagnostics
- `--memory`: heap allocation and memory-growth investigation
- `--cache`: locality and cache-miss inspection
- `--aof-disk`: AOF and disk-pressure investigation
- `--network`: loopback, socket, and network-path investigation

Tool-centric flags such as `--flamegraph`, `--perf-stat`, `--samply`, `--heaptrack`, and `--massif` still exist and are useful when the bottleneck class is already known.

### Profiler With Benchmark Workloads

The profiler can reuse benchmark workload definitions.

```bash
just profiler --scheduler --bench-manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml
just profiler --scheduler --bench-request .artifacts/benchmarks/requests/<request>.json
just profiler --scheduler --bench-manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml --compare-to .artifacts/profiling/<previous-session>
```

This uses `vortex_bench attach` and writes benchmark-side artifacts under the profiler session `bench/` subtree.

### BPF Escalation And Session Diffs

The question-first profiler modes now carry their matching Linux BPF escalation artifacts:

- `just profiler --scheduler ...` writes `bpf-runqlat.txt` when `runqlat` is available
- `just profiler --aof-disk ...` writes `bpf-biolatency.txt` when `biolatency` is available
- `just profiler --network ...` writes `bpf-tcpretrans.txt` when `tcpretrans` is available

`--compare-to` now also attempts to write `diff-flamegraph.svg` when both sessions contain `perf.data` and either Brendan Gregg's FlameGraph scripts or `inferno` are installed.

Interpretation rules:

- `bpf-runqlat.txt`: scheduler delay histogram; look for a fat right tail before blaming CPU hotspots alone
- `bpf-biolatency.txt`: block-device latency histogram; this is the first place to check when AOF durability work stretches p99
- `bpf-tcpretrans.txt`: retransmit events during the session; zero output is valid and usually means the run saw no retransmits
- `diff-flamegraph.svg`: red stacks grew versus baseline, blue stacks shrank versus baseline

### Profiler Manifests

Profiler manifests live under `scripts/profiler/manifests/`.

Important shipped examples:

- `scripts/profiler/manifests/cpu-set-heavy.yaml`
- `scripts/profiler/manifests/full-suite.yaml`
- `scripts/profiler/manifests/memory-write-pressure.yaml`
- `scripts/profiler/manifests/scheduler-read-heavy.yaml`
- `scripts/profiler/manifests/aof-disk-write-pressure.yaml`
- `scripts/profiler/manifests/network-mixed.yaml`

Profiler manifests pin modes, server settings, workload settings, and profiler frequency. CLI flags still override them.

## Profiler Notes And Session Outputs

Every profiler session writes:

- `session.json`
- `notes.md`
- `summary.json`
- optional `summary-compare.json`
- `host/` telemetry artifacts
- optional `bench/` benchmark bridge artifacts

`notes.md` is the human interpretation template. Fill it in immediately after the session while context is still fresh.

Minimum note fields:

- the question you were trying to answer
- the primary hotspot or pressure signal
- the likely interpretation
- the next change or next diagnostic action

Do not skip the note. Raw flamegraphs and `perf` output are not a substitute for explicit interpretation.

For sessions against Vortex, the host telemetry summary now also captures boundary snapshots of `INFO runtime`. Read those counters alongside the flamegraph or perf output to distinguish batching, expiry, and eviction work from scheduler or I/O pressure.

## Recommended Optimization Workflow

This is the operating loop derived from the performance methodology and systems-performance material.

1. Define one performance question.
2. Freeze the workload, runtime policy, and environment.
3. Run a benchmark baseline.
4. Capture host and service context in the same run.
5. Run the matching profiler session.
6. Form one hypothesis.
7. Make one targeted change.
8. Re-run the same benchmark and the same profiler or diagnostic capture.
9. Record the result, interpretation, and remaining uncertainty.

## Practical Rules

- Benchmark and profiling are different jobs. Do not treat a flamegraph as proof that externally visible performance improved.
- Prefer manifests for repeatable scenarios.
- Use manifest `repeat` or CLI `--repeat` for engineering-grade and citation-grade runs.
- Preserve raw artifacts. Another engineer should be able to audit the same run later.
- Do not aggregate mismatched runtime settings and call it a comparison. The report now flags those scenarios as invalid.
- Treat `io_uring` as a hypothesis, not a status symbol. Measure `polling`, `auto`, and `uring` on the same workload.
- One code change, one hypothesis, one before/after evidence set.

## CI Publication

For benchmark jobs, publish at minimum:

- `.artifacts/benchmarks/reports/latest/report.json`
- `.artifacts/benchmarks/reports/latest/report.csv`
- `.artifacts/benchmarks/reports/latest/report.md`
- the request manifest or resolved request used for the run
- the source summary JSON files for each benchmark session included in the report

For profiler jobs, publish at minimum:

- `summary.json`
- `summary-compare.json` when `--compare-to` was used
- `notes.md`
- `host/*host-telemetry-summary.json`
- `diff-flamegraph.svg` when present

Publish raw `perf.data`, FlameGraph intermediates, or full host JSONL only when the review actually needs them. They are valuable, but they are much larger and noisier than the summary contract.

PR or CI summaries should always state:

- workload manifest or request path
- server runtime configuration differences
- repeat count or evidence tier
- whether BPF tools, PMU counters, or diff flamegraphs were available

## Validation Examples

Recent end-to-end validations for the current toolchain include:

- `just benchmark-local --manifest vortex-benchmark/manifests/examples/local-native-full-cycle.yaml`
- `just benchmark-local --manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml --duration 3s`
- `just benchmark-local --manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark-repeat.yaml`
- `just benchmark-local --manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark-repeat.yaml --threads 8`
- `bash vortex-benchmark/bin/vortex_bench report --summary-file <threads-4-summary> --summary-file <threads-8-summary> --output-dir .artifacts/benchmarks/validation/invalid-comparison-repeat-threads`
- `just profiler --criterion --package vortex-engine --bench-target engine --filter cmd_get_inline`
- `just profiler --scheduler --bench-manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml --duration 5`

Those validations prove the current benchmark and profiler stack is not just documented, but executable.