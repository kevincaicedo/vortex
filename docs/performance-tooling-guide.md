# Performance Tooling Guide

This guide is the operator surface for Vortex benchmark and profiler work. The tools measure workloads, explain bottlenecks, and publish artifacts. Product interpretation lives in project-owned evidence ledgers, not in the benchmark or profiler report generator.

## Primary Commands

Use `just benchmark` for benchmark sessions. It is the primary benchmark
entrypoint and prints its full option/help surface with `just benchmark --help`:

```bash
just benchmark \
  --workload-manifest vortex-benchmark/manifests/examples/local-native-full-cycle.yaml \
  --target-mode local \
  --artifact-root .artifacts/benchmarks/local-dev \
  --profile engineering
```

Use `just profiler` for root-cause sessions:

```bash
just profiler --cpu --command SET,GET --duration 20 --artifact-root .artifacts/profiling/local-dev
```

Expert subcommands remain available for debugging:

```bash
just benchmark setup ...
just benchmark attach ...
just benchmark report ...
just benchmark teardown ...
```

## Target Modes

`just benchmark` supports one target model:

| Mode | Use | Service ownership |
|------|-----|-------------------|
| `local` | Start local native or Docker services, run workloads, report, teardown | Managed |
| `host-port` | Benchmark an existing RESP endpoint | External; never stopped |
| `ssh-managed` | Run an explicit remote start command, benchmark the endpoint locally or from `--ssh-load-host`, optionally stop and copy artifacts back | Managed by the provided SSH commands |
| `ssh-attach` | Benchmark an existing remote RESP endpoint locally or from `--ssh-load-host` and capture remote metadata | External; never stopped |

Examples:

```bash
# Local native or Docker session
just benchmark \
  --db vortex,redis \
  --native \
  --backend redis-benchmark \
  --command SET,GET,INCR \
  --duration 30s \
  --artifact-root .artifacts/benchmarks/local

# Attach to an existing endpoint
just benchmark \
  --db vortex \
  --target-mode host-port \
  --target-url 127.0.0.1:16379 \
  --backend redis-benchmark \
  --command PING,GET,SET \
  --artifact-root .artifacts/benchmarks/attach

# Remote managed service with explicit lifecycle commands
just benchmark \
  --db vortex \
  --target-mode ssh-managed \
  --ssh-target perfbox \
  --ssh-workdir /srv/vortex \
  --ssh-copy-source \
  --ssh-build-command 'cargo build --release -p vortex-server --bin vortex-server' \
  --ssh-start-command 'cd /srv/vortex && ./vortex-server --bind 0.0.0.0:16379 --threads 4' \
  --ssh-stop-command 'pkill -INT vortex-server' \
  --target-url perfbox:16379 \
  --backend memtier_benchmark \
  --workload uniform-mixed \
  --artifact-root .artifacts/benchmarks/remote

# Remote service plus remote load generator
just benchmark \
  --db vortex \
  --target-mode ssh-managed \
  --ssh-target servicebox \
  --ssh-port 2222 \
  --ssh-identity-file ~/.ssh/perfbox_ed25519 \
  --ssh-option StrictHostKeyChecking=accept-new \
  --ssh-load-host loadbox \
  --ssh-workdir /srv/vortex \
  --ssh-copy-source \
  --ssh-build-command 'cargo build --release -p vortex-server --bin vortex-server' \
  --ssh-start-command 'cd /srv/vortex && target/release/vortex-server --bind 0.0.0.0:16379 --threads 4' \
  --ssh-stop-command 'pkill -INT vortex-server' \
  --target-url servicebox:16379 \
  --backend redis-benchmark \
  --command PING,GET,SET \
  --artifact-root .artifacts/benchmarks/remote-load

# Remote attach without service mutation
just benchmark \
  --db vortex \
  --target-mode ssh-attach \
  --ssh-target perfbox \
  --target-url perfbox:16379 \
  --backend redis-benchmark \
  --command PING \
  --artifact-root .artifacts/benchmarks/remote
```

SSH benchmark artifact copy-back lands under `<artifact-root>/remote/<timestamp>/`; the session `plan.md` records that path before execution.
Use `--ssh-port`, `--ssh-identity-file`, `--ssh-config`, `--ssh-option`, and
`--ssh-connect-timeout` when the remote target does not use default SSH
transport settings. The same transport settings are used for SSH commands,
source sync, and artifact copy-back.

## Profiles And Progress

Profiles set defaults without hiding the resolved request:

| Profile | Intent |
|---------|--------|
| `quick` | Fast local smoke-scale measurement, one repeat |
| `engineering` | Default development measurement |
| `citation` | Three repeats by default and stricter validity metadata |
| `diagnostic` | Measurement paired with deeper profiler or telemetry context |

Useful universal flags:

```bash
--artifact-root DIR   # one root for session, request, results, reports, logs
--dry-run             # resolve preflight/session/plan without running
--explain             # write a human-readable resolved plan
--json                # stream progress events as JSON lines
--no-color            # stable uncolored output
--no-report           # skip automatic report rendering
```

TTY output uses one live progress line for long-running phases, with a spinner,
bar, step count, and ETA. Non-TTY logs use stable aligned progress rows, and
`--json` emits newline-delimited progress events for CI or dashboards. Report
generation prints a compact benchmark result table and writes the full
Markdown/JSON/CSV report under `reports/latest/`.

## Artifact Contract

Benchmark artifacts default to `.artifacts/benchmarks/`:

| Path | Contents |
|------|----------|
| `sessions/<target-mode>/<timestamp>/session.json` | Common session header |
| `sessions/<target-mode>/<timestamp>/preflight.json` | Platform/tool preflight |
| `sessions/<target-mode>/<timestamp>/plan.md` | Resolved dry-run or explain plan |
| `environments/` | Managed or attached service state |
| `requests/` | Resolved benchmark request |
| `results/` | Normalized run summaries |
| `backend-runs/` | Raw backend outputs |
| `reports/<timestamp>_report.{json,csv,md}` | Archived reports |
| `reports/latest/` | Stable report copies only |
| `runtime/` | Runtime state such as AOF files |

Profiler artifacts default to `.artifacts/profiling/` and include:

| Path | Contents |
|------|----------|
| `session.json` | Common profiler session header |
| `summary.json` | Machine-readable profiler summary |
| `notes.md` | Engineer notes template |
| `preflight.json` | Tool/platform preflight for dry-run sessions |
| `host/*summary.json` | Host telemetry rollups |
| Tool outputs | Perf, flamegraph, Instruments, Samply, heaptrack, cachegrind, or BPF outputs |

Stable `latest/` copies are for reports only. Raw heavy artifacts stay in timestamped session directories.

## Reports

Benchmark reports are neutral measurement reports. They include:

- workload contract and target mode
- database target configuration
- repeat statistics and latency distribution
- p99.9 when the backend provides it
- client saturation verdict
- likely limiting resource hypothesis
- host telemetry and memory attribution
- invalid-comparison warnings
- machine-readable JSON, CSV, and Markdown generated from the same payload

Reports refuse strong cross-database interpretation when workloads, backend, target mode, telemetry mode, thread count, data shape, AOF/eviction policy, or client saturation status differ. When a comparison is still useful for exploration, it is labeled advisory.

Offline rendering uses completed summaries:

```bash
just benchmark report \
  --summary-file .artifacts/benchmarks/results/<run>-summary.json \
  --output-dir .artifacts/benchmarks

just benchmark report \
  --baseline-summary-file .artifacts/benchmarks/baseline/results/<run>-summary.json \
  --candidate-summary-file .artifacts/benchmarks/candidate/results/<run>-summary.json \
  --output-dir .artifacts/benchmarks/diff
```

## Profiler Modes

Start with the question:

| Question | Command |
|----------|---------|
| CPU hotspot | `just profiler --cpu --command SET,GET --duration 20` |
| Scheduler or run queue | `just profiler --scheduler --bench-manifest <manifest>` |
| Lock or off-CPU wait | `just profiler --lock-offcpu --command SET --duration 10` |
| Allocation/RSS growth | `just profiler --memory --command SET --duration 15` |
| Cache locality | `just profiler --cache --command SET --threads 1` |
| AOF/disk stalls | `just profiler --aof-disk --command SET,INCR --duration 20` |
| Network/socket pressure | `just profiler --network --command SET,GET --duration 20` |
| Tool availability | `just profiler --check` or `just profiler --dry-run --cpu --command PING` |

Remote profiler capture delegates to a remote checkout and copies artifacts
back under `<artifact-root>/remote/<timestamp>`:

```bash
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

Profiler modes never run root-required tools automatically. Sudo is used only through an explicit tool path and the session records which tool needed it.

## Diagnostic Packs

Benchmark and profiler sessions record low-overhead host evidence when available:

- CPU utilization, run queue, context switches
- process CPU, RSS, faults, and I/O counters
- memory pressure, dirty/writeback pages, swap, reclaim
- network bytes, errors, retransmits, socket queues
- disk bytes, queue depth, and I/O time
- tool availability and permission status

Linux diagnostic tools include `vmstat`, `mpstat`, `pidstat`, `iostat`, `sar`, `ss`, `nstat`, `perf`, `trace-cmd`, BCC tools, and `bpftrace` when available. macOS sessions use `vm_stat`, `iostat`, `netstat`, `sysctl`, Instruments, and Samply where available. Missing metrics are reported explicitly.

## Troubleshooting

| Symptom | Check |
|---------|-------|
| Missing `perf` or PMU counters | `just profiler --check`; inspect `perf_event_paranoid` and PMU status |
| Missing BPF tools | Use `--dry-run`; install BCC or `bpftrace`, or choose scheduler/perf fallback |
| Missing Instruments | Run `xcrun xctrace list templates` on macOS |
| Redis tools missing | Install `redis-benchmark`, `redis-cli`, or `memtier_benchmark` |
| Port already in use | Use a different `--port-base` or stop stale listeners |
| Attach target mutated unexpectedly | Use `--target-mode host-port` or `ssh-attach`; these modes mark ownership external |
| Docker rows fail at high thread counts | Check container memory and database maxmemory |
| Thermal/power variance | Set performance power mode and rerun |
| Client saturation | Increase load-host capacity, reduce workload, or split service/load hosts |

## Working Loop

1. Use `--dry-run --explain` to confirm target, workload, runtime policy, and artifact paths.
2. Run `just benchmark` with the smallest workload that answers the measurement question.
3. Read `reports/latest/report.md` for human review and `report.json` for automation.
4. If the report points to CPU, scheduler, lock/off-CPU, memory, disk, or network pressure, run the matching profiler mode.
5. Keep product decisions in the evidence ledger, with links back to the benchmark and profiler artifact paths.
