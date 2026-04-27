# Usage

## Tool Flow

`vortex_bench` is split into five steps:

1. `setup` starts the selected database services and writes an environment state file.
2. `attach` records an already running endpoint as an environment state file.
3. `run` executes one or more benchmark backends against that saved environment.
4. `report` aggregates normalized run summaries into JSON, CSV, Markdown, and chart artifacts.
5. `teardown` stops managed services or detaches external targets recorded in the saved environment state.

The normal entrypoint from the repository root is `bash vortex-benchmark/bin/vortex_bench ...`. The `just benchmark ...` recipe is a thin wrapper around the same tool.

For local developer loops there is also a single-cycle wrapper:

```bash
just benchmark-local
just benchmark-local --manifest vortex-benchmark/manifests/examples/local-native-memtier.yaml
just benchmark-local --db vortex,redis --native --backend custom-rust --workload multi_key_only
```

`just benchmark-local` runs setup, run, report, and teardown as one operation. When no arguments are provided it defaults to `vortex-benchmark/manifests/examples/local-native-full-cycle.yaml` and writes artifacts under `.artifacts/benchmarks/local-dev/`.

## Quick Start

Native baseline comparison:

```bash
bash vortex-benchmark/bin/vortex_bench setup \
  --db vortex,redis \
  --native \
  --workload uniform-read_heavy \
  --duration 30s

bash vortex-benchmark/bin/vortex_bench run \
  --state-file .artifacts/benchmarks/environments/<state>.json \
  --backend redis-benchmark \
  --command SET,GET,INCR

bash vortex-benchmark/bin/vortex_bench report \
  --results-dir .artifacts/benchmarks/results \
  --output-dir .artifacts/benchmarks

bash vortex-benchmark/bin/vortex_bench teardown \
  --state-file .artifacts/benchmarks/environments/<state>.json
```

Attach an already running profiled server:

```bash
bash vortex-benchmark/bin/vortex_bench attach \
  --db vortex \
  --host 127.0.0.1 \
  --port 16379 \
  --pid 4242 \
  --label ttl-profile

bash vortex-benchmark/bin/vortex_bench run \
  --state-file .artifacts/benchmarks/environments/<attached-state>.json \
  --backend redis-benchmark \
  --command SET,GET,INCR
```

Citation-grade repeatability can live in the manifest itself:

```bash
just benchmark-local --manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark-repeat.yaml
```

CLI `--repeat` still overrides the manifest value when you want to change replicate count without editing the scenario file.

Explicit AOF scenario:

```bash
bash vortex-benchmark/bin/vortex_bench setup \
  --db redis \
  --native \
  --command SET,GET,INCR \
  --aof-enabled \
  --aof-fsync everysec \
  --duration 20s
```

Explicit eviction scenario:

```bash
bash vortex-benchmark/bin/vortex_bench setup \
  --db redis \
  --native \
  --workload uniform-write_heavy \
  --maxmemory 64mb \
  --eviction-policy allkeys-lru \
  --duration 20s
```

## Quick Flags Versus Manifests

Use CLI flags when:

- the run is exploratory or one-off
- you only need a small number of options
- you are iterating locally and do not need a reusable scenario file

Use manifests when:

- the run should be reproducible in CI or Pages publication
- you want to pin databases, workloads, backends, and runtime policies together
- you want the scenario to live in version control under `vortex-benchmark/manifests/`

CLI flags and manifests are merged into one resolved benchmark spec. CLI flags win when both sides define the same field.

## Runtime Policy Flags

The setup and run commands now expose an explicit runtime policy surface:

- `--aof-enabled` / `--aof-disabled`
- `--aof-fsync always|everysec|no`
- `--maxmemory <size>`
- `--eviction-policy <policy>`

These values are recorded in the saved environment state. Later `run` commands validate that the requested runtime policy still matches the selected state file instead of silently reusing the wrong environment.

## Artifact Layout

The default artifact root is `.artifacts/benchmarks/` under the repository root and contains:

- `environments/` — saved setup state files
- `logs/` — per-service startup and teardown logs
- `requests/` — normalized run-request artifacts
- `results/` — normalized run summaries and per-backend result JSON
- `backend-runs/` — raw backend output files
- `reports/` — timestamped reports plus stable publication outputs
- `runtime/` — per-service runtime files such as AOF outputs

The stable publication paths are:

- `reports/latest/report.json`
- `reports/latest/report.csv`
- `reports/latest/report.md`
- `reports/latest/assets/`
- `reports/index.json`

Attached environment states look like normal setup-produced state files and can be consumed by `run` and `report` without any profiling-specific special casing.