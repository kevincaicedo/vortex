# vortex_benchmark

`vortex_benchmark` is the new system-benchmark toolchain for VortexDB.

It is being built as a new top-level folder under `vortex-benchmark/` and will replace the current ad-hoc benchmark flow built around:

- `scripts/compare.sh`
- `scripts/bench-commands.sh`
- `scripts/validate-benchmarks.sh`
- `tools/vortex-bench`

## Scope

This tool focuses on external and system benchmarking:

- database setup and teardown
- workload orchestration
- multi-database comparison
- artifact generation
- reproducible benchmark manifests
- report generation with graphs
- CI-friendly raw outputs

## Documentation

- [docs/usage.md](docs/usage.md) — CLI flow, setup, run, report, and teardown usage.
- [docs/manifests-and-workloads.md](docs/manifests-and-workloads.md) — manifest schema, runtime policies, and workload scenario authoring.
- [docs/support-matrix.md](docs/support-matrix.md) — backend command and workload support plus native vs container database support.
- [docs/reporting-and-ci.md](docs/reporting-and-ci.md) — report artifacts, result interpretation, Pages publication, and CI integration.
- [docs/profiling-workflow.md](docs/profiling-workflow.md) — unified profiling workflow across Criterion, `vortex_bench`, `attach`, and OS-native profilers.
- [docs/extending.md](docs/extending.md) — how to add workloads, databases, backends, and new report metrics.

## Explicit Non-Goals

This tool does not own Criterion and microbenchmark work.

- Criterion benches will move into the individual crates that own those components.
- The current `tools/vortex-bench` crate is legacy and will be removed in a later migration.
- The split of microbenchmarks into crate-local benches is tracked as a separate task.

## Phase 1 Status

Phase 1 is complete.

Phase 1 deliverables:

- [docs/phase-1-research-audit.md](docs/phase-1-research-audit.md)
- [docs/phase-1-architecture-plan.md](docs/phase-1-architecture-plan.md)
- [docs/phase-1-migration-plan.md](docs/phase-1-migration-plan.md)

## Phase 2 Status

Phase 2 is complete.

Implemented now:

- Python package scaffold under `python/vortex_benchmark/`
- thin repo wrapper at `bin/vortex_bench`
- unified CLI surface for `setup`, `run`, and `teardown`
- native setup for Vortex and Redis
- container setup for Vortex, Redis, Dragonfly, and Valkey
- readiness checks via RESP `PING`
- machine-readable environment state files and per-service log artifacts

Current Phase 2 boundary:

- `run` currently resolves and records a normalized run request under `.artifacts/requests/`
- actual benchmark backend execution remains Phase 4 work
- Phase 3 extended this setup with manifest schema validation and workload catalog resolution

## Phase 3 Status

Phase 3 is now landed.

Implemented now:

- JSON and YAML benchmark manifest parsing
- manifest schema validation for databases, workloads, command groups, resource settings, and environment settings
- a built-in workload taxonomy and standard library for the Phase 3 workload set
- canonical command-group validation with benchmark-facing groups for `strings`, `keys`, `server`, and `transactions`
- merged manifest-plus-CLI resolution so setup and run can both consume the same normalized spec
- example manifests under `manifests/examples/`

Current Phase 3 boundary:

- manifest parsing and validation are live
- `run` writes a normalized request artifact containing the resolved manifest, command expansion, and workload metadata

## Phase 4 Status

Phase 4 is now landed.

Implemented now:

- modular backend execution under `python/vortex_benchmark/backends/`
- `redis-benchmark` command execution with standardized JSON summaries and raw CSV/stderr artifacts
- `memtier_benchmark` workload execution with thread sweeps, JSON capture, and HDR latency files
- standalone Rust load generator under `rust/custom-loadgen/` for multi-key and transactional workloads
- `run` now executes resolved backends, writes normalized run summaries, and keeps backend raw artifacts separate from reports

Current Phase 4 boundary:

- backend execution is live for `redis-benchmark`, `memtier_benchmark`, and `custom-rust`
- `.artifacts/results/` now stores normalized run summaries and per-backend result JSON
- `.artifacts/backend-runs/` stores raw backend outputs such as CSV, JSON, stderr logs, HDR files, and custom backend run files

## Phase 5 Status

Phase 5 is now landed.

Implemented now:

- host and per-run observability capture for database and process memory, cache hits/misses, eviction counters, and AOF state deltas
- normalized report aggregation across multiple run summaries with machine-readable JSON and CSV exports
- Markdown report generation under `.artifacts/reports/` with embedded chart assets
- Python chart rendering for radar profiles, latency heatmaps, and scaling sweeps
- percentile coverage extended through `P99.999` where backend data is available

Current Phase 5 boundary:

- report generation is live through `vortex_bench report`
- `.artifacts/reports/` stores timestamped Markdown, JSON/CSV, and chart artifacts
- `.artifacts/reports/latest/` stores stable `report.json`, `report.csv`, `report.md`, and chart copies for CI and Pages consumers
- `.artifacts/reports/index.json` stores the report history index used for automated publication and regression tracking
- AOF overhead and eviction sections populate when the source summaries include those run conditions

## Phase 7 Status

Phase 7 is now landed.

Implemented now:

- explicit `runtime_config` support in manifests, setup, and saved environment state for AOF, fsync policy, maxmemory, and eviction policy scenarios
- CLI flags for setup and run validation: `--aof-enabled`, `--aof-disabled`, `--aof-fsync`, `--maxmemory`, and `--eviction-policy`
- runtime-aware adapter launches for Vortex, Redis, Valkey, and Dragonfly support validation
- stable report publication outputs under `.artifacts/reports/latest/` and `.artifacts/reports/index.json`
- example CI manifests for baseline, AOF, and eviction regression suites
- workflow integration in `.github/workflows/competitive-bench.yml` so Pages and artifact uploads consume the report JSON/CSV/Markdown outputs directly
- documentation for usage, manifests, support matrices, reporting, and extension points

Phase 3 validation highlights:

- `bash vortex-benchmark/bin/vortex_bench setup --workload-manifest vortex-benchmark/manifests/examples/read-heavy-native.yaml --db redis ...` created a real native Redis environment state file
- `bash vortex-benchmark/bin/vortex_bench run --state-file ... --workload-manifest vortex-benchmark/manifests/examples/mixed-container.json --db redis ...` wrote a normalized run request artifact
- `bash vortex-benchmark/bin/vortex_bench run --state-file ... --command-group invalid-group` failed with a validation error as expected

## Current CLI

From the repo root:

```bash
just benchmark --setup --db vortex,redis --native
just benchmark --setup --db vortex,redis --workload uniform-read_heavy --duration 60s
just benchmark --setup --db redis --native --aof-enabled --aof-fsync everysec --command SET,GET,INCR
just benchmark --setup --db redis --native --maxmemory 64mb --eviction-policy allkeys-lru --workload uniform-write_heavy
just benchmark --setup --workload-manifest vortex-benchmark/manifests/examples/read-heavy-native.yaml
just benchmark --setup --workload-manifest vortex-benchmark/manifests/examples/ci-regression-native.yaml
just benchmark-local
just benchmark-local --manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml
just benchmark-local --db vortex,redis --native --backend redis-benchmark --command SET,GET,INCR
just benchmark run --state-file vortex-benchmark/.artifacts/environments/<state>.json --workload uniform-read_heavy --duration 60s
just benchmark run --state-file vortex-benchmark/.artifacts/environments/<state>.json --workload-manifest vortex-benchmark/manifests/examples/mixed-container.json
just benchmark run --state-file vortex-benchmark/.artifacts/environments/<state>.json --workload-manifest vortex-benchmark/manifests/examples/aof-everysec-native.yaml
just benchmark run --state-file vortex-benchmark/.artifacts/environments/<state>.json --backend redis-benchmark --command SET,GET,INCR
just benchmark run --state-file vortex-benchmark/.artifacts/environments/<state>.json --backend memtier_benchmark --workload uniform-mixed
just benchmark run --state-file vortex-benchmark/.artifacts/environments/<state>.json --backend custom-rust --workload multi_key_tx_mixed
just benchmark attach --db vortex --host 127.0.0.1 --port 16379 --pid 4242 --label ttl-profile
just benchmark report --results-dir vortex-benchmark/.artifacts/results --output-dir vortex-benchmark/.artifacts
just benchmark teardown --state-file vortex-benchmark/.artifacts/environments/<state>.json
```

Generated artifacts currently live under `vortex-benchmark/.artifacts/`:

- `environments/` — machine-readable service state files
- `logs/` — per-service startup and teardown logs
- `requests/` — normalized run-request artifacts used to bridge into later backend phases
- `results/` — normalized run summaries plus per-backend result JSON
- `backend-runs/` — raw backend execution artifacts such as CSV, JSON, HDR files, and stderr logs
- `reports/` — timestamped Markdown reports, JSON/CSV exports, and chart images
- `reports/latest/` — stable published copies of the newest report and charts
- `reports/index.json` — machine-readable history index for CI, Pages, and regression dashboards
- `runtime/` — per-environment runtime files such as AOF outputs and mounted service runtime state
- `local-dev/` — per-cycle local developer benchmark runs created by `just benchmark-local`

## Backend Support

- `redis-benchmark` is the point-command backend. It is the right path for explicit command and command-group sweeps, and it replaces the old `compare.sh` and `bench-commands.sh` execution role with modular adapter code.
- `memtier_benchmark` is the mixed-workload backend. It currently targets non-transaction single-key workload families such as `uniform-*`, `zipfian-*`, `hot-key`, and `single_key_mixed`.
- `custom-rust` is the fallback backend for deterministic RESP/TCP execution when the external tools are a poor fit, especially for multi-key and transactional workload families.

## Backend Settings

Backend-specific tuning currently comes from the manifest `settings` section.

- `redis_benchmark` or `redis-benchmark`: `requests`, `clients`, `pipeline`, `keyspace_size`
- `memtier_benchmark`: `requests`, `clients`, `pipeline`, `thread_sweep`, `keyspace_size`, `data_size`, `rate_limiting`, `key_prefix`
- `custom-rust` or `custom_rust`: `profile`, `thread_sweep`, `keyspace_size`, `ops_per_thread`, `warmup_ops`, `value_size`

Manifest files support these top-level sections:

- `databases`
- `workloads`
- `commands`
- `command_groups`
- `backends`
- `duration`
- `environment`
- `resource_config`
- `runtime_config`
- `settings`

`runtime_config` currently supports:

- `aof_enabled`
- `aof_fsync`
- `maxmemory`
- `eviction_policy`

Built-in workload names currently standardized by Phase 3:

- `uniform-read_only`
- `uniform-read_heavy`
- `uniform-mixed`
- `uniform-write_heavy`
- `uniform-write_only`
- `zipfian-read_heavy`
- `zipfian-mixed`
- `transaction`
- `multi-key operations`
- `hot-key`
- `single_key_mixed`
- `multi_key_only`
- `transaction_only`
- `single_key_tx_mixed`
- `multi_key_tx_mixed`

## Planned Tool Shape

The implementation direction chosen in Phase 1 is:

- Python orchestrator for manifests, setup, reporting, graphs, and artifact handling
- Rust custom load generator backend for deterministic RESP/TCP workloads
- thin `just` and shell wrappers only where they improve ergonomics

Planned high-level structure:

```text
vortex-benchmark/
  README.md
  docs/
  python/
  rust/
  manifests/
  reports/
```

Phases 2 through 5 have turned this into a real benchmark harness with setup, manifest resolution, backend execution, normalized results, and report generation.