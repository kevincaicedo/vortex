# Reporting And CI

## Report Artifacts

Each `report` run now produces two views of the same data:

- timestamped archives under `reports/[timestamp]_report.{json,csv,md}` plus `reports/[timestamp]_assets/`
- stable publication copies under `reports/latest/`

The report history index lives at `reports/index.json`.

The stable layout is designed for CI, regression dashboards, and Pages publication without workflow-specific copy rules inside the reporting code.

## How To Read The Outputs

The normalized JSON and CSV rows include:

- database and backend identity
- series label and thread count
- throughput and latency percentiles
- effective runtime configuration: AOF enabled, fsync policy, maxmemory, and eviction policy
- observability deltas for memory, cache hits and misses, evictions, expirations, and AOF size

The Markdown report adds:

- database target summary table
- workload summary table
- latency and throughput table
- memory, cache, and eviction table
- AOF overhead section when comparable baseline and AOF rows exist
- eviction observations when evictions were recorded
- chart embeds for radar, latency heatmap, and scaling sweep

## CI Workflow Shape

`.github/workflows/competitive-bench.yml` now uses `vortex_bench` end to end:

1. install native benchmark dependencies
2. run one or more manifest-driven setup/run/teardown scenarios
3. aggregate summaries with `vortex_bench report`
4. upload `reports/` as a workflow artifact
5. publish `reports/latest/` and `reports/index.json` to Pages-friendly paths

The current CI examples are:

- `ci-regression-native.yaml` — baseline Vortex versus Redis comparison
- `aof-everysec-native.yaml` — Redis AOF overhead comparison input
- `eviction-allkeys-lru-native.yaml` — Redis eviction pressure scenario

## Historical Comparison

`reports/index.json` is intended to be the machine-readable entrypoint for later automation. It records:

- the newest stable report pointers
- the archived report list
- metadata such as row count, databases, backends, and generation time

Consumers should read `reports/latest/report.json` for the newest data and `reports/index.json` when they need history traversal.