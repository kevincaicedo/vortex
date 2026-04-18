# Extending The Harness

## Add A New Workload

1. Add a `WorkloadDefinition` in `python/vortex_benchmark/catalog.py`.
2. Decide whether the workload is single-key, multi-key, transactional, or hot-key.
3. Add a manifest example under `manifests/examples/`.
4. Confirm backend routing still makes sense:
   - non-transactional single-key workloads go to `memtier_benchmark` by default
   - multi-key and transactional workloads go to `custom-rust`

## Add A New Database

1. Create a new adapter under `python/vortex_benchmark/db/`.
2. Implement native and or container launch support.
3. Implement `resolve_runtime_config()` so the adapter records the effective launch-time runtime policy.
4. Reject unsupported runtime policy combinations explicitly in `validate_runtime_config()`.
5. Register the adapter in `python/vortex_benchmark/db/registry.py`.
6. Update the support matrix docs and add at least one example manifest.

## Add A New Backend

1. Add a backend module under `python/vortex_benchmark/backends/`.
2. Implement a `run_*_backend(context)` function that returns `BackendExecutionRecord`.
3. Normalize raw backend output into a stable result JSON under `results/`.
4. Register the runner in `python/vortex_benchmark/backends/__init__.py`.
5. Update docs and add a manifest that exercises the new backend.

## Add New Metrics Or Report Fields

1. Capture new service or process data in `python/vortex_benchmark/telemetry.py`.
2. Normalize it into report rows in `python/vortex_benchmark/reporting/collector.py`.
3. Add columns in `python/vortex_benchmark/commands/report.py` if the CSV needs stable ordering.
4. Surface the new metric in `python/vortex_benchmark/reporting/markdown.py` or the chart renderer when it is user-facing.
5. Keep the stable publication contract intact so `reports/latest/` and `reports/index.json` do not need workflow-specific hacks.

## Modularity Rules

- Keep orchestration in `commands/`, not in shell scripts.
- Keep database startup concerns in `db/`.
- Keep backend execution concerns in `backends/`.
- Keep aggregation and rendering concerns in `reporting/`.
- Prefer adding a small focused module over growing one giant file or one giant workflow script.