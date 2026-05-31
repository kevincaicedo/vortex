# Reporting And CI

Reports are neutral measurement artifacts. They describe the workload, target, telemetry, comparison validity, and likely bottleneck. Release decisions are recorded outside the benchmark package.

## Report Outputs

Every rendered report writes:

- `reports/<timestamp>_report.json`
- `reports/<timestamp>_report.csv`
- `reports/<timestamp>_report.md`
- `reports/<timestamp>_assets/`
- `reports/latest/report.json`
- `reports/latest/report.csv`
- `reports/latest/report.md`
- `reports/index.json`

Only report files are copied to `latest/`. Raw backend outputs remain in timestamped directories.

## Report Contents

The report includes:

- session and source-run metadata
- target mode and service ownership
- workload contract and runtime policy
- host validity warnings
- repeat statistics
- throughput and latency distributions
- p99.9 when available
- benchmark-client saturation verdict
- server/host telemetry summary
- memory and AOF diagnostics
- invalid-comparison warnings
- advisory comparison tables only when inputs differ

The JSON, CSV, and Markdown files are rendered from the same normalized payload.

## Offline Rendering

```bash
bash vortex-benchmark/bin/vortex_bench report \
  --summary-file .artifacts/benchmarks/results/<run>-summary.json \
  --output-dir .artifacts/benchmarks
```

Multiple summaries may be passed with repeated `--summary-file`.

Before/after reports are also offline and match only identical workload
signatures:

```bash
bash vortex-benchmark/bin/vortex_bench report \
  --baseline-summary-file .artifacts/benchmarks/baseline/results/<run>-summary.json \
  --candidate-summary-file .artifacts/benchmarks/candidate/results/<run>-summary.json \
  --output-dir .artifacts/benchmarks/diff
```

## CI Shape

CI should split work into independent jobs:

1. smoke
2. quick benchmark
3. repeat benchmark
4. report-only validation
5. artifact upload
6. optional publication

Local and host-port target modes should be exercised in regular CI. SSH target modes should run as manual or nightly jobs when infrastructure is available.
The repository CI now has separate smoke, quick-benchmark, report-only, and manual repeat-benchmark jobs.

Example quick benchmark:

```bash
bash vortex-benchmark/bin/vortex_bench run \
  --workload-manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml \
  --target-mode local \
  --artifact-root .artifacts/benchmarks/ci-quick \
  --profile quick \
  --json \
  --no-color
```

Example report validation:

```bash
bash vortex-benchmark/bin/vortex_bench report \
  --results-dir .artifacts/benchmarks/ci-quick/results \
  --output-dir .artifacts/benchmarks/ci-quick
```

## Publication Contract

Dashboards and Pages jobs should consume:

- `reports/latest/report.json` for the newest report payload
- `reports/latest/report.md` for human review
- `reports/index.json` for report history traversal

Do not parse raw backend stdout for CI decisions. Use `report.json` or the normalized summary files.
