# vortex_benchmark

`vortex_benchmark` is the system benchmark toolchain for Vortex and compatible RESP databases. It is artifact-first, target-mode aware, and report-neutral: it measures runs, preserves context, and leaves product policy outside the tool.

## Primary Command

```bash
just benchmark \
  --workload-manifest vortex-benchmark/manifests/examples/local-native-full-cycle.yaml \
  --target-mode local \
  --artifact-root .artifacts/benchmarks/local-dev \
  --profile engineering
```

Run `just benchmark --help` for the full option reference and examples. The
command can set up, preflight, execute, report, publish latest report copies,
and tear down managed services. Expert subcommands remain available:

```bash
just benchmark setup
just benchmark attach
just benchmark run --state-file ...
just benchmark report
just benchmark teardown
```

## Target Modes

| Mode | Description |
|------|-------------|
| `local` | Start local native or Docker services and tear them down |
| `host-port` | Attach to an existing RESP endpoint without mutating it |
| `ssh-managed` | Run explicit SSH start/stop commands and collect remote metadata |
| `ssh-attach` | Attach to an existing remote RESP endpoint without mutating it |

Native mode supports Vortex and Redis. Container mode supports Vortex, Redis, Dragonfly, and Valkey.

## Profiles

| Profile | Purpose |
|---------|---------|
| `quick` | Fast exploratory run |
| `engineering` | Default development measurement |
| `citation` | Repeated run metadata for stronger publication workflows |
| `diagnostic` | Measurement paired with profiler or telemetry context |

## Output

Artifacts default to `.artifacts/benchmarks/`:

- `sessions/<target-mode>/<timestamp>/session.json`
- `sessions/<target-mode>/<timestamp>/preflight.json`
- `sessions/<target-mode>/<timestamp>/plan.md`
- `environments/`
- `requests/`
- `results/`
- `backend-runs/`
- `reports/`
- `reports/latest/`
- `runtime/`

Interactive terminals use one live progress line with spinner, step count, and
ETA. CI/log streams use stable line-oriented progress, and `--json` emits
machine-readable progress events. Report generation prints a compact result
table and writes full Markdown, JSON, and CSV artifacts.

## Backends

- `redis-benchmark` for point-command sweeps
- `memtier_benchmark` for mixed single-key workloads
- `custom-rust` for deterministic multi-key and transactional workloads

## Documentation

- [docs/usage.md](docs/usage.md)
- [docs/manifests-and-workloads.md](docs/manifests-and-workloads.md)
- [docs/support-matrix.md](docs/support-matrix.md)
- [docs/reporting-and-ci.md](docs/reporting-and-ci.md)
- [docs/profiling-workflow.md](docs/profiling-workflow.md)
- [docs/extending.md](docs/extending.md)

## Development Notes

The Python package lives under `vortex-benchmark/python/vortex_benchmark/`; `bin/vortex_bench` sets `PYTHONPATH` for in-tree use. Keep new outputs under the artifact root and keep report generation offline-capable from existing summary files.
