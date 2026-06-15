# Usage

`just benchmark` is the primary benchmark command. It can set up a managed target, attach to an existing target, execute workloads, collect telemetry, render reports, and tear down managed services from one command. Run `just benchmark --help` for the full operator help page with every option and examples.

## Quick Start

```bash
just benchmark \
  --workload-manifest vortex-benchmark/manifests/examples/local-native-full-cycle.yaml \
  --target-mode local \
  --artifact-root .artifacts/benchmarks/local-dev \
  --profile engineering
```

Dry-run the same shape first:

```bash
just benchmark \
  --workload-manifest vortex-benchmark/manifests/examples/local-native-full-cycle.yaml \
  --target-mode local \
  --artifact-root .artifacts/benchmarks/local-dev \
  --dry-run \
  --explain
```

## Target Modes

| Mode | Example |
|------|---------|
| Local managed | `--target-mode local --db vortex,redis --native` |
| Docker managed | `--target-mode local --db vortex,redis,dragonfly,valkey --container` |
| Existing endpoint | `--target-mode host-port --db vortex --target-url 127.0.0.1:16379` |
| SSH managed | `--target-mode ssh-managed --ssh-target perfbox --ssh-start-command '...' --target-url perfbox:16379` |
| SSH attach | `--target-mode ssh-attach --ssh-target perfbox --target-url perfbox:16379` |

Attach modes mark service ownership as external and do not stop or reconfigure the endpoint.

## Common Examples

Native command sweep:

```bash
just benchmark \
  --db vortex,redis \
  --native \
  --backend redis-benchmark \
  --command SET,GET,INCR \
  --duration 30s \
  --artifact-root .artifacts/benchmarks/native
```

Docker comparison:

```bash
just benchmark \
  --db vortex,redis,dragonfly,valkey \
  --container \
  --backend memtier_benchmark \
  --workload uniform-mixed \
  --duration 30s \
  --artifact-root .artifacts/benchmarks/docker
```

Host-port attach:

```bash
just benchmark \
  --db vortex \
  --target-mode host-port \
  --target-url 127.0.0.1:16379 \
  --backend redis-benchmark \
  --command PING,GET,SET \
  --artifact-root .artifacts/benchmarks/attach
```

Remote managed:

```bash
just benchmark \
  --db vortex \
  --target-mode ssh-managed \
  --ssh-target perfbox \
  --ssh-workdir /srv/vortex \
  --ssh-start-command 'cd /srv/vortex && ./vortex-server --bind 0.0.0.0:16379 --threads 4' \
  --ssh-stop-command 'pkill -INT vortex-server' \
  --ssh-artifact-path /srv/vortex/.artifacts \
  --target-url perfbox:16379 \
  --backend redis-benchmark \
  --command PING \
  --artifact-root .artifacts/benchmarks/remote
```

Remote load host:

```bash
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
```

Remote benchmark artifacts copied back over SSH are stored under `<artifact-root>/remote/<timestamp>/`, and the resolved `plan.md` records the exact return path.
Use `--ssh-port`, `--ssh-identity-file`, `--ssh-config`, `--ssh-option`, and
`--ssh-connect-timeout` for non-default SSH transport. The options are applied
consistently to remote commands, source copy, remote load delegation, and
artifact return.

## Profiles

| Profile | Behavior |
|---------|----------|
| `quick` | Fast exploratory run; repeat defaults to 1 |
| `engineering` | Default development metadata |
| `citation` | Repeat defaults to 3 and records citation-grade validity metadata |
| `diagnostic` | Engineering metadata for a benchmark paired with profiler evidence |

CLI flags still override manifest values.

## Progress And Output

```bash
--json       # JSON-lines progress stream
--no-color   # uncolored stable console output
--explain    # write plan.md
--dry-run    # write session/preflight/plan only
--no-report  # run workload but skip report rendering
```

Interactive terminals use one live progress line with spinner, progress bar,
step count, and ETA. Log/CI output is stable and row-oriented. Report rendering
prints a compact result table to the console and writes full Markdown, JSON,
and CSV artifacts under `reports/latest/`.

## Artifact Layout

The default root is `.artifacts/benchmarks/`.

| Path | Purpose |
|------|---------|
| `sessions/<target-mode>/<timestamp>/session.json` | Common session metadata |
| `sessions/<target-mode>/<timestamp>/preflight.json` | Tool and platform preflight |
| `sessions/<target-mode>/<timestamp>/plan.md` | Resolved execution plan |
| `environments/` | Managed or attached service state |
| `requests/` | Resolved benchmark request |
| `results/` | Normalized run summaries |
| `backend-runs/` | Raw backend output |
| `reports/` | Timestamped JSON/CSV/Markdown reports |
| `reports/latest/` | Stable report copies |
| `runtime/` | Runtime files such as AOF output |

## Expert Subcommands

The old split flow remains for debugging:

```bash
just benchmark setup ...
just benchmark run --state-file .artifacts/benchmarks/environments/<state>.json ...
just benchmark report --summary-file .artifacts/benchmarks/results/<run>-summary.json
just benchmark report \
  --baseline-summary-file .artifacts/benchmarks/baseline/results/<run>-summary.json \
  --candidate-summary-file .artifacts/benchmarks/candidate/results/<run>-summary.json \
  --output-dir .artifacts/benchmarks/diff
just benchmark teardown --state-file .artifacts/benchmarks/environments/<state>.json
```
