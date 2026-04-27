# Manifests And Workloads

## Manifest Schema

Benchmark manifests support JSON and YAML and currently accept these top-level sections:

| Section | Purpose |
|---------|---------|
| `schema_version` | Required manifest schema version. Current value: `1`. |
| `name` | Short scenario identifier. |
| `description` | Human-readable scenario description. |
| `databases` | One or more database targets. |
| `workloads` | One or more named workloads from the built-in catalog. |
| `commands` | Explicit point commands for `redis-benchmark`. |
| `command_groups` | High-level command bundles such as `strings`, `keys`, `server`, and `transactions`. |
| `backends` | One or more backends such as `redis-benchmark`, `memtier_benchmark`, or `custom-rust`. |
| `repeat` | Replicate count for every selected scenario when CLI `--repeat` is not provided. |
| `duration` | Duration literal such as `20s`, `5m`, or `1h`. |
| `environment` | Mode, state-file, output-root, port-base, and build behavior. |
| `resource_config` | CPU, memory, and thread allocation hints for service startup. |
| `runtime_config` | AOF, fsync, maxmemory, and eviction-policy startup settings. |
| `settings` | Backend-specific benchmark settings. |

## Runtime Config

`runtime_config` is the explicit setup-time policy surface for persistence and eviction scenarios.

Supported keys:

- `aof_enabled`
- `aof_fsync`
- `maxmemory`
- `eviction_policy`

Example:

```yaml
runtime_config:
  aof_enabled: true
  aof_fsync: everysec
  maxmemory: 64mb
  eviction_policy: allkeys-lru
```

These settings are applied during `setup`. They are not intended to be changed later with ad hoc `CONFIG SET` calls during validation.

## Example Scenario Files

The repository now ships CI-oriented example manifests:

- `manifests/examples/ci-regression-native.yaml`
- `manifests/examples/aof-everysec-native.yaml`
- `manifests/examples/eviction-allkeys-lru-native.yaml`
- `manifests/examples/local-native-redis-benchmark-repeat.yaml`

The intended workflow for a new scenario file is:

1. Copy the closest example under `manifests/examples/`.
2. Pick the databases and execution mode.
3. Choose either `commands` or `workloads` or both.
4. Add `repeat` when the scenario should carry its own replicate count.
5. Pin backend-specific settings in `settings`.
6. Add `runtime_config` if the scenario depends on AOF, maxmemory, or eviction.

Example repeat-aware snippet:

```yaml
schema_version: 1
name: citation-read-heavy
databases: [vortex, redis]
backends: [redis-benchmark]
commands: [GET, SET]
repeat: 3
duration: 5s
environment:
  mode: native
```

CLI `--repeat` overrides manifest `repeat`. When neither is set, the resolved benchmark spec defaults to `1`.

## Quick Scenario File Versus Built-In Workload Definitions

There are two different concepts in the current tool:

- Scenario files: manifest files under `manifests/` that describe one benchmark run.
- Built-in workload definitions: named workload catalog entries in `python/vortex_benchmark/catalog.py`.

The harness does not currently load standalone workload-definition files from disk. If you need a new named workload family, add it to `python/vortex_benchmark/catalog.py` and then create a manifest file that uses it.

## Adding A New Named Workload

1. Add a new `WorkloadDefinition` entry in `python/vortex_benchmark/catalog.py`.
2. Set the workload metadata correctly: distribution, key pattern, read ratio, write ratio, `multi_key`, `transactional`, and `hot_key`.
3. Choose default commands and command groups that match the intended semantics.
4. Add at least one manifest under `manifests/examples/` that exercises the new workload.
5. Use `memtier_benchmark` for non-transactional single-key workloads and `custom-rust` for multi-key or transactional workloads.