#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT}/.artifacts/memory-attribution/latest"
KEYS=100000
VALUE_SIZE=16
SHARDS=64
TTL_MS=60000
MULTI_KEY_WIDTH=16
LATENCY_SAMPLE_RATE=64
SKIP_BUILD=false

usage() {
    cat <<'EOF'
Usage: scripts/memory-attribution-matrix.sh [options]

Options:
  --out-dir PATH             Artifact directory (default: .artifacts/memory-attribution/latest)
  --keys N                   Key count for engine rows (default: 100000)
  --value-size N             Value size in bytes (default: 16)
  --shards N                 Engine shard count, power of two (default: 64)
  --ttl-ms N                 Resident TTL duration for TTL row (default: 60000)
  --multi-key-width N        MSET/MGET batch width where used (default: 16)
  --latency-sample-rate N    Probe latency sample rate (default: 64)
  --skip-build               Reuse target/release/examples/engine_probe
  --help                     Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --out-dir)
            OUT_DIR="$2"
            shift 2
            ;;
        --keys)
            KEYS="$2"
            shift 2
            ;;
        --value-size)
            VALUE_SIZE="$2"
            shift 2
            ;;
        --shards)
            SHARDS="$2"
            shift 2
            ;;
        --ttl-ms)
            TTL_MS="$2"
            shift 2
            ;;
        --multi-key-width)
            MULTI_KEY_WIDTH="$2"
            shift 2
            ;;
        --latency-sample-rate)
            LATENCY_SAMPLE_RATE="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

mkdir -p "${OUT_DIR}/engine"
OUT_DIR="$(cd "${OUT_DIR}" && pwd)"

if [[ "${SKIP_BUILD}" != true ]]; then
    cargo build -p vortex-engine --release --features profiling-tools --example engine_probe
fi

ENGINE_BIN="${ROOT}/target/release/examples/engine_probe"
if [[ ! -x "${ENGINE_BIN}" ]]; then
    echo "missing engine probe binary: ${ENGINE_BIN}" >&2
    echo "run without --skip-build, or build the engine_probe example first" >&2
    exit 1
fi

COMMON_ARGS=(
    --keys "${KEYS}"
    --value-size "${VALUE_SIZE}"
    --shards "${SHARDS}"
    --latency-sample-rate "${LATENCY_SAMPLE_RATE}"
)

run_engine_row() {
    local row_id="$1"
    shift
    local json_path="${OUT_DIR}/engine/${row_id}.json"
    local stdout_path="${OUT_DIR}/engine/${row_id}.stdout.json"

    echo "running ${row_id}"
    "${ENGINE_BIN}" "$@" --json "${json_path}" >"${stdout_path}"
}

run_engine_row engine-storage-no-features \
    --workload set-inline-string \
    "${COMMON_ARGS[@]}"

run_engine_row engine-command-no-features \
    --workload set-inline-string-command \
    "${COMMON_ARGS[@]}"

run_engine_row engine-ttl \
    --workload set-inline-string-ttl \
    --ttl-ms "${TTL_MS}" \
    "${COMMON_ARGS[@]}"

run_engine_row engine-maxmemory-lru \
    --workload eviction-headroom \
    --eviction-policy all-keys-lru \
    "${COMMON_ARGS[@]}"

run_engine_row engine-maxmemory-lfu \
    --workload eviction-headroom \
    --eviction-policy all-keys-lfu \
    "${COMMON_ARGS[@]}"

run_engine_row engine-aof-lsn \
    --workload set-inline-string-command \
    --aof-recording \
    "${COMMON_ARGS[@]}"

python3 - "${OUT_DIR}" "${ROOT}" "${KEYS}" "${VALUE_SIZE}" "${SHARDS}" "${TTL_MS}" "${MULTI_KEY_WIDTH}" <<'PY'
import csv
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
root = Path(sys.argv[2])
keys = sys.argv[3]
value_size = sys.argv[4]
shards = sys.argv[5]
ttl_ms = sys.argv[6]
multi_key_width = sys.argv[7]

engine_dir = out_dir / "engine"
fields = [
    "row_id",
    "layer",
    "status",
    "workload",
    "driver",
    "aof_recording",
    "logical_dataset_bytes",
    "table_allocated_bytes",
    "allocator_allocated_bytes",
    "allocator_resident_bytes",
    "process_rss_bytes",
    "bytes_per_live_key",
    "throughput_ops_per_second",
    "p99_ns",
    "shards",
    "table_total_slots",
    "capacity_slack_slots",
    "live_keys",
    "expiring_keys",
    "command",
    "notes",
]

engine_commands = {
    "engine-storage-no-features": f"target/release/examples/engine_probe --workload set-inline-string --keys {keys} --value-size {value_size} --shards {shards}",
    "engine-command-no-features": f"target/release/examples/engine_probe --workload set-inline-string-command --keys {keys} --value-size {value_size} --shards {shards}",
    "engine-ttl": f"target/release/examples/engine_probe --workload set-inline-string-ttl --ttl-ms {ttl_ms} --keys {keys} --value-size {value_size} --shards {shards}",
    "engine-maxmemory-lru": f"target/release/examples/engine_probe --workload eviction-headroom --eviction-policy all-keys-lru --keys {keys} --value-size {value_size} --shards {shards}",
    "engine-maxmemory-lfu": f"target/release/examples/engine_probe --workload eviction-headroom --eviction-policy all-keys-lfu --keys {keys} --value-size {value_size} --shards {shards}",
    "engine-aof-lsn": f"target/release/examples/engine_probe --workload set-inline-string-command --aof-recording --keys {keys} --value-size {value_size} --shards {shards}",
}

row_notes = {
    "engine-storage-no-features": "Direct table/keyspace storage path; excludes RESP parsing and command mutation coordination.",
    "engine-command-no-features": "In-process RESP parse plus command mutation path; no IO, WATCH, AOF, or maxmemory.",
    "engine-ttl": "Resident TTL metadata row; keys are not expired before measurement.",
    "engine-maxmemory-lru": "Maxmemory enabled with all-keys-lru and enough headroom for admission.",
    "engine-maxmemory-lfu": "Maxmemory enabled with all-keys-lfu and enough headroom for admission.",
    "engine-aof-lsn": "AOF recording feature enabled for command LSN stamping; no disk writer is started.",
}

rows = []
for json_path in sorted(engine_dir.glob("*.json")):
    if json_path.name.endswith(".stdout.json"):
        continue
    row_id = json_path.stem
    with json_path.open() as handle:
        data = json.load(handle)
    latency = data.get("latency_ns") or {}
    rows.append({
        "row_id": row_id,
        "layer": "engine",
        "status": "measured",
        "workload": data.get("workload"),
        "driver": data.get("driver"),
        "aof_recording": data.get("aof_recording"),
        "logical_dataset_bytes": data.get("table_logical_bytes"),
        "table_allocated_bytes": data.get("table_allocated_bytes"),
        "allocator_allocated_bytes": data.get("allocator_allocated_bytes"),
        "allocator_resident_bytes": data.get("allocator_resident_bytes"),
        "process_rss_bytes": data.get("process_rss_bytes"),
        "bytes_per_live_key": data.get("bytes_per_live_key"),
        "throughput_ops_per_second": data.get("throughput_ops_per_second"),
        "p99_ns": latency.get("p99"),
        "shards": data.get("shards"),
        "table_total_slots": data.get("table_total_slots"),
        "capacity_slack_slots": data.get("capacity_slack_slots"),
        "live_keys": data.get("live_keys"),
        "expiring_keys": data.get("expiring_keys"),
        "command": engine_commands.get(row_id, ""),
        "notes": row_notes.get(row_id, ""),
    })

external_rows = [
    {
        "row_id": "vortex-server-one-thread-polling",
        "layer": "server",
        "status": "external-manual",
        "command": "just profiler --memory --io-backend polling --threads 1 --command SET,GET,MGET,MSET --duration 30",
        "notes": "Measures vortex-server, parser, IO backend, runtime, allocator, and engine together; fill metrics from profiler summary.",
    },
    {
        "row_id": "vortex-server-multi-thread-polling",
        "layer": "server",
        "status": "external-manual",
        "command": "just profiler --memory --io-backend polling --threads 4 --command SET,GET,MGET,MSET --duration 30",
        "notes": "Same as one-thread row with reactor/thread scaling overhead included.",
    },
    {
        "row_id": "vortex-server-redis-comparison",
        "layer": "server",
        "status": "external-manual",
        "command": "just benchmark-local --manifest vortex-benchmark/manifests/examples/local-native-full-cycle.yaml",
        "notes": "Use the full local comparison manifest for server-level throughput/RSS evidence.",
    },
    {
        "row_id": "redis-baseline-equivalent",
        "layer": "redis",
        "status": "external-manual",
        "command": "just benchmark-local --manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark-polling.yaml",
        "notes": "Redis baseline must use equivalent key count, value size, command mix, and persistence setting.",
    },
]

for row in external_rows:
    full = {field: "" for field in fields}
    full.update(row)
    rows.append(full)

csv_path = out_dir / "matrix.csv"
with csv_path.open("w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fields})

json_path = out_dir / "matrix.json"
with json_path.open("w") as handle:
    json.dump({"rows": rows}, handle, indent=2, sort_keys=True)
    handle.write("\n")

plan_path = out_dir / "matrix-plan.md"
with plan_path.open("w") as handle:
    handle.write("# Vortex Memory Attribution Matrix\n\n")
    handle.write(f"- Keys: {keys}\n")
    handle.write(f"- Value size: {value_size} bytes\n")
    handle.write(f"- Shards: {shards}\n")
    handle.write(f"- Resident TTL row: {ttl_ms} ms\n")
    handle.write(f"- Multi-key width hint: {multi_key_width}\n\n")
    handle.write("## Measured Engine Rows\n\n")
    for row in rows:
        if row.get("status") == "measured":
            handle.write(f"- `{row['row_id']}`: `{row['command']}`\n")
    handle.write("\n## External Rows To Fill From Server/Redis Runs\n\n")
    for row in rows:
        if row.get("status") == "external-manual":
            handle.write(f"- `{row['row_id']}`: `{row['command']}`\n")
            handle.write(f"  Notes: {row['notes']}\n")
    handle.write("\n## Metric Contract\n\n")
    handle.write("Every completed row should populate logical dataset bytes, table allocated bytes when applicable, allocator allocated/resident bytes, process RSS, bytes per live key, throughput, p99, shard count, and capacity slack.\n")

print(f"wrote {csv_path.relative_to(root)}")
print(f"wrote {json_path.relative_to(root)}")
print(f"wrote {plan_path.relative_to(root)}")
PY
