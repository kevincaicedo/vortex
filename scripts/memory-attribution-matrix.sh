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
RUN_SHARD_SWEEP=true
SHARD_SWEEP_VALUES=(1024 4096 16384 65536)
ENGINE_BYTES_PER_KEY_ALLOWED=192
ENGINE_BYTES_PER_KEY_NARROWED=256
FULL_SERVER_RSS_ALLOWED_RATIO=1.50
FULL_SERVER_RSS_NARROWED_RATIO=2.00

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
  --no-shard-sweep           Skip 1K/4K/16K/64K shard-count attribution rows
  --engine-bpk-allowed N     Engine bytes/live-key allowed threshold (default: 192)
  --engine-bpk-narrowed N    Engine bytes/live-key narrowed threshold (default: 256)
  --full-rss-allowed-ratio N Full-server RSS/Redis allowed ratio (default: 1.50)
  --full-rss-narrowed-ratio N Full-server RSS/Redis narrowed ratio (default: 2.00)
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
        --no-shard-sweep)
            RUN_SHARD_SWEEP=false
            shift
            ;;
        --engine-bpk-allowed)
            ENGINE_BYTES_PER_KEY_ALLOWED="$2"
            shift 2
            ;;
        --engine-bpk-narrowed)
            ENGINE_BYTES_PER_KEY_NARROWED="$2"
            shift 2
            ;;
        --full-rss-allowed-ratio)
            FULL_SERVER_RSS_ALLOWED_RATIO="$2"
            shift 2
            ;;
        --full-rss-narrowed-ratio)
            FULL_SERVER_RSS_NARROWED_RATIO="$2"
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

COMMON_ARGS_BASE=(
    --keys "${KEYS}"
    --value-size "${VALUE_SIZE}"
    --latency-sample-rate "${LATENCY_SAMPLE_RATE}"
)
COMMON_ARGS=("${COMMON_ARGS_BASE[@]}" --shards "${SHARDS}")

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

if [[ "${RUN_SHARD_SWEEP}" == true ]]; then
    for sweep_shards in "${SHARD_SWEEP_VALUES[@]}"; do
        run_engine_row "engine-shard-sweep-${sweep_shards}" \
            --workload set-inline-string \
            "${COMMON_ARGS_BASE[@]}" \
            --shards "${sweep_shards}"
    done
fi

python3 - "${OUT_DIR}" "${ROOT}" "${KEYS}" "${VALUE_SIZE}" "${SHARDS}" "${TTL_MS}" "${MULTI_KEY_WIDTH}" "${ENGINE_BYTES_PER_KEY_ALLOWED}" "${ENGINE_BYTES_PER_KEY_NARROWED}" "${FULL_SERVER_RSS_ALLOWED_RATIO}" "${FULL_SERVER_RSS_NARROWED_RATIO}" "${RUN_SHARD_SWEEP}" <<'PY'
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
engine_bpk_allowed = float(sys.argv[8])
engine_bpk_narrowed = float(sys.argv[9])
full_rss_allowed_ratio = float(sys.argv[10])
full_rss_narrowed_ratio = float(sys.argv[11])
run_shard_sweep = sys.argv[12].lower() == "true"

engine_dir = out_dir / "engine"
fields = [
    "row_id",
    "layer",
    "status",
    "workload",
    "driver",
    "aof_recording",
    "engine_scope",
    "full_server_scope",
    "logical_dataset_bytes",
    "engine_logical_dataset_bytes",
    "table_allocated_bytes",
    "table_total_slots",
    "capacity_slack_slots",
    "tombstone_slots",
    "load_factor",
    "live_keys",
    "expiring_keys",
    "bytes_per_live_key",
    "io_fixed_buffer_reserved_bytes",
    "io_fixed_buffer_committed_bytes",
    "per_connection_state_bytes",
    "allocator_allocated_bytes",
    "allocator_active_bytes",
    "allocator_resident_bytes",
    "allocator_mapped_bytes",
    "allocator_retained_bytes",
    "process_rss_bytes",
    "throughput_ops_per_second",
    "p99_ns",
    "shards",
    "engine_memory_claim_decision",
    "engine_memory_claim_reason",
    "full_server_rss_claim_decision",
    "full_server_rss_claim_reason",
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

if run_shard_sweep:
    for sweep_shards in (1024, 4096, 16384, 65536):
        engine_commands[f"engine-shard-sweep-{sweep_shards}"] = (
            "target/release/examples/engine_probe "
            f"--workload set-inline-string --keys {keys} --value-size {value_size} --shards {sweep_shards}"
        )

row_notes = {
    "engine-storage-no-features": "Direct table/keyspace storage path; excludes RESP parsing and command mutation coordination.",
    "engine-command-no-features": "In-process RESP parse plus command mutation path; no IO, WATCH, AOF, or maxmemory.",
    "engine-ttl": "Resident TTL metadata row; keys are not expired before measurement.",
    "engine-maxmemory-lru": "Maxmemory enabled with all-keys-lru and enough headroom for admission.",
    "engine-maxmemory-lfu": "Maxmemory enabled with all-keys-lfu and enough headroom for admission.",
    "engine-aof-lsn": "AOF recording feature enabled for command LSN stamping; no disk writer is started.",
}

if run_shard_sweep:
    for sweep_shards in (1024, 4096, 16384, 65536):
        row_notes[f"engine-shard-sweep-{sweep_shards}"] = (
            "Shard-count sweep row for per-shard metadata and table slack cost; "
            "engine-only attribution, not a full-server Redis parity claim."
        )


def engine_decision(bytes_per_live_key):
    if bytes_per_live_key is None:
        return "Rejected", "missing engine bytes/live-key"
    value = float(bytes_per_live_key)
    if value <= engine_bpk_allowed:
        return "Allowed", f"engine bytes/live-key {value:.2f} <= {engine_bpk_allowed:.2f}"
    if value <= engine_bpk_narrowed:
        return "Narrowed", (
            f"engine bytes/live-key {value:.2f} <= {engine_bpk_narrowed:.2f}; "
            "claim must name workload and shard count"
        )
    return "Rejected", f"engine bytes/live-key {value:.2f} > {engine_bpk_narrowed:.2f}"


def full_server_decision_for_engine_row():
    return "Rejected", "engine-only row cannot support full-server RSS or Redis parity claim"

rows = []
for json_path in sorted(engine_dir.glob("*.json")):
    if json_path.name.endswith(".stdout.json"):
        continue
    row_id = json_path.stem
    with json_path.open() as handle:
        data = json.load(handle)
    latency = data.get("latency_ns") or {}
    engine_claim, engine_reason = engine_decision(data.get("bytes_per_live_key"))
    full_claim, full_reason = full_server_decision_for_engine_row()
    rows.append({
        "row_id": row_id,
        "layer": "engine",
        "status": "measured",
        "workload": data.get("workload"),
        "driver": data.get("driver"),
        "aof_recording": data.get("aof_recording"),
        "engine_scope": "engine_only",
        "full_server_scope": "",
        "logical_dataset_bytes": data.get("table_logical_bytes"),
        "engine_logical_dataset_bytes": data.get("table_logical_bytes"),
        "table_allocated_bytes": data.get("table_allocated_bytes"),
        "table_total_slots": data.get("table_total_slots"),
        "capacity_slack_slots": data.get("capacity_slack_slots"),
        "tombstone_slots": data.get("tombstone_slots"),
        "load_factor": data.get("load_factor"),
        "live_keys": data.get("live_keys"),
        "expiring_keys": data.get("expiring_keys"),
        "bytes_per_live_key": data.get("bytes_per_live_key"),
        "io_fixed_buffer_reserved_bytes": "",
        "io_fixed_buffer_committed_bytes": "",
        "per_connection_state_bytes": "",
        "allocator_allocated_bytes": data.get("allocator_allocated_bytes"),
        "allocator_active_bytes": data.get("allocator_active_bytes"),
        "allocator_resident_bytes": data.get("allocator_resident_bytes"),
        "allocator_mapped_bytes": data.get("allocator_mapped_bytes"),
        "allocator_retained_bytes": data.get("allocator_retained_bytes"),
        "process_rss_bytes": data.get("process_rss_bytes"),
        "throughput_ops_per_second": data.get("throughput_ops_per_second"),
        "p99_ns": latency.get("p99"),
        "shards": data.get("shards"),
        "engine_memory_claim_decision": engine_claim,
        "engine_memory_claim_reason": engine_reason,
        "full_server_rss_claim_decision": full_claim,
        "full_server_rss_claim_reason": full_reason,
        "command": engine_commands.get(row_id, ""),
        "notes": row_notes.get(row_id, ""),
    })

external_rows = [
    {
        "row_id": "vortex-server-one-thread-polling",
        "layer": "server",
        "status": "external-manual",
        "engine_memory_claim_decision": "Rejected",
        "engine_memory_claim_reason": "server row not populated yet",
        "full_server_rss_claim_decision": "Rejected",
        "full_server_rss_claim_reason": "server RSS/Redis evidence not populated yet",
        "command": "just profiler --memory --io-backend polling --threads 1 --command SET,GET,MGET,MSET --duration 30",
        "notes": "Measures vortex-server, parser, IO backend, runtime, allocator, and engine together; fill metrics from profiler summary.",
    },
    {
        "row_id": "vortex-server-multi-thread-polling",
        "layer": "server",
        "status": "external-manual",
        "engine_memory_claim_decision": "Rejected",
        "engine_memory_claim_reason": "server row not populated yet",
        "full_server_rss_claim_decision": "Rejected",
        "full_server_rss_claim_reason": "server RSS/Redis evidence not populated yet",
        "command": "just profiler --memory --io-backend polling --threads 4 --command SET,GET,MGET,MSET --duration 30",
        "notes": "Same as one-thread row with reactor/thread scaling overhead included.",
    },
    {
        "row_id": "vortex-server-redis-comparison",
        "layer": "server",
        "status": "external-manual",
        "engine_memory_claim_decision": "Rejected",
        "engine_memory_claim_reason": "server row not populated yet",
        "full_server_rss_claim_decision": "Rejected",
        "full_server_rss_claim_reason": "server RSS/Redis evidence not populated yet",
        "command": "just benchmark-local --manifest vortex-benchmark/manifests/examples/local-native-full-cycle.yaml",
        "notes": "Use the full local comparison manifest for server-level throughput/RSS evidence.",
    },
    {
        "row_id": "redis-baseline-equivalent",
        "layer": "redis",
        "status": "external-manual",
        "engine_memory_claim_decision": "Rejected",
        "engine_memory_claim_reason": "Redis row has no Vortex engine attribution",
        "full_server_rss_claim_decision": "Rejected",
        "full_server_rss_claim_reason": "Redis baseline row must be paired with a Vortex server row",
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
    json.dump(
        {
            "thresholds": {
                "engine_bytes_per_live_key_allowed": engine_bpk_allowed,
                "engine_bytes_per_live_key_narrowed": engine_bpk_narrowed,
                "full_server_rss_vs_redis_allowed_ratio": full_rss_allowed_ratio,
                "full_server_rss_vs_redis_narrowed_ratio": full_rss_narrowed_ratio,
            },
            "rows": rows,
        },
        handle,
        indent=2,
        sort_keys=True,
    )
    handle.write("\n")

plan_path = out_dir / "matrix-plan.md"
with plan_path.open("w") as handle:
    handle.write("# Vortex Memory Attribution Matrix\n\n")
    handle.write(f"- Keys: {keys}\n")
    handle.write(f"- Value size: {value_size} bytes\n")
    handle.write(f"- Shards: {shards}\n")
    handle.write(f"- Resident TTL row: {ttl_ms} ms\n")
    handle.write(f"- Multi-key width hint: {multi_key_width}\n\n")
    handle.write("## Claim Thresholds\n\n")
    handle.write(f"- Engine bytes/live-key Allowed: <= {engine_bpk_allowed:.2f}\n")
    handle.write(f"- Engine bytes/live-key Narrowed: <= {engine_bpk_narrowed:.2f}\n")
    handle.write(f"- Full-server RSS/Redis Allowed ratio: <= {full_rss_allowed_ratio:.2f}\n")
    handle.write(f"- Full-server RSS/Redis Narrowed ratio: <= {full_rss_narrowed_ratio:.2f}\n\n")
    handle.write("## Measured Engine Rows\n\n")
    for row in rows:
        if row.get("status") == "measured":
            handle.write(f"- `{row['row_id']}`: `{row['command']}`\n")
            handle.write(
                f"  Engine claim: {row['engine_memory_claim_decision']} - "
                f"{row['engine_memory_claim_reason']}\n"
            )
            handle.write(
                f"  Full-server RSS claim: {row['full_server_rss_claim_decision']} - "
                f"{row['full_server_rss_claim_reason']}\n"
            )
    handle.write("\n## External Rows To Fill From Server/Redis Runs\n\n")
    for row in rows:
        if row.get("status") == "external-manual":
            handle.write(f"- `{row['row_id']}`: `{row['command']}`\n")
            handle.write(f"  Notes: {row['notes']}\n")
    handle.write("\n## Metric Contract\n\n")
    handle.write("Every completed row should populate logical dataset bytes, table allocated bytes, table total slots, capacity slack slots, tombstones, load factor, bytes per live key, allocator allocated/active/resident/mapped/retained bytes when available, process RSS, throughput, p99, shard count, and explicit Allowed/Narrowed/Rejected claim decisions.\n")
    handle.write("Engine-only rows must reject full-server RSS and Redis parity claims. Server rows must separate engine attribution, IO fixed-buffer reservation/commitment, per-connection state, allocator state, and process RSS before any Redis comparison is allowed.\n")

print(f"wrote {csv_path.relative_to(root)}")
print(f"wrote {json_path.relative_to(root)}")
print(f"wrote {plan_path.relative_to(root)}")
PY
