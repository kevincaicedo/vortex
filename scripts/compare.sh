#!/usr/bin/env bash
# scripts/compare.sh — Benchmark VortexDB against Redis, Dragonfly, and Valkey
#
# Starts each server in a Docker container, runs identical redis-benchmark
# workloads, and outputs a comparison table.
#
# Prerequisites:
#   - docker
#   - redis-benchmark (redis-tools package)
#
# Usage:
#   ./scripts/compare.sh [-n requests] [-c clients] [-P pipeline] [-t tests]
#
# Example:
#   ./scripts/compare.sh -n 500000 -c 100 -P 16
#   ./scripts/compare.sh -t PING,SET,GET,INCR,MSET

set -euo pipefail

# ── Defaults ──
REQUESTS=100000
CLIENTS=50
PIPELINE=16
TESTS="PING,SET,GET,INCR,MSET"
PORT_BASE=16379
RESULTS_DIR="$(mktemp -d)"
VORTEX_LOG=""

while getopts "n:c:P:t:" opt; do
    case $opt in
        n) REQUESTS=$OPTARG ;;
        c) CLIENTS=$OPTARG ;;
        P) PIPELINE=$OPTARG ;;
        t) TESTS=$OPTARG ;;
        *) echo "Usage: $0 [-n requests] [-c clients] [-P pipeline] [-t tests]"; exit 1 ;;
    esac
done

cleanup() {
    echo "Cleaning up containers and processes..."
    docker rm -f vortex-bench-redis vortex-bench-dragonfly vortex-bench-valkey 2>/dev/null || true
    if [ -n "${VORTEX_PID:-}" ]; then
        kill "$VORTEX_PID" 2>/dev/null || true
        wait "$VORTEX_PID" 2>/dev/null || true
    fi
    rm -rf "$RESULTS_DIR"
}
trap cleanup EXIT

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          VortexDB Comparison Benchmark Suite                ║"
echo "║  Requests: $REQUESTS  Clients: $CLIENTS  Pipeline: $PIPELINE"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "Benchmark tests: $TESTS"
echo ""

wait_for_server() {
    local name=$1
    local port=$2
    local attempts=${3:-30}

    for ((i = 1; i <= attempts; i++)); do
        if redis-benchmark -h 127.0.0.1 -p "$port" -n 1 -t PING -q >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    echo "WARNING: $name on port $port is not responding after ${attempts}s"
    return 1
}

# ── Start containers ──
declare -A SERVERS
SERVERS=(
    [redis]="redis:8-alpine"
    [dragonfly]="docker.dragonflydb.io/dragonflydb/dragonfly:latest"
    [valkey]="valkey/valkey:9-alpine"
)

declare -A PORTS
PORT=$PORT_BASE

echo "Building VortexDB release binary..."
cargo build --release --bin vortex-server

echo "Starting VortexDB..."
VORTEX_LOG="$RESULTS_DIR/vortex.log"
./target/release/vortex-server --bind "127.0.0.1:$PORT" >"$VORTEX_LOG" 2>&1 &
VORTEX_PID=$!
PORTS[vortex]=$PORT
PORT=$((PORT + 1))

for name in redis dragonfly valkey; do
    image="${SERVERS[$name]}"
    container="vortex-bench-$name"

    echo "Starting $name ($image) on port $PORT..."
    docker run -d --rm \
        --name "$container" \
        -p "$PORT:6379" \
        --memory=2g \
        --cpus=2 \
        "$image" >/dev/null

    PORTS[$name]=$PORT
    PORT=$((PORT + 1))
done

# Wait for servers to be ready
echo "Waiting for servers to start..."
for name in vortex redis dragonfly valkey; do
    port="${PORTS[$name]}"
    if ! wait_for_server "$name" "$port"; then
        if [ "$name" = "vortex" ] && [ -f "$VORTEX_LOG" ]; then
            echo "Vortex startup log:"
            tail -n 50 "$VORTEX_LOG"
        fi
    fi
done

# ── Run benchmarks ──
run_benchmark() {
    local name=$1
    local port=$2
    local output="$RESULTS_DIR/$name.txt"
    local error_output="$RESULTS_DIR/$name.err"

    echo "Benchmarking $name (port $port)..."
    if ! redis-benchmark \
        -h 127.0.0.1 \
        -p "$port" \
        -n "$REQUESTS" \
        -c "$CLIENTS" \
        -P "$PIPELINE" \
        -t "$TESTS" \
        --csv \
        > "$output" 2> "$error_output"; then
        echo "WARNING: benchmark failed for $name; stderr saved to $error_output"
        : > "$output"
        return 1
    fi
}

collect_result_rows() {
    awk -F',' '
        FNR == 1 { next }
        {
            label = $1
            gsub(/"/, "", label)
            if (label != "" && !seen[label]++) {
                print label
            }
        }
    ' "$RESULTS_DIR"/vortex.txt "$RESULTS_DIR"/redis.txt "$RESULTS_DIR"/dragonfly.txt "$RESULTS_DIR"/valkey.txt
}

extract_rps() {
    local file=$1
    local label=$2
    local value

    value=$(grep -F -m1 "\"$label\"" "$file" 2>/dev/null | cut -d',' -f2 | tr -d '"' || true)
    if [ -n "$value" ]; then
        echo "$value"
    else
        echo "N/A"
    fi
}

for name in vortex redis dragonfly valkey; do
    run_benchmark "$name" "${PORTS[$name]}" || true
done

# ── Print comparison table ──
echo ""
echo "╔═════════════════════════════════════════════════════════════════════════════════════════╗"
echo "║                                Results (requests/sec)                                   ║"
echo "╠════════════════╦═══════════════╦═══════════════╦═══════════════╦════════════════════════╣"
printf "║ %-14s ║ %-13s ║ %-13s ║ %-13s ║ %-22s ║\n" "Test" "Vortex" "Redis" "Dragonfly" "Valkey"
echo "╠════════════════╬═══════════════╬═══════════════╬═══════════════╬════════════════════════╣"

# Parse CSV output and build table using the actual benchmark labels.
mapfile -t RESULT_ROWS < <(collect_result_rows)
for test in "${RESULT_ROWS[@]}"; do
    vortex_rps=$(extract_rps "$RESULTS_DIR/vortex.txt" "$test")
    redis_rps=$(extract_rps "$RESULTS_DIR/redis.txt" "$test")
    dragonfly_rps=$(extract_rps "$RESULTS_DIR/dragonfly.txt" "$test")
    valkey_rps=$(extract_rps "$RESULTS_DIR/valkey.txt" "$test")

    printf "║ %-14s ║ %13s ║ %13s ║ %13s ║ %22s ║\n" "$test" "$vortex_rps" "$redis_rps" "$dragonfly_rps" "$valkey_rps"
done

echo "╚════════════════╩═══════════════╩═══════════════╩═══════════════╩════════════════════════╝"
echo ""
echo "Results saved to: $RESULTS_DIR/"
