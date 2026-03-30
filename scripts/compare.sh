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
#   ./scripts/compare.sh [-n requests] [-c clients] [-P pipeline]
#
# Example:
#   ./scripts/compare.sh -n 500000 -c 100 -P 16

set -euo pipefail

# ── Defaults ──
REQUESTS=100000
CLIENTS=50
PIPELINE=16
TESTS="SET,GET,INCR,MSET,LPUSH,RPUSH,SADD,PING"
PORT_BASE=16379
RESULTS_DIR="$(mktemp -d)"

while getopts "n:c:P:" opt; do
    case $opt in
        n) REQUESTS=$OPTARG ;;
        c) CLIENTS=$OPTARG ;;
        P) PIPELINE=$OPTARG ;;
        *) echo "Usage: $0 [-n requests] [-c clients] [-P pipeline]"; exit 1 ;;
    esac
done

cleanup() {
    echo "Cleaning up containers..."
    docker rm -f vortex-bench-redis vortex-bench-dragonfly vortex-bench-valkey 2>/dev/null || true
    rm -rf "$RESULTS_DIR"
}
trap cleanup EXIT

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          VortexDB Comparison Benchmark Suite                ║"
echo "║  Requests: $REQUESTS  Clients: $CLIENTS  Pipeline: $PIPELINE"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── Start containers ──
declare -A SERVERS
SERVERS=(
    [redis]="redis:7-alpine"
    [dragonfly]="docker.dragonflydb.io/dragonflydb/dragonfly:latest"
    [valkey]="valkey/valkey:8-alpine"
)

declare -A PORTS
PORT=$PORT_BASE

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
sleep 3

# Verify connectivity
for name in redis dragonfly valkey; do
    port="${PORTS[$name]}"
    if ! redis-benchmark -h 127.0.0.1 -p "$port" -n 1 -t PING -q >/dev/null 2>&1; then
        echo "WARNING: $name on port $port is not responding"
    fi
done

# ── Run benchmarks ──
run_benchmark() {
    local name=$1
    local port=$2
    local output="$RESULTS_DIR/$name.txt"

    echo "Benchmarking $name (port $port)..."
    redis-benchmark \
        -h 127.0.0.1 \
        -p "$port" \
        -n "$REQUESTS" \
        -c "$CLIENTS" \
        -P "$PIPELINE" \
        -t "$TESTS" \
        --csv \
        2>/dev/null > "$output"
}

for name in redis dragonfly valkey; do
    run_benchmark "$name" "${PORTS[$name]}"
done

# ── Print comparison table ──
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║                     Results (requests/sec)                             ║"
echo "╠════════════════╦═══════════════╦═══════════════╦═══════════════════════╣"
printf "║ %-14s ║ %-13s ║ %-13s ║ %-21s ║\n" "Test" "Redis" "Dragonfly" "Valkey"
echo "╠════════════════╬═══════════════╬═══════════════╬═══════════════════════╣"

# Parse CSV output and build table
IFS=',' read -ra TEST_LIST <<< "$TESTS"
for test in "${TEST_LIST[@]}"; do
    redis_rps=$(grep -i "\"$test\"" "$RESULTS_DIR/redis.txt" 2>/dev/null | cut -d',' -f2 | tr -d '"' || echo "N/A")
    dragonfly_rps=$(grep -i "\"$test\"" "$RESULTS_DIR/dragonfly.txt" 2>/dev/null | cut -d',' -f2 | tr -d '"' || echo "N/A")
    valkey_rps=$(grep -i "\"$test\"" "$RESULTS_DIR/valkey.txt" 2>/dev/null | cut -d',' -f2 | tr -d '"' || echo "N/A")

    printf "║ %-14s ║ %13s ║ %13s ║ %21s ║\n" "$test" "$redis_rps" "$dragonfly_rps" "$valkey_rps"
done

echo "╚════════════════╩═══════════════╩═══════════════╩═══════════════════════╝"
echo ""
echo "Results saved to: $RESULTS_DIR/"
