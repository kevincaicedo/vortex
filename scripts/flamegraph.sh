#!/usr/bin/env bash
# scripts/flamegraph.sh — Generate CPU flamegraph for VortexDB
#
# Prerequisites:
#   cargo install flamegraph
#   # On Linux: install perf (linux-tools-common)
#   # On macOS: uses dtrace (requires SIP partially disabled)
#
# Usage:
#   ./scripts/flamegraph.sh [binary-args...]
#
# Examples:
#   ./scripts/flamegraph.sh                          # Profile default server
#   ./scripts/flamegraph.sh --port 6380 --threads 4  # Custom args

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="$ROOT_DIR/target/flamegraph"

mkdir -p "$OUTPUT_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SVG_FILE="$OUTPUT_DIR/vortex-$TIMESTAMP.svg"

echo "=== VortexDB Flamegraph ==="
echo "Output: $SVG_FILE"
echo ""

# Build with debug symbols in release mode
export CARGO_PROFILE_RELEASE_DEBUG=2

cd "$ROOT_DIR"

cargo flamegraph \
    --bin vortex-server \
    --output "$SVG_FILE" \
    --root \
    -- "$@" &

VORTEX_PID=$!

# Let the server warm up, then generate load
sleep 2

echo "Server started (PID $VORTEX_PID). Running benchmark load..."

# Use vortex-loadgen if available, otherwise redis-benchmark
if command -v redis-benchmark &>/dev/null; then
    redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -P 16 -t SET,GET -q
elif cargo run --bin vortex-loadgen -- --help &>/dev/null 2>&1; then
    cargo run --release --bin vortex-loadgen -- -n 100000 -c 50 -P 16 -t SET,GET
else
    echo "No benchmark tool found. Install redis-benchmark or build vortex-loadgen."
fi

# Stop the server
kill "$VORTEX_PID" 2>/dev/null || true
wait "$VORTEX_PID" 2>/dev/null || true

echo ""
echo "Flamegraph saved to: $SVG_FILE"
echo "Open in a browser to explore the call stacks."
