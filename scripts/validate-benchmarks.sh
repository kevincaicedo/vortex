#!/usr/bin/env bash
# scripts/validate-benchmarks.sh — Run benchmarks and validate against targets
#
# Runs the Criterion engine benchmark suite, extracts key metrics,
# compares against Phase 3 targets, and produces a summary report.
#
# Usage:
#   ./scripts/validate-benchmarks.sh [--json] [--ci]
#
# Flags:
#   --json  Output results as JSON (for CI consumption)
#   --ci    Strict mode — exit 1 if any target is missed

set -euo pipefail

JSON_OUTPUT=false
CI_MODE=false
for arg in "$@"; do
    case $arg in
        --json) JSON_OUTPUT=true ;;
        --ci) CI_MODE=true ;;
    esac
done

RESULTS_DIR="$(mktemp -d)"
BENCH_OUTPUT="$RESULTS_DIR/bench-output.txt"

cleanup() { rm -rf "$RESULTS_DIR"; }
trap cleanup EXIT

echo "═══════════════════════════════════════════════════════════════"
echo "  VortexDB Phase 3 — Benchmark Validation Suite"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# ── Run benchmarks ──
echo "Running Criterion benchmarks (this may take several minutes)..."
BENCH_ARGS="--output-format bencher"
if $CI_MODE; then
    # Reduced sampling for CI to keep total runtime under 5 minutes
    BENCH_ARGS="$BENCH_ARGS --sample-size 20 --measurement-time 3 --warm-up-time 1"
fi
cargo bench -p vortex-engine --bench engine -- $BENCH_ARGS 2>/dev/null | tee "$BENCH_OUTPUT"
echo ""

# ── Parse results ──
extract_time_ns() {
    local name="$1"
    # Bencher format: test <name> ... bench:    <N> ns/iter (+/- <M>)
    grep "test $name " "$BENCH_OUTPUT" | sed -E 's/.*bench: *([0-9,]+) ns.*/\1/' | tr -d ','
}

# ── Phase 3 Target Validation ──
PASS=0
FAIL=0
RESULTS=()

check_target() {
    local name="$1"
    local measured="$2"
    local target="$3"
    local op="$4"  # "le" for ≤, "ge" for ≥
    local unit="$5"

    if [[ -z "$measured" || "$measured" == "0" ]]; then
        echo "  ⚠  $name: NOT FOUND"
        RESULTS+=("{\"name\":\"$name\",\"status\":\"missing\"}")
        return
    fi

    local status
    if [[ "$op" == "le" ]]; then
        if (( measured <= target )); then
            status="PASS"
            PASS=$((PASS + 1))
        else
            status="FAIL"
            FAIL=$((FAIL + 1))
        fi
        echo "  $([ "$status" = "PASS" ] && echo "✅" || echo "❌")  $name: ${measured} ${unit} (target ≤${target} ${unit}) — $status"
    elif [[ "$op" == "ge" ]]; then
        if (( measured >= target )); then
            status="PASS"
            PASS=$((PASS + 1))
        else
            status="FAIL"
            FAIL=$((FAIL + 1))
        fi
        echo "  $([ "$status" = "PASS" ] && echo "✅" || echo "❌")  $name: ${measured} ${unit} (target ≥${target} ${unit}) — $status"
    fi
    RESULTS+=("{\"name\":\"$name\",\"measured\":$measured,\"target\":$target,\"unit\":\"$unit\",\"status\":\"$status\"}")
}

echo "── Phase 3 Performance Targets ──"
echo ""

GET_NS=$(extract_time_ns "cmd_get_inline")
SET_NS=$(extract_time_ns "cmd_set_inline")
DEL_NS=$(extract_time_ns "cmd_del_inline")
INCR_NS=$(extract_time_ns "cmd_incr")
MGET_NS=$(extract_time_ns "cmd_mget_100")
EXISTS_NS=$(extract_time_ns "cmd_exists")
TTL_NS=$(extract_time_ns "cmd_ttl")
SCAN_US=$(extract_time_ns "cmd_scan_10k")
PING_NS=$(extract_time_ns "cmd_ping")
THROUGHPUT_NS=$(extract_time_ns "throughput_get_set_1m")

check_target "GET (inline)" "$GET_NS" 50 "le" "ns"
check_target "SET (inline)" "$SET_NS" 70 "le" "ns"
check_target "DEL (inline)" "$DEL_NS" 40 "le" "ns"
check_target "INCR (in-place)" "$INCR_NS" 30 "le" "ns"
check_target "MGET 100" "$MGET_NS" 5000 "le" "ns"
check_target "EXISTS" "$EXISTS_NS" 45 "le" "ns"
check_target "TTL" "$TTL_NS" 50 "le" "ns"

if [[ -n "$THROUGHPUT_NS" && "$THROUGHPUT_NS" != "0" ]]; then
    # throughput_get_set_1m does 100K ops per iteration; time is in ns
    OPS_PER_SEC=$(( 100000 * 1000000000 / THROUGHPUT_NS ))
    OPS_M=$(( OPS_PER_SEC / 1000000 ))
    check_target "Throughput GET/SET 1M" "$OPS_M" 5 "ge" "M ops/sec"
fi

echo ""
echo "── Swiss Table vs HashMap Baseline ──"
echo ""

ST_HIT=$(extract_time_ns "swiss_table_lookup_hit/1000000")
HM_HIT=$(extract_time_ns "hashmap_baseline_lookup/hit/1000000")
if [[ -n "$ST_HIT" && -n "$HM_HIT" && "$ST_HIT" -gt 0 ]]; then
    RATIO=$(( HM_HIT * 100 / ST_HIT ))
    echo "  Lookup hit @ 1M:  SwissTable ${ST_HIT}ns vs HashMap ${HM_HIT}ns — ${RATIO}% (target ≥200%)"
fi

ST_MISS=$(extract_time_ns "swiss_table_lookup_miss/1000000")
HM_MISS=$(extract_time_ns "hashmap_baseline_lookup/miss/1000000")
if [[ -n "$ST_MISS" && -n "$HM_MISS" && "$ST_MISS" -gt 0 ]]; then
    RATIO=$(( HM_MISS * 100 / ST_MISS ))
    echo "  Lookup miss @ 1M: SwissTable ${ST_MISS}ns vs HashMap ${HM_MISS}ns — ${RATIO}%"
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Summary: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════════════════════════════════"

if $JSON_OUTPUT; then
    echo ""
    echo "JSON_RESULTS_START"
    echo "{\"pass\":$PASS,\"fail\":$FAIL,\"results\":[$(IFS=,; echo "${RESULTS[*]}")]}"
    echo "JSON_RESULTS_END"
fi

if $CI_MODE && [[ $FAIL -gt 0 ]]; then
    echo ""
    echo "CI VALIDATION FAILED: $FAIL targets missed"
    exit 1
fi
