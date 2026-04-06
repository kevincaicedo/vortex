#!/usr/bin/env bash
# scripts/bench-commands.sh — Benchmark VortexDB commands not covered by redis-benchmark -t
#
# Uses redis-benchmark with raw command mode to benchmark every VortexDB
# command that doesn't have a built-in redis-benchmark test.
#
# Usage:
#   ./scripts/bench-commands.sh -p PORT [-n requests] [-c clients] [-P pipeline] [--csv] [--output FILE]
#
# Output format matches redis-benchmark --csv and preserves latency columns.

set -euo pipefail

# ── Defaults ──
PORT=""
REQUESTS=100000
CLIENTS=50
PIPELINE=16
OUTPUT_CSV=false
OUTPUT_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -p) PORT=$2; shift 2 ;;
        -n) REQUESTS=$2; shift 2 ;;
        -c) CLIENTS=$2; shift 2 ;;
        -P) PIPELINE=$2; shift 2 ;;
        --csv) OUTPUT_CSV=true; shift ;;
        --output) OUTPUT_FILE=$2; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "$PORT" ]]; then
    echo "Error: -p PORT is required" >&2
    exit 1
fi

HOST="127.0.0.1"

# Temporary directory for intermediate results
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

# ── Benchmark helper ──
# Runs a single raw command benchmark and extracts the CSV line
bench_raw() {
    local label=$1
    shift
    local args=("$@")

    # Flush state between benchmarks to prevent SwissTable swell / cache pollution.
    redis-cli -h "$HOST" -p "$PORT" FLUSHALL > /dev/null 2>&1 || true

    local csv_file="$TMPDIR/${label}.csv"
    local err_file="$TMPDIR/${label}.err"

    # redis-benchmark raw command mode: pass the command arguments directly.
    if redis-benchmark \
        -h "$HOST" -p "$PORT" \
        -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" \
        --csv \
        "${args[@]}" \
        > "$csv_file" 2> "$err_file"; then
        local data_line
        data_line=$(awk -F',' -v label="$label" '
            NR == 2 {
                OFS = ","
                $1 = "\"" label "\""
                print
                exit
            }
        ' "$csv_file")
        if [[ -n "$data_line" ]]; then
            echo "$data_line"
            return 0
        fi
    fi

    echo "\"$label\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\""
    return 0
}

# Variant for O(N) commands (KEYS, SCAN, FLUSHDB) — reduced request count
# and no pipelining to avoid overwhelming the server.
bench_raw_slow() {
    local label=$1
    shift
    local args=("$@")

    # Flush state between benchmarks to prevent SwissTable swell / cache pollution.
    redis-cli -h "$HOST" -p "$PORT" FLUSHALL > /dev/null 2>&1 || true

    local csv_file="$TMPDIR/${label}.csv"
    local err_file="$TMPDIR/${label}.err"
    local slow_n=10000
    local slow_c=10
    local slow_p=1

    if redis-benchmark \
        -h "$HOST" -p "$PORT" \
        -n "$slow_n" -c "$slow_c" -P "$slow_p" \
        --csv \
        "${args[@]}" \
        > "$csv_file" 2> "$err_file"; then
        local data_line
        data_line=$(awk -F',' -v label="$label" '
            NR == 2 {
                OFS = ","
                $1 = "\"" label "\""
                print
                exit
            }
        ' "$csv_file")
        if [[ -n "$data_line" ]]; then
            echo "$data_line"
            return 0
        fi
    fi

    echo "\"$label\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\""
    return 0
}

bench_rename_manual() {
    local label=$1
    local pair_count=$(((REQUESTS + 1) / 2))
    local command_file="$TMPDIR/${label}.pipe"
    local elapsed
    local actual_ops=$((pair_count * 2))
    local keyspace=1024
    local i
    local key_id

    : > "$command_file"
    for ((i = 0; i < pair_count; i++)); do
        key_id=$(((i % keyspace) + 1))
        printf 'RENAME bench:rename:%d bench:rename:tmp:%d\r\n' "$key_id" "$key_id" >> "$command_file"
        printf 'RENAME bench:rename:tmp:%d bench:rename:%d\r\n' "$key_id" "$key_id" >> "$command_file"
    done

    elapsed=$(
        TIMEFORMAT='%3R'
        { time cat "$command_file" | redis-cli -h "$HOST" -p "$PORT" --pipe >/dev/null 2>&1; } 2>&1
    )

    if [[ -n "$elapsed" && "$elapsed" != "0.000" ]]; then
        local rps
        local avg_latency
        rps=$(echo "scale=2; $actual_ops / $elapsed" | bc 2>/dev/null || echo "N/A")
        avg_latency=$(echo "scale=3; ($elapsed * 1000) / $actual_ops" | bc 2>/dev/null || echo "N/A")
        echo "\"$label\",\"$rps\",\"$avg_latency\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\""
        return 0
    fi

    echo "\"$label\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\""
    return 0
}

bench_mget_manual() {
    local label=$1
    local command_file="$TMPDIR/${label}.pipe"
    local elapsed
    local i

    : > "$command_file"
    for ((i = 0; i < REQUESTS; i++)); do
        printf 'MGET bench:key:1 bench:key:2 bench:key:3\r\n' >> "$command_file"
    done

    elapsed=$(
        TIMEFORMAT='%3R'
        { time cat "$command_file" | redis-cli -h "$HOST" -p "$PORT" --pipe >/dev/null 2>&1; } 2>&1
    )

    if [[ -n "$elapsed" && "$elapsed" != "0.000" ]]; then
        local rps
        local avg_latency
        rps=$(echo "scale=2; $REQUESTS / $elapsed" | bc 2>/dev/null || echo "N/A")
        avg_latency=$(echo "scale=3; ($elapsed * 1000) / $REQUESTS" | bc 2>/dev/null || echo "N/A")
        echo "\"$label\",\"$rps\",\"$avg_latency\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\""
        return 0
    fi

    echo "\"$label\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\",\"N/A\""
    return 0
}

# ── Seed test data ──
# Pre-populate keys so read benchmarks have data to work with
seed_data() {
    local pipe_cmds=""
    for i in $(seq 1 1000); do
        pipe_cmds+="SET bench:key:$i value_$i\r\n"
    done
    # Also set keys for GETRANGE/STRLEN that need longer values
    for i in $(seq 1 100); do
        pipe_cmds+="SET bench:long:$i $(head -c 200 /dev/urandom | base64 | head -c 200)\r\n"
    done
    # Set keys with TTL for TTL/PTTL tests
    for i in $(seq 1 100); do
        pipe_cmds+="SET bench:ttl:$i value_$i EX 3600\r\n"
    done
    # Set keys used by the manual RENAME benchmark.
    for i in $(seq 1 1024); do
        pipe_cmds+="SET bench:rename:$i value_$i\r\n"
    done

    printf "$pipe_cmds" | redis-cli -h "$HOST" -p "$PORT" --pipe >/dev/null 2>&1 || true
}

echo "Seeding test data..." >&2
seed_data

# ── CSV header ──
RESULTS="\"test\",\"rps\",\"avg_latency_ms\",\"min_latency_ms\",\"p50_latency_ms\",\"p95_latency_ms\",\"p99_latency_ms\",\"max_latency_ms\""

echo "Running custom command benchmarks on port $PORT..." >&2

# ── String Commands ──

# SETNX — SET if Not eXists (uses __rand_int__ for unique keys)
line=$(bench_raw "SETNX" -r 1000000 SETNX "__rand_int__:setnx" "value")
RESULTS="$RESULTS"$'\n'"$line"

# SETEX — SET with EXpiry
line=$(bench_raw "SETEX" -r 1000000 SETEX "__rand_int__:setex" 3600 "value")
RESULTS="$RESULTS"$'\n'"$line"

# PSETEX — SET with millisecond EXpiry
line=$(bench_raw "PSETEX" -r 1000000 PSETEX "__rand_int__:psetex" 3600000 "value")
RESULTS="$RESULTS"$'\n'"$line"

# GETSET — atomic GET old + SET new (deprecated but supported)
line=$(bench_raw "GETSET" -r 1000000 GETSET "__rand_int__:getset" "newvalue")
RESULTS="$RESULTS"$'\n'"$line"

# GETDEL — GET and DELETE atomically
line=$(bench_raw "GETDEL" -r 1000000 GETDEL "__rand_int__:getdel")
RESULTS="$RESULTS"$'\n'"$line"

# GETEX — GET and set EXpiry
line=$(bench_raw "GETEX" GETEX "bench:key:1" EX 3600)
RESULTS="$RESULTS"$'\n'"$line"

# APPEND — append to existing string (random keys to avoid O(n²) single-key growth)
line=$(bench_raw "APPEND" -r 1000000 APPEND "__rand_int__:append" "x")
RESULTS="$RESULTS"$'\n'"$line"

# STRLEN — get string length
line=$(bench_raw "STRLEN" STRLEN "bench:key:1")
RESULTS="$RESULTS"$'\n'"$line"

# GETRANGE — substring extraction
line=$(bench_raw "GETRANGE" GETRANGE "bench:long:1" 0 49)
RESULTS="$RESULTS"$'\n'"$line"

# SETRANGE — in-place string modification
line=$(bench_raw "SETRANGE" SETRANGE "bench:long:1" 10 "REPLACED")
RESULTS="$RESULTS"$'\n'"$line"

# DECRBY — decrement by N
line=$(bench_raw "DECRBY" DECRBY "bench:counter:decrby" 1)
RESULTS="$RESULTS"$'\n'"$line"

# INCRBY — increment by N
line=$(bench_raw "INCRBY" INCRBY "bench:counter:incrby" 1)
RESULTS="$RESULTS"$'\n'"$line"

# INCRBYFLOAT — increment by float
line=$(bench_raw "INCRBYFLOAT" INCRBYFLOAT "bench:counter:float" 1.5)
RESULTS="$RESULTS"$'\n'"$line"

# DECR — decrement by 1
line=$(bench_raw "DECR" DECR "bench:counter:decr")
RESULTS="$RESULTS"$'\n'"$line"

# MSETNX — MSET if none exist (2 keys)
line=$(bench_raw "MSETNX" -r 1000000 MSETNX "__rand_int__:msetnx:a" "va" "__rand_int__:msetnx:b" "vb")
RESULTS="$RESULTS"$'\n'"$line"

# MGET — multi-key GET (3 keys)
line=$(bench_raw "MGET" MGET "bench:key:1" "bench:key:2" "bench:key:3")
RESULTS="$RESULTS"$'\n'"$line"

# ── Key Management Commands ──

# DEL — delete a key
line=$(bench_raw "DEL" -r 1000000 DEL "__rand_int__:del")
RESULTS="$RESULTS"$'\n'"$line"

# UNLINK — async delete
line=$(bench_raw "UNLINK" -r 1000000 UNLINK "__rand_int__:unlink")
RESULTS="$RESULTS"$'\n'"$line"

# EXISTS — check key existence
line=$(bench_raw "EXISTS" EXISTS "bench:key:1")
RESULTS="$RESULTS"$'\n'"$line"

# EXPIRE — set TTL in seconds
line=$(bench_raw "EXPIRE" EXPIRE "bench:key:1" 3600)
RESULTS="$RESULTS"$'\n'"$line"

# PEXPIRE — set TTL in milliseconds
line=$(bench_raw "PEXPIRE" PEXPIRE "bench:key:2" 3600000)
RESULTS="$RESULTS"$'\n'"$line"

# EXPIREAT — set TTL as Unix timestamp
line=$(bench_raw "EXPIREAT" EXPIREAT "bench:key:3" 2000000000)
RESULTS="$RESULTS"$'\n'"$line"

# PEXPIREAT — set TTL as Unix millisecond timestamp
line=$(bench_raw "PEXPIREAT" PEXPIREAT "bench:key:4" 2000000000000)
RESULTS="$RESULTS"$'\n'"$line"

# TTL — get remaining TTL in seconds
line=$(bench_raw "TTL" TTL "bench:ttl:1")
RESULTS="$RESULTS"$'\n'"$line"

# PTTL — get remaining TTL in milliseconds
line=$(bench_raw "PTTL" PTTL "bench:ttl:1")
RESULTS="$RESULTS"$'\n'"$line"

# PERSIST — remove TTL
line=$(bench_raw "PERSIST" PERSIST "bench:ttl:2")
RESULTS="$RESULTS"$'\n'"$line"

# TYPE — get key type
line=$(bench_raw "TYPE" TYPE "bench:key:1")
RESULTS="$RESULTS"$'\n'"$line"

# RENAME — rename key to itself (valid op, measures full RENAME path with native latency)
line=$(bench_raw "RENAME" RENAME "bench:rename:1" "bench:rename:1")
RESULTS="$RESULTS"$'\n'"$line"

# TOUCH — update last access time
line=$(bench_raw "TOUCH" TOUCH "bench:key:1")
RESULTS="$RESULTS"$'\n'"$line"

# COPY — copy key to another key
line=$(bench_raw "COPY" COPY "bench:key:1" "bench:key:copy:dest" REPLACE)
RESULTS="$RESULTS"$'\n'"$line"

# RANDOMKEY — get a random key
line=$(bench_raw "RANDOMKEY" RANDOMKEY)
RESULTS="$RESULTS"$'\n'"$line"

# DBSIZE — number of keys
line=$(bench_raw "DBSIZE" DBSIZE)
RESULTS="$RESULTS"$'\n'"$line"

# KEYS — pattern match all keys (O(N), use reduced settings to avoid blocking)
line=$(bench_raw_slow "KEYS" KEYS "bench:key:*")
RESULTS="$RESULTS"$'\n'"$line"

# SCAN — cursor-based iteration (O(N), use reduced settings)
line=$(bench_raw_slow "SCAN" SCAN 0 COUNT 100)
RESULTS="$RESULTS"$'\n'"$line"

# FLUSHDB — flush the database (reduced settings since it clears data)
line=$(bench_raw_slow "FLUSHDB" FLUSHDB)
RESULTS="$RESULTS"$'\n'"$line"

# ── Server Commands ──

# ECHO — echo back a message
line=$(bench_raw "ECHO" ECHO "hello")
RESULTS="$RESULTS"$'\n'"$line"

# TIME — server time
line=$(bench_raw "TIME" TIME)
RESULTS="$RESULTS"$'\n'"$line"

# ── Output results ──
if [[ -n "$OUTPUT_FILE" ]]; then
    echo "$RESULTS" > "$OUTPUT_FILE"
    echo "Results saved to: $OUTPUT_FILE" >&2
elif [[ "$OUTPUT_CSV" == true ]]; then
    echo "$RESULTS"
else
    # Pretty-print as table
    echo ""
    echo "Custom Command Benchmark Results (port $PORT)"
    echo "═══════════════════════════════════════════════"
    echo "$RESULTS" | tail -n +2 | while IFS=',' read -r test rps _rest; do
        test=$(echo "$test" | tr -d '"')
        rps=$(echo "$rps" | tr -d '"')
        printf "  %-20s %s rps\n" "$test" "$rps"
    done
fi

echo "" >&2
echo "Custom command benchmarks complete." >&2
