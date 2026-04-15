#!/usr/bin/env bash
# scripts/compare.sh — VortexDB Competitive Benchmark Suite v2
#
# Benchmarks VortexDB against Redis, Dragonfly, and Valkey across ALL
# supported commands plus an optional memtier mixed-workload suite.
# Produces terminal tables, JSON, and Markdown output.
#
# Prerequisites:
#   - docker
#   - redis-benchmark (redis-tools package)
#   - redis-cli (redis-tools package)
#   - bc (for ratio calculations)
#   - memtier_benchmark (optional, required only with --memtier)
#
# Usage:
#   ./scripts/compare.sh [options]
#
# Options:
#   -n NUM       Number of requests per test (default: 100000)
#   -c NUM       Number of parallel clients (default: 50)
#   -P NUM       Pipeline depth (default: 16)
#   -t TESTS     Comma-separated redis-benchmark -t tests (default: all supported)
#   --json       Write JSON results to benchmarks/results-{timestamp}.json
#   --markdown   Write Markdown results to benchmarks/results-{timestamp}.md
#   --latency    Capture p50/p95/p99 latency percentiles per command
#   --runs N     Execute benchmarks N times with statistical analysis (default: 1)
#   --custom     Also run custom command benchmarks (bench-commands.sh)
#   --memtier    Also run memtier_benchmark mixed-workload suite
#   --memtier-threads LIST
#                Comma-separated memtier thread counts (default: 1,2,4,8)
#   --memtier-requests NUM
#                Memtier requests per client (default: 2000)
#   --memtier-clients NUM
#                Memtier clients per thread (default: 50)
#   --memtier-pipeline NUM
#                Memtier pipeline depth (default: 1)
#   --memtier-data-size NUM
#                Memtier value size in bytes (default: 384)
#   --memtier-ratio RATIO
#                Memtier read:write ratio (default: 1:15)
#   --no-build   Skip building VortexDB (use existing binary / Docker image)
#   --no-docker  Skip Docker containers (expect servers already running on ports)
#   --docker-all Run ALL databases (including VortexDB) in Docker with identical
#                resource limits for a fair apples-to-apples comparison
#   --native     Run VortexDB and Redis both natively (no Docker at all).
#                Requires redis-server installed locally.
#   --cpus NUM   CPU limit per Docker container (default: 4)
#   --memory SZ  Memory limit per container (default: 2g)
#   --help       Show this help message
#
# Examples:
#   ./scripts/compare.sh -n 500000 -c 100 -P 16
#   ./scripts/compare.sh --json --markdown --runs 3
#   ./scripts/compare.sh --json --markdown --custom --memtier
#   ./scripts/compare.sh --custom --latency --runs 5

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Defaults ──
REQUESTS=100000
CLIENTS=50
PIPELINE=16
# redis-benchmark built-in tests (full list for Redis-compatible servers)
DEFAULT_TESTS="PING_INLINE,PING_MBULK,SET,GET,INCR,LPUSH,RPUSH,LPOP,RPOP,SADD,HSET,SPOP,MSET"
# Subset that Vortex supports (no list/set/hash commands yet)
VORTEX_BUILTIN_TESTS="PING_INLINE,PING_MBULK,SET,GET,INCR,MSET"
TESTS=""
PORT_BASE=16379
RESULTS_DIR="$(mktemp -d)"
VORTEX_LOG=""
OUTPUT_JSON=false
OUTPUT_MARKDOWN=false
CAPTURE_LATENCY=false
NUM_RUNS=1
RUN_CUSTOM=false
RUN_MEMTIER=false
DO_BUILD=true
DO_DOCKER=true
DOCKER_ALL=false
NATIVE_MODE=false
DOCKER_CPUS=4
DOCKER_MEMORY="2g"
MEMTIER_THREADS="1,2,4,8"
MEMTIER_REQUESTS=2000
MEMTIER_CLIENTS=50
MEMTIER_PIPELINE=1
MEMTIER_DATA_SIZE=384
MEMTIER_RATIO="1:15"
MEMTIER_KEY_MINIMUM=1
MEMTIER_KEY_MAXIMUM=1000000
MEMTIER_KEY_MEDIAN=500000
MEMTIER_KEY_STDDEV=166667
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
BENCHMARKS_DIR="$PROJECT_ROOT/benchmarks"

# Server names and ports stored in files for bash 3.x compatibility
SERVERS="vortex redis dragonfly valkey"

# Key-value store using files (portable, no associative arrays needed)
kv_dir=""
kv_init() { kv_dir="$RESULTS_DIR/kv"; mkdir -p "$kv_dir"; }
kv_set()  { echo "$2" > "$kv_dir/$1"; }
kv_get()  { cat "$kv_dir/$1" 2>/dev/null || echo ""; }

# ── Parse arguments ──
while [[ $# -gt 0 ]]; do
    case $1 in
        -n) REQUESTS=$2; shift 2 ;;
        -c) CLIENTS=$2; shift 2 ;;
        -P) PIPELINE=$2; shift 2 ;;
        -t) TESTS=$2; shift 2 ;;
        --json) OUTPUT_JSON=true; shift ;;
        --markdown) OUTPUT_MARKDOWN=true; shift ;;
        --latency) CAPTURE_LATENCY=true; shift ;;
        --runs) NUM_RUNS=$2; shift 2 ;;
        --custom) RUN_CUSTOM=true; shift ;;
        --memtier) RUN_MEMTIER=true; shift ;;
        --memtier-threads) MEMTIER_THREADS=$2; shift 2 ;;
        --memtier-requests) MEMTIER_REQUESTS=$2; shift 2 ;;
        --memtier-clients) MEMTIER_CLIENTS=$2; shift 2 ;;
        --memtier-pipeline) MEMTIER_PIPELINE=$2; shift 2 ;;
        --memtier-data-size) MEMTIER_DATA_SIZE=$2; shift 2 ;;
        --memtier-ratio) MEMTIER_RATIO=$2; shift 2 ;;
        --no-build) DO_BUILD=false; shift ;;
        --no-docker) DO_DOCKER=false; shift ;;
        --docker-all) DOCKER_ALL=true; shift ;;
        --native) NATIVE_MODE=true; shift ;;
        --cpus) DOCKER_CPUS=$2; shift 2 ;;
        --memory) DOCKER_MEMORY=$2; shift 2 ;;
        --help|-h)
            sed -n '/^# Usage:/,/^[^#]/p' "$0" | grep '^#' | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

[[ -z "$TESTS" ]] && TESTS="$DEFAULT_TESTS"
kv_init

# ── Mode validation ──
if [[ "$NATIVE_MODE" == true && "$DOCKER_ALL" == true ]]; then
    echo "ERROR: --native and --docker-all are mutually exclusive" >&2
    exit 1
fi
if [[ "$NATIVE_MODE" == true ]]; then
    DO_DOCKER=false
    SERVERS="vortex redis"
fi

# ── Utility functions ──

log_info()  { echo "  [INFO] $*"; }
log_warn()  { echo "  [WARN] $*" >&2; }

require_command() {
    local tool=$1
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "ERROR: required command '$tool' not found in PATH" >&2
        exit 1
    fi
}

memtier_threads_list() {
    echo "$MEMTIER_THREADS" | tr ',' ' '
}

cleanup() {
    echo ""
    log_info "Cleaning up..."
    if [[ "$DO_DOCKER" == true ]]; then
        docker rm -f vortex-bench-redis vortex-bench-dragonfly vortex-bench-valkey 2>/dev/null || true
    fi
    if [[ "$DOCKER_ALL" == true ]]; then
        docker rm -f vortex-bench-vortex 2>/dev/null || true
    fi
    if [[ -n "${VORTEX_PID:-}" ]]; then
        kill "$VORTEX_PID" 2>/dev/null || true
        wait "$VORTEX_PID" 2>/dev/null || true
    fi
    if [[ -n "${REDIS_PID:-}" ]]; then
        kill "$REDIS_PID" 2>/dev/null || true
        wait "$REDIS_PID" 2>/dev/null || true
    fi
    rm -rf "$RESULTS_DIR"
}
trap cleanup EXIT

require_command redis-benchmark
require_command redis-cli
if [[ "$RUN_MEMTIER" == true ]]; then
    require_command memtier_benchmark
fi

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

    log_warn "$name on port $port is not responding after ${attempts}s"
    return 1
}

format_number() {
    local n=$1
    if [[ "$n" == "N/A" || -z "$n" ]]; then
        echo "N/A"
        return
    fi
    n=$(echo "$n" | cut -d'.' -f1)
    # Pure shell thousand-separator (portable to bash 3.2+)
    local result=""
    local count=0
    local len=${#n}
    local i
    for ((i = len - 1; i >= 0; i--)); do
        if [[ $count -gt 0 && $((count % 3)) -eq 0 ]]; then
            result=",${result}"
        fi
        result="${n:$i:1}${result}"
        count=$((count + 1))
    done
    echo "$result"
}

calc_ratio() {
    local vortex=$1
    local other=$2
    if [[ "$vortex" == "N/A" || "$other" == "N/A" || -z "$vortex" || -z "$other" ]]; then
        echo "N/A"
        return
    fi
    local other_num vortex_num
    other_num=$(echo "$other" | tr -d ',' | cut -d'.' -f1)
    vortex_num=$(echo "$vortex" | tr -d ',' | cut -d'.' -f1)
    if [[ "$other_num" -eq 0 ]]; then
        echo "inf"
        return
    fi
    echo "scale=1; $vortex_num / $other_num" | bc 2>/dev/null || echo "N/A"
}

json_value() {
    if [[ "$1" == "N/A" || -z "$1" ]]; then
        echo "null"
    else
        echo "$1"
    fi
}

extract_rps() {
    local file=$1
    local label=$2
    local value
    value=$(grep -F -m1 "\"$label\"" "$file" 2>/dev/null | cut -d',' -f2 | tr -d '"' || true)
    if [[ -n "$value" ]]; then
        echo "$value"
    else
        echo "N/A"
    fi
}

extract_latency() {
    local file=$1
    local label=$2
    local field=$3
    local value
    value=$(grep -F -m1 "\"$label\"" "$file" 2>/dev/null | cut -d',' -f"$field" | tr -d '"' || true)
    if [[ -n "$value" ]]; then
        echo "$value"
    else
        echo "N/A"
    fi
}

flush_server() {
    local port=$1
    redis-cli -h 127.0.0.1 -p "$port" FLUSHALL >/dev/null 2>&1 || true
}

run_memtier_benchmark() {
    local name=$1
    local port=$2
    local thread_count=$3
    local run_id=$4
    local output="$RESULTS_DIR/memtier_${name}_t${thread_count}_run${run_id}.txt"
    local error_output="$RESULTS_DIR/memtier_${name}_t${thread_count}_run${run_id}.err"

    flush_server "$port"

    if ! memtier_benchmark \
        -s 127.0.0.1 \
        -p "$port" \
        --protocol=redis \
        -t "$thread_count" \
        -c "$MEMTIER_CLIENTS" \
        --requests="$MEMTIER_REQUESTS" \
        --pipeline="$MEMTIER_PIPELINE" \
        --data-size="$MEMTIER_DATA_SIZE" \
        --ratio="$MEMTIER_RATIO" \
        --key-pattern=G:G \
        --key-minimum="$MEMTIER_KEY_MINIMUM" \
        --key-maximum="$MEMTIER_KEY_MAXIMUM" \
        --key-median="$MEMTIER_KEY_MEDIAN" \
        --key-stddev="$MEMTIER_KEY_STDDEV" \
        --distinct-client-seed \
        --hide-histogram \
        > "$output" 2> "$error_output"; then
        log_warn "Memtier benchmark failed for $name (${thread_count}T, run $run_id)"
        return 1
    fi
}

extract_memtier_field() {
    local file=$1
    local row=$2
    local field=$3
    local value

    value=$(awk -v row="$row" -v field="$field" '$1 == row { print $field; exit }' "$file" 2>/dev/null || true)
    if [[ -n "$value" && "$value" != "---" ]]; then
        echo "$value"
    else
        echo "N/A"
    fi
}

average_memtier_field() {
    local name=$1
    local thread_count=$2
    local row=$3
    local field=$4
    local sum=0
    local count=0

    for run in $(seq 1 "$NUM_RUNS"); do
        local file="$RESULTS_DIR/memtier_${name}_t${thread_count}_run${run}.txt"
        local val
        val=$(extract_memtier_field "$file" "$row" "$field")
        if [[ "$val" != "N/A" && -n "$val" ]]; then
            sum=$(echo "$sum + $val" | bc 2>/dev/null || echo "$sum")
            count=$((count + 1))
        fi
    done

    if [[ $count -eq 0 ]]; then
        echo "N/A"
        return
    fi

    echo "scale=3; $sum / $count" | bc 2>/dev/null || echo "N/A"
}

average_memtier_field_with_stats() {
    local name=$1
    local thread_count=$2
    local row=$3
    local field=$4
    local sum=0
    local count=0
    local values=""

    for run in $(seq 1 "$NUM_RUNS"); do
        local file="$RESULTS_DIR/memtier_${name}_t${thread_count}_run${run}.txt"
        local val
        val=$(extract_memtier_field "$file" "$row" "$field")
        if [[ "$val" != "N/A" && -n "$val" ]]; then
            sum=$(echo "$sum + $val" | bc 2>/dev/null || echo "$sum")
            count=$((count + 1))
            values="$values $val"
        fi
    done

    if [[ $count -eq 0 ]]; then
        echo "N/A|N/A|N/A"
        return
    fi

    local mean
    mean=$(echo "scale=3; $sum / $count" | bc 2>/dev/null || echo "N/A")

    local stddev="0"
    if [[ $count -gt 1 ]]; then
        local sq_sum=0
        local v
        for v in $values; do
            sq_sum=$(echo "$sq_sum + ($v - $mean) * ($v - $mean)" | bc 2>/dev/null || echo "$sq_sum")
        done
        stddev=$(echo "scale=3; sqrt($sq_sum / ($count - 1))" | bc 2>/dev/null || echo "0")
    fi

    local ci95="0"
    if [[ $count -gt 1 && "$stddev" != "0" ]]; then
        ci95=$(echo "scale=3; 1.96 * $stddev / sqrt($count)" | bc 2>/dev/null || echo "0")
    fi

    echo "${mean}|${stddev}|${ci95}"
}

get_port() {
    kv_get "port_$1"
}

filter_vortex_tests() {
    local requested=$1
    local filtered=""
    local old_ifs=$IFS
    local test

    IFS=','
    for test in $requested; do
        case ",${VORTEX_BUILTIN_TESTS}," in
            *,"${test}",*)
                if [[ -n "$filtered" ]]; then
                    filtered="${filtered},${test}"
                else
                    filtered="$test"
                fi
                ;;
        esac
    done
    IFS=$old_ifs

    if [[ -n "$filtered" ]]; then
        echo "$filtered"
    else
        echo "$VORTEX_BUILTIN_TESTS"
    fi
}

# ── Banner ──
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║         VortexDB Competitive Benchmark Suite v2                ║"
echo "╠══════════════════════════════════════════════════════════════════╣"
printf "║  Requests: %-8s  Clients: %-6s  Pipeline: %-6s        ║\n" "$REQUESTS" "$CLIENTS" "$PIPELINE"
printf "║  Runs: %-4s  JSON: %-5s  Markdown: %-5s  Latency: %-5s   ║\n" "$NUM_RUNS" "$OUTPUT_JSON" "$OUTPUT_MARKDOWN" "$CAPTURE_LATENCY"
if [[ "$DOCKER_ALL" == true ]]; then
printf "║  Docker-All: yes    CPUs: %-4s  Memory: %-8s              ║\n" "$DOCKER_CPUS" "$DOCKER_MEMORY"
elif [[ "$NATIVE_MODE" == true ]]; then
printf "║  Native: yes (VortexDB + Redis, no Docker)                    ║\n"
fi
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "Benchmark tests: $TESTS"
[[ "$RUN_CUSTOM" == true ]] && echo "Custom command benchmarks: enabled"
if [[ "$RUN_MEMTIER" == true ]]; then
    echo "Memtier mixed workload: enabled (${MEMTIER_THREADS} threads, ratio ${MEMTIER_RATIO})"
fi
echo ""

# ── Build VortexDB ──
if [[ "$DO_BUILD" == true ]]; then
    if [[ "$DOCKER_ALL" == true ]]; then
        log_info "Building VortexDB Docker image..."
        (cd "$PROJECT_ROOT" && docker build -t vortexdb:latest .)
    else
        log_info "Building VortexDB release binary..."
        (cd "$PROJECT_ROOT" && cargo build --release --bin vortex-server)
    fi
fi

# ── Docker image versions ──
IMAGE_redis="redis:8.6.2-alpine3.23"
IMAGE_dragonfly="docker.dragonflydb.io/dragonflydb/dragonfly:latest"
IMAGE_valkey="valkey/valkey:9.1-alpine3.23"
IMAGE_vortex="vortexdb:latest"

PORT=$PORT_BASE

# ── Start VortexDB ──
if [[ "$DOCKER_ALL" == true ]]; then
    log_info "Starting VortexDB (Docker) on port $PORT..."
    docker run -d --rm \
        --name vortex-bench-vortex \
        -p "$PORT:6379" \
        --memory="$DOCKER_MEMORY" \
        --cpus="$DOCKER_CPUS" \
        --security-opt seccomp=unconfined \
        --ulimit memlock=-1 \
        "$IMAGE_vortex" \
        --bind "0.0.0.0:6379" --threads "$DOCKER_CPUS" >/dev/null
    kv_set "port_vortex" "$PORT"
    PORT=$((PORT + 1))
else
    log_info "Starting VortexDB (native) on port $PORT..."
    VORTEX_LOG="$RESULTS_DIR/vortex.log"
    "$PROJECT_ROOT/target/release/vortex-server" --bind "127.0.0.1:$PORT" >"$VORTEX_LOG" 2>&1 &
    VORTEX_PID=$!
    kv_set "port_vortex" "$PORT"
    PORT=$((PORT + 1))
fi

# ── Start competitor databases ──
if [[ "$NATIVE_MODE" == true ]]; then
    # Native mode: start Redis locally without Docker
    if ! command -v redis-server >/dev/null 2>&1; then
        echo "ERROR: redis-server not found. Install Redis to use --native mode." >&2
        exit 1
    fi
    # Detect available CPU cores for io-threads (cap at 4 for consistency)
    local_cpus=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)
    redis_threads=$((local_cpus > 4 ? 4 : local_cpus))
    REDIS_LOG="$RESULTS_DIR/redis.log"
    log_info "Starting Redis (native, io-threads $redis_threads) on port $PORT..."
    redis-server \
        --port "$PORT" \
        --save "" --appendonly no \
        --io-threads "$redis_threads" --io-threads-do-reads yes \
        --hz 100 --loglevel warning \
        --protected-mode no \
        --daemonize no >"$REDIS_LOG" 2>&1 &
    REDIS_PID=$!
    kv_set "port_redis" "$PORT"
    PORT=$((PORT + 1))
elif [[ "$DO_DOCKER" == true ]]; then
    # Redis 8.6.2 — optimal benchmark configuration
    log_info "Starting Redis ($IMAGE_redis) on port $PORT..."
    docker run -d --rm \
        --name vortex-bench-redis \
        -p "$PORT:6379" \
        --memory="$DOCKER_MEMORY" \
        --cpus="$DOCKER_CPUS" \
        "$IMAGE_redis" \
        redis-server \
          --save "" --appendonly no \
          --io-threads "$DOCKER_CPUS" --io-threads-do-reads yes \
          --hz 100 --loglevel warning \
          --protected-mode no \
          --maxmemory 1800mb --maxmemory-policy noeviction >/dev/null
    kv_set "port_redis" "$PORT"
    PORT=$((PORT + 1))

    # Dragonfly — optimal benchmark configuration
    log_info "Starting Dragonfly ($IMAGE_dragonfly) on port $PORT..."
    docker run -d --rm \
        --name vortex-bench-dragonfly \
        -p "$PORT:6379" \
        --memory="$DOCKER_MEMORY" \
        --cpus="$DOCKER_CPUS" \
        --ulimit memlock=-1 \
        "$IMAGE_dragonfly" \
        dragonfly \
          --logtostdout \
          --proactor_threads "$DOCKER_CPUS" \
          --hz 100 \
          --dbfilename "" \
          --pipeline_squash 10 \
          --version_check false \
          --maxmemory 1800mb >/dev/null
    kv_set "port_dragonfly" "$PORT"
    PORT=$((PORT + 1))

    # Valkey 9.1 — optimal benchmark configuration
    log_info "Starting Valkey ($IMAGE_valkey) on port $PORT..."
    docker run -d --rm \
        --name vortex-bench-valkey \
        -p "$PORT:6379" \
        --memory="$DOCKER_MEMORY" \
        --cpus="$DOCKER_CPUS" \
        "$IMAGE_valkey" \
        valkey-server \
          --save "" --appendonly no \
          --io-threads "$DOCKER_CPUS" --io-threads-do-reads yes \
          --hz 100 --loglevel warning \
          --protected-mode no \
          --maxmemory 1800mb --maxmemory-policy noeviction >/dev/null
    kv_set "port_valkey" "$PORT"
    PORT=$((PORT + 1))
fi

log_info "Waiting for servers to start..."
for name in $SERVERS; do
    port=$(get_port "$name")
    if [[ -n "$port" ]]; then
        if ! wait_for_server "$name" "$port"; then
            if [[ "$name" == "vortex" && -f "$VORTEX_LOG" ]]; then
                echo "Vortex startup log:"
                tail -n 20 "$VORTEX_LOG"
            fi
        fi
    fi
done

# ── Benchmark execution ──

run_standard_benchmark() {
    local name=$1
    local port=$2
    local run_id=$3
    local output="$RESULTS_DIR/${name}_run${run_id}.csv"
    local error_output="$RESULTS_DIR/${name}_run${run_id}.err"

    # Use the vortex-compatible subset for vortex, full list for others
    local test_list="$TESTS"
    if [[ "$name" == "vortex" ]]; then
        test_list=$(filter_vortex_tests "$TESTS")
    fi

    if ! redis-benchmark \
        -h 127.0.0.1 \
        -p "$port" \
        -n "$REQUESTS" \
        -c "$CLIENTS" \
        -P "$PIPELINE" \
        -t "$test_list" \
        --csv \
        > "$output" 2> "$error_output"; then
        log_warn "Benchmark failed for $name (run $run_id)"
        return 1
    fi
}

run_custom_command_benchmark() {
    local name=$1
    local port=$2
    local run_id=$3

    if [[ -x "$SCRIPT_DIR/bench-commands.sh" ]]; then
        "$SCRIPT_DIR/bench-commands.sh" \
            -p "$port" \
            -n "$REQUESTS" \
            -c "$CLIENTS" \
            -P "$PIPELINE" \
            --csv \
            --output "$RESULTS_DIR/${name}_custom_run${run_id}.csv" \
            2>"$RESULTS_DIR/${name}_custom_run${run_id}.err" || {
            log_warn "Custom benchmarks failed for $name (run $run_id)"
        }
    else
        log_warn "bench-commands.sh not found or not executable — skipping custom benchmarks"
        RUN_CUSTOM=false
    fi
}

for run in $(seq 1 "$NUM_RUNS"); do
    if [[ $NUM_RUNS -gt 1 ]]; then
        echo ""
        echo "--- Run $run of $NUM_RUNS ---"
    fi

    for name in $SERVERS; do
        port=$(get_port "$name")
        [[ -z "$port" ]] && continue

        log_info "Benchmarking $name (port $port)..."
        run_standard_benchmark "$name" "$port" "$run" || true

        if [[ "$RUN_CUSTOM" == true ]]; then
            log_info "Running custom command benchmarks on $name..."
            run_custom_command_benchmark "$name" "$port" "$run" || true
        fi

        if [[ "$RUN_MEMTIER" == true ]]; then
            for thread_count in $(memtier_threads_list); do
                log_info "Running memtier mixed workload on $name (${thread_count}T)..."
                run_memtier_benchmark "$name" "$port" "$thread_count" "$run" || true
            done
        fi
    done
done

# ── Collect and aggregate results ──

collect_result_rows() {
    local suffix=$1
    local files=""
    for name in $SERVERS; do
        files="$files $RESULTS_DIR/${name}${suffix}"
    done
    # shellcheck disable=SC2086
    awk -F',' '
        FNR == 1 { next }
        {
            label = $1
            gsub(/"/, "", label)
            if (label != "" && !seen[label]++) {
                print label
            }
        }
    ' $files 2>/dev/null
}

average_rps() {
    local name=$1
    local label=$2
    local suffix=$3
    local sum=0
    local count=0
    local values=""

    for run in $(seq 1 "$NUM_RUNS"); do
        local file="$RESULTS_DIR/${name}${suffix}${run}.csv"
        local val
        val=$(extract_rps "$file" "$label")
        if [[ "$val" != "N/A" && -n "$val" ]]; then
            sum=$(echo "$sum + $val" | bc 2>/dev/null || echo "$sum")
            count=$((count + 1))
            values="$values $val"
        fi
    done

    if [[ $count -eq 0 ]]; then
        echo "N/A|N/A"
        return
    fi

    local mean
    mean=$(echo "scale=2; $sum / $count" | bc 2>/dev/null || echo "N/A")

    local stddev="0"
    if [[ $count -gt 1 ]]; then
        local sq_sum=0
        for v in $values; do
            sq_sum=$(echo "$sq_sum + ($v - $mean) * ($v - $mean)" | bc 2>/dev/null || echo "$sq_sum")
        done
        stddev=$(echo "scale=2; sqrt($sq_sum / ($count - 1))" | bc 2>/dev/null || echo "0")
    fi

    # CI95 = 1.96 * stddev / sqrt(N)
    local ci95="0"
    if [[ $count -gt 1 && "$stddev" != "0" ]]; then
        ci95=$(echo "scale=2; 1.96 * $stddev / sqrt($count)" | bc 2>/dev/null || echo "0")
    fi

    echo "${mean}|${stddev}|${ci95}"
}

average_csv_field() {
    local name=$1
    local label=$2
    local suffix=$3
    local field=$4
    local sum=0
    local count=0

    for run in $(seq 1 "$NUM_RUNS"); do
        local file="$RESULTS_DIR/${name}${suffix}${run}.csv"
        local val
        val=$(extract_latency "$file" "$label" "$field")
        if [[ "$val" != "N/A" && -n "$val" ]]; then
            sum=$(echo "$sum + $val" | bc 2>/dev/null || echo "$sum")
            count=$((count + 1))
        fi
    done

    if [[ $count -eq 0 ]]; then
        echo "N/A"
        return
    fi

    echo "scale=3; $sum / $count" | bc 2>/dev/null || echo "N/A"
}

detect_result_suffix() {
    local test=$1
    local name
    local sample

    for name in $SERVERS; do
        sample=$(extract_rps "$RESULTS_DIR/${name}_run1.csv" "$test")
        if [[ "$sample" != "N/A" && -n "$sample" ]]; then
            echo "_run"
            return
        fi

        sample=$(extract_rps "$RESULTS_DIR/${name}_custom_run1.csv" "$test")
        if [[ "$sample" != "N/A" && -n "$sample" ]]; then
            echo "_custom_run"
            return
        fi
    done

    echo ""
}

# Assemble final data into files
mkdir -p "$RESULTS_DIR/final"
ALL_TESTS_FILE="$RESULTS_DIR/all_tests.txt"
: > "$ALL_TESTS_FILE"

# Standard tests
collect_result_rows "_run1.csv" | while IFS= read -r test; do
    [[ -z "$test" ]] && continue
    echo "$test" >> "$ALL_TESTS_FILE"
    for name in $SERVERS; do
        result=$(average_rps "$name" "$test" "_run")
        kv_set "rps__${test}__${name}" "${result%%|*}"
        local_rest="${result#*|}"
        kv_set "sd__${test}__${name}" "${local_rest%%|*}"
        kv_set "ci95__${test}__${name}" "${local_rest##*|}"
    done
done

# Custom command tests
if [[ "$RUN_CUSTOM" == true ]]; then
    collect_result_rows "_custom_run1.csv" | while IFS= read -r test; do
        [[ -z "$test" ]] && continue
        echo "$test" >> "$ALL_TESTS_FILE"
        for name in $SERVERS; do
            result=$(average_rps "$name" "$test" "_custom_run")
            kv_set "rps__${test}__${name}" "${result%%|*}"
            local_rest="${result#*|}"
            kv_set "sd__${test}__${name}" "${local_rest%%|*}"
            kv_set "ci95__${test}__${name}" "${local_rest##*|}"
        done
    done
fi

if [[ "$RUN_MEMTIER" == true ]]; then
    for thread_count in $(memtier_threads_list); do
        for row in Sets Gets Totals; do
            for name in $SERVERS; do
                result=$(average_memtier_field_with_stats "$name" "$thread_count" "$row" 2)
                kv_set "memtier_ops__${row}__${thread_count}__${name}" "${result%%|*}"
                local_rest="${result#*|}"
                kv_set "memtier_sd__${row}__${thread_count}__${name}" "${local_rest%%|*}"
                kv_set "memtier_ci95__${row}__${thread_count}__${name}" "${local_rest##*|}"
                kv_set "memtier_hits__${row}__${thread_count}__${name}" "$(average_memtier_field "$name" "$thread_count" "$row" 3)"
                kv_set "memtier_misses__${row}__${thread_count}__${name}" "$(average_memtier_field "$name" "$thread_count" "$row" 4)"
                kv_set "memtier_avg__${row}__${thread_count}__${name}" "$(average_memtier_field "$name" "$thread_count" "$row" 5)"
                kv_set "memtier_p50__${row}__${thread_count}__${name}" "$(average_memtier_field "$name" "$thread_count" "$row" 6)"
                kv_set "memtier_p99__${row}__${thread_count}__${name}" "$(average_memtier_field "$name" "$thread_count" "$row" 7)"
                kv_set "memtier_p999__${row}__${thread_count}__${name}" "$(average_memtier_field "$name" "$thread_count" "$row" 8)"
            done
        done
    done
fi

# ── Print terminal table ──
echo ""
if [[ "$NATIVE_MODE" == true ]]; then
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║                    Results (requests/sec)                     ║"
    if [[ $NUM_RUNS -gt 1 ]]; then
        printf "║                    Averaged over %d runs                      ║\n" "$NUM_RUNS"
    fi
    echo "╠══════════════════╦═══════════════╦═══════════════╦════════════╣"
    printf "║ %-16s ║ %13s ║ %13s ║ %8s   ║\n" "Test" "Vortex" "Redis" "vs Redis"
    echo "╠══════════════════╬═══════════════╬═══════════════╬════════════╣"

    while IFS= read -r test; do
        [[ -z "$test" ]] && continue
        v_rps=$(kv_get "rps__${test}__vortex"); [[ -z "$v_rps" ]] && v_rps="N/A"
        r_rps=$(kv_get "rps__${test}__redis"); [[ -z "$r_rps" ]] && r_rps="N/A"
        vs_redis=$(calc_ratio "$v_rps" "$r_rps")
        [[ "$vs_redis" != "N/A" && "$vs_redis" != "inf" ]] && vs_redis="${vs_redis}x"

        printf "║ %-16s ║ %13s ║ %13s ║ %8s   ║\n" \
            "$test" \
            "$(format_number "$v_rps")" \
            "$(format_number "$r_rps")" \
            "$vs_redis"
    done < "$ALL_TESTS_FILE"

    echo "╚══════════════════╩═══════════════╩═══════════════╩════════════╝"
else
    echo "╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗"
    echo "║                                    Results (requests/sec)                                                  ║"
    if [[ $NUM_RUNS -gt 1 ]]; then
        printf "║                              Averaged over %d runs                                                        ║\n" "$NUM_RUNS"
    fi
    echo "╠══════════════════╦═══════════════╦═══════════════╦═══════════════╦═══════════════╦════════╦════════╦════════╣"
    printf "║ %-16s ║ %13s ║ %13s ║ %13s ║ %13s ║ %6s ║ %6s ║ %6s ║\n" \
        "Test" "Vortex" "Redis" "Dragonfly" "Valkey" "vs Red" "vs Drg" "vs Val"
    echo "╠══════════════════╬═══════════════╬═══════════════╬═══════════════╬═══════════════╬════════╬════════╬════════╣"

    while IFS= read -r test; do
        [[ -z "$test" ]] && continue
        v_rps=$(kv_get "rps__${test}__vortex")
        r_rps=$(kv_get "rps__${test}__redis")
        d_rps=$(kv_get "rps__${test}__dragonfly")
        k_rps=$(kv_get "rps__${test}__valkey")
        [[ -z "$v_rps" ]] && v_rps="N/A"
        [[ -z "$r_rps" ]] && r_rps="N/A"
        [[ -z "$d_rps" ]] && d_rps="N/A"
        [[ -z "$k_rps" ]] && k_rps="N/A"

        vs_redis=$(calc_ratio "$v_rps" "$r_rps")
        vs_dragon=$(calc_ratio "$v_rps" "$d_rps")
        vs_valkey=$(calc_ratio "$v_rps" "$k_rps")

        [[ "$vs_redis" != "N/A" && "$vs_redis" != "inf" ]] && vs_redis="${vs_redis}x"
        [[ "$vs_dragon" != "N/A" && "$vs_dragon" != "inf" ]] && vs_dragon="${vs_dragon}x"
        [[ "$vs_valkey" != "N/A" && "$vs_valkey" != "inf" ]] && vs_valkey="${vs_valkey}x"

        printf "║ %-16s ║ %13s ║ %13s ║ %13s ║ %13s ║ %6s ║ %6s ║ %6s ║\n" \
            "$test" \
            "$(format_number "$v_rps")" \
            "$(format_number "$r_rps")" \
            "$(format_number "$d_rps")" \
            "$(format_number "$k_rps")" \
            "$vs_redis" "$vs_dragon" "$vs_valkey"
    done < "$ALL_TESTS_FILE"

    echo "╚══════════════════╩═══════════════╩═══════════════╩═══════════════╩═══════════════╩════════╩════════╩════════╝"
fi

if [[ "$RUN_MEMTIER" == true ]]; then
    echo ""
    echo "Memtier Mixed Workload — Totals (ops/sec)"
    echo "  ratio=${MEMTIER_RATIO} requests/client=${MEMTIER_REQUESTS} clients/thread=${MEMTIER_CLIENTS} pipeline=${MEMTIER_PIPELINE} data-size=${MEMTIER_DATA_SIZE}B"

    if [[ "$NATIVE_MODE" == true ]]; then
        printf "%-8s | %-13s | %-13s | %-10s\n" "Threads" "Vortex" "Redis" "vs Redis"
        printf "%-8s-+-%-13s-+-%-13s-+-%-10s\n" "--------" "-------------" "-------------" "----------"
        for thread_count in $(memtier_threads_list); do
            v_ops=$(kv_get "memtier_ops__Totals__${thread_count}__vortex"); [[ -z "$v_ops" ]] && v_ops="N/A"
            r_ops=$(kv_get "memtier_ops__Totals__${thread_count}__redis"); [[ -z "$r_ops" ]] && r_ops="N/A"
            vs_redis=$(calc_ratio "$v_ops" "$r_ops")
            [[ "$vs_redis" != "N/A" && "$vs_redis" != "inf" ]] && vs_redis="${vs_redis}x"
            printf "%-8s | %-13s | %-13s | %-10s\n" \
                "${thread_count}T" \
                "$(format_number "$v_ops")" \
                "$(format_number "$r_ops")" \
                "$vs_redis"
        done
    else
        printf "%-8s | %-13s | %-13s | %-13s | %-13s | %-10s | %-10s | %-10s\n" \
            "Threads" "Vortex" "Redis" "Dragonfly" "Valkey" "vs Redis" "vs Drg" "vs Val"
        printf "%-8s-+-%-13s-+-%-13s-+-%-13s-+-%-13s-+-%-10s-+-%-10s-+-%-10s\n" \
            "--------" "-------------" "-------------" "-------------" "-------------" "----------" "----------" "----------"
        for thread_count in $(memtier_threads_list); do
            v_ops=$(kv_get "memtier_ops__Totals__${thread_count}__vortex"); [[ -z "$v_ops" ]] && v_ops="N/A"
            r_ops=$(kv_get "memtier_ops__Totals__${thread_count}__redis"); [[ -z "$r_ops" ]] && r_ops="N/A"
            d_ops=$(kv_get "memtier_ops__Totals__${thread_count}__dragonfly"); [[ -z "$d_ops" ]] && d_ops="N/A"
            k_ops=$(kv_get "memtier_ops__Totals__${thread_count}__valkey"); [[ -z "$k_ops" ]] && k_ops="N/A"
            vs_redis=$(calc_ratio "$v_ops" "$r_ops")
            vs_dragon=$(calc_ratio "$v_ops" "$d_ops")
            vs_valkey=$(calc_ratio "$v_ops" "$k_ops")
            [[ "$vs_redis" != "N/A" && "$vs_redis" != "inf" ]] && vs_redis="${vs_redis}x"
            [[ "$vs_dragon" != "N/A" && "$vs_dragon" != "inf" ]] && vs_dragon="${vs_dragon}x"
            [[ "$vs_valkey" != "N/A" && "$vs_valkey" != "inf" ]] && vs_valkey="${vs_valkey}x"
            printf "%-8s | %-13s | %-13s | %-13s | %-13s | %-10s | %-10s | %-10s\n" \
                "${thread_count}T" \
                "$(format_number "$v_ops")" \
                "$(format_number "$r_ops")" \
                "$(format_number "$d_ops")" \
                "$(format_number "$k_ops")" \
                "$vs_redis" "$vs_dragon" "$vs_valkey"
        done
    fi
fi

# ── JSON output ──
if [[ "$OUTPUT_JSON" == true ]]; then
    mkdir -p "$BENCHMARKS_DIR"
    JSON_FILE="$BENCHMARKS_DIR/results-${TIMESTAMP}.json"

    {
        echo "{"
        echo "  \"timestamp\": \"$TIMESTAMP\","
        echo "  \"config\": {"
        echo "    \"requests\": $REQUESTS,"
        echo "    \"clients\": $CLIENTS,"
        echo "    \"pipeline\": $PIPELINE,"
        echo "    \"runs\": $NUM_RUNS,"
        echo "    \"mode\": \"$(if [[ "$NATIVE_MODE" == true ]]; then echo "native"; elif [[ "$DOCKER_ALL" == true ]]; then echo "docker-all"; else echo "mixed"; fi)\""
        echo "  },"
        echo "  \"results\": ["

        first_entry=true
        while IFS= read -r test; do
            [[ -z "$test" ]] && continue
            [[ "$first_entry" == true ]] && first_entry=false || echo "    ,"
            v_rps=$(kv_get "rps__${test}__vortex"); [[ -z "$v_rps" ]] && v_rps="N/A"
            r_rps=$(kv_get "rps__${test}__redis"); [[ -z "$r_rps" ]] && r_rps="N/A"
            v_sd=$(kv_get "sd__${test}__vortex"); [[ -z "$v_sd" ]] && v_sd="0"
            r_sd=$(kv_get "sd__${test}__redis"); [[ -z "$r_sd" ]] && r_sd="0"
            v_ci=$(kv_get "ci95__${test}__vortex"); [[ -z "$v_ci" ]] && v_ci="0"
            r_ci=$(kv_get "ci95__${test}__redis"); [[ -z "$r_ci" ]] && r_ci="0"
            vs_redis=$(calc_ratio "$v_rps" "$r_rps")

            echo "    {"
            echo "      \"test\": \"$test\","
            echo "      \"rps\": {"
            echo "        \"vortex\": $(json_value "$v_rps"),"
            echo "        \"redis\": $(json_value "$r_rps")"

            if [[ "$NATIVE_MODE" != true ]]; then
                d_rps=$(kv_get "rps__${test}__dragonfly"); [[ -z "$d_rps" ]] && d_rps="N/A"
                k_rps=$(kv_get "rps__${test}__valkey"); [[ -z "$k_rps" ]] && k_rps="N/A"
                echo "        ,\"dragonfly\": $(json_value "$d_rps")"
                echo "        ,\"valkey\": $(json_value "$k_rps")"
            fi

            echo "      },"
            echo "      \"stddev\": {"
            echo "        \"vortex\": $(json_value "$v_sd"),"
            echo "        \"redis\": $(json_value "$r_sd")"

            if [[ "$NATIVE_MODE" != true ]]; then
                d_sd=$(kv_get "sd__${test}__dragonfly"); [[ -z "$d_sd" ]] && d_sd="0"
                k_sd=$(kv_get "sd__${test}__valkey"); [[ -z "$k_sd" ]] && k_sd="0"
                echo "        ,\"dragonfly\": $(json_value "$d_sd")"
                echo "        ,\"valkey\": $(json_value "$k_sd")"
            fi

            echo "      },"
            echo "      \"ci95\": {"
            echo "        \"vortex\": $(json_value "$v_ci"),"
            echo "        \"redis\": $(json_value "$r_ci")"

            if [[ "$NATIVE_MODE" != true ]]; then
                d_ci=$(kv_get "ci95__${test}__dragonfly"); [[ -z "$d_ci" ]] && d_ci="0"
                k_ci=$(kv_get "ci95__${test}__valkey"); [[ -z "$k_ci" ]] && k_ci="0"
                echo "        ,\"dragonfly\": $(json_value "$d_ci")"
                echo "        ,\"valkey\": $(json_value "$k_ci")"
            fi

            echo "      },"
            echo "      \"ratios\": {"
            echo "        \"vs_redis\": $(json_value "$vs_redis")"

            if [[ "$NATIVE_MODE" != true ]]; then
                vs_dragon=$(calc_ratio "$v_rps" "$d_rps")
                vs_valkey=$(calc_ratio "$v_rps" "$k_rps")
                echo "        ,\"vs_dragonfly\": $(json_value "$vs_dragon")"
                echo "        ,\"vs_valkey\": $(json_value "$vs_valkey")"
            fi

            echo "      }"
            echo "    }"
        done < "$ALL_TESTS_FILE"

        echo "  ]"
        if [[ "$RUN_MEMTIER" == true ]]; then
            echo "  ,\"memtier\": {"
            echo "    \"config\": {"
            echo "      \"threads\": [$(echo "$MEMTIER_THREADS" | sed 's/,/, /g')],"
            echo "      \"requests_per_client\": $MEMTIER_REQUESTS,"
            echo "      \"clients_per_thread\": $MEMTIER_CLIENTS,"
            echo "      \"pipeline\": $MEMTIER_PIPELINE,"
            echo "      \"data_size\": $MEMTIER_DATA_SIZE,"
            echo "      \"ratio\": \"$MEMTIER_RATIO\","
            echo "      \"key_pattern\": \"G:G\","
            echo "      \"key_minimum\": $MEMTIER_KEY_MINIMUM,"
            echo "      \"key_maximum\": $MEMTIER_KEY_MAXIMUM,"
            echo "      \"key_median\": $MEMTIER_KEY_MEDIAN,"
            echo "      \"key_stddev\": $MEMTIER_KEY_STDDEV"
            echo "    },"
            echo "    \"results\": ["

            first_thread=true
            for thread_count in $(memtier_threads_list); do
                [[ "$first_thread" == true ]] && first_thread=false || echo "      ,"
                echo "      {"
                echo "        \"threads\": $thread_count,"

                for row in Sets Gets Totals; do
                    row_key=$(echo "$row" | tr 'A-Z' 'a-z')
                    echo "        \"$row_key\": {"
                    first_server=true
                    for name in $SERVERS; do
                        [[ "$first_server" == true ]] && first_server=false || echo "          ,"
                        ops=$(kv_get "memtier_ops__${row}__${thread_count}__${name}"); [[ -z "$ops" ]] && ops="N/A"
                        hits=$(kv_get "memtier_hits__${row}__${thread_count}__${name}"); [[ -z "$hits" ]] && hits="N/A"
                        misses=$(kv_get "memtier_misses__${row}__${thread_count}__${name}"); [[ -z "$misses" ]] && misses="N/A"
                        avg=$(kv_get "memtier_avg__${row}__${thread_count}__${name}"); [[ -z "$avg" ]] && avg="N/A"
                        p50=$(kv_get "memtier_p50__${row}__${thread_count}__${name}"); [[ -z "$p50" ]] && p50="N/A"
                        p99=$(kv_get "memtier_p99__${row}__${thread_count}__${name}"); [[ -z "$p99" ]] && p99="N/A"
                        p999=$(kv_get "memtier_p999__${row}__${thread_count}__${name}"); [[ -z "$p999" ]] && p999="N/A"
                        sd=$(kv_get "memtier_sd__${row}__${thread_count}__${name}"); [[ -z "$sd" ]] && sd="N/A"
                        ci=$(kv_get "memtier_ci95__${row}__${thread_count}__${name}"); [[ -z "$ci" ]] && ci="N/A"
                        echo "          \"$name\": {"
                        echo "            \"ops_sec\": $(json_value "$ops"),"
                        echo "            \"hits_sec\": $(json_value "$hits"),"
                        echo "            \"misses_sec\": $(json_value "$misses"),"
                        echo "            \"avg_latency_ms\": $(json_value "$avg"),"
                        echo "            \"p50_latency_ms\": $(json_value "$p50"),"
                        echo "            \"p99_latency_ms\": $(json_value "$p99"),"
                        echo "            \"p999_latency_ms\": $(json_value "$p999"),"
                        echo "            \"stddev_ops_sec\": $(json_value "$sd"),"
                        echo "            \"ci95_ops_sec\": $(json_value "$ci")"
                        echo "          }"
                    done
                    echo "        },"
                done

                v_ops=$(kv_get "memtier_ops__Totals__${thread_count}__vortex"); [[ -z "$v_ops" ]] && v_ops="N/A"
                r_ops=$(kv_get "memtier_ops__Totals__${thread_count}__redis"); [[ -z "$r_ops" ]] && r_ops="N/A"
                echo "        \"ratios\": {"
                echo "          \"vs_redis\": $(json_value "$(calc_ratio "$v_ops" "$r_ops")")"
                if [[ "$NATIVE_MODE" != true ]]; then
                    d_ops=$(kv_get "memtier_ops__Totals__${thread_count}__dragonfly"); [[ -z "$d_ops" ]] && d_ops="N/A"
                    k_ops=$(kv_get "memtier_ops__Totals__${thread_count}__valkey"); [[ -z "$k_ops" ]] && k_ops="N/A"
                    echo "          ,\"vs_dragonfly\": $(json_value "$(calc_ratio "$v_ops" "$d_ops")")"
                    echo "          ,\"vs_valkey\": $(json_value "$(calc_ratio "$v_ops" "$k_ops")")"
                fi
                echo "        }"
                echo "      }"
            done

            echo "    ]"
            echo "  }"
        fi
        echo "}"
    } > "$JSON_FILE"

    echo ""
    log_info "JSON results saved to: $JSON_FILE"
fi

# ── Markdown output ──
if [[ "$OUTPUT_MARKDOWN" == true ]]; then
    mkdir -p "$BENCHMARKS_DIR"
    MD_FILE="$BENCHMARKS_DIR/results-${TIMESTAMP}.md"

    {
        echo "# VortexDB Competitive Benchmark Results"
        echo ""
        echo "**Date:** $(date -u +%Y-%m-%d)"
        echo "**Config:** ${REQUESTS} requests, ${CLIENTS} clients, pipeline ${PIPELINE}"
        [[ $NUM_RUNS -gt 1 ]] && echo "**Runs:** ${NUM_RUNS} (mean values)"
        if [[ "$DOCKER_ALL" == true ]]; then
            echo "**Mode:** All databases containerized with identical resource limits"
            echo "**Resources per container:** ${DOCKER_CPUS} CPUs, ${DOCKER_MEMORY} RAM"
        elif [[ "$NATIVE_MODE" == true ]]; then
            echo "**Mode:** Native (both VortexDB and Redis running natively, no Docker)"
        fi

        if [[ "$NATIVE_MODE" == true ]]; then
            echo ""
            echo "## Throughput (requests/sec)"
            echo ""
            echo "| Test | VortexDB | Redis | vs Redis |"
            echo "|------|----------|-------|----------|"

            while IFS= read -r test; do
                [[ -z "$test" ]] && continue
                v_rps=$(kv_get "rps__${test}__vortex"); [[ -z "$v_rps" ]] && v_rps="N/A"
                r_rps=$(kv_get "rps__${test}__redis"); [[ -z "$r_rps" ]] && r_rps="N/A"
                vs_redis=$(calc_ratio "$v_rps" "$r_rps")
                [[ "$vs_redis" != "N/A" && "$vs_redis" != "inf" ]] && vs_redis="**${vs_redis}x**"
                echo "| $test | $(format_number "$v_rps") | $(format_number "$r_rps") | $vs_redis |"
            done < "$ALL_TESTS_FILE"
        else
            echo "**Versions:** Redis ${IMAGE_redis}, Dragonfly ${IMAGE_dragonfly}, Valkey ${IMAGE_valkey}"
            echo ""
            echo "## Throughput (requests/sec)"
            echo ""
            echo "| Test | VortexDB | Redis 8 | Dragonfly | Valkey 9 | vs Redis | vs Dragonfly | vs Valkey |"
            echo "|------|----------|---------|-----------|----------|----------|--------------|-----------|"

            while IFS= read -r test; do
                [[ -z "$test" ]] && continue
                v_rps=$(kv_get "rps__${test}__vortex"); [[ -z "$v_rps" ]] && v_rps="N/A"
                r_rps=$(kv_get "rps__${test}__redis"); [[ -z "$r_rps" ]] && r_rps="N/A"
                d_rps=$(kv_get "rps__${test}__dragonfly"); [[ -z "$d_rps" ]] && d_rps="N/A"
                k_rps=$(kv_get "rps__${test}__valkey"); [[ -z "$k_rps" ]] && k_rps="N/A"
                vs_redis=$(calc_ratio "$v_rps" "$r_rps")
                vs_dragon=$(calc_ratio "$v_rps" "$d_rps")
                vs_valkey=$(calc_ratio "$v_rps" "$k_rps")

                [[ "$vs_redis" != "N/A" && "$vs_redis" != "inf" ]] && vs_redis="**${vs_redis}x**"
                [[ "$vs_dragon" != "N/A" && "$vs_dragon" != "inf" ]] && vs_dragon="**${vs_dragon}x**"
                [[ "$vs_valkey" != "N/A" && "$vs_valkey" != "inf" ]] && vs_valkey="**${vs_valkey}x**"

                echo "| $test | $(format_number "$v_rps") | $(format_number "$r_rps") | $(format_number "$d_rps") | $(format_number "$k_rps") | $vs_redis | $vs_dragon | $vs_valkey |"
            done < "$ALL_TESTS_FILE"
        fi

        if [[ "$RUN_MEMTIER" == true ]]; then
            echo ""
            echo "## Memtier Mixed Workload"
            echo ""
            echo "Mixed Gaussian workload driven by \`memtier_benchmark\` and published alongside the point-command \`redis-benchmark\` suite."
            echo ""
            echo "- Ratio: ${MEMTIER_RATIO}"
            echo "- Requests per client: ${MEMTIER_REQUESTS}"
            echo "- Clients per thread: ${MEMTIER_CLIENTS}"
            echo "- Pipeline: ${MEMTIER_PIPELINE}"
            echo "- Data size: ${MEMTIER_DATA_SIZE} bytes"
            echo "- Thread sweep: ${MEMTIER_THREADS}"

            if [[ "$NATIVE_MODE" == true ]]; then
                echo ""
                echo "### Totals (ops/sec)"
                echo ""
                echo "| Threads | VortexDB | Redis 8 | vs Redis |"
                echo "|---------|----------|---------|----------|"
                for thread_count in $(memtier_threads_list); do
                    v_ops=$(kv_get "memtier_ops__Totals__${thread_count}__vortex"); [[ -z "$v_ops" ]] && v_ops="N/A"
                    r_ops=$(kv_get "memtier_ops__Totals__${thread_count}__redis"); [[ -z "$r_ops" ]] && r_ops="N/A"
                    vs_redis=$(calc_ratio "$v_ops" "$r_ops")
                    [[ "$vs_redis" != "N/A" && "$vs_redis" != "inf" ]] && vs_redis="**${vs_redis}x**"
                    echo "| ${thread_count} | $(format_number "$v_ops") | $(format_number "$r_ops") | $vs_redis |"
                done

                echo ""
                echo "### Totals Latency (ms)"
                echo ""
                echo "| Threads | Vortex p50 | Vortex p99 | Vortex p99.9 | Redis p50 | Redis p99 | Redis p99.9 |"
                echo "|---------|-----------:|-----------:|-------------:|----------:|----------:|------------:|"
                for thread_count in $(memtier_threads_list); do
                    v_p50=$(kv_get "memtier_p50__Totals__${thread_count}__vortex"); [[ -z "$v_p50" ]] && v_p50="N/A"
                    v_p99=$(kv_get "memtier_p99__Totals__${thread_count}__vortex"); [[ -z "$v_p99" ]] && v_p99="N/A"
                    v_p999=$(kv_get "memtier_p999__Totals__${thread_count}__vortex"); [[ -z "$v_p999" ]] && v_p999="N/A"
                    r_p50=$(kv_get "memtier_p50__Totals__${thread_count}__redis"); [[ -z "$r_p50" ]] && r_p50="N/A"
                    r_p99=$(kv_get "memtier_p99__Totals__${thread_count}__redis"); [[ -z "$r_p99" ]] && r_p99="N/A"
                    r_p999=$(kv_get "memtier_p999__Totals__${thread_count}__redis"); [[ -z "$r_p999" ]] && r_p999="N/A"
                    echo "| ${thread_count} | ${v_p50} | ${v_p99} | ${v_p999} | ${r_p50} | ${r_p99} | ${r_p999} |"
                done
            else
                echo ""
                echo "### Totals (ops/sec)"
                echo ""
                echo "| Threads | VortexDB | Redis 8 | Dragonfly | Valkey 9 | vs Redis | vs Dragonfly | vs Valkey |"
                echo "|---------|----------|---------|-----------|----------|----------|--------------|-----------|"
                for thread_count in $(memtier_threads_list); do
                    v_ops=$(kv_get "memtier_ops__Totals__${thread_count}__vortex"); [[ -z "$v_ops" ]] && v_ops="N/A"
                    r_ops=$(kv_get "memtier_ops__Totals__${thread_count}__redis"); [[ -z "$r_ops" ]] && r_ops="N/A"
                    d_ops=$(kv_get "memtier_ops__Totals__${thread_count}__dragonfly"); [[ -z "$d_ops" ]] && d_ops="N/A"
                    k_ops=$(kv_get "memtier_ops__Totals__${thread_count}__valkey"); [[ -z "$k_ops" ]] && k_ops="N/A"
                    vs_redis=$(calc_ratio "$v_ops" "$r_ops")
                    vs_dragon=$(calc_ratio "$v_ops" "$d_ops")
                    vs_valkey=$(calc_ratio "$v_ops" "$k_ops")
                    [[ "$vs_redis" != "N/A" && "$vs_redis" != "inf" ]] && vs_redis="**${vs_redis}x**"
                    [[ "$vs_dragon" != "N/A" && "$vs_dragon" != "inf" ]] && vs_dragon="**${vs_dragon}x**"
                    [[ "$vs_valkey" != "N/A" && "$vs_valkey" != "inf" ]] && vs_valkey="**${vs_valkey}x**"
                    echo "| ${thread_count} | $(format_number "$v_ops") | $(format_number "$r_ops") | $(format_number "$d_ops") | $(format_number "$k_ops") | $vs_redis | $vs_dragon | $vs_valkey |"
                done

                echo ""
                echo "### Totals Latency (ms)"
                echo ""
                echo "| Threads | Vortex p50 | Vortex p99 | Redis p50 | Redis p99 | Dragonfly p50 | Dragonfly p99 | Valkey p50 | Valkey p99 |"
                echo "|---------|-----------:|-----------:|----------:|----------:|--------------:|--------------:|-----------:|-----------:|"
                for thread_count in $(memtier_threads_list); do
                    v_p50=$(kv_get "memtier_p50__Totals__${thread_count}__vortex"); [[ -z "$v_p50" ]] && v_p50="N/A"
                    v_p99=$(kv_get "memtier_p99__Totals__${thread_count}__vortex"); [[ -z "$v_p99" ]] && v_p99="N/A"
                    r_p50=$(kv_get "memtier_p50__Totals__${thread_count}__redis"); [[ -z "$r_p50" ]] && r_p50="N/A"
                    r_p99=$(kv_get "memtier_p99__Totals__${thread_count}__redis"); [[ -z "$r_p99" ]] && r_p99="N/A"
                    d_p50=$(kv_get "memtier_p50__Totals__${thread_count}__dragonfly"); [[ -z "$d_p50" ]] && d_p50="N/A"
                    d_p99=$(kv_get "memtier_p99__Totals__${thread_count}__dragonfly"); [[ -z "$d_p99" ]] && d_p99="N/A"
                    k_p50=$(kv_get "memtier_p50__Totals__${thread_count}__valkey"); [[ -z "$k_p50" ]] && k_p50="N/A"
                    k_p99=$(kv_get "memtier_p99__Totals__${thread_count}__valkey"); [[ -z "$k_p99" ]] && k_p99="N/A"
                    echo "| ${thread_count} | ${v_p50} | ${v_p99} | ${r_p50} | ${r_p99} | ${d_p50} | ${d_p99} | ${k_p50} | ${k_p99} |"
                done
            fi
        fi

        echo ""
        echo "---"
        echo ""
        echo "*Generated by \`scripts/compare.sh v2\` — [VortexDB](https://github.com/kevincaicedo/vortex)*"
    } > "$MD_FILE"

    echo ""
    log_info "Markdown results saved to: $MD_FILE"
fi

# ── Latency report ──
if [[ "$CAPTURE_LATENCY" == true ]]; then
    echo ""
    echo "--- Latency Percentiles (ms) ---"
    echo ""

    printf "%-16s " "Test"
    for name in $SERVERS; do
        printf "| %-36s " "$name (p50/p95/p99/p999)"
    done
    echo ""
    printf "%-16s " "----------------"
    for name in $SERVERS; do
        printf "| %-36s " "------------------------------------"
    done
    echo ""

    while IFS= read -r test; do
        [[ -z "$test" ]] && continue
        test_suffix=$(detect_result_suffix "$test")
        printf "%-16s " "$test"
        for name in $SERVERS; do
            if [[ -n "$test_suffix" ]]; then
                p50=$(average_csv_field "$name" "$test" "$test_suffix" 5)
                p95=$(average_csv_field "$name" "$test" "$test_suffix" 6)
                p99=$(average_csv_field "$name" "$test" "$test_suffix" 7)
                p999=$(average_csv_field "$name" "$test" "$test_suffix" 8)
            else
                p50="N/A"
                p95="N/A"
                p99="N/A"
                p999="N/A"
            fi
            printf "| %7s / %7s / %7s / %7s " "$p50" "$p95" "$p99" "$p999"
        done
        echo ""
    done < "$ALL_TESTS_FILE"

    # Append latency to markdown if enabled
    if [[ "$OUTPUT_MARKDOWN" == true && -f "${MD_FILE:-}" ]]; then
        {
            echo ""
            echo "## Latency Percentiles (ms)"
            echo ""
            printf '%s' "| Test |"
            for name in $SERVERS; do printf " %s p50 | %s p95 | %s p99 | %s p999 |" "$name" "$name" "$name" "$name"; done
            echo ""
            printf '%s' "|------|"
            for name in $SERVERS; do printf '%s' "------:|------:|------:|------:|"; done
            echo ""

            while IFS= read -r test; do
                [[ -z "$test" ]] && continue
                test_suffix=$(detect_result_suffix "$test")
                printf "| %s |" "$test"
                for name in $SERVERS; do
                    if [[ -n "$test_suffix" ]]; then
                        p50=$(average_csv_field "$name" "$test" "$test_suffix" 5)
                        p95=$(average_csv_field "$name" "$test" "$test_suffix" 6)
                        p99=$(average_csv_field "$name" "$test" "$test_suffix" 7)
                        p999=$(average_csv_field "$name" "$test" "$test_suffix" 8)
                    else
                        p50="N/A"
                        p95="N/A"
                        p99="N/A"
                        p999="N/A"
                    fi
                    printf " %s | %s | %s | %s |" "$p50" "$p95" "$p99" "$p999"
                done
                echo ""
            done < "$ALL_TESTS_FILE"
        } >> "$MD_FILE"
        log_info "Latency data appended to: $MD_FILE"
    fi
fi

# ── Summary ──
echo ""
echo "--- Summary ---"
echo ""
total_tests=$(wc -l < "$ALL_TESTS_FILE" | tr -d ' ')
echo "  Tests run: $total_tests"
echo "  Runs per test: $NUM_RUNS"
[[ "$OUTPUT_JSON" == true ]] && echo "  JSON: ${JSON_FILE:-N/A}"
[[ "$OUTPUT_MARKDOWN" == true ]] && echo "  Markdown: ${MD_FILE:-N/A}"
echo ""

# Check >=2x Redis target
all_passing=true
while IFS= read -r test; do
    [[ -z "$test" ]] && continue
    v_rps=$(kv_get "rps__${test}__vortex"); [[ -z "$v_rps" ]] && v_rps="N/A"
    r_rps=$(kv_get "rps__${test}__redis"); [[ -z "$r_rps" ]] && r_rps="N/A"
    ratio=$(calc_ratio "$v_rps" "$r_rps")
    if [[ "$ratio" != "N/A" && "$ratio" != "inf" ]]; then
        passing=$(echo "$ratio >= 2.0" | bc 2>/dev/null || echo "0")
        if [[ "$passing" -ne 1 ]]; then
            log_warn "BELOW TARGET: $test — ${ratio}x Redis (target: >=2.0x)"
            all_passing=false
        fi
    fi
done < "$ALL_TESTS_FILE"

if [[ "$all_passing" == true ]]; then
    echo "  ✓ ALL tests >=2x Redis — target met!"
else
    echo "  ✗ Some tests below 2x Redis target — see warnings above"
fi

echo ""
log_info "Done."
