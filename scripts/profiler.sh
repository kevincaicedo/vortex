#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Vortex Profiler — profiling tool manager for vortex-server and engine targets
# ─────────────────────────────────────────────────────────────────────────────
#
# Self-contained orchestrator: detects OS, builds the profiling target,
# starts vortex-server or a standalone engine binary, runs native profilers,
# generates load internally when needed,
# and collects all artifacts into a timestamped session directory.
#
# Usage:  just profiler [mode flags] [options]
#
# See:    just profiler --help
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORIGINAL_ARGS=("$@")

# Source component scripts
source "${SCRIPTS_DIR}/profiler/common.sh"
source "${SCRIPTS_DIR}/profiler/host.sh"
source "${SCRIPTS_DIR}/profiler/bench.sh"
source "${SCRIPTS_DIR}/profiler/summary.sh"
source "${SCRIPTS_DIR}/profiler/build.sh"
source "${SCRIPTS_DIR}/profiler/server.sh"
source "${SCRIPTS_DIR}/profiler/cpu.sh"
source "${SCRIPTS_DIR}/profiler/c2c.sh"
source "${SCRIPTS_DIR}/profiler/memory.sh"
source "${SCRIPTS_DIR}/profiler/cache.sh"
source "${SCRIPTS_DIR}/profiler/criterion.sh"
source "${SCRIPTS_DIR}/profiler/check.sh"

# ── Usage ────────────────────────────────────────────────────────────────────
usage() {
    cat <<'EOF'
Vortex Profiler — profiling tool manager for vortex-server and engine targets

Usage: just profiler [mode flags] [options]

Modes (at least one required, combinable):
  --cpu              Full CPU profiling (flamegraph + perf stat + perf record)
    --scheduler        Scheduler-focused diagnostics with host context
        --lock-offcpu      Lock wait and off-CPU diagnostics with blocking classification
  --memory           Heap allocation profiling (heaptrack / massif / Instruments)
  --cache            Cache locality analysis (cachegrind)
    --c2c              Cache-line contention analysis (perf c2c on Linux)
    --aof-disk         AOF and disk-focused diagnostics with host context
    --network          Network-focused diagnostics with host context
  --all              Run cpu + memory + cache sequentially

Specific tools (only one, runs that tool alone):
  --flamegraph         Flamegraph SVG only
  --perf-stat          perf stat hardware counters only (Linux)
  --samply             Samply interactive profiler
  --instruments        Instruments Time Profiler (macOS)
  --heaptrack          Heaptrack allocation profiler
  --cachegrind         Cachegrind cache simulation
  --callgrind          Callgrind instruction counting + call graph
  --massif             Massif chronological heap snapshot

Criterion (standalone, no server):
  --criterion        Run Criterion micro-benchmarks
  --filter PATTERN   Benchmark filter pattern
  --package NAME     Cargo package (default: vortex-bench)
  --bench-target T   Cargo bench target name

Workload (how load is generated internally):
  --command CMDS     Commands for redis-benchmark (e.g. SET,GET,INCR)
  --duration SECS    Load duration in seconds (default: 15)
  --clients N        Number of parallel clients (default: 50)
  --manifest PATH    Profiling manifest YAML (see scripts/profiler/manifests/)
    --bench-manifest   Use a vortex_bench manifest as the load source
    --bench-request    Use a prior vortex_bench run-request JSON as the load source
    --compare-to PATH  Write a machine-readable comparison against an earlier session

Server configuration:
  --threads N        Server thread count (default: 4)
  --aof              Enable AOF persistence
  --maxmemory SIZE   Set max memory (e.g. 64mb, 1gb)
  --eviction POLICY  Set eviction policy (e.g. allkeys-lru)
    --io-backend KIND  Explicit Vortex I/O backend: auto, uring, or polling
    --ring-size N      io_uring submission queue size override
        --fixed-buffers N  Vortex fixed I/O buffer count override
    --sqpoll-idle-ms N SQPOLL idle timeout in milliseconds
  --host HOST        Bind address (default: 127.0.0.1)
  --port PORT        Bind port (default: 16379)
  --bin PATH         Use pre-built binary instead of building

Engine target:
    --engine-example NAME  Cargo example in vortex-engine to profile (for example: engine_probe)
    --engine-bin PATH      Use a pre-built engine binary instead of building an example
    --engine-args ARGS     Shell-quoted argument string forwarded to the engine binary
    --                    Treat the remaining arguments as engine-target arguments

Profiler tuning:
  --frequency N      Sampling frequency for perf/flamegraph (default: 99)

Diagnostics:
  --check            Show OS, available tools, binary status
    --dry-run          Write tool-check artifacts without starting a capture (currently for --c2c)

Environment:
    .env               If present at repo root (or scripts/profiler/.env), it is loaded automatically
    HOST_PASSWORD      Optional sudo password used for macOS profilers that require elevation

macOS note:
    profiling binary   Auto-signed with get-task-allow so Instruments can attach to vortex-server

Examples:
  just profiler --command SET,GET
    just profiler --scheduler --bench-manifest vortex-benchmark/manifests/examples/local-native-redis-benchmark.yaml
        just profiler --lock-offcpu --command SET --duration 10
  just profiler --cpu --command SET,GET --duration 20
  just profiler --flamegraph --command SET --threads 2
  just profiler --memory --command SET --duration 15
    just profiler --c2c --dry-run --command SET,GET --duration 10
  just profiler --cache --command SET --threads 1
    just profiler-engine --c2c --engine-example c2c_probe -- --variant unpadded --threads 4 --duration-seconds 3
  just profiler --callgrind --command SET --threads 1
  just profiler --all --command SET,GET
  just profiler --manifest scripts/profiler/manifests/cpu-set-heavy.yaml
    just profiler-engine --memory -- --workload set-inline-string --keys 1000000 --value-size 16 --shards 64
  just profiler --criterion --filter cmd_get_inline
  just profiler --check
EOF
    exit 0
}

# ── Defaults ─────────────────────────────────────────────────────────────────
MODE_CPU=false
MODE_SCHEDULER=false
MODE_LOCK_OFFCPU=false
MODE_MEMORY=false
MODE_CACHE=false
MODE_C2C=false
MODE_AOF_DISK=false
MODE_NETWORK=false
MODE_ALL=false
MODE_CRITERION=false
MODE_CHECK=false
DRY_RUN=false

# Specific tools
TOOL_FLAMEGRAPH=false
TOOL_PERF_STAT=false
TOOL_SAMPLY=false
TOOL_INSTRUMENTS=false
TOOL_HEAPTRACK=false
TOOL_CACHEGRIND=false
TOOL_CALLGRIND=false
TOOL_MASSIF=false

# Workload
COMMAND=""
DURATION=15
DURATION_SET_BY_CLI=false
CLIENTS=50
MANIFEST=""
COMPARE_TO=""

# Server
HOST="127.0.0.1"
PORT="16379"
THREADS=4
AOF=false
AOF_SET_BY_CLI=false
MAXMEMORY=""
EVICTION=""
IO_BACKEND=""
RING_SIZE=""
FIXED_BUFFERS=""
SQPOLL_IDLE_MS=""
BIN_OVERRIDE=""

# Target selection
PROFILER_TARGET_KIND="server"
ENGINE_EXAMPLE=""
ENGINE_BIN_OVERRIDE=""
ENGINE_ARGS_RAW=""
ENGINE_TARGET_ARGS=()

# Profiler tuning
FREQUENCY=99

# Criterion
CRITERION_PACKAGE="vortex-bench"
CRITERION_BENCH_TARGET=""
CRITERION_FILTER=""

# ── Argument parsing ─────────────────────────────────────────────────────────
[[ $# -eq 0 ]] && usage

while [[ $# -gt 0 ]]; do
    case "$1" in
        # Modes
        --cpu)          MODE_CPU=true;           shift ;;
        --scheduler)    MODE_SCHEDULER=true;     shift ;;
        --lock-offcpu)  MODE_LOCK_OFFCPU=true;   shift ;;
        --memory)       MODE_MEMORY=true;        shift ;;
        --cache)        MODE_CACHE=true;         shift ;;
        --c2c)          MODE_C2C=true;           shift ;;
        --aof-disk)     MODE_AOF_DISK=true;      shift ;;
        --network)      MODE_NETWORK=true;       shift ;;
        --all)          MODE_ALL=true;           shift ;;
        --criterion)    MODE_CRITERION=true;     shift ;;
        --check)        MODE_CHECK=true;         shift ;;
        --dry-run)      DRY_RUN=true;            shift ;;

        # Specific tools
        --flamegraph)   TOOL_FLAMEGRAPH=true;    shift ;;
        --perf-stat)    TOOL_PERF_STAT=true;     shift ;;
        --samply)       TOOL_SAMPLY=true;        shift ;;
        --instruments)  TOOL_INSTRUMENTS=true;   shift ;;
        --heaptrack)    TOOL_HEAPTRACK=true;     shift ;;
        --cachegrind)   TOOL_CACHEGRIND=true;    shift ;;
        --callgrind)    TOOL_CALLGRIND=true;     shift ;;
        --massif)       TOOL_MASSIF=true;        shift ;;

        # Workload
        --command)      COMMAND="$2";            shift 2 ;;
        --duration)     DURATION="$2"; DURATION_SET_BY_CLI=true; shift 2 ;;
        --clients)      CLIENTS="$2";            shift 2 ;;
        --manifest)     MANIFEST="$2";           shift 2 ;;
        --bench-manifest) BENCH_MANIFEST="$2";  shift 2 ;;
        --bench-request) BENCH_REQUEST="$2";    shift 2 ;;
        --compare-to)   COMPARE_TO="$2";         shift 2 ;;

        # Server
        --host)         HOST="$2";               shift 2 ;;
        --port)         PORT="$2";               shift 2 ;;
        --threads)      THREADS="$2";            shift 2 ;;
        --aof)          AOF=true; AOF_SET_BY_CLI=true; shift ;;
        --maxmemory)    MAXMEMORY="$2";          shift 2 ;;
        --eviction)     EVICTION="$2";           shift 2 ;;
        --io-backend)   IO_BACKEND="$2";         shift 2 ;;
        --ring-size)    RING_SIZE="$2";          shift 2 ;;
        --fixed-buffers) FIXED_BUFFERS="$2";     shift 2 ;;
        --sqpoll-idle-ms) SQPOLL_IDLE_MS="$2";   shift 2 ;;
        --bin)          BIN_OVERRIDE="$2";       shift 2 ;;
        --engine-example) PROFILER_TARGET_KIND="engine"; ENGINE_EXAMPLE="$2"; shift 2 ;;
        --engine-bin)   PROFILER_TARGET_KIND="engine"; ENGINE_BIN_OVERRIDE="$2"; shift 2 ;;
        --engine-args)  PROFILER_TARGET_KIND="engine"; ENGINE_ARGS_RAW="$2"; shift 2 ;;

        # Profiler tuning
        --frequency)    FREQUENCY="$2";          shift 2 ;;

        # Criterion
        --filter)       CRITERION_FILTER="$2";   shift 2 ;;
        --package)      CRITERION_PACKAGE="$2";  shift 2 ;;
        --bench-target) CRITERION_BENCH_TARGET="$2"; shift 2 ;;

        # Help
        -h|--help)      usage ;;

        --)
            shift
            if [[ "$PROFILER_TARGET_KIND" == "engine" ]]; then
                ENGINE_TARGET_ARGS=("$@")
                break
            fi
            fatal "Bare '--' is only supported for engine targets"
            ;;

        *)
            fatal "Unknown option: $1. Run 'just profiler --help' for usage."
            ;;
    esac
done

# ── Load manifest if provided ────────────────────────────────────────────────
if [[ -n "$MANIFEST" ]]; then
    if [[ ! -f "$MANIFEST" ]]; then
        fatal "Manifest not found: ${MANIFEST}"
    fi
    info "Loading manifest: ${MANIFEST}"
    eval "$(python3 "${SCRIPTS_DIR}/profiler/parse_manifest.py" "$MANIFEST")"

    # Apply manifest values (CLI flags take precedence — only fill empty fields)
    [[ -z "$COMMAND"   && -n "${MANIFEST_WORKLOAD_COMMAND:-}" ]]   && COMMAND="$MANIFEST_WORKLOAD_COMMAND"
    [[ "$DURATION" == 15 && -n "${MANIFEST_WORKLOAD_DURATION:-}" ]] && DURATION="$MANIFEST_WORKLOAD_DURATION"
    [[ "$CLIENTS" == 50 && -n "${MANIFEST_WORKLOAD_CLIENTS:-}" ]]  && CLIENTS="$MANIFEST_WORKLOAD_CLIENTS"
    [[ "$THREADS" == 4  && -n "${MANIFEST_SERVER_THREADS:-}" ]]    && THREADS="$MANIFEST_SERVER_THREADS"
    [[ -z "$MAXMEMORY" && -n "${MANIFEST_SERVER_MAXMEMORY:-}" ]]   && MAXMEMORY="$MANIFEST_SERVER_MAXMEMORY"
    [[ -z "$EVICTION"  && -n "${MANIFEST_SERVER_EVICTION:-}" ]]    && EVICTION="$MANIFEST_SERVER_EVICTION"
    [[ -z "${VORTEX_AOF_FSYNC:-}" && -n "${MANIFEST_SERVER_AOF_FSYNC:-}" ]] && export VORTEX_AOF_FSYNC="$MANIFEST_SERVER_AOF_FSYNC"
    [[ "$FREQUENCY" == 99 && -n "${MANIFEST_PROFILER_FREQUENCY:-}" ]] && FREQUENCY="$MANIFEST_PROFILER_FREQUENCY"
    if [[ "${MANIFEST_SERVER_AOF:-}" == "true" ]]; then AOF=true; fi

    # Apply manifest modes
    if [[ -n "${MANIFEST_MODES:-}" ]]; then
        IFS=',' read -ra _manifest_modes <<< "$MANIFEST_MODES"
        for _m in "${_manifest_modes[@]}"; do
            case "$_m" in
                cpu)              MODE_CPU=true ;;
                scheduler)        MODE_SCHEDULER=true ;;
                lock-offcpu)      MODE_LOCK_OFFCPU=true ;;
                memory)           MODE_MEMORY=true ;;
                cache)            MODE_CACHE=true ;;
                c2c)              MODE_C2C=true ;;
                aof-disk)         MODE_AOF_DISK=true ;;
                network)          MODE_NETWORK=true ;;
                all)              MODE_ALL=true ;;
                criterion)        MODE_CRITERION=true ;;
                flamegraph)       TOOL_FLAMEGRAPH=true ;;
                perf-stat)        TOOL_PERF_STAT=true ;;
                samply)           TOOL_SAMPLY=true ;;
                instruments)      TOOL_INSTRUMENTS=true ;;
                heaptrack)        TOOL_HEAPTRACK=true ;;
                cachegrind)       TOOL_CACHEGRIND=true ;;
                callgrind)        TOOL_CALLGRIND=true ;;
                massif)           TOOL_MASSIF=true ;;
            esac
        done
    fi
    ok "Manifest loaded: ${MANIFEST_NAME:-unknown} — ${MANIFEST_DESCRIPTION:-}"
fi

# ── Resolve: --all expands to cpu + memory + cache ───────────────────────────
if $MODE_ALL; then
    MODE_CPU=true
    MODE_MEMORY=true
    MODE_CACHE=true
fi

if $MODE_AOF_DISK && [[ "$AOF_SET_BY_CLI" != "true" ]] && ! benchmark_bridge_enabled; then
    AOF=true
fi

# ── Default mode: if only --command is given with no mode, default to --cpu ──
has_any_mode() {
    $MODE_CPU || $MODE_SCHEDULER || $MODE_LOCK_OFFCPU || $MODE_MEMORY || $MODE_CACHE || $MODE_C2C || $MODE_AOF_DISK || $MODE_NETWORK || $MODE_CRITERION || $MODE_CHECK || \
    $TOOL_FLAMEGRAPH || $TOOL_PERF_STAT || $TOOL_SAMPLY || $TOOL_INSTRUMENTS || \
    $TOOL_HEAPTRACK || $TOOL_CACHEGRIND || $TOOL_CALLGRIND || $TOOL_MASSIF
}

if ! has_any_mode; then
    if profiling_target_is_engine || [[ -n "$COMMAND" ]] || benchmark_bridge_enabled; then
        TOOL_FLAMEGRAPH=true
        info "No mode specified, defaulting to --flamegraph"
    else
        fatal "No mode specified. Run 'just profiler --help' for usage."
    fi
fi

if $DRY_RUN && ! $MODE_C2C && ! $MODE_CHECK; then
    fatal "--dry-run is currently supported only with --c2c or --check"
fi

if [[ "${HOST_SAMPLER_INTERVAL_SECONDS:-1}" =~ ^1(\.0+)?$ ]] && {
    $MODE_MEMORY || $MODE_CACHE || $TOOL_HEAPTRACK || $TOOL_CACHEGRIND || $TOOL_CALLGRIND || $TOOL_MASSIF;
}; then
    HOST_SAMPLER_INTERVAL_SECONDS="0.25"
    info "Using 0.25s host telemetry sampling for short-lived memory/cache profiling sessions"
fi

shell_join() {
    local rendered=""
    local arg
    for arg in "$@"; do
        printf -v rendered '%s%q ' "$rendered" "$arg"
    done
    printf '%s' "${rendered% }"
}

profiling_target_is_engine() {
    [[ "$PROFILER_TARGET_KIND" == "engine" ]]
}

parse_engine_args_string() {
    local raw="$1"
    has_cmd python3 || fatal "python3 is required to parse --engine-args"
    python3 - "$raw" <<'PY'
from __future__ import annotations

import shlex
import sys

for arg in shlex.split(sys.argv[1]):
    print(arg)
PY
}

engine_target_args_contain_flag() {
    local flag="$1"
    local arg

    for arg in "${ENGINE_TARGET_ARGS[@]}"; do
        if [[ "$arg" == "$flag" || "$arg" == "${flag}="* ]]; then
            return 0
        fi
    done

    return 1
}

engine_target_arg_value() {
    local flag="$1"
    local arg index next_index

    for index in "${!ENGINE_TARGET_ARGS[@]}"; do
        arg="${ENGINE_TARGET_ARGS[$index]}"
        if [[ "$arg" == "${flag}="* ]]; then
            printf '%s' "${arg#*=}"
            return 0
        fi
        if [[ "$arg" == "$flag" ]]; then
            next_index=$((index + 1))
            if (( next_index < ${#ENGINE_TARGET_ARGS[@]} )); then
                printf '%s' "${ENGINE_TARGET_ARGS[$next_index]}"
                return 0
            fi
        fi
    done

    return 1
}

materialize_engine_target_args() {
    local session_dir="$1"
    local parsed=()
    local arg

    if ! profiling_target_is_engine; then
        return 0
    fi

    if [[ ${#ENGINE_TARGET_ARGS[@]} -eq 0 && -n "$ENGINE_ARGS_RAW" ]]; then
        while IFS= read -r arg; do
            [[ -z "$arg" ]] && continue
            parsed+=("$arg")
        done < <(parse_engine_args_string "$ENGINE_ARGS_RAW")
        ENGINE_TARGET_ARGS=("${parsed[@]}")
    fi

    if [[ ${#ENGINE_TARGET_ARGS[@]} -eq 0 ]]; then
        fatal "Engine targets require arguments after '--' or via --engine-args"
    fi

    if ! engine_target_args_contain_flag "--json"; then
        ENGINE_TARGET_ARGS+=(--json "${session_dir}/engine-target-summary.json")
    fi
}

resolve_requested_tools() {
    local tools=()

    $MODE_CPU && tools+=("cpu-suite")
    $MODE_SCHEDULER && tools+=("scheduler-focus")
    $MODE_LOCK_OFFCPU && tools+=("lock-offcpu-focus")
    $MODE_MEMORY && tools+=("memory-suite")
    $MODE_CACHE && tools+=("cache-suite")
    $MODE_C2C && tools+=("c2c-focus")
    $MODE_AOF_DISK && tools+=("aof-disk-focus")
    $MODE_NETWORK && tools+=("network-focus")
    $MODE_CRITERION && tools+=("criterion")
    $TOOL_FLAMEGRAPH && tools+=("flamegraph")
    $TOOL_PERF_STAT && tools+=("perf-stat")
    $TOOL_SAMPLY && tools+=("samply")
    $TOOL_INSTRUMENTS && tools+=("instruments")
    $TOOL_HEAPTRACK && tools+=("heaptrack")
    $TOOL_CACHEGRIND && tools+=("cachegrind")
    $TOOL_CALLGRIND && tools+=("callgrind")
    $TOOL_MASSIF && tools+=("massif")

    printf '%s\n' "${tools[@]}"
}

resolve_workload_source() {
    if profiling_target_is_engine; then
        printf '%s' "engine-target"
        return 0
    fi
    if benchmark_bridge_enabled; then
        printf '%s' "$(benchmark_workload_source_label)"
        return 0
    fi
    if [[ -n "$MANIFEST" ]]; then
        printf '%s' "profiler-manifest"
        return 0
    fi
    if [[ -n "$COMMAND" ]]; then
        printf '%s' "redis-benchmark"
        return 0
    fi
    printf '%s' "manual-or-none"
}

describe_workload() {
    if profiling_target_is_engine; then
        printf '%s' "engine=$(basename "${PROFILING_BINARY:-${ENGINE_EXAMPLE:-engine_probe}}"); args=$(shell_join "${ENGINE_TARGET_ARGS[@]}")"
        return 0
    fi
    if benchmark_bridge_enabled; then
        printf '%s' "$(benchmark_workload_description_text)"
        return 0
    fi
    if [[ -n "$MANIFEST" ]]; then
        printf '%s' "manifest=${MANIFEST}; command=${COMMAND:-n/a}; duration=${DURATION}s; clients=${CLIENTS}"
        return 0
    fi
    if [[ -n "$COMMAND" ]]; then
        printf '%s' "redis-benchmark command set ${COMMAND} for ${DURATION}s with ${CLIENTS} clients"
        return 0
    fi
    printf '%s' "no internal load configured"
}

prepare_session_contract_context() {
    local mode="$1"
    local cargo_profile="$2"
    local binary_path="$3"
    local tool_name
    local engine_duration=""

    SESSION_MODE="$mode"
    SESSION_CARGO_PROFILE="$cargo_profile"
    SESSION_BINARY_PATH="$binary_path"
    SESSION_COMMAND_LINE="$(shell_join "$0" "${ORIGINAL_ARGS[@]}")"
    SESSION_WORKLOAD_SOURCE="$(resolve_workload_source)"
    SESSION_WORKLOAD_DESCRIPTION="$(describe_workload)"
    if profiling_target_is_engine; then
        SESSION_WORKLOAD_COMMAND="$(shell_join "${ENGINE_TARGET_ARGS[@]}")"
        engine_duration="$(engine_target_arg_value "--duration-seconds" || true)"
        if [[ -z "$engine_duration" ]]; then
            engine_duration="$(engine_target_arg_value "--duration" || true)"
        fi
        SESSION_WORKLOAD_DURATION="$engine_duration"
        SESSION_WORKLOAD_CLIENTS=""
    else
        SESSION_WORKLOAD_COMMAND="$COMMAND"
        SESSION_WORKLOAD_DURATION="$DURATION"
        SESSION_WORKLOAD_CLIENTS="$CLIENTS"
    fi
    if benchmark_bridge_enabled; then
        SESSION_WORKLOAD_MANIFEST_PATH="$BENCH_EFFECTIVE_MANIFEST"
        SESSION_WORKLOAD_REQUEST_PATH="$BENCH_EFFECTIVE_REQUEST_PATH"
    else
        SESSION_WORKLOAD_MANIFEST_PATH="$MANIFEST"
        SESSION_WORKLOAD_REQUEST_PATH=""
    fi
    SESSION_TOOLS_REQUESTED=""
    SESSION_TOOLS_EXECUTED=""
    SESSION_TARGET_PIDS=""

    while IFS= read -r tool_name; do
        [[ -z "$tool_name" ]] && continue
        record_requested_tool "$tool_name"
    done < <(resolve_requested_tools)
}

# ── Check mode ───────────────────────────────────────────────────────────────
if $MODE_CHECK; then
    run_check_mode
    exit 0
fi

# ── Criterion mode (no server, no build of vortex-server) ────────────────────
if $MODE_CRITERION; then
    if profiling_target_is_engine; then
        fatal "--criterion cannot be combined with engine target profiling"
    fi
    if benchmark_bridge_enabled; then
        fatal "--bench-manifest and --bench-request are only supported for server-based profiler sessions"
    fi
    SESSION_DIR="$(make_session_dir criterion)"
    mkdir -p "${SESSION_DIR}"
    prepare_session_contract_context "criterion" "criterion" ""
    initialize_session_contract "$SESSION_DIR"
    start_host_sampler_pack "$SESSION_DIR" "$HOST" "$PORT"

    printf "${C_BOLD}${C_BLUE}╔═══════════════════════════════════════════════╗${C_RESET}\n"
    printf "${C_BOLD}${C_BLUE}║    Vortex Profiler — Criterion Benchmarks     ║${C_RESET}\n"
    printf "${C_BOLD}${C_BLUE}╚═══════════════════════════════════════════════╝${C_RESET}\n\n"
    printf "  OS:      %s\n" "$OS"
    printf "  Session: %s\n\n" "$SESSION_DIR"

    record_executed_tool "criterion"
    run_criterion_mode "$SESSION_DIR" "$CRITERION_PACKAGE" "$CRITERION_BENCH_TARGET" "$CRITERION_FILTER"
    stop_host_sampler_pack
    generate_post_session_summary "$SESSION_DIR" "$COMPARE_TO"
    finalize_session_contract "$SESSION_DIR" "completed" 0

    echo ""
    printf "${C_BOLD}${C_GREEN}═══ Session Complete ═══${C_RESET}\n"
    printf "  Artifacts: %s\n" "$SESSION_DIR"
    exit 0
fi

# ── Target-based profiling ───────────────────────────────────────────────────
SESSION_DIR="$(make_session_dir profiling)"
register_cleanup
resolve_benchmark_bridge "$SESSION_DIR"
if profiling_target_is_engine && benchmark_bridge_enabled; then
    fatal "--bench-manifest and --bench-request are only supported for server-based profiler sessions"
fi
if benchmark_bridge_enabled && [[ "$DURATION_SET_BY_CLI" != "true" ]] && [[ -n "$BENCH_EFFECTIVE_DURATION_SECONDS" ]]; then
    DURATION="$BENCH_EFFECTIVE_DURATION_SECONDS"
fi

if profiling_target_is_engine; then
    HOST=""
    PORT=""
fi

# Build the binary (unless --bin was provided)
build_profiling_binary "$BIN_OVERRIDE"
materialize_engine_target_args "$SESSION_DIR"
prepare_session_contract_context "profiling" "$PROFILING_CARGO_PROFILE" "$PROFILING_BINARY"
initialize_session_contract "$SESSION_DIR"
start_host_sampler_pack "$SESSION_DIR" "$HOST" "$PORT"

# Print session banner
printf "\n${C_BOLD}${C_BLUE}╔═══════════════════════════════════════════════╗${C_RESET}\n"
printf "${C_BOLD}${C_BLUE}║        Vortex Profiler — Session               ║${C_RESET}\n"
printf "${C_BOLD}${C_BLUE}╚═══════════════════════════════════════════════╝${C_RESET}\n\n"
printf "  OS:        %s (%s)\n" "$OS" "$(uname -m)"
printf "  Binary:    %s\n" "$PROFILING_BINARY"
if profiling_target_is_engine; then
    printf "  Target:    engine\n"
    if [[ -n "$ENGINE_EXAMPLE" ]]; then printf "  Example:   %s\n" "$ENGINE_EXAMPLE"; fi
    printf "  Args:      %s\n" "$(shell_join "${ENGINE_TARGET_ARGS[@]}")"
else
    printf "  Bind:      %s:%s\n" "$HOST" "$PORT"
    printf "  Threads:   %s\n" "$THREADS"
    printf "  Command:   %s\n" "${COMMAND:-<none — no load>}"
fi
if profiling_target_is_engine; then
    banner_duration="$(engine_target_arg_value "--duration-seconds" || true)"
    if [[ -z "$banner_duration" ]]; then
        banner_duration="$(engine_target_arg_value "--duration" || true)"
    fi
    printf "  Duration:  %ss\n" "${banner_duration:-n/a}"
else
    printf "  Duration:  %ss\n" "$DURATION"
fi
printf "  Frequency: %s Hz\n" "$FREQUENCY"
printf "  Session:   %s\n" "$SESSION_DIR"
if $DRY_RUN; then printf "  DryRun:    enabled\n"; fi
if [[ -n "$MANIFEST" ]]; then printf "  Manifest:  %s\n" "$MANIFEST"; fi
if $AOF; then printf "  AOF:       enabled\n"; fi
if [[ -n "$MAXMEMORY" ]]; then printf "  MaxMemory: %s\n" "$MAXMEMORY"; fi
if [[ -n "$EVICTION" ]]; then printf "  Eviction:  %s\n" "$EVICTION"; fi
if [[ -n "$IO_BACKEND" ]]; then printf "  I/O:       %s\n" "$IO_BACKEND"; fi
if [[ -n "$RING_SIZE" ]]; then printf "  RingSize:  %s\n" "$RING_SIZE"; fi
if [[ -n "$FIXED_BUFFERS" ]]; then printf "  FixedBufs: %s\n" "$FIXED_BUFFERS"; fi
if [[ -n "$SQPOLL_IDLE_MS" ]]; then printf "  SQPOLL:    %s ms\n" "$SQPOLL_IDLE_MS"; fi
echo ""

# Common args passed to every profiling function:
# $1=session $2=host $3=port $4=threads $5=aof $6=maxmemory $7=eviction
# $8=command $9=duration $10=clients $11=frequency (for CPU tools)
COMMON_ARGS=("$SESSION_DIR" "$HOST" "$PORT" "$THREADS" "$AOF" "$MAXMEMORY" "$EVICTION" "$COMMAND" "$DURATION" "$CLIENTS")

# ── Dispatch specific tools ──────────────────────────────────────────────────
if $TOOL_FLAMEGRAPH; then
    record_executed_tool "flamegraph"
    run_flamegraph "${COMMON_ARGS[@]}" "$FREQUENCY"
fi

if $TOOL_PERF_STAT; then
    record_executed_tool "perf-stat"
    run_perf_stat "${COMMON_ARGS[@]}"
fi

if $TOOL_SAMPLY; then
    record_executed_tool "samply"
    run_samply "${COMMON_ARGS[@]}"
fi

if $TOOL_INSTRUMENTS; then
    record_executed_tool "instruments"
    run_instruments "${COMMON_ARGS[@]}"
fi

if $TOOL_HEAPTRACK; then
    record_executed_tool "heaptrack"
    run_heaptrack "${COMMON_ARGS[@]}"
fi

if $TOOL_CACHEGRIND; then
    record_executed_tool "cachegrind"
    run_cachegrind "${COMMON_ARGS[@]}"
fi

if $TOOL_CALLGRIND; then
    record_executed_tool "callgrind"
    run_callgrind "${COMMON_ARGS[@]}"
fi

if $TOOL_MASSIF; then
    record_executed_tool "massif"
    run_massif "${COMMON_ARGS[@]}"
fi

# ── Dispatch composite modes ────────────────────────────────────────────────
if $MODE_CPU; then
    record_executed_tool "cpu-suite"
    run_cpu_all "${COMMON_ARGS[@]}" "$FREQUENCY"
fi

if $MODE_SCHEDULER; then
    record_executed_tool "scheduler-focus"
    run_scheduler_focus "${COMMON_ARGS[@]}"
fi

if $MODE_LOCK_OFFCPU; then
    record_executed_tool "lock-offcpu-focus"
    run_lock_offcpu_focus "${COMMON_ARGS[@]}"
fi

if $MODE_MEMORY; then
    record_executed_tool "memory-suite"
    run_memory_all "${COMMON_ARGS[@]}"
fi

if $MODE_CACHE; then
    record_executed_tool "cache-suite"
    run_cachegrind "${COMMON_ARGS[@]}"
fi

if $MODE_C2C; then
    record_executed_tool "c2c-focus"
    run_c2c_focus "${COMMON_ARGS[@]}"
fi

if $MODE_AOF_DISK; then
    record_executed_tool "aof-disk-focus"
    run_aof_disk_focus "${COMMON_ARGS[@]}"
fi

if $MODE_NETWORK; then
    record_executed_tool "network-focus"
    run_network_focus "${COMMON_ARGS[@]}"
fi

# ── Session summary ──────────────────────────────────────────────────────────
stop_host_sampler_pack

# Generate differential flamegraph if --compare-to was specified
if [[ -n "$COMPARE_TO" ]]; then
    run_diff_flamegraph "$SESSION_DIR" "$COMPARE_TO"
fi

generate_post_session_summary "$SESSION_DIR" "$COMPARE_TO"
finalize_session_contract "$SESSION_DIR" "completed" 0

echo ""
printf "${C_BOLD}${C_GREEN}═══ Session Complete ═══${C_RESET}\n\n"
printf "  Artifacts:\n"

# List generated artifacts
for f in "${SESSION_DIR}"/*; do
    if [[ -f "$f" || -d "$f" ]]; then
        local_name="$(basename "$f")"
        case "$local_name" in
            *.svg)          printf "    ${C_GREEN}🔥 %s${C_RESET}\n" "$local_name" ;;
            *.data)         printf "    ${C_CYAN}📊 %s${C_RESET}\n" "$local_name" ;;
            *.trace)        printf "    ${C_CYAN}📊 %s${C_RESET}\n" "$local_name" ;;
            *.out)          printf "    ${C_CYAN}📊 %s${C_RESET}\n" "$local_name" ;;
            *.json)         printf "    ${C_CYAN}📊 %s${C_RESET}\n" "$local_name" ;;
            *.gz)           printf "    ${C_CYAN}📊 %s${C_RESET}\n" "$local_name" ;;
            *-summary.txt)  printf "    ${C_YELLOW}📋 %s${C_RESET}\n" "$local_name" ;;
            *.log)          printf "    ${C_DIM}📝 %s${C_RESET}\n" "$local_name" ;;
            criterion)      printf "    ${C_GREEN}📈 %s/${C_RESET}\n" "$local_name" ;;
            *)              printf "    ${C_DIM}   %s${C_RESET}\n" "$local_name" ;;
        esac
    fi
done

echo ""
printf "  Session: %s\n\n" "$SESSION_DIR"
