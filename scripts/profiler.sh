#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Vortex Profiler — profiling tool manager for vortex-server
# ─────────────────────────────────────────────────────────────────────────────
#
# Self-contained orchestrator: detects OS, builds the profiling binary,
# starts vortex-server, runs native profilers, generates load internally,
# and collects all artifacts into a timestamped session directory.
#
# Usage:  just profiler [mode flags] [options]
#
# See:    just profiler --help
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source component scripts
source "${SCRIPTS_DIR}/profiler/common.sh"
source "${SCRIPTS_DIR}/profiler/build.sh"
source "${SCRIPTS_DIR}/profiler/server.sh"
source "${SCRIPTS_DIR}/profiler/cpu.sh"
source "${SCRIPTS_DIR}/profiler/memory.sh"
source "${SCRIPTS_DIR}/profiler/cache.sh"
source "${SCRIPTS_DIR}/profiler/criterion.sh"
source "${SCRIPTS_DIR}/profiler/check.sh"

# ── Usage ────────────────────────────────────────────────────────────────────
usage() {
    cat <<'EOF'
Vortex Profiler — profiling tool manager for vortex-server

Usage: just profiler [mode flags] [options]

Modes (at least one required, combinable):
  --cpu              Full CPU profiling (flamegraph + perf stat + perf record)
  --memory           Heap allocation profiling (heaptrack / massif / Instruments)
  --cache            Cache locality analysis (cachegrind)
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

Server configuration:
  --threads N        Server thread count (default: 4)
  --aof              Enable AOF persistence
  --maxmemory SIZE   Set max memory (e.g. 64mb, 1gb)
  --eviction POLICY  Set eviction policy (e.g. allkeys-lru)
  --host HOST        Bind address (default: 127.0.0.1)
  --port PORT        Bind port (default: 16379)
  --bin PATH         Use pre-built binary instead of building

Profiler tuning:
  --frequency N      Sampling frequency for perf/flamegraph (default: 99)

Diagnostics:
  --check            Show OS, available tools, binary status

Environment:
    .env               If present at repo root (or scripts/profiler/.env), it is loaded automatically
    HOST_PASSWORD      Optional sudo password used for macOS profilers that require elevation

macOS note:
    profiling binary   Auto-signed with get-task-allow so Instruments can attach to vortex-server

Examples:
  just profiler --command SET,GET
  just profiler --cpu --command SET,GET --duration 20
  just profiler --flamegraph --command SET --threads 2
  just profiler --memory --command SET --duration 15
  just profiler --cache --command SET --threads 1
  just profiler --callgrind --command SET --threads 1
  just profiler --all --command SET,GET
  just profiler --manifest scripts/profiler/manifests/cpu-set-heavy.yaml
  just profiler --criterion --filter cmd_get_inline
  just profiler --check
EOF
    exit 0
}

# ── Defaults ─────────────────────────────────────────────────────────────────
MODE_CPU=false
MODE_MEMORY=false
MODE_CACHE=false
MODE_ALL=false
MODE_CRITERION=false
MODE_CHECK=false

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
CLIENTS=50
MANIFEST=""

# Server
HOST="127.0.0.1"
PORT="16379"
THREADS=4
AOF=false
MAXMEMORY=""
EVICTION=""
BIN_OVERRIDE=""

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
        --memory)       MODE_MEMORY=true;        shift ;;
        --cache)        MODE_CACHE=true;         shift ;;
        --all)          MODE_ALL=true;           shift ;;
        --criterion)    MODE_CRITERION=true;     shift ;;
        --check)        MODE_CHECK=true;         shift ;;

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
        --duration)     DURATION="$2";           shift 2 ;;
        --clients)      CLIENTS="$2";            shift 2 ;;
        --manifest)     MANIFEST="$2";           shift 2 ;;

        # Server
        --host)         HOST="$2";               shift 2 ;;
        --port)         PORT="$2";               shift 2 ;;
        --threads)      THREADS="$2";            shift 2 ;;
        --aof)          AOF=true;                shift ;;
        --maxmemory)    MAXMEMORY="$2";          shift 2 ;;
        --eviction)     EVICTION="$2";           shift 2 ;;
        --bin)          BIN_OVERRIDE="$2";       shift 2 ;;

        # Profiler tuning
        --frequency)    FREQUENCY="$2";          shift 2 ;;

        # Criterion
        --filter)       CRITERION_FILTER="$2";   shift 2 ;;
        --package)      CRITERION_PACKAGE="$2";  shift 2 ;;
        --bench-target) CRITERION_BENCH_TARGET="$2"; shift 2 ;;

        # Help
        -h|--help)      usage ;;

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
    [[ "$FREQUENCY" == 99 && -n "${MANIFEST_PROFILER_FREQUENCY:-}" ]] && FREQUENCY="$MANIFEST_PROFILER_FREQUENCY"
    if [[ "${MANIFEST_SERVER_AOF:-}" == "true" ]]; then AOF=true; fi

    # Apply manifest modes
    if [[ -n "${MANIFEST_MODES:-}" ]]; then
        IFS=',' read -ra _manifest_modes <<< "$MANIFEST_MODES"
        for _m in "${_manifest_modes[@]}"; do
            case "$_m" in
                cpu)              MODE_CPU=true ;;
                memory)           MODE_MEMORY=true ;;
                cache)            MODE_CACHE=true ;;
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

# ── Default mode: if only --command is given with no mode, default to --cpu ──
has_any_mode() {
    $MODE_CPU || $MODE_MEMORY || $MODE_CACHE || $MODE_CRITERION || $MODE_CHECK || \
    $TOOL_FLAMEGRAPH || $TOOL_PERF_STAT || $TOOL_SAMPLY || $TOOL_INSTRUMENTS || \
    $TOOL_HEAPTRACK || $TOOL_CACHEGRIND || $TOOL_CALLGRIND || $TOOL_MASSIF
}

if ! has_any_mode; then
    if [[ -n "$COMMAND" ]]; then
        TOOL_FLAMEGRAPH=true
        info "No mode specified, defaulting to --flamegraph"
    else
        fatal "No mode specified. Run 'just profiler --help' for usage."
    fi
fi

# ── Check mode ───────────────────────────────────────────────────────────────
if $MODE_CHECK; then
    run_check_mode
    exit 0
fi

# ── Criterion mode (no server, no build of vortex-server) ────────────────────
if $MODE_CRITERION; then
    SESSION_DIR="$(make_session_dir criterion)"
    mkdir -p "${SESSION_DIR}"

    printf "${C_BOLD}${C_BLUE}╔═══════════════════════════════════════════════╗${C_RESET}\n"
    printf "${C_BOLD}${C_BLUE}║    Vortex Profiler — Criterion Benchmarks     ║${C_RESET}\n"
    printf "${C_BOLD}${C_BLUE}╚═══════════════════════════════════════════════╝${C_RESET}\n\n"
    printf "  OS:      %s\n" "$OS"
    printf "  Session: %s\n\n" "$SESSION_DIR"

    run_criterion_mode "$SESSION_DIR" "$CRITERION_PACKAGE" "$CRITERION_BENCH_TARGET" "$CRITERION_FILTER"

    echo ""
    printf "${C_BOLD}${C_GREEN}═══ Session Complete ═══${C_RESET}\n"
    printf "  Artifacts: %s\n" "$SESSION_DIR"
    exit 0
fi

# ── Server-based profiling ───────────────────────────────────────────────────
SESSION_DIR="$(make_session_dir profiling)"
register_cleanup

# Build the binary (unless --bin was provided)
build_profiling_binary "$BIN_OVERRIDE"

# Print session banner
printf "\n${C_BOLD}${C_BLUE}╔═══════════════════════════════════════════════╗${C_RESET}\n"
printf "${C_BOLD}${C_BLUE}║        Vortex Profiler — Session               ║${C_RESET}\n"
printf "${C_BOLD}${C_BLUE}╚═══════════════════════════════════════════════╝${C_RESET}\n\n"
printf "  OS:        %s (%s)\n" "$OS" "$(uname -m)"
printf "  Binary:    %s\n" "$PROFILING_BINARY"
printf "  Bind:      %s:%s\n" "$HOST" "$PORT"
printf "  Threads:   %s\n" "$THREADS"
printf "  Command:   %s\n" "${COMMAND:-<none — no load>}"
printf "  Duration:  %ss\n" "$DURATION"
printf "  Frequency: %s Hz\n" "$FREQUENCY"
printf "  Session:   %s\n" "$SESSION_DIR"
if [[ -n "$MANIFEST" ]]; then printf "  Manifest:  %s\n" "$MANIFEST"; fi
if $AOF; then printf "  AOF:       enabled\n"; fi
if [[ -n "$MAXMEMORY" ]]; then printf "  MaxMemory: %s\n" "$MAXMEMORY"; fi
if [[ -n "$EVICTION" ]]; then printf "  Eviction:  %s\n" "$EVICTION"; fi
echo ""

# Common args passed to every profiling function:
# $1=session $2=host $3=port $4=threads $5=aof $6=maxmemory $7=eviction
# $8=command $9=duration $10=clients $11=frequency (for CPU tools)
COMMON_ARGS=("$SESSION_DIR" "$HOST" "$PORT" "$THREADS" "$AOF" "$MAXMEMORY" "$EVICTION" "$COMMAND" "$DURATION" "$CLIENTS")

# ── Dispatch specific tools ──────────────────────────────────────────────────
if $TOOL_FLAMEGRAPH; then
    run_flamegraph "${COMMON_ARGS[@]}" "$FREQUENCY"
fi

if $TOOL_PERF_STAT; then
    run_perf_stat "${COMMON_ARGS[@]}"
fi

if $TOOL_SAMPLY; then
    run_samply "${COMMON_ARGS[@]}"
fi

if $TOOL_INSTRUMENTS; then
    run_instruments "${COMMON_ARGS[@]}"
fi

if $TOOL_HEAPTRACK; then
    run_heaptrack "${COMMON_ARGS[@]}"
fi

if $TOOL_CACHEGRIND; then
    run_cachegrind "${COMMON_ARGS[@]}"
fi

if $TOOL_CALLGRIND; then
    run_callgrind "${COMMON_ARGS[@]}"
fi

if $TOOL_MASSIF; then
    run_massif "${COMMON_ARGS[@]}"
fi

# ── Dispatch composite modes ────────────────────────────────────────────────
if $MODE_CPU; then
    run_cpu_all "${COMMON_ARGS[@]}" "$FREQUENCY"
fi

if $MODE_MEMORY; then
    run_memory_all "${COMMON_ARGS[@]}"
fi

if $MODE_CACHE; then
    run_cachegrind "${COMMON_ARGS[@]}"
fi

# ── Session summary ──────────────────────────────────────────────────────────
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
