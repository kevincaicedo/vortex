#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/bench.sh — vortex_bench workload bridge helpers
# ─────────────────────────────────────────────────────────────────────────────

# Expects common.sh to be sourced already.

BENCH_MANIFEST=""
BENCH_REQUEST=""
BENCH_EFFECTIVE_MANIFEST=""
BENCH_EFFECTIVE_REQUEST_PATH=""
BENCH_EFFECTIVE_SOURCE=""
BENCH_EFFECTIVE_DESCRIPTION=""
BENCH_EFFECTIVE_DURATION_SECONDS=""
BENCH_RESOURCE_THREADS=""
BENCH_RUNTIME_AOF_ENABLED=""
BENCH_RUNTIME_MAXMEMORY=""
BENCH_RUNTIME_EVICTION_POLICY=""
BENCH_RUNTIME_IO_BACKEND=""
BENCH_RUNTIME_RING_SIZE=""
BENCH_RUNTIME_SQPOLL_IDLE_MS=""
BENCH_OUTPUT_DIR=""
BENCH_STATE_FILE=""

benchmark_bridge_enabled() {
    [[ -n "$BENCH_MANIFEST" || -n "$BENCH_REQUEST" ]]
}

benchmark_wrapper_path() {
    printf '%s' "${REPO_ROOT}/vortex-benchmark/bin/vortex_bench"
}

resolve_benchmark_bridge() {
    local session_dir="$1"

    benchmark_bridge_enabled || return 0

    BENCH_OUTPUT_DIR="${session_dir}/bench"
    mkdir -p "$BENCH_OUTPUT_DIR"

    if ! has_cmd python3; then
        fatal "python3 is required for profiler benchmark bridge resolution"
    fi

    if [[ ! -f "$(benchmark_wrapper_path)" ]]; then
        fatal "vortex_bench wrapper not found at $(benchmark_wrapper_path)"
    fi

    local args=(--output-dir "$BENCH_OUTPUT_DIR")
    if [[ -n "$BENCH_MANIFEST" ]]; then
        args+=(--manifest "$BENCH_MANIFEST")
    fi
    if [[ -n "$BENCH_REQUEST" ]]; then
        args+=(--request "$BENCH_REQUEST")
    fi

    eval "$(python3 "${PROFILER_SCRIPT_DIR}/prepare_benchmark_bridge.py" "${args[@]}")"

    if [[ "$AOF_SET_BY_CLI" != "true" && -n "$BENCH_RUNTIME_AOF_ENABLED" ]]; then
        if [[ "$BENCH_RUNTIME_AOF_ENABLED" == "true" ]]; then
            AOF=true
        else
            AOF=false
        fi
    fi
    [[ -z "$MAXMEMORY" && -n "$BENCH_RUNTIME_MAXMEMORY" ]] && MAXMEMORY="$BENCH_RUNTIME_MAXMEMORY"
    [[ -z "$EVICTION" && -n "$BENCH_RUNTIME_EVICTION_POLICY" ]] && EVICTION="$BENCH_RUNTIME_EVICTION_POLICY"
    [[ -z "$IO_BACKEND" && -n "$BENCH_RUNTIME_IO_BACKEND" ]] && IO_BACKEND="$BENCH_RUNTIME_IO_BACKEND"
    [[ -z "$RING_SIZE" && -n "$BENCH_RUNTIME_RING_SIZE" ]] && RING_SIZE="$BENCH_RUNTIME_RING_SIZE"
    [[ -z "$SQPOLL_IDLE_MS" && -n "$BENCH_RUNTIME_SQPOLL_IDLE_MS" ]] && SQPOLL_IDLE_MS="$BENCH_RUNTIME_SQPOLL_IDLE_MS"
    [[ "$THREADS" == "4" && -n "$BENCH_RESOURCE_THREADS" ]] && THREADS="$BENCH_RESOURCE_THREADS"

    SESSION_WORKLOAD_MANIFEST_PATH="$BENCH_EFFECTIVE_MANIFEST"
    SESSION_WORKLOAD_REQUEST_PATH="$BENCH_EFFECTIVE_REQUEST_PATH"
}

benchmark_workload_source_label() {
    if benchmark_bridge_enabled; then
        printf '%s' "$BENCH_EFFECTIVE_SOURCE"
        return 0
    fi
    printf '%s' ""
}

benchmark_workload_description_text() {
    if benchmark_bridge_enabled; then
        printf '%s' "$BENCH_EFFECTIVE_DESCRIPTION"
        return 0
    fi
    printf '%s' ""
}

generate_benchmark_load() {
    local host="$1" port="$2" duration="$3" logfile="$4"
    local pid=""
    local attach_cmd=()
    local run_cmd=()

    benchmark_bridge_enabled || return 1

    pid="$(discover_pid_by_port "$port")"
    if [[ -z "$pid" ]]; then
        fatal "unable to resolve the profiled server pid for vortex_bench attach at ${host}:${port}"
    fi
    record_session_pid "$pid"

    BENCH_STATE_FILE="${BENCH_OUTPUT_DIR}/environments/profiler-attach-state.json"
    SESSION_WORKLOAD_ATTACH_STATE="$BENCH_STATE_FILE"

    attach_cmd=(
        bash "$(benchmark_wrapper_path)"
        attach
        --db vortex
        --native
        --output-dir "$BENCH_OUTPUT_DIR"
        --state-file "$BENCH_STATE_FILE"
        --host "$host"
        --port "$port"
        --pid "$pid"
        --label profiler
        --binary-path "$PROFILING_BINARY"
    )

    if [[ "$AOF" == "true" ]]; then
        attach_cmd+=(--aof-enabled)
    else
        attach_cmd+=(--aof-disabled)
    fi
    [[ -n "$THREADS" ]] && attach_cmd+=(--threads "$THREADS")
    [[ -n "$MAXMEMORY" ]] && attach_cmd+=(--maxmemory "$MAXMEMORY")
    [[ -n "$EVICTION" ]] && attach_cmd+=(--eviction-policy "$EVICTION")
    [[ -n "$IO_BACKEND" ]] && attach_cmd+=(--io-backend "$IO_BACKEND")
    [[ -n "$RING_SIZE" ]] && attach_cmd+=(--ring-size "$RING_SIZE")
    [[ -n "$SQPOLL_IDLE_MS" ]] && attach_cmd+=(--sqpoll-idle-ms "$SQPOLL_IDLE_MS")

    info "Attaching profiler session to vortex_bench artifact root"
    "${attach_cmd[@]}" >"${BENCH_OUTPUT_DIR}/attach.log" 2>&1 \
        || fatal "vortex_bench attach failed. See ${BENCH_OUTPUT_DIR}/attach.log"

    run_cmd=(
        bash "$(benchmark_wrapper_path)"
        run
        --state-file "$BENCH_STATE_FILE"
        --output-dir "$BENCH_OUTPUT_DIR"
        --db vortex
        --workload-manifest "$BENCH_EFFECTIVE_MANIFEST"
        --duration "${duration}s"
    )

    info "Generating load: vortex_bench run --workload-manifest $(basename "$BENCH_EFFECTIVE_MANIFEST")"
    "${run_cmd[@]}" >"$logfile" 2>&1 &
    LOAD_PID=$!
}