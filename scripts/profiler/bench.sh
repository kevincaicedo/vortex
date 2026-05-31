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
BENCH_RUNTIME_AOF_FSYNC=""
BENCH_RUNTIME_AOF_MAX_PENDING_FSYNC_BYTES=""
BENCH_RUNTIME_MAXMEMORY=""
BENCH_SERVER_MAXMEMORY=""
BENCH_RUNTIME_EVICTION_POLICY=""
BENCH_RUNTIME_IO_BACKEND=""
BENCH_RUNTIME_RING_SIZE=""
BENCH_RUNTIME_FIXED_BUFFERS=""
BENCH_RUNTIME_SQPOLL_IDLE_MS=""
BENCH_RUNTIME_TELEMETRY_MODE=""
BENCH_RUNTIME_TELEMETRY_LOCAL_SAMPLE_RATE=""
BENCH_RUNTIME_TELEMETRY_FLUSH_INTERVAL_MS=""
BENCH_OUTPUT_DIR=""
BENCH_STATE_FILE=""

benchmark_bridge_enabled() {
    [[ -n "$BENCH_MANIFEST" || -n "$BENCH_REQUEST" ]]
}

benchmark_wrapper_path() {
    printf '%s' "${REPO_ROOT}/vortex-benchmark/bin/vortex_bench"
}

benchmark_python_path() {
    if [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
        printf '%s' "${REPO_ROOT}/.venv/bin/python"
    elif [[ -x "${REPO_ROOT}/vortex-benchmark/python/.venv/bin/python" ]]; then
        printf '%s' "${REPO_ROOT}/vortex-benchmark/python/.venv/bin/python"
    elif [[ -x "${REPO_ROOT}/../.venv/bin/python" ]]; then
        printf '%s' "${REPO_ROOT}/../.venv/bin/python"
    else
        printf '%s' "python3"
    fi
}

resolve_benchmark_bridge() {
    local session_dir="$1"
    local python_bin=""

    benchmark_bridge_enabled || return 0

    BENCH_OUTPUT_DIR="${session_dir}/bench"
    mkdir -p "$BENCH_OUTPUT_DIR"

    if ! has_cmd python3; then
        fatal "python3 is required for profiler benchmark bridge resolution"
    fi

    if [[ ! -f "$(benchmark_wrapper_path)" ]]; then
        fatal "vortex_bench wrapper not found at $(benchmark_wrapper_path)"
    fi

    python_bin="$(benchmark_python_path)"
    local args=(--output-dir "$BENCH_OUTPUT_DIR")
    if [[ -n "$BENCH_MANIFEST" ]]; then
        args+=(--manifest "$BENCH_MANIFEST")
    fi
    if [[ -n "$BENCH_REQUEST" ]]; then
        args+=(--request "$BENCH_REQUEST")
    fi

    eval "$("$python_bin" "${PROFILER_SCRIPT_DIR}/prepare_benchmark_bridge.py" "${args[@]}")"

    if [[ "$AOF_SET_BY_CLI" != "true" && -n "$BENCH_RUNTIME_AOF_ENABLED" ]]; then
        if [[ "$BENCH_RUNTIME_AOF_ENABLED" == "true" ]]; then
            AOF=true
        else
            AOF=false
        fi
    fi
    [[ -z "${VORTEX_AOF_FSYNC:-}" && -n "$BENCH_RUNTIME_AOF_FSYNC" ]] && export VORTEX_AOF_FSYNC="$BENCH_RUNTIME_AOF_FSYNC"
    [[ -z "${VORTEX_AOF_MAX_PENDING_FSYNC_BYTES:-}" && -n "$BENCH_RUNTIME_AOF_MAX_PENDING_FSYNC_BYTES" ]] && export VORTEX_AOF_MAX_PENDING_FSYNC_BYTES="$BENCH_RUNTIME_AOF_MAX_PENDING_FSYNC_BYTES"
    [[ -z "$MAXMEMORY" && -n "$BENCH_SERVER_MAXMEMORY" ]] && MAXMEMORY="$BENCH_SERVER_MAXMEMORY"
    [[ -z "$EVICTION" && -n "$BENCH_RUNTIME_EVICTION_POLICY" ]] && EVICTION="$BENCH_RUNTIME_EVICTION_POLICY"
    [[ -z "$IO_BACKEND" && -n "$BENCH_RUNTIME_IO_BACKEND" ]] && IO_BACKEND="$BENCH_RUNTIME_IO_BACKEND"
    [[ -z "$RING_SIZE" && -n "$BENCH_RUNTIME_RING_SIZE" ]] && RING_SIZE="$BENCH_RUNTIME_RING_SIZE"
    [[ -z "$FIXED_BUFFERS" && -n "$BENCH_RUNTIME_FIXED_BUFFERS" ]] && FIXED_BUFFERS="$BENCH_RUNTIME_FIXED_BUFFERS"
    [[ -z "$SQPOLL_IDLE_MS" && -n "$BENCH_RUNTIME_SQPOLL_IDLE_MS" ]] && SQPOLL_IDLE_MS="$BENCH_RUNTIME_SQPOLL_IDLE_MS"
    [[ -z "${VORTEX_TELEMETRY_MODE:-}" && -n "$BENCH_RUNTIME_TELEMETRY_MODE" ]] && export VORTEX_TELEMETRY_MODE="$BENCH_RUNTIME_TELEMETRY_MODE"
    [[ -z "${VORTEX_TELEMETRY_LOCAL_SAMPLE_RATE:-}" && -n "$BENCH_RUNTIME_TELEMETRY_LOCAL_SAMPLE_RATE" ]] && export VORTEX_TELEMETRY_LOCAL_SAMPLE_RATE="$BENCH_RUNTIME_TELEMETRY_LOCAL_SAMPLE_RATE"
    [[ -z "${VORTEX_TELEMETRY_FLUSH_INTERVAL_MS:-}" && -n "$BENCH_RUNTIME_TELEMETRY_FLUSH_INTERVAL_MS" ]] && export VORTEX_TELEMETRY_FLUSH_INTERVAL_MS="$BENCH_RUNTIME_TELEMETRY_FLUSH_INTERVAL_MS"
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
    local attach_maxmemory=""
    local attach_aof_fsync=""
    local attach_aof_max_pending_fsync_bytes=""
    local attach_telemetry_mode=""
    local attach_telemetry_local_sample_rate=""
    local attach_telemetry_flush_interval_ms=""
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
    attach_maxmemory="${BENCH_RUNTIME_MAXMEMORY:-$MAXMEMORY}"
    attach_aof_fsync="${VORTEX_AOF_FSYNC:-$BENCH_RUNTIME_AOF_FSYNC}"
    attach_aof_max_pending_fsync_bytes="${VORTEX_AOF_MAX_PENDING_FSYNC_BYTES:-$BENCH_RUNTIME_AOF_MAX_PENDING_FSYNC_BYTES}"
    attach_telemetry_mode="${VORTEX_TELEMETRY_MODE:-$BENCH_RUNTIME_TELEMETRY_MODE}"
    attach_telemetry_local_sample_rate="${VORTEX_TELEMETRY_LOCAL_SAMPLE_RATE:-$BENCH_RUNTIME_TELEMETRY_LOCAL_SAMPLE_RATE}"
    attach_telemetry_flush_interval_ms="${VORTEX_TELEMETRY_FLUSH_INTERVAL_MS:-$BENCH_RUNTIME_TELEMETRY_FLUSH_INTERVAL_MS}"
    [[ -n "$THREADS" ]] && attach_cmd+=(--threads "$THREADS")
    [[ -n "$attach_aof_fsync" ]] && attach_cmd+=(--aof-fsync "$attach_aof_fsync")
    [[ -n "$attach_aof_max_pending_fsync_bytes" ]] && attach_cmd+=(--aof-max-pending-fsync-bytes "$attach_aof_max_pending_fsync_bytes")
    [[ -n "$attach_maxmemory" ]] && attach_cmd+=(--maxmemory "$attach_maxmemory")
    [[ -n "$EVICTION" ]] && attach_cmd+=(--eviction-policy "$EVICTION")
    [[ -n "$IO_BACKEND" ]] && attach_cmd+=(--io-backend "$IO_BACKEND")
    [[ -n "$RING_SIZE" ]] && attach_cmd+=(--ring-size "$RING_SIZE")
    [[ -n "$FIXED_BUFFERS" ]] && attach_cmd+=(--fixed-buffers "$FIXED_BUFFERS")
    [[ -n "$SQPOLL_IDLE_MS" ]] && attach_cmd+=(--sqpoll-idle-ms "$SQPOLL_IDLE_MS")
    [[ -n "$attach_telemetry_mode" ]] && attach_cmd+=(--telemetry-mode "$attach_telemetry_mode")
    [[ -n "$attach_telemetry_local_sample_rate" ]] && attach_cmd+=(--telemetry-local-sample-rate "$attach_telemetry_local_sample_rate")
    [[ -n "$attach_telemetry_flush_interval_ms" ]] && attach_cmd+=(--telemetry-flush-interval-ms "$attach_telemetry_flush_interval_ms")

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
