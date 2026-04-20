#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/server.sh — server lifecycle and load generation
# ─────────────────────────────────────────────────────────────────────────────

# Expects common.sh to be sourced already.

SERVER_PID=""
LOAD_PID=""

# ── Cleanup trap ─────────────────────────────────────────────────────────────
_profiler_cleanup() {
    local exit_code=$?
    # Kill load generator first
    if [[ -n "$LOAD_PID" ]] && kill -0 "$LOAD_PID" 2>/dev/null; then
        kill "$LOAD_PID" 2>/dev/null || true
        wait "$LOAD_PID" 2>/dev/null || true
        LOAD_PID=""
    fi
    # Then kill server
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -INT "$SERVER_PID" 2>/dev/null || true
        # Give it 3s to shut down gracefully, then force
        local i=0
        while kill -0 "$SERVER_PID" 2>/dev/null && [[ $i -lt 30 ]]; do
            sleep 0.1
            i=$((i + 1))
        done
        if kill -0 "$SERVER_PID" 2>/dev/null; then
            kill -9 "$SERVER_PID" 2>/dev/null || true
        fi
        wait "$SERVER_PID" 2>/dev/null || true
        info "Server (pid ${SERVER_PID}) stopped"
        SERVER_PID=""
    fi
    return $exit_code
}

register_cleanup() {
    trap _profiler_cleanup EXIT INT TERM
}

# ── Build server args ────────────────────────────────────────────────────────
build_server_args() {
    local host="$1" port="$2" threads="$3" aof="$4" maxmemory="$5" eviction="$6"

    local args=("--bind" "${host}:${port}" "--threads" "$threads")

    if [[ "$aof" == "true" ]]; then
        args+=("--aof-enabled")
    fi
    if [[ -n "$maxmemory" ]]; then
        args+=("--max-memory" "$maxmemory")
    fi
    if [[ -n "$eviction" ]]; then
        args+=("--eviction-policy" "$eviction")
    fi
    if [[ -n "${IO_BACKEND:-}" ]]; then
        args+=("--io-backend" "$IO_BACKEND")
    fi
    if [[ -n "${RING_SIZE:-}" ]]; then
        args+=("--ring-size" "$RING_SIZE")
    fi
    if [[ -n "${SQPOLL_IDLE_MS:-}" ]]; then
        args+=("--sqpoll-idle-ms" "$SQPOLL_IDLE_MS")
    fi

    printf '%s\n' "${args[@]}"
}

# ── Start server in background ───────────────────────────────────────────────
start_server() {
    local host="$1" port="$2" threads="$3" aof="$4" maxmemory="$5" eviction="$6" logfile="$7"

    local args=()
    while IFS= read -r arg; do
        args+=("$arg")
    done < <(build_server_args "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction")

    info "Starting vortex-server: ${PROFILING_BINARY} ${args[*]}"
    "$PROFILING_BINARY" "${args[@]}" >"$logfile" 2>&1 &
    SERVER_PID=$!
    info "Server started (pid ${SERVER_PID})"

    wait_for_server "$host" "$port" 30
}

# ── Load generation ──────────────────────────────────────────────────────────
generate_load() {
    local host="$1" port="$2" command="$3" duration="$4" clients="$5" logfile="$6"

    if [[ -z "$command" ]]; then
        warn "No --command specified. No load will be generated."
        warn "Hint: run load manually: redis-benchmark -h ${host} -p ${port} -t SET,GET -c 50"
        return 0
    fi

    if has_cmd redis-benchmark; then
        # Estimate requests to fill the duration
        local approx_requests=$(( clients * 5000 * duration / 10 ))
        info "Generating load: redis-benchmark -t ${command} (${duration}s, ${clients} clients)"
        redis-benchmark \
            -h "$host" -p "$port" \
            -t "$command" \
            -c "$clients" \
            -d 256 \
            --threads 4 \
            -n "$approx_requests" \
            >"$logfile" 2>&1 &
        LOAD_PID=$!
    else
        warn "redis-benchmark not found. Sleeping for ${duration}s instead."
        warn "Install redis-tools to enable automatic load generation."
        sleep "$duration" &
        LOAD_PID=$!
    fi
}

# ── Wait for load to finish ──────────────────────────────────────────────────
wait_for_load() {
    local duration="$1"
    if [[ -n "$LOAD_PID" ]] && kill -0 "$LOAD_PID" 2>/dev/null; then
        info "Waiting for load to complete (pid ${LOAD_PID})..."
        wait "$LOAD_PID" 2>/dev/null || true
        LOAD_PID=""
        ok "Load generation complete"
    else
        info "Capturing for ${duration}s..."
        sleep "$duration"
    fi
}
