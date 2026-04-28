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
    trap _profiler_cleanup_and_finalize EXIT INT TERM
}

_profiler_cleanup_and_finalize() {
    local exit_code=$?

    trap - EXIT INT TERM
    _profiler_cleanup >/dev/null 2>&1 || true
    if declare -F stop_host_sampler_pack >/dev/null 2>&1; then
        stop_host_sampler_pack || true
    fi
    if declare -F generate_post_session_summary >/dev/null 2>&1 && [[ -n "${SESSION_DIR:-}" ]]; then
        generate_post_session_summary "$SESSION_DIR" "${COMPARE_TO:-}" || true
    fi

    if declare -F finalize_session_contract >/dev/null 2>&1 && [[ -n "${SESSION_DIR:-}" ]]; then
        if [[ "$exit_code" -eq 0 ]]; then
            finalize_session_contract "$SESSION_DIR" "completed" "$exit_code" || true
        else
            finalize_session_contract "$SESSION_DIR" "failed" "$exit_code" || true
        fi
    fi

    exit "$exit_code"
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
    if [[ -n "${FIXED_BUFFERS:-}" ]]; then
        args+=("--fixed-buffers" "$FIXED_BUFFERS")
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
    record_session_pid "$SERVER_PID"
    info "Server started (pid ${SERVER_PID})"

    wait_for_server "$host" "$port" 30
}

pid_is_live() {
    local pid="$1"
    local state=""

    if ! kill -0 "$pid" 2>/dev/null; then
        return 1
    fi

    if has_cmd ps; then
        state="$(ps -o stat= -p "$pid" 2>/dev/null | awk 'NR==1 { print $1 }')"
        if [[ "$state" == Z* ]]; then
            return 1
        fi
    fi

    return 0
}

stop_profiled_pid() {
    local pid="$1"
    local label="$2"
    local attempts=0

    kill -INT "$pid" 2>/dev/null || true
    while pid_is_live "$pid" && [[ $attempts -lt 20 ]]; do
        sleep 0.1
        attempts=$((attempts + 1))
    done
    if ! pid_is_live "$pid"; then
        return 0
    fi

    warn "${label} did not stop after SIGINT; escalating to SIGTERM"
    kill -TERM "$pid" 2>/dev/null || true
    attempts=0
    while pid_is_live "$pid" && [[ $attempts -lt 50 ]]; do
        sleep 0.1
        attempts=$((attempts + 1))
    done
    if ! pid_is_live "$pid"; then
        return 0
    fi

    warn "${label} did not stop after SIGTERM; escalating to SIGKILL"
    kill -KILL "$pid" 2>/dev/null || true
}

start_profiled_shutdown_controller() {
    local profiler_name="$1" tool_pid="$2" host="$3" port="$4"
    local command="$5" duration="$6" clients="$7" ready_timeout="$8"
    local load_log="$9" server_log="${10}"

    (
        local ready=false
        local deadline=$(( $(date +%s) + ready_timeout ))

        info "Waiting for server at ${host}:${port}..."
        while [[ $(date +%s) -lt $deadline ]]; do
            if ! kill -0 "$tool_pid" 2>/dev/null; then
                warn "${profiler_name} target exited before the server became ready. See ${server_log}"
                break
            fi

            if probe_server_ready "$host" "$port"; then
                local discovered_pid
                discovered_pid="$(discover_pid_by_port "$port")"
                record_session_pid "$discovered_pid"
                ok "Server ready at ${host}:${port}"
                ready=true
                break
            fi

            sleep 0.3
        done

        if [[ "$ready" == "true" ]]; then
            if [[ -n "$command" ]]; then
                generate_load "$host" "$port" "$command" "$duration" "$clients" "$load_log"
                wait_for_load "$duration"
            else
                info "No load configured for ${profiler_name}; idling for ${duration}s before shutdown"
                sleep "$duration"
            fi
        else
            warn "Skipping ${profiler_name} load generation because the server never became ready. See ${server_log}"
        fi

        local profiled_pid=""
        profiled_pid="$(discover_pid_by_port "$port")"
        if [[ -n "$profiled_pid" ]] && pid_is_live "$profiled_pid"; then
            info "Stopping ${profiler_name} target server (pid ${profiled_pid})..."
            stop_profiled_pid "$profiled_pid" "${profiler_name} target"
        elif pid_is_live "$tool_pid"; then
            info "Stopping ${profiler_name} profiler wrapper (pid ${tool_pid})..."
            stop_profiled_pid "$tool_pid" "${profiler_name} profiler wrapper"
        fi
    ) &

    printf '%s\n' "$!"
}

# ── Load generation ──────────────────────────────────────────────────────────
generate_load() {
    local host="$1" port="$2" command="$3" duration="$4" clients="$5" logfile="$6"

    if declare -F benchmark_bridge_enabled >/dev/null 2>&1 && benchmark_bridge_enabled; then
        generate_benchmark_load "$host" "$port" "$duration" "$logfile"
        return 0
    fi

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
