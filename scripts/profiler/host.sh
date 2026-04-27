#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/host.sh — lightweight host sampler lifecycle
# ─────────────────────────────────────────────────────────────────────────────

# Expects common.sh to be sourced already.

HOST_SAMPLER_DIR=""
HOST_TELEMETRY_SAMPLES_PATH=""
HOST_TELEMETRY_SUMMARY_PATH=""
HOST_SAMPLER_INTERVAL_SECONDS="${HOST_SAMPLER_INTERVAL_SECONDS:-1}"
HOST_SAMPLER_PIDS=()

_start_host_sampler_process() {
    local output_path="$1"
    shift

    (
        while true; do
            "$@"
            echo ""
            sleep "$HOST_SAMPLER_INTERVAL_SECONDS"
        done
    ) >"$output_path" 2>&1 &

    HOST_SAMPLER_PIDS+=("$!")
}

_sample_socket_summary() {
    local host="$1" port="$2"

    printf 'timestamp=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'target=%s:%s\n' "$host" "$port"

    if [[ "$OS" == "linux" ]]; then
        if has_cmd ss; then
            ss -s || true
            ss -tin "sport = :${port}" 2>/dev/null || true
        fi
        [[ -r /proc/net/sockstat ]] && cat /proc/net/sockstat
        [[ -r /proc/net/sockstat6 ]] && cat /proc/net/sockstat6
        return 0
    fi

    if [[ "$OS" == "macos" ]] && has_cmd netstat; then
        netstat -s -p tcp 2>/dev/null | head -80 || true
    fi
}

_sample_process_probe() {
    local host="$1" port="$2"
    local pid=""

    pid="$(discover_pid_by_port "$port")"

    printf 'timestamp=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'target=%s:%s\n' "$host" "$port"
    printf 'pid=%s\n' "${pid:-none}"

    if [[ -z "$pid" ]]; then
        return 0
    fi

    if [[ "$OS" == "linux" ]]; then
        [[ -r "/proc/${pid}/status" ]] && cat "/proc/${pid}/status"
        [[ -r "/proc/${pid}/io" ]] && cat "/proc/${pid}/io"
        [[ -r "/proc/${pid}/stat" ]] && cat "/proc/${pid}/stat"
        return 0
    fi

    if [[ "$OS" == "macos" ]] && has_cmd ps; then
        ps -o pid,ppid,pcpu,pmem,rss,vsz,stat,time,command -p "$pid" || true
    fi
}

start_host_sampler_pack() {
    local session_dir="$1" host="$2" port="$3"
    local session_label

    if [[ ${#HOST_SAMPLER_PIDS[@]} -gt 0 ]]; then
        return 0
    fi

    HOST_SAMPLER_DIR="${session_dir}/host"
    mkdir -p "$HOST_SAMPLER_DIR"

    session_label="$(basename "$session_dir")"
    HOST_TELEMETRY_SAMPLES_PATH="${HOST_SAMPLER_DIR}/${session_label}-host-telemetry.jsonl"
    HOST_TELEMETRY_SUMMARY_PATH="${HOST_SAMPLER_DIR}/${session_label}-host-telemetry-summary.json"

    if has_cmd python3; then
        python3 "${PROFILER_SCRIPT_DIR}/host_telemetry_runner.py" \
            --output-dir "$HOST_SAMPLER_DIR" \
            --label "$session_label" \
            --interval-seconds "$HOST_SAMPLER_INTERVAL_SECONDS" \
            >"${HOST_SAMPLER_DIR}/host-telemetry.log" 2>&1 &
        HOST_SAMPLER_PIDS+=("$!")
    else
        warn "python3 not found; skipping machine-readable host telemetry capture"
    fi

    _start_host_sampler_process "${HOST_SAMPLER_DIR}/socket-summary.log" _sample_socket_summary "$host" "$port"
    _start_host_sampler_process "${HOST_SAMPLER_DIR}/process-probe.log" _sample_process_probe "$host" "$port"
}

stop_host_sampler_pack() {
    local pid

    if [[ ${#HOST_SAMPLER_PIDS[@]} -eq 0 ]]; then
        return 0
    fi

    for pid in "${HOST_SAMPLER_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done

    for pid in "${HOST_SAMPLER_PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    HOST_SAMPLER_PIDS=()
}