#!/usr/bin/env bash
# Remote profiler delegation helpers.

# Expects common.sh and profiler.sh argument state to be loaded.

_remote_append_requested_tools() {
    local -n args_ref="$1"

    $MODE_CPU && args_ref+=(--cpu)
    $MODE_SCHEDULER && args_ref+=(--scheduler)
    $MODE_LOCK_OFFCPU && args_ref+=(--lock-offcpu)
    $MODE_MEMORY && args_ref+=(--memory)
    $MODE_CACHE && args_ref+=(--cache)
    $MODE_C2C && args_ref+=(--c2c)
    $MODE_AOF_DISK && args_ref+=(--aof-disk)
    $MODE_NETWORK && args_ref+=(--network)
    $MODE_ALL && args_ref+=(--all)
    $TOOL_FLAMEGRAPH && args_ref+=(--flamegraph)
    $TOOL_PERF_STAT && args_ref+=(--perf-stat)
    $TOOL_SAMPLY && args_ref+=(--samply)
    $TOOL_INSTRUMENTS && args_ref+=(--instruments)
    $TOOL_HEAPTRACK && args_ref+=(--heaptrack)
    $TOOL_CACHEGRIND && args_ref+=(--cachegrind)
    $TOOL_CALLGRIND && args_ref+=(--callgrind)
    $TOOL_MASSIF && args_ref+=(--massif)
}

_remote_append_server_and_workload_args() {
    local -n args_ref="$1"

    [[ -n "$COMMAND" ]] && args_ref+=(--command "$COMMAND")
    [[ -n "$DURATION" ]] && args_ref+=(--duration "$DURATION")
    [[ -n "$CLIENTS" ]] && args_ref+=(--clients "$CLIENTS")
    [[ -n "$MANIFEST" ]] && args_ref+=(--manifest "$MANIFEST")
    [[ -n "${BENCH_MANIFEST:-}" ]] && args_ref+=(--bench-manifest "$BENCH_MANIFEST")
    [[ -n "${BENCH_REQUEST:-}" ]] && args_ref+=(--bench-request "$BENCH_REQUEST")
    [[ -n "$THREADS" ]] && args_ref+=(--threads "$THREADS")
    [[ "$AOF" == "true" ]] && args_ref+=(--aof)
    [[ -n "$MAXMEMORY" ]] && args_ref+=(--maxmemory "$MAXMEMORY")
    [[ -n "$EVICTION" ]] && args_ref+=(--eviction "$EVICTION")
    [[ -n "$IO_BACKEND" ]] && args_ref+=(--io-backend "$IO_BACKEND")
    [[ -n "$RING_SIZE" ]] && args_ref+=(--ring-size "$RING_SIZE")
    [[ -n "$FIXED_BUFFERS" ]] && args_ref+=(--fixed-buffers "$FIXED_BUFFERS")
    [[ -n "$SQPOLL_IDLE_MS" ]] && args_ref+=(--sqpoll-idle-ms "$SQPOLL_IDLE_MS")
    [[ -n "$FREQUENCY" ]] && args_ref+=(--frequency "$FREQUENCY")
}

_remote_copy_source() {
    local remote_root="$1"
    if [[ "$remote_root" == /* ]]; then
        printf '%s' "$remote_root"
    else
        printf '%s/%s' "${SSH_WORKDIR%/}" "${remote_root#./}"
    fi
}

_remote_append_ssh_transport_args() {
    local -n args_ref="$1"
    local mode="${2:-ssh}"

    [[ -n "${SSH_CONFIG:-}" ]] && args_ref+=(-F "$SSH_CONFIG")
    [[ -n "${SSH_IDENTITY_FILE:-}" ]] && args_ref+=(-i "$SSH_IDENTITY_FILE")
    if [[ -n "${SSH_PORT:-}" ]]; then
        if [[ "$mode" == "scp" ]]; then
            args_ref+=(-P "$SSH_PORT")
        else
            args_ref+=(-p "$SSH_PORT")
        fi
    fi
    [[ -n "${SSH_CONNECT_TIMEOUT:-}" ]] && args_ref+=(-o "ConnectTimeout=${SSH_CONNECT_TIMEOUT}")
    local option
    for option in "${SSH_OPTIONS[@]:-}"; do
        [[ -n "$option" ]] && args_ref+=(-o "$option")
    done
}

run_remote_profiler_session() {
    local session_dir="$1"
    local remote_root="${SSH_ARTIFACT_PATH:-.artifacts/profiling/${session_dir##*/}}"
    local local_return_root="${SESSION_ARTIFACT_ROOT:-${REPO_ROOT}/.artifacts/profiling}/remote/${session_dir##*/}"
    local remote_args=()
    local remote_mode="host-port"
    local remote_command=""
    local remote_rc=0
    local start_rc=0
    local stop_rc=0
    local ssh_args=()
    local scp_args=()

    require_cmd ssh
    [[ -n "$SSH_WORKDIR" ]] || fatal "${TARGET_MODE} requires --ssh-workdir so the remote profiler checkout is explicit"
    _remote_append_ssh_transport_args ssh_args ssh
    _remote_append_ssh_transport_args scp_args scp

    _remote_append_requested_tools remote_args
    _remote_append_server_and_workload_args remote_args
    remote_args+=(--artifact-root "$remote_root" --no-color)
    if $JSON_OUTPUT; then
        remote_args+=(--json)
    fi

    if [[ "$TARGET_MODE" == "ssh-managed" ]]; then
        remote_mode="host-port"
    fi
    remote_args+=(--target-mode "$remote_mode" --host "$HOST" --port "$PORT")

    {
        printf 'ssh_target=%s\n' "$SSH_TARGET"
        printf 'ssh_workdir=%s\n' "$SSH_WORKDIR"
        printf 'ssh_port=%s\n' "${SSH_PORT:-}"
        printf 'ssh_identity_file=%s\n' "${SSH_IDENTITY_FILE:-}"
        printf 'ssh_config=%s\n' "${SSH_CONFIG:-}"
        printf 'ssh_options=%s\n' "$(printf '%s ' "${SSH_OPTIONS[@]:-}")"
        printf 'ssh_connect_timeout=%s\n' "${SSH_CONNECT_TIMEOUT:-}"
        printf 'remote_artifact_root=%s\n' "$remote_root"
        printf 'local_artifact_return=%s\n' "$local_return_root"
        printf 'remote_target_mode=%s\n' "$remote_mode"
    } >"${session_dir}/remote-plan.txt"

    ssh "${ssh_args[@]}" "$SSH_TARGET" "cd $(printf '%q' "$SSH_WORKDIR") && uname -a && command -v perf >/dev/null 2>&1 && perf --version || true" \
        >"${session_dir}/remote-preflight.txt" 2>"${session_dir}/remote-preflight.err" || true

    if [[ "$TARGET_MODE" == "ssh-managed" ]]; then
        [[ -n "$SSH_START_COMMAND" ]] || fatal "ssh-managed requires --ssh-start-command"
        info "Running remote profiler start command on ${SSH_TARGET}"
        ssh "${ssh_args[@]}" "$SSH_TARGET" "cd $(printf '%q' "$SSH_WORKDIR") && ${SSH_START_COMMAND}" \
            >"${session_dir}/remote-start.log" 2>&1 || start_rc=$?
        if [[ "$start_rc" -ne 0 ]]; then
            warn "Remote start command failed with exit ${start_rc}; see ${session_dir}/remote-start.log"
            return "$start_rc"
        fi
    fi

    remote_command="cd $(printf '%q' "$SSH_WORKDIR") && bash scripts/profiler.sh $(shell_join "${remote_args[@]}")"
    printf '%s\n' "$remote_command" >"${session_dir}/remote-command.txt"
    info "Delegating profiler capture to ${SSH_TARGET}"
    ssh "${ssh_args[@]}" "$SSH_TARGET" "$remote_command" >"${session_dir}/remote-profiler.log" 2>&1 || remote_rc=$?

    if [[ "$TARGET_MODE" == "ssh-managed" && -n "$SSH_STOP_COMMAND" ]]; then
        info "Running remote profiler stop command on ${SSH_TARGET}"
        ssh "${ssh_args[@]}" "$SSH_TARGET" "cd $(printf '%q' "$SSH_WORKDIR") && ${SSH_STOP_COMMAND}" \
            >"${session_dir}/remote-stop.log" 2>&1 || stop_rc=$?
        if [[ "$stop_rc" -ne 0 ]]; then
            warn "Remote stop command failed with exit ${stop_rc}; see ${session_dir}/remote-stop.log"
        fi
    fi

    if has_cmd scp; then
        mkdir -p "$local_return_root"
        scp "${scp_args[@]}" -r "${SSH_TARGET}:$(_remote_copy_source "$remote_root")" "$local_return_root/" \
            >"${session_dir}/remote-artifact-copy.log" 2>&1 || {
            warn "Remote artifact copy failed; see ${session_dir}/remote-artifact-copy.log"
        }
    fi

    return "$remote_rc"
}
