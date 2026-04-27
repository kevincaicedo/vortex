#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/common.sh — shared helpers for the profiling orchestrator
# ─────────────────────────────────────────────────────────────────────────────

# ── Colors ───────────────────────────────────────────────────────────────────
if [[ -t 1 ]]; then
    C_RESET='\033[0m'
    C_BOLD='\033[1m'
    C_DIM='\033[2m'
    C_GREEN='\033[32m'
    C_YELLOW='\033[33m'
    C_RED='\033[31m'
    C_CYAN='\033[36m'
    C_BLUE='\033[34m'
else
    C_RESET='' C_BOLD='' C_DIM='' C_GREEN='' C_YELLOW='' C_RED='' C_CYAN='' C_BLUE=''
fi

info()  { printf "${C_CYAN}▸${C_RESET} %s\n" "$*"; }
ok()    { printf "${C_GREEN}✓${C_RESET} %s\n" "$*"; }
warn()  { printf "${C_YELLOW}⚠${C_RESET} %s\n" "$*" >&2; }
err()   { printf "${C_RED}✗${C_RESET} %s\n" "$*" >&2; }
fatal() { err "$@"; exit 1; }
header(){ printf "\n${C_BOLD}${C_BLUE}═══ %s ═══${C_RESET}\n\n" "$*"; }

# ── Resolve repo root ────────────────────────────────────────────────────────
PROFILER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${PROFILER_SCRIPT_DIR}/../.." && pwd)"

# ── OS detection ─────────────────────────────────────────────────────────────
detect_os() {
    case "$(uname -s)" in
        Linux)  echo "linux" ;;
        Darwin) echo "macos" ;;
        *)      echo "unsupported" ;;
    esac
}

OS="$(detect_os)"

# ── Session state ───────────────────────────────────────────────────────────
SESSION_METADATA_FILE=""
SESSION_NOTES_FILE=""
SESSION_MODE=""
SESSION_CARGO_PROFILE=""
SESSION_BINARY_PATH=""
SESSION_COMMAND_LINE=""
SESSION_WORKLOAD_SOURCE=""
SESSION_WORKLOAD_DESCRIPTION=""
SESSION_WORKLOAD_COMMAND=""
SESSION_WORKLOAD_DURATION=""
SESSION_WORKLOAD_CLIENTS=""
SESSION_WORKLOAD_MANIFEST_PATH=""
SESSION_WORKLOAD_REQUEST_PATH=""
SESSION_WORKLOAD_ATTACH_STATE=""
SESSION_SUMMARY_PATH=""
SESSION_TOOLS_REQUESTED=""
SESSION_TOOLS_EXECUTED=""
SESSION_TARGET_PIDS=""

# ── Optional environment loading ─────────────────────────────────────────────
PROFILER_ENV_FILE=""
PROFILER_MACOS_ENTITLEMENTS_FILE="${PROFILER_SCRIPT_DIR}/macos-debug.entitlements"

load_profiler_env() {
    local candidate
    for candidate in "${REPO_ROOT}/.env" "${REPO_ROOT}/scripts/profiler/.env"; do
        if [[ -f "$candidate" ]]; then
            PROFILER_ENV_FILE="$candidate"
            # shellcheck disable=SC1090
            source "$candidate"
            info "Loaded profiler environment from ${candidate}"
            return 0
        fi
    done
}

load_profiler_env

# ── Tool helpers ─────────────────────────────────────────────────────────────
has_cmd() { command -v "$1" >/dev/null 2>&1; }

require_cmd() {
    local name="$1"
    if ! has_cmd "$name"; then
        fatal "Required tool '${name}' is not installed. Run 'just profiler --check' to see available tools."
    fi
}

_append_unique_line() {
    local var_name="$1"
    local value="$2"
    local current="${!var_name:-}"
    local existing

    [[ -z "$value" ]] && return 0

    while IFS= read -r existing; do
        [[ -z "$existing" ]] && continue
        if [[ "$existing" == "$value" ]]; then
            return 0
        fi
    done <<< "$current"

    if [[ -z "$current" ]]; then
        printf -v "$var_name" '%s' "$value"
    else
        printf -v "$var_name" '%s\n%s' "$current" "$value"
    fi
}

record_requested_tool() {
    _append_unique_line SESSION_TOOLS_REQUESTED "$1"
}

record_executed_tool() {
    _append_unique_line SESSION_TOOLS_EXECUTED "$1"
}

record_session_pid() {
    local pid="${1:-}"
    [[ -z "$pid" ]] && return 0
    _append_unique_line SESSION_TARGET_PIDS "$pid"
}

discover_pid_by_port() {
    local port="$1"

    if [[ "$OS" == "linux" ]] && has_cmd ss; then
        ss -ltnp 2>/dev/null \
            | awk -v port=":${port}$" '$4 ~ port { print }' \
            | grep -o 'pid=[0-9]\+' \
            | head -1 \
            | cut -d= -f2 || true
        return 0
    fi

    if [[ "$OS" == "macos" ]] && has_cmd lsof; then
        lsof -tiTCP:"${port}" -sTCP:LISTEN 2>/dev/null | head -1 || true
        return 0
    fi

    return 0
}

# ── Privilege helpers ────────────────────────────────────────────────────────
SUDO_SESSION_READY=false

ensure_sudo_access() {
    local purpose="${1:-sudo access required}"

    if [[ "$SUDO_SESSION_READY" == "true" ]]; then
        return 0
    fi

    if [[ -n "${HOST_PASSWORD:-}" ]]; then
        info "Authenticating sudo using HOST_PASSWORD from environment"
        if printf '%s\n' "$HOST_PASSWORD" | sudo -S -p '' -v >/dev/null 2>&1; then
            SUDO_SESSION_READY=true
            return 0
        fi
        warn "HOST_PASSWORD authentication failed. Falling back to interactive sudo prompt."
    fi

    sudo -v || fatal "$purpose"
    SUDO_SESSION_READY=true
}

run_with_sudo() {
    local purpose="$1"
    shift
    ensure_sudo_access "$purpose"
    sudo "$@"
}

# ── macOS attachability helpers ──────────────────────────────────────────────
binary_has_get_task_allow() {
    local binary="$1"
    local entitlements_output

    [[ -x "$binary" ]] || return 1
    has_cmd codesign || return 1

    entitlements_output="$(codesign -d --entitlements - "$binary" 2>&1 || true)"
    [[ "$entitlements_output" == *"com.apple.security.get-task-allow"* ]] || return 1
    [[ "$entitlements_output" == *"[Bool] true"* ]]
}

ensure_macos_debuggable_binary() {
    local binary="$1"

    [[ "$OS" == "macos" ]] || return 0
    [[ -x "$binary" ]] || fatal "Binary '${binary}' does not exist or is not executable"

    if binary_has_get_task_allow "$binary"; then
        return 0
    fi

    has_cmd codesign || fatal "codesign is required on macOS to make profiler targets attachable"
    [[ -f "$PROFILER_MACOS_ENTITLEMENTS_FILE" ]] || fatal "Missing macOS profiler entitlements file: ${PROFILER_MACOS_ENTITLEMENTS_FILE}"

    info "Ad-hoc signing profiling binary for macOS task inspection"
    codesign --force --sign - --entitlements "$PROFILER_MACOS_ENTITLEMENTS_FILE" "$binary" >/dev/null 2>&1 \
        || fatal "codesign failed for ${binary}; Instruments cannot attach without com.apple.security.get-task-allow"
    ok "Profiling binary is now debuggable by Instruments"
}

# ── Timestamp ────────────────────────────────────────────────────────────────
timestamp() { date -u "+%Y%m%d-%H%M%S"; }

probe_server_ready() {
    local host="$1" port="$2"

    if has_cmd redis-cli; then
        redis-cli -h "$host" -p "$port" PING 2>/dev/null | grep -q '^PONG$'
        return $?
    fi

    echo -ne "*1\r\n\$4\r\nPING\r\n" | nc -w 1 "$host" "$port" 2>/dev/null | grep -q "PONG"
}

# ── Server readiness probe (pure bash + nc) ──────────────────────────────────
wait_for_server_ready() {
    local host="$1" port="$2" timeout="${3:-20}"
    local deadline=$(( $(date +%s) + timeout ))

    info "Waiting for server at ${host}:${port}..."
    while [[ $(date +%s) -lt $deadline ]]; do
        # Try sending PING and check for PONG
        if probe_server_ready "$host" "$port"; then
            local discovered_pid
            discovered_pid="$(discover_pid_by_port "$port")"
            record_session_pid "$discovered_pid"
            ok "Server ready at ${host}:${port}"
            return 0
        fi
        sleep 0.3
    done

    return 1
}

wait_for_server() {
    local host="$1" port="$2" timeout="${3:-20}"

    if wait_for_server_ready "$host" "$port" "$timeout"; then
        return 0
    fi

    fatal "Server at ${host}:${port} did not become ready within ${timeout}s"
}

# ── Default profiling binary path ────────────────────────────────────────────
PROFILING_BINARY="${REPO_ROOT}/target/profiling/vortex-server"

# ── Session directory factory ────────────────────────────────────────────────
make_session_dir() {
    local tag="${1:-session}"
    local dir="${REPO_ROOT}/.artifacts/profiling/$(timestamp)-${tag}"
    mkdir -p "$dir"
    echo "$dir"
}

create_session_notes_template() {
    local session_dir="$1"
    SESSION_NOTES_FILE="${session_dir}/notes.md"
    cat >"${SESSION_NOTES_FILE}" <<'EOF'
# Profiler Session Notes

## Question

-

## Hotspot

-

## Interpretation

-

## Follow-Up

-
EOF
}

write_session_contract() {
    local session_dir="$1"
    local session_status="${2:-running}"
    local session_exit_code="${3:-0}"
    SESSION_METADATA_FILE="${session_dir}/session.json"

    SESSION_DIR="$session_dir" \
    REPO_ROOT="$REPO_ROOT" \
    SESSION_STATUS="$session_status" \
    SESSION_EXIT_CODE="$session_exit_code" \
    SESSION_MODE="$SESSION_MODE" \
    SESSION_CARGO_PROFILE="$SESSION_CARGO_PROFILE" \
    SESSION_BINARY_PATH="$SESSION_BINARY_PATH" \
    SESSION_COMMAND_LINE="$SESSION_COMMAND_LINE" \
    SESSION_WORKLOAD_SOURCE="$SESSION_WORKLOAD_SOURCE" \
    SESSION_WORKLOAD_DESCRIPTION="$SESSION_WORKLOAD_DESCRIPTION" \
    SESSION_WORKLOAD_COMMAND="$SESSION_WORKLOAD_COMMAND" \
    SESSION_WORKLOAD_DURATION="$SESSION_WORKLOAD_DURATION" \
    SESSION_WORKLOAD_CLIENTS="$SESSION_WORKLOAD_CLIENTS" \
    SESSION_WORKLOAD_MANIFEST_PATH="$SESSION_WORKLOAD_MANIFEST_PATH" \
    SESSION_WORKLOAD_REQUEST_PATH="$SESSION_WORKLOAD_REQUEST_PATH" \
    SESSION_WORKLOAD_ATTACH_STATE="$SESSION_WORKLOAD_ATTACH_STATE" \
    SESSION_SUMMARY_PATH="$SESSION_SUMMARY_PATH" \
    SESSION_TOOLS_REQUESTED="$SESSION_TOOLS_REQUESTED" \
    SESSION_TOOLS_EXECUTED="$SESSION_TOOLS_EXECUTED" \
    SESSION_TARGET_PIDS="$SESSION_TARGET_PIDS" \
    SESSION_NOTES_FILE="$SESSION_NOTES_FILE" \
    python3 - <<'PY'
from __future__ import annotations

import json
import os
import platform
import socket
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def split_lines(name: str) -> list[str]:
    raw = os.environ.get(name, "")
    return [line for line in raw.splitlines() if line.strip()]


def run(command: list[str], cwd: str | None = None) -> str | None:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            text=True,
            capture_output=True,
        )
    except FileNotFoundError:
        return None

    if result.returncode != 0:
        return None

    output = (result.stdout or result.stderr or "").strip()
    return output or None


session_dir = Path(os.environ["SESSION_DIR"]).resolve()
repo_root = Path(os.environ["REPO_ROOT"]).resolve()

artifacts = [
    str(path)
    for path in sorted(session_dir.rglob("*"))
    if path.is_file() and path.name != "session.json"
]
artifacts.append(str(session_dir / "session.json"))
artifacts = sorted(set(artifacts))

requested_tools = split_lines("SESSION_TOOLS_REQUESTED")
executed_tools = split_lines("SESSION_TOOLS_EXECUTED") or requested_tools
target_pids = []
for raw_pid in split_lines("SESSION_TARGET_PIDS"):
    try:
        target_pids.append(int(raw_pid))
    except ValueError:
        continue

payload = {
    "schema_version": 1,
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "session_id": session_dir.name,
    "session_dir": str(session_dir),
    "status": os.environ.get("SESSION_STATUS") or "running",
    "exit_code": int(os.environ.get("SESSION_EXIT_CODE") or "0"),
    "mode": os.environ.get("SESSION_MODE") or None,
    "os": platform.system(),
    "host": socket.gethostname(),
    "kernel": {
        "release": platform.release(),
        "version": platform.version(),
    },
    "architecture": platform.machine(),
    "binary_path": os.environ.get("SESSION_BINARY_PATH") or None,
    "cargo_profile": os.environ.get("SESSION_CARGO_PROFILE") or None,
    "git": {
        "revision": run(["git", "rev-parse", "HEAD"], cwd=str(repo_root)),
        "dirty": bool(run(["git", "status", "--porcelain"], cwd=str(repo_root)) or ""),
    },
    "tool": executed_tools[0] if len(executed_tools) == 1 else None,
    "tools_requested": requested_tools,
    "tools_executed": executed_tools,
    "command_line": os.environ.get("SESSION_COMMAND_LINE") or None,
    "pid": target_pids[0] if len(target_pids) == 1 else None,
    "target_pids": target_pids,
    "workload_description": os.environ.get("SESSION_WORKLOAD_DESCRIPTION") or None,
    "workload": {
        "source": os.environ.get("SESSION_WORKLOAD_SOURCE") or None,
        "description": os.environ.get("SESSION_WORKLOAD_DESCRIPTION") or None,
        "command": os.environ.get("SESSION_WORKLOAD_COMMAND") or None,
        "duration_seconds": int(os.environ["SESSION_WORKLOAD_DURATION"])
        if (os.environ.get("SESSION_WORKLOAD_DURATION") or "").isdigit()
        else None,
        "clients": int(os.environ["SESSION_WORKLOAD_CLIENTS"])
        if (os.environ.get("SESSION_WORKLOAD_CLIENTS") or "").isdigit()
        else None,
        "manifest_path": os.environ.get("SESSION_WORKLOAD_MANIFEST_PATH") or None,
        "request_path": os.environ.get("SESSION_WORKLOAD_REQUEST_PATH") or None,
        "attach_state_file": os.environ.get("SESSION_WORKLOAD_ATTACH_STATE") or None,
    },
    "notes_path": os.environ.get("SESSION_NOTES_FILE") or None,
    "summary_path": os.environ.get("SESSION_SUMMARY_PATH") or None,
    "artifact_paths": artifacts,
}

(session_dir / "session.json").write_text(
    json.dumps(payload, indent=2) + "\n",
    encoding="utf-8",
)
PY
}

initialize_session_contract() {
    local session_dir="$1"
    create_session_notes_template "$session_dir"
    write_session_contract "$session_dir" "running" 0
}

finalize_session_contract() {
    local session_dir="$1"
    local session_status="${2:-completed}"
    local session_exit_code="${3:-0}"
    write_session_contract "$session_dir" "$session_status" "$session_exit_code"
}
