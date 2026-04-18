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

# ── Server readiness probe (pure bash + nc) ──────────────────────────────────
wait_for_server_ready() {
    local host="$1" port="$2" timeout="${3:-20}"
    local deadline=$(( $(date +%s) + timeout ))

    info "Waiting for server at ${host}:${port}..."
    while [[ $(date +%s) -lt $deadline ]]; do
        # Try sending PING and check for PONG
        if echo -ne "*1\r\n\$4\r\nPING\r\n" | nc -w 1 "$host" "$port" 2>/dev/null | grep -q "PONG"; then
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
