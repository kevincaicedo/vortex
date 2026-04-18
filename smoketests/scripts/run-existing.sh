#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [[ "$*" == *"--server-url"* ]]; then
    cargo run -p vortex-smoketests -- run "$@"
else
    cargo run -p vortex-smoketests -- run --server-url "${VORTEX_SMOKE_SERVER_URL:-redis://127.0.0.1:6379/}" "$@"
fi
