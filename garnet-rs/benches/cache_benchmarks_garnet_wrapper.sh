#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SERVER_BIN="${CACHE_BENCH_GARNET_SERVER_BIN:-${REPO_ROOT}/garnet-rs/target/release/garnet-server}"
HOST="${CACHE_BENCH_GARNET_HOST:-127.0.0.1}"
FALLBACK_PORT="${CACHE_BENCH_GARNET_PORT:-6389}"
READ_BUFFER_SIZE="${CACHE_BENCH_GARNET_READ_BUFFER_SIZE:-}"
VERSION_STRING="${CACHE_BENCH_GARNET_VERSION:-garnet-rs}"

if [[ "${1:-}" == "--version" ]]; then
    echo "${VERSION_STRING}"
    exit 0
fi

if [[ ! -x "${SERVER_BIN}" ]]; then
    if [[ -f "${REPO_ROOT}/garnet-rs/Cargo.toml" ]]; then
        cargo build -p garnet-server --release --manifest-path "${REPO_ROOT}/garnet-rs/Cargo.toml" >/dev/null
    fi
fi
if [[ ! -x "${SERVER_BIN}" ]]; then
    echo "garnet-server binary not found: ${SERVER_BIN}" >&2
    exit 1
fi

PORT="${FALLBACK_PORT}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --port)
            PORT="${2:-${PORT}}"
            shift 2
            ;;
        --unixsocket)
            echo "unix socket mode is not supported by garnet-rs server wrapper" >&2
            exit 2
            ;;
        --miniothreads|--maxiothreads|--minthreads|--maxthreads|--index|--memory|--readcache)
            shift 2
            ;;
        --no-obj|--aof-null-device)
            shift 1
            ;;
        *)
            shift 1
            ;;
    esac
done

export GARNET_BIND_ADDR="${HOST}:${PORT}"
if [[ -n "${READ_BUFFER_SIZE}" ]]; then
    export GARNET_READ_BUFFER_SIZE="${READ_BUFFER_SIZE}"
fi

# Match the expected process name for cache-benchmarks cleanup (pkill garnet).
exec -a garnet "${SERVER_BIN}"
