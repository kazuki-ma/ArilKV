#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SERVER_BIN="${CACHE_BENCH_GARNET_SERVER_BIN:-${REPO_ROOT}/garnet-rs/target/release/garnet-server}"
HOST="${CACHE_BENCH_GARNET_HOST:-127.0.0.1}"
FALLBACK_PORT="${CACHE_BENCH_GARNET_PORT:-6389}"
READ_BUFFER_SIZE="${CACHE_BENCH_GARNET_READ_BUFFER_SIZE:-}"
VERSION_STRING="${CACHE_BENCH_GARNET_VERSION:-garnet-rs}"
PAGE_SIZE_BITS="${CACHE_BENCH_GARNET_PAGE_SIZE_BITS:-${GARNET_TSAVORITE_PAGE_SIZE_BITS:-12}}"
HASH_INDEX_SIZE_BITS="${CACHE_BENCH_GARNET_HASH_INDEX_SIZE_BITS:-${GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS:-}}"
STRING_STORE_SHARDS="${CACHE_BENCH_GARNET_STRING_STORE_SHARDS:-${GARNET_TSAVORITE_STRING_STORE_SHARDS:-}}"
THREAD_HINT_RAW=""
MEMORY_LIMIT_RAW=""
INDEX_LIMIT_RAW=""

parse_size_to_bytes() {
    local raw="${1,,}"
    if [[ ! "${raw}" =~ ^([0-9]+)([kmgt]?b?)$ ]]; then
        return 1
    fi

    local value="${BASH_REMATCH[1]}"
    local suffix="${BASH_REMATCH[2]}"
    local multiplier=1
    case "${suffix}" in
        ""|b)
            multiplier=1
            ;;
        k|kb)
            multiplier=1024
            ;;
        m|mb)
            multiplier=$((1024 * 1024))
            ;;
        g|gb)
            multiplier=$((1024 * 1024 * 1024))
            ;;
        t|tb)
            multiplier=$((1024 * 1024 * 1024 * 1024))
            ;;
        *)
            return 1
            ;;
    esac
    echo $((value * multiplier))
}

floor_log2() {
    local value="$1"
    local bits=0
    while (( value > 1 )); do
        value=$((value >> 1))
        bits=$((bits + 1))
    done
    echo "${bits}"
}

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
        --memory)
            MEMORY_LIMIT_RAW="${2:-}"
            shift 2
            ;;
        --index)
            INDEX_LIMIT_RAW="${2:-}"
            shift 2
            ;;
        --unixsocket)
            echo "unix socket mode is not supported by garnet-rs server wrapper" >&2
            exit 2
            ;;
        --miniothreads|--maxiothreads|--readcache)
            shift 2
            ;;
        --minthreads|--maxthreads)
            THREAD_HINT_RAW="${2:-}"
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

if [[ -z "${HASH_INDEX_SIZE_BITS}" && -n "${INDEX_LIMIT_RAW}" ]]; then
    index_bytes="$(parse_size_to_bytes "${INDEX_LIMIT_RAW}")" || {
        echo "invalid --index value: ${INDEX_LIMIT_RAW}" >&2
        exit 2
    }
    if (( index_bytes >= 64 )); then
        bucket_count=$((index_bytes / 64))
        derived_bits="$(floor_log2 "${bucket_count}")"
        if (( derived_bits < 1 )); then
            derived_bits=1
        elif (( derived_bits > 30 )); then
            derived_bits=30
        fi
        HASH_INDEX_SIZE_BITS="${derived_bits}"
    fi
fi

if [[ -n "${HASH_INDEX_SIZE_BITS}" ]]; then
    export GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS}"
fi

if [[ -z "${STRING_STORE_SHARDS}" && "${THREAD_HINT_RAW}" =~ ^[0-9]+$ ]] && (( THREAD_HINT_RAW > 0 )); then
    STRING_STORE_SHARDS="${THREAD_HINT_RAW}"
fi
if [[ -n "${STRING_STORE_SHARDS}" ]]; then
    export GARNET_TSAVORITE_STRING_STORE_SHARDS="${STRING_STORE_SHARDS}"
fi

effective_page_size_bits=12
if [[ "${PAGE_SIZE_BITS}" =~ ^[0-9]+$ ]] && (( PAGE_SIZE_BITS >= 1 && PAGE_SIZE_BITS <= 30 )); then
    effective_page_size_bits="${PAGE_SIZE_BITS}"
    export GARNET_TSAVORITE_PAGE_SIZE_BITS="${PAGE_SIZE_BITS}"
fi
if [[ -n "${MEMORY_LIMIT_RAW}" ]]; then
    memory_bytes="$(parse_size_to_bytes "${MEMORY_LIMIT_RAW}")" || {
        echo "invalid --memory value: ${MEMORY_LIMIT_RAW}" >&2
        exit 2
    }
    page_size=$((1 << effective_page_size_bits))
    max_pages=$((memory_bytes / page_size))
    if (( max_pages < 1 )); then
        max_pages=1
    fi
    export GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES="${max_pages}"
fi

# Match the expected process name for cache-benchmarks cleanup (pkill garnet).
exec -a garnet "${SERVER_BIN}"
