#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

IMAGE="${CACHE_BENCH_GARNET_DOCKER_IMAGE:-garnet-rs-cachebench:latest}"
DOCKERFILE="${CACHE_BENCH_GARNET_DOCKERFILE:-${REPO_ROOT}/garnet-rs/benches/Dockerfile.garnet-rs-cachebench}"
HOST_BIND="${CACHE_BENCH_GARNET_HOST_BIND:-127.0.0.1}"
FALLBACK_PORT="${CACHE_BENCH_GARNET_PORT:-6389}"
READ_BUFFER_SIZE="${CACHE_BENCH_GARNET_READ_BUFFER_SIZE:-}"
VERSION_STRING="${CACHE_BENCH_GARNET_VERSION:-garnet-rs (docker:${IMAGE})}"
PAGE_SIZE_BITS="${CACHE_BENCH_GARNET_PAGE_SIZE_BITS:-${GARNET_TSAVORITE_PAGE_SIZE_BITS:-12}}"
HASH_INDEX_SIZE_BITS="${CACHE_BENCH_GARNET_HASH_INDEX_SIZE_BITS:-${GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS:-}}"
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
            echo "unix socket mode is not supported by garnet-rs docker wrapper" >&2
            exit 2
            ;;
        --miniothreads|--maxiothreads|--minthreads|--maxthreads|--readcache)
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

effective_page_size_bits=12
if [[ "${PAGE_SIZE_BITS}" =~ ^[0-9]+$ ]] && (( PAGE_SIZE_BITS >= 1 && PAGE_SIZE_BITS <= 30 )); then
    effective_page_size_bits="${PAGE_SIZE_BITS}"
fi

MAX_IN_MEMORY_PAGES=""
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
    MAX_IN_MEMORY_PAGES="${max_pages}"
fi

if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
    docker build -f "${DOCKERFILE}" -t "${IMAGE}" "${REPO_ROOT}/garnet-rs" >/dev/null
fi

CONTAINER_NAME="cachebench-garnet-${PORT}"
docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

RUN_ARGS=(
    --rm
    --name "${CONTAINER_NAME}"
    -p "${HOST_BIND}:${PORT}:${PORT}"
    -e "GARNET_BIND_ADDR=0.0.0.0:${PORT}"
)

if [[ -n "${READ_BUFFER_SIZE}" ]]; then
    RUN_ARGS+=( -e "GARNET_READ_BUFFER_SIZE=${READ_BUFFER_SIZE}" )
fi
if [[ -n "${HASH_INDEX_SIZE_BITS}" ]]; then
    RUN_ARGS+=( -e "GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS=${HASH_INDEX_SIZE_BITS}" )
fi
if [[ -n "${PAGE_SIZE_BITS}" ]]; then
    RUN_ARGS+=( -e "GARNET_TSAVORITE_PAGE_SIZE_BITS=${PAGE_SIZE_BITS}" )
fi
if [[ -n "${MAX_IN_MEMORY_PAGES}" ]]; then
    RUN_ARGS+=( -e "GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES=${MAX_IN_MEMORY_PAGES}" )
fi

# cache-benchmarks terminates by process name (pkill garnet),
# so we set argv0 to garnet for compatibility.
exec -a garnet docker run "${RUN_ARGS[@]}" "${IMAGE}"
