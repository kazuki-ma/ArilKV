#!/usr/bin/env bash
set -euo pipefail

IMAGE="${CACHE_BENCH_DRAGONFLY_IMAGE:-docker.dragonflydb.io/dragonflydb/dragonfly:v1.36.0}"
VERSION_STRING="${CACHE_BENCH_DRAGONFLY_VERSION:-dragonfly (docker:${IMAGE})}"
HOST_BIND="${CACHE_BENCH_DRAGONFLY_HOST_BIND:-127.0.0.1}"
DEFAULT_PORT="${CACHE_BENCH_DRAGONFLY_PORT:-6379}"

if [[ "${1:-}" == "--version" ]]; then
    echo "${VERSION_STRING}"
    exit 0
fi

PORT="${DEFAULT_PORT}"
ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --port)
            PORT="${2:-${PORT}}"
            ARGS+=("$1" "${2:-}")
            shift 2
            ;;
        --unixsocket)
            echo "unix socket mode is not supported by dragonfly docker wrapper" >&2
            exit 2
            ;;
        *)
            ARGS+=("$1")
            shift 1
            ;;
    esac
done

CONTAINER_NAME="cachebench-dragonfly-${PORT}"

docker pull "${IMAGE}" >/dev/null
docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

# cache-benchmarks terminates by process name (pkill dragonfly),
# so we set argv0 to dragonfly for compatibility.
exec -a dragonfly docker run \
    --rm \
    --name "${CONTAINER_NAME}" \
    -p "${HOST_BIND}:${PORT}:${PORT}" \
    "${IMAGE}" \
    "${ARGS[@]}"
