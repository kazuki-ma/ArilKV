#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
GARNET_RS_ROOT="${WORKSPACE_ROOT}/garnet-rs"

RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/results/command-coverage-$(date +%Y%m%d-%H%M%S)}"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7.2-alpine}"
DRAGONFLY_IMAGE="${DRAGONFLY_IMAGE:-docker.dragonflydb.io/dragonflydb/dragonfly:v1.36.0}"
REDIS_PORT="${REDIS_PORT:-6391}"
GARNET_PORT="${GARNET_PORT:-6392}"
DRAGONFLY_PORT="${DRAGONFLY_PORT:-6393}"
GARNET_SERVER_CMD="${GARNET_SERVER_CMD:-cargo run -p garnet-server --release}"

mkdir -p "${RESULT_DIR}"

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required command: $1" >&2
        exit 2
    fi
}

require_cmd docker
require_cmd redis-cli
require_cmd jq
require_cmd awk
require_cmd comm

REDIS_CID=""
DRAGONFLY_CID=""
GARNET_PID=""

cleanup() {
    if [[ -n "${GARNET_PID}" ]]; then
        kill "${GARNET_PID}" >/dev/null 2>&1 || true
        wait "${GARNET_PID}" >/dev/null 2>&1 || true
    fi
    if [[ -n "${REDIS_CID}" ]]; then
        docker kill "${REDIS_CID}" >/dev/null 2>&1 || true
    fi
    if [[ -n "${DRAGONFLY_CID}" ]]; then
        docker kill "${DRAGONFLY_CID}" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

wait_for_ping() {
    local port="$1"
    for _ in $(seq 1 300); do
        if redis-cli -h 127.0.0.1 -p "${port}" PING >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

collect_commands() {
    local port="$1"
    local output_path="$2"
    redis-cli -h 127.0.0.1 -p "${port}" --json COMMAND \
        | jq -r 'if (.[0] | type) == "array" then .[][0] else .[] end' \
        | tr '[:lower:]' '[:upper:]' \
        | sort -u > "${output_path}"
}

REDIS_CID="$(docker run -d --rm -p "${REDIS_PORT}:6379" "${REDIS_IMAGE}")"
DRAGONFLY_CID="$(docker run -d --rm -p "${DRAGONFLY_PORT}:6379" "${DRAGONFLY_IMAGE}" --port=6379)"

if ! wait_for_ping "${REDIS_PORT}"; then
    echo "redis startup failed on port ${REDIS_PORT}" >&2
    exit 1
fi
if ! wait_for_ping "${DRAGONFLY_PORT}"; then
    echo "dragonfly startup failed on port ${DRAGONFLY_PORT}" >&2
    exit 1
fi

(
    cd "${GARNET_RS_ROOT}"
    GARNET_BIND_ADDR="127.0.0.1:${GARNET_PORT}" bash -lc "${GARNET_SERVER_CMD}"
) >"${RESULT_DIR}/garnet-server.log" 2>&1 &
GARNET_PID="$!"

if ! wait_for_ping "${GARNET_PORT}"; then
    echo "garnet startup failed on port ${GARNET_PORT}" >&2
    exit 1
fi

collect_commands "${REDIS_PORT}" "${RESULT_DIR}/redis-commands.txt"
collect_commands "${DRAGONFLY_PORT}" "${RESULT_DIR}/dragonfly-commands.txt"
collect_commands "${GARNET_PORT}" "${RESULT_DIR}/garnet-commands.txt"

comm -23 "${RESULT_DIR}/redis-commands.txt" "${RESULT_DIR}/garnet-commands.txt" \
    > "${RESULT_DIR}/redis-missing-in-garnet.txt"
comm -23 "${RESULT_DIR}/garnet-commands.txt" "${RESULT_DIR}/redis-commands.txt" \
    > "${RESULT_DIR}/garnet-not-in-redis.txt"
comm -23 "${RESULT_DIR}/garnet-commands.txt" "${RESULT_DIR}/dragonfly-commands.txt" \
    > "${RESULT_DIR}/garnet-not-in-dragonfly.txt"
comm -23 "${RESULT_DIR}/redis-commands.txt" "${RESULT_DIR}/dragonfly-commands.txt" \
    > "${RESULT_DIR}/redis-not-in-dragonfly.txt"
comm -13 "${RESULT_DIR}/redis-commands.txt" "${RESULT_DIR}/dragonfly-commands.txt" \
    > "${RESULT_DIR}/dragonfly-not-in-redis.txt"

redis_total="$(wc -l < "${RESULT_DIR}/redis-commands.txt" | tr -d ' ')"
dragonfly_total="$(wc -l < "${RESULT_DIR}/dragonfly-commands.txt" | tr -d ' ')"
garnet_total="$(wc -l < "${RESULT_DIR}/garnet-commands.txt" | tr -d ' ')"
missing_total="$(wc -l < "${RESULT_DIR}/redis-missing-in-garnet.txt" | tr -d ' ')"

coverage_pct="$(awk -v g="${garnet_total}" -v r="${redis_total}" \
    'BEGIN { if (r == 0) print "0.00"; else printf "%.2f", (g * 100.0) / r }')"

{
    echo "command coverage audit"
    echo "result_dir=${RESULT_DIR}"
    echo "redis_total=${redis_total}"
    echo "dragonfly_total=${dragonfly_total}"
    echo "garnet_total=${garnet_total}"
    echo "redis_missing_in_garnet=${missing_total}"
    echo "garnet_vs_redis_coverage_pct=${coverage_pct}"
    echo "garnet_not_in_dragonfly=$(wc -l < "${RESULT_DIR}/garnet-not-in-dragonfly.txt" | tr -d ' ')"
    echo "redis_not_in_dragonfly=$(wc -l < "${RESULT_DIR}/redis-not-in-dragonfly.txt" | tr -d ' ')"
    echo "dragonfly_not_in_redis=$(wc -l < "${RESULT_DIR}/dragonfly-not-in-redis.txt" | tr -d ' ')"
} | tee "${RESULT_DIR}/summary.txt"

echo ""
echo "top redis commands missing in garnet (first 40):"
head -n 40 "${RESULT_DIR}/redis-missing-in-garnet.txt" || true
