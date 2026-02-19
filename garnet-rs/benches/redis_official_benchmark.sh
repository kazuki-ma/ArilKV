#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORK_ROOT="${REDIS_BENCH_WORK_ROOT:-${REPO_ROOT}/.redis-benchmark}"
RESULTS_DIR="${REDIS_BENCH_RESULTS_DIR:-${SCRIPT_DIR}/results}"
REDIS_DOWNLOAD_URL="${REDIS_BENCH_DOWNLOAD_URL:-https://download.redis.io/redis-stable.tar.gz}"
REDIS_ARCHIVE="${WORK_ROOT}/redis-stable.tar.gz"
REDIS_SRC_DIR="${WORK_ROOT}/redis-stable"

HOST="${REDIS_BENCH_HOST:-127.0.0.1}"
PORT="${REDIS_BENCH_PORT:-6389}"
REQUESTS="${REDIS_BENCH_REQUESTS:-100000}"
CLIENTS="${REDIS_BENCH_CLIENTS:-50}"
DATA_SIZE="${REDIS_BENCH_DATA_SIZE:-32}"
TESTS="${REDIS_BENCH_TESTS:-ping_mbulk,set,get,incr}"
READY_TEST="${REDIS_BENCH_READY_TEST:-ping_mbulk}"

START_SERVER="${REDIS_BENCH_START_SERVER:-1}"
FORCE_DOWNLOAD="${REDIS_BENCH_FORCE_DOWNLOAD:-0}"
SERVER_BUILD_MODE="${REDIS_BENCH_SERVER_BUILD_MODE:-release}"
SERVER_BIN="${REDIS_BENCH_SERVER_BIN:-${REPO_ROOT}/garnet-rs/target/${SERVER_BUILD_MODE}/garnet-server}"
SERVER_LOG="${WORK_ROOT}/garnet-server-${PORT}.log"

mkdir -p "${WORK_ROOT}"
mkdir -p "${RESULTS_DIR}"

SERVER_PID=""

cleanup() {
    if [[ -n "${SERVER_PID}" ]]; then
        kill "${SERVER_PID}" >/dev/null 2>&1 || true
        wait "${SERVER_PID}" >/dev/null 2>&1 || true
    fi
}

trap cleanup EXIT

ensure_redis_benchmark() {
    if [[ "${FORCE_DOWNLOAD}" != "1" ]] && command -v redis-benchmark >/dev/null 2>&1; then
        command -v redis-benchmark
        return
    fi

    if [[ ! -x "${REDIS_SRC_DIR}/src/redis-benchmark" ]]; then
        rm -rf "${REDIS_SRC_DIR}"
        curl -fsSL "${REDIS_DOWNLOAD_URL}" -o "${REDIS_ARCHIVE}"
        tar -xzf "${REDIS_ARCHIVE}" -C "${WORK_ROOT}"
        make -C "${REDIS_SRC_DIR}" redis-benchmark MALLOC=libc >/dev/null
    fi

    echo "${REDIS_SRC_DIR}/src/redis-benchmark"
}

wait_for_server() {
    local benchmark_bin="$1"
    local attempts=120
    local sleep_sec=0.1
    for ((i = 0; i < attempts; i++)); do
        if "${benchmark_bin}" -h "${HOST}" -p "${PORT}" -n 1 -c 1 -t "${READY_TEST}" -q >/dev/null 2>&1; then
            return 0
        fi
        sleep "${sleep_sec}"
    done
    return 1
}

start_server_if_needed() {
    if [[ "${START_SERVER}" != "1" ]]; then
        return
    fi

    if [[ ! -x "${SERVER_BIN}" ]]; then
        if [[ "${SERVER_BUILD_MODE}" == "release" ]]; then
            cargo build -p garnet-server --release --manifest-path "${REPO_ROOT}/garnet-rs/Cargo.toml"
        else
            cargo build -p garnet-server --manifest-path "${REPO_ROOT}/garnet-rs/Cargo.toml"
        fi
    fi

    GARNET_BIND_ADDR="${HOST}:${PORT}" "${SERVER_BIN}" >"${SERVER_LOG}" 2>&1 &
    SERVER_PID="$!"
}

BENCHMARK_BIN="$(ensure_redis_benchmark)"
start_server_if_needed

if ! wait_for_server "${BENCHMARK_BIN}"; then
    echo "server did not become ready at ${HOST}:${PORT}" >&2
    exit 1
fi

TIMESTAMP="$(date +"%Y%m%d-%H%M%S")"
OUT_FILE="${RESULTS_DIR}/redis-official-benchmark-${TIMESTAMP}.txt"

{
    echo "# Redis official benchmark run"
    echo "# benchmark_bin=${BENCHMARK_BIN}"
    echo "# host=${HOST}"
    echo "# port=${PORT}"
    echo "# requests=${REQUESTS}"
    echo "# clients=${CLIENTS}"
    echo "# data_size=${DATA_SIZE}"
    echo "# tests=${TESTS}"
    echo "# start_server=${START_SERVER}"
    echo
    "${BENCHMARK_BIN}" \
        -h "${HOST}" \
        -p "${PORT}" \
        -n "${REQUESTS}" \
        -c "${CLIENTS}" \
        -d "${DATA_SIZE}" \
        -t "${TESTS}" \
        -q
} | tee "${OUT_FILE}"

echo
echo "saved benchmark report: ${OUT_FILE}"
