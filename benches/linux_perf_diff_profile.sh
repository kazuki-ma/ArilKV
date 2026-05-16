#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MANIFEST_PATH="${REPO_ROOT}/Cargo.toml"

GARNET_BIN="${GARNET_BIN:-${REPO_ROOT}/target/release/garnet-server}"
DRAGONFLY_BIN="${DRAGONFLY_BIN:-$(command -v dragonfly || true)}"
VALKEY_BIN="${VALKEY_BIN:-$(command -v valkey-server || command -v redis-server || true)}"
MEMTIER_BIN="${MEMTIER_BIN:-$(command -v memtier_benchmark || true)}"
HOST="${HOST:-127.0.0.1}"
FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-}"

TARGETS="${TARGETS:-garnet dragonfly valkey}"
WORKLOADS="${WORKLOADS:-set get}"
SERVER_CPU_SET="${SERVER_CPU_SET:-}"
CLIENT_CPU_SET="${CLIENT_CPU_SET:-}"
TOKIO_WORKER_THREADS="${TOKIO_WORKER_THREADS:-}"
DRAGONFLY_PROACTOR_THREADS="${DRAGONFLY_PROACTOR_THREADS:-}"
DRAGONFLY_CONN_IO_THREADS="${DRAGONFLY_CONN_IO_THREADS:-}"
VALKEY_IO_THREADS="${VALKEY_IO_THREADS:-}"
VALKEY_IO_THREADS_DO_READS="${VALKEY_IO_THREADS_DO_READS:-yes}"
THREADS="${THREADS:-8}"
CONNS="${CONNS:-16}"
REQUESTS="${REQUESTS:-50000}"
PRELOAD_REQUESTS="${PRELOAD_REQUESTS:-${REQUESTS}}"
PIPELINE="${PIPELINE:-1}"
SIZE_RANGE="${SIZE_RANGE:-1-1024}"
PERF_FREQ="${PERF_FREQ:-99}"
CAPTURE_PERF="${CAPTURE_PERF:-1}"
PORT_BASE="${PORT_BASE:-16389}"
OUTDIR="${OUTDIR:-/tmp/garnet-linux-perf-diff-$(date +%Y%m%d-%H%M%S)}"

SERVER_PID=""
PERF_PREFIX=()

require_command() {
    local command="$1"
    if ! command -v "${command}" >/dev/null 2>&1; then
        echo "missing required command: ${command}" >&2
        exit 1
    fi
}

cleanup() {
    if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        kill "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

if [[ "$(uname -s)" != "Linux" ]]; then
    echo "linux_perf_diff_profile.sh must run on Linux." >&2
    exit 1
fi

require_command nc
require_command taskset
require_command file
if [[ "${CAPTURE_PERF}" != "0" ]]; then
    require_command perf
fi
if [[ -z "${MEMTIER_BIN}" ]]; then
    echo "missing required command: memtier_benchmark" >&2
    exit 1
fi

needs_garnet_build=0
if [[ ! -x "${GARNET_BIN}" ]]; then
    needs_garnet_build=1
elif ! file "${GARNET_BIN}" | grep -q 'ELF'; then
    # Cross-platform worktrees can contain a non-Linux binary at the same path.
    needs_garnet_build=1
fi

if (( needs_garnet_build )); then
    cargo build -p garnet-server --release --manifest-path "${MANIFEST_PATH}" >/dev/null
fi
if [[ ! -x "${GARNET_BIN}" ]]; then
    echo "garnet-server binary not found: ${GARNET_BIN}" >&2
    exit 1
fi

mkdir -p "${OUTDIR}"

assign_default_cpu_sets() {
    if [[ -n "${SERVER_CPU_SET}" && -n "${CLIENT_CPU_SET}" ]]; then
        return 0
    fi

    local cpu_count split server_default client_default
    cpu_count="$(nproc)"
    if (( cpu_count <= 1 )); then
        server_default="0"
        client_default="0"
    elif (( cpu_count == 2 )); then
        server_default="0"
        client_default="1"
    else
        split=$((cpu_count / 2))
        if (( split < 1 )); then
            split=1
        fi
        server_default="0-$((split - 1))"
        client_default="${split}-$((cpu_count - 1))"
    fi

    if [[ -z "${SERVER_CPU_SET}" ]]; then
        SERVER_CPU_SET="${server_default}"
    fi
    if [[ -z "${CLIENT_CPU_SET}" ]]; then
        CLIENT_CPU_SET="${client_default}"
    fi
}

configure_perf_prefix() {
    if [[ "$(id -u)" -eq 0 ]]; then
        PERF_PREFIX=()
        return 0
    fi
    if command -v sudo >/dev/null 2>&1; then
        PERF_PREFIX=(sudo)
        return 0
    fi
    PERF_PREFIX=()
}

assign_default_cpu_sets
configure_perf_prefix

validate_memtier_log() {
    local mode="$1"
    local expected_requests="$2"
    local log_file="$3"

    if grep -q 'Connection error:' "${log_file}"; then
        echo "connection errors detected in ${log_file}" >&2
        exit 1
    fi
    if grep -q 'handle error response:' "${log_file}"; then
        echo "server error responses detected in ${log_file}" >&2
        exit 1
    fi

    local got_threads got_conns got_requests
    got_threads="$(awk '/Threads$/{print $1; exit}' "${log_file}")"
    got_conns="$(awk '/Connections per thread$/{print $1; exit}' "${log_file}")"
    got_requests="$(awk '/Requests per client$/{print $1; exit}' "${log_file}")"
    if [[ "${got_threads}" != "${THREADS}" ]]; then
        echo "unexpected thread count in ${log_file}: expected ${THREADS}, got ${got_threads}" >&2
        exit 1
    fi
    if [[ "${got_conns}" != "${CONNS}" ]]; then
        echo "unexpected connection count in ${log_file}: expected ${CONNS}, got ${got_conns}" >&2
        exit 1
    fi
    if [[ "${got_requests}" != "${expected_requests}" ]]; then
        echo "unexpected request count in ${log_file}: expected ${expected_requests}, got ${got_requests}" >&2
        exit 1
    fi

    local run_avg_ops
    run_avg_ops="$(perl -ne 'if (/\[RUN #1.*avg:\s*([0-9.]+)\)\s*ops\/sec/) { $ops = $1; } END { print $ops if defined $ops; }' "${log_file}")"
    if ! awk -v avg="${run_avg_ops:-0}" 'BEGIN { exit !(avg > 0) }'; then
        echo "RUN avg Ops/sec was non-positive in ${log_file}" >&2
        exit 1
    fi

    if [[ "${mode}" == "set" ]]; then
        if ! awk '/^Sets[[:space:]]/{ok=($2 > 0)} END{exit !(ok)}' "${log_file}" 2>/dev/null; then
            echo "SET Ops/sec was non-positive in ${log_file}" >&2
            exit 1
        fi
    fi
    if [[ "${mode}" == "get" ]]; then
        if ! awk '/^Gets[[:space:]]/{ok=($2 > 0)} END{exit !(ok)}' "${log_file}" 2>/dev/null; then
            echo "GET Ops/sec was non-positive in ${log_file}" >&2
            exit 1
        fi
    fi
}

run_memtier() {
    local mode="$1"
    local requests="$2"
    local port="$3"
    local out_log="$4"
    local ratio="1:0"
    if [[ "${mode}" == "get" ]]; then
        ratio="0:1"
    fi

    taskset -c "${CLIENT_CPU_SET}" \
        "${MEMTIER_BIN}" \
        -s "${HOST}" \
        -p "${port}" \
        -t "${THREADS}" \
        -c "${CONNS}" \
        -n "${requests}" \
        --ratio "${ratio}" \
        --pipeline "${PIPELINE}" \
        --data-size-range "${SIZE_RANGE}" \
        --distinct-client-seed \
        --hide-histogram \
        --key-pattern=P:P \
        --key-prefix "" \
        --print-percentiles 50,90,99,99.9,99.99 \
        >"${out_log}" 2>&1

    validate_memtier_log "${mode}" "${requests}" "${out_log}"
}

stop_server() {
    if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        kill "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
    SERVER_PID=""
}

wait_for_ping_ready() {
    local port="$1"
    for _ in $(seq 1 200); do
        if printf '*1\r\n$4\r\nPING\r\n' | nc -w 1 "${HOST}" "${port}" 2>/dev/null | grep -q '^+PONG'; then
            return 0
        fi
        sleep 0.05
    done
    echo "server failed PING readiness check on ${HOST}:${port}" >&2
    exit 1
}

start_target_server() {
    local target="$1"
    local port="$2"
    local server_log="$3"
    local -a server_cmd=()

    stop_server

    if nc -z "${HOST}" "${port}" 2>/dev/null; then
        echo "port ${port} already in use; adjust PORT_BASE or stop the existing process" >&2
        exit 1
    fi

    if [[ "${target}" == "garnet" ]]; then
        server_cmd=(taskset -c "${SERVER_CPU_SET}" env)
        if [[ -n "${TOKIO_WORKER_THREADS}" ]]; then
            server_cmd+=("TOKIO_WORKER_THREADS=${TOKIO_WORKER_THREADS}")
        fi
        server_cmd+=(
            "GARNET_BIND_ADDR=${HOST}:${port}"
            "GARNET_TSAVORITE_STRING_STORE_SHARDS=${GARNET_TSAVORITE_STRING_STORE_SHARDS:-2}"
            "GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES=${GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES:-262144}"
            "${GARNET_BIN}"
        )
        "${server_cmd[@]}" >"${server_log}" 2>&1 &
        SERVER_PID=$!
    elif [[ "${target}" == "dragonfly" ]]; then
        if [[ -z "${DRAGONFLY_BIN}" ]]; then
            echo "dragonfly binary not found; set DRAGONFLY_BIN=/path/to/dragonfly" >&2
            exit 1
        fi
        server_cmd=(
            taskset -c "${SERVER_CPU_SET}"
            "${DRAGONFLY_BIN}"
            --bind "${HOST}"
            --port "${port}"
            --dbfilename ""
        )
        if [[ -n "${DRAGONFLY_PROACTOR_THREADS}" ]]; then
            server_cmd+=(--proactor_threads "${DRAGONFLY_PROACTOR_THREADS}")
        fi
        if [[ -n "${DRAGONFLY_CONN_IO_THREADS}" ]]; then
            server_cmd+=(--conn_io_threads "${DRAGONFLY_CONN_IO_THREADS}")
        fi
        "${server_cmd[@]}" >"${server_log}" 2>&1 &
        SERVER_PID=$!
    elif [[ "${target}" == "valkey" ]]; then
        if [[ -z "${VALKEY_BIN}" ]]; then
            echo "valkey-server binary not found; set VALKEY_BIN=/path/to/valkey-server" >&2
            exit 1
        fi
        server_cmd=(
            taskset -c "${SERVER_CPU_SET}"
            "${VALKEY_BIN}"
            --bind "${HOST}"
            --port "${port}"
            --save ""
            --appendonly no
            --protected-mode no
        )
        if [[ -n "${VALKEY_IO_THREADS}" ]]; then
            server_cmd+=(--io-threads "${VALKEY_IO_THREADS}")
            server_cmd+=(--io-threads-do-reads "${VALKEY_IO_THREADS_DO_READS}")
        fi
        "${server_cmd[@]}" >"${server_log}" 2>&1 &
        SERVER_PID=$!
    else
        echo "unknown target: ${target}" >&2
        exit 1
    fi

    for _ in $(seq 1 200); do
        if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
            echo "${target} server exited before ready (see ${server_log})" >&2
            exit 1
        fi
        if nc -z "${HOST}" "${port}" 2>/dev/null; then
            return 0
        fi
        sleep 0.05
    done

    echo "${target} server did not become ready on ${HOST}:${port}" >&2
    exit 1
}

capture_perf_profile() {
    local target="$1"
    local mode="$2"
    local run_dir="$3"
    local port="$4"
    local perf_data="${run_dir}/perf.data"
    local bench_log="${run_dir}/memtier-${mode}.log"
    local perf_report="${run_dir}/perf-report-${mode}.txt"
    local perf_script="${run_dir}/perf-script-${mode}.txt"
    local perf_report_stderr="${run_dir}/perf-report-${mode}.stderr.log"
    local perf_script_stderr="${run_dir}/perf-script-${mode}.stderr.log"

    run_memtier "set" "${PRELOAD_REQUESTS}" "${port}" "${run_dir}/preload.log"

    if [[ "${CAPTURE_PERF}" == "0" ]]; then
        run_memtier "${mode}" "${REQUESTS}" "${port}" "${bench_log}"
        return 0
    fi

    taskset -c "${CLIENT_CPU_SET}" \
        "${MEMTIER_BIN}" \
        -s "${HOST}" \
        -p "${port}" \
        -t "${THREADS}" \
        -c "${CONNS}" \
        -n "${REQUESTS}" \
        --ratio "$([[ "${mode}" == "set" ]] && echo "1:0" || echo "0:1")" \
        --pipeline "${PIPELINE}" \
        --data-size-range "${SIZE_RANGE}" \
        --distinct-client-seed \
        --hide-histogram \
        --key-pattern=P:P \
        --key-prefix "" \
        --print-percentiles 50,90,99,99.9,99.99 \
        >"${bench_log}" 2>&1 &
    local bench_pid=$!

    "${PERF_PREFIX[@]}" perf record -F "${PERF_FREQ}" -g -p "${SERVER_PID}" -o "${perf_data}" >/dev/null 2>&1 &
    local perf_pid=$!

    wait "${bench_pid}"
    validate_memtier_log "${mode}" "${REQUESTS}" "${bench_log}"

    if kill -0 "${perf_pid}" 2>/dev/null; then
        kill -INT "${perf_pid}" 2>/dev/null || true
    fi
    wait "${perf_pid}" 2>/dev/null || true

    perf report --stdio --no-children -i "${perf_data}" >"${perf_report}" 2>"${perf_report_stderr}" || true
    perf script -i "${perf_data}" >"${perf_script}" 2>"${perf_script_stderr}" || true

    if [[ -n "${FLAMEGRAPH_DIR}" ]] && [[ -f "${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" ]] && [[ -f "${FLAMEGRAPH_DIR}/flamegraph.pl" ]]; then
        "${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" "${perf_script}" | \
            "${FLAMEGRAPH_DIR}/flamegraph.pl" >"${run_dir}/flame-${target}-${mode}.svg"
    fi
}

target_index=0
for target in ${TARGETS}; do
    port=$((PORT_BASE + target_index))
    target_dir="${OUTDIR}/${target}"
    mkdir -p "${target_dir}"
    start_target_server "${target}" "${port}" "${target_dir}/server.log"
    wait_for_ping_ready "${port}"
    for workload in ${WORKLOADS}; do
        run_dir="${target_dir}/${workload}"
        mkdir -p "${run_dir}"
        capture_perf_profile "${target}" "${workload}" "${run_dir}" "${port}"
    done
    stop_server
    target_index=$((target_index + 1))
done

echo "linux perf differential profiling completed"
echo "outdir=${OUTDIR}"
