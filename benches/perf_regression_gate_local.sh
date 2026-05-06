#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVER_BIN="${SERVER_BIN:-${REPO_ROOT}/target/release/garnet-server}"
MANIFEST_PATH="${REPO_ROOT}/Cargo.toml"
MEMTIER_BIN="${MEMTIER_BIN:-$(command -v memtier_benchmark || true)}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-16389}"
THREADS="${THREADS:-4}"
CONNS="${CONNS:-8}"
REQUESTS="${REQUESTS:-10000}"
PRELOAD_REQUESTS="${PRELOAD_REQUESTS:-${REQUESTS}}"
PIPELINE="${PIPELINE:-1}"
SIZE_RANGE="${SIZE_RANGE:-1-1024}"
HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS:-25}"
MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES:-1048576}"
STRING_STORE_SHARDS="${STRING_STORE_SHARDS:-}"
OWNER_THREADS="${OWNER_THREADS:-${GARNET_STRING_OWNER_THREADS:-}}"
RUNS="${RUNS:-5}"
OUTDIR="${OUTDIR:-/tmp/garnet-perf-gate-$(date +%Y%m%d-%H%M%S)}"

MIN_MEDIAN_SET_OPS="${MIN_MEDIAN_SET_OPS:-0}"
MIN_MEDIAN_GET_OPS="${MIN_MEDIAN_GET_OPS:-0}"
MAX_MEDIAN_SET_P99_MS="${MAX_MEDIAN_SET_P99_MS:-0}"
MAX_MEDIAN_GET_P99_MS="${MAX_MEDIAN_GET_P99_MS:-0}"

SERVER_PID=""

require_command() {
    local command="$1"
    if ! command -v "${command}" >/dev/null 2>&1; then
        echo "missing required command: ${command}" >&2
        exit 1
    fi
}

ensure_server_binary() {
    if [[ ! -x "${SERVER_BIN}" ]]; then
        cargo build -p garnet-server --release --manifest-path "${MANIFEST_PATH}" >/dev/null
    fi
}

stop_server() {
    if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        kill "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
    SERVER_PID=""
    sleep 0.2
}

cleanup() {
    stop_server
}
trap cleanup EXIT

start_server() {
    local server_log="$1"
    stop_server
    if nc -z "${HOST}" "${PORT}" 2>/dev/null; then
        echo "port ${PORT} is already in use; set PORT=<free-port> and retry" >&2
        exit 1
    fi

    local -a env_args
    env_args=(
        "GARNET_BIND_ADDR=${HOST}:${PORT}"
        "GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS=${HASH_INDEX_SIZE_BITS}"
        "GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES=${MAX_IN_MEMORY_PAGES}"
    )
    if [[ -n "${STRING_STORE_SHARDS}" ]]; then
        env_args+=("GARNET_TSAVORITE_STRING_STORE_SHARDS=${STRING_STORE_SHARDS}")
    fi
    if [[ -n "${OWNER_THREADS}" ]]; then
        env_args+=("GARNET_STRING_OWNER_THREADS=${OWNER_THREADS}")
    fi

    env "${env_args[@]}" "${SERVER_BIN}" >"${server_log}" 2>&1 &
    SERVER_PID=$!

    for _ in $(seq 1 200); do
        if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
            echo "server exited before becoming ready" >&2
            exit 1
        fi
        if nc -z "${HOST}" "${PORT}" 2>/dev/null; then
            return 0
        fi
        sleep 0.05
    done

    echo "server failed to become ready on ${HOST}:${PORT}" >&2
    exit 1
}

wait_for_ping_ready() {
    for _ in $(seq 1 200); do
        if printf '*1\r\n$4\r\nPING\r\n' | nc -w 1 "${HOST}" "${PORT}" 2>/dev/null | grep -q '^+PONG'; then
            return 0
        fi
        sleep 0.05
    done
    echo "server failed PING readiness check on ${HOST}:${PORT}" >&2
    exit 1
}

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

    local totals_ops run_avg_ops sets_ops gets_ops
    totals_ops="$(awk '/^Totals[[:space:]]/{ops=$2} END{print ops+0}' "${log_file}")"
    run_avg_ops="$(perl -ne 'if (/\[RUN #1.*avg:\s*([0-9.]+)\)\s*ops\/sec/) { $ops = $1; } END { print $ops if defined $ops; }' "${log_file}")"
    sets_ops="$(awk '/^Sets[[:space:]]/{print $2+0; exit}' "${log_file}")"
    gets_ops="$(awk '/^Gets[[:space:]]/{print $2+0; exit}' "${log_file}")"

    if ! awk -v total="${totals_ops}" -v avg="${run_avg_ops:-0}" 'BEGIN { exit !((total > 0) || (avg > 0)) }'; then
        echo "Neither Totals nor RUN avg Ops/sec was positive in ${log_file}" >&2
        exit 1
    fi

    if [[ "${mode}" == "set" ]] && ! awk -v value="${sets_ops}" -v avg="${run_avg_ops:-0}" 'BEGIN { exit !((value > 0) || (avg > 0)) }'; then
        echo "SET Ops/sec and RUN avg were non-positive in ${log_file}" >&2
        exit 1
    fi
    if [[ "${mode}" == "get" ]] && ! awk -v value="${gets_ops}" -v avg="${run_avg_ops:-0}" 'BEGIN { exit !((value > 0) || (avg > 0)) }'; then
        echo "GET Ops/sec and RUN avg were non-positive in ${log_file}" >&2
        exit 1
    fi
}

run_memtier() {
    local ratio="$1"
    local requests="$2"
    local output="$3"
    "${MEMTIER_BIN}" \
        -s "${HOST}" \
        -p "${PORT}" \
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
        >"${output}" 2>&1
}

extract_ops_and_p99() {
    local file="$1"
    local row="$2"
    local ops p99
    ops="$(perl -ne 'if (/\[RUN #1.*avg:\s*([0-9.]+)\)\s*ops\/sec/) { $ops=$1; } END { print $ops if defined $ops; }' "${file}")"
    # memtier table columns: ... p50 (6), p90 (7), p99 (8)
    p99="$(awk -v row="${row}" '$1==row{print $8; exit}' "${file}")"
    if [[ -z "${ops}" ]]; then
        ops="0.00"
    fi
    if [[ -z "${p99}" ]]; then
        p99="0.00"
    fi
    echo "${ops},${p99}"
}

require_command nc
require_command awk
if [[ -z "${MEMTIER_BIN}" ]]; then
    echo "missing required command: memtier_benchmark" >&2
    exit 1
fi
if [[ ! "${RUNS}" =~ ^[0-9]+$ ]] || (( RUNS < 1 )); then
    echo "RUNS must be a positive integer; got ${RUNS}" >&2
    exit 1
fi

ensure_server_binary
mkdir -p "${OUTDIR}"
RESULTS_CSV="${OUTDIR}/runs.csv"
SUMMARY_TXT="${OUTDIR}/summary.txt"
echo "run,set_ops,set_p99_ms,get_ops,get_p99_ms" >"${RESULTS_CSV}"

for run_idx in $(seq 1 "${RUNS}"); do
    run_dir="${OUTDIR}/run-${run_idx}"
    mkdir -p "${run_dir}"
    start_server "${run_dir}/server.log"
    wait_for_ping_ready

    set_log="${run_dir}/set.log"
    preload_log="${run_dir}/preload.log"
    get_log="${run_dir}/get.log"

    run_memtier "1:0" "${REQUESTS}" "${set_log}"
    validate_memtier_log "set" "${REQUESTS}" "${set_log}"

    run_memtier "1:0" "${PRELOAD_REQUESTS}" "${preload_log}"
    validate_memtier_log "set" "${PRELOAD_REQUESTS}" "${preload_log}"

    run_memtier "0:1" "${REQUESTS}" "${get_log}"
    validate_memtier_log "get" "${REQUESTS}" "${get_log}"

    set_metrics="$(extract_ops_and_p99 "${set_log}" "Sets")"
    get_metrics="$(extract_ops_and_p99 "${get_log}" "Gets")"
    echo "${run_idx},${set_metrics},${get_metrics}" >>"${RESULTS_CSV}"

    stop_server
done

python3 - "${RESULTS_CSV}" "${SUMMARY_TXT}" \
    "${MIN_MEDIAN_SET_OPS}" "${MIN_MEDIAN_GET_OPS}" \
    "${MAX_MEDIAN_SET_P99_MS}" "${MAX_MEDIAN_GET_P99_MS}" <<'PY'
import csv
import statistics
import sys

results_csv, summary_txt, min_set_ops, min_get_ops, max_set_p99, max_get_p99 = sys.argv[1:]

rows = []
with open(results_csv, newline="") as f:
    for row in csv.DictReader(f):
        rows.append(
            {
                "set_ops": float(row["set_ops"]),
                "set_p99_ms": float(row["set_p99_ms"]),
                "get_ops": float(row["get_ops"]),
                "get_p99_ms": float(row["get_p99_ms"]),
            }
        )

if not rows:
    print("no benchmark rows found", file=sys.stderr)
    sys.exit(1)

median_set_ops = statistics.median(r["set_ops"] for r in rows)
median_get_ops = statistics.median(r["get_ops"] for r in rows)
median_set_p99 = statistics.median(r["set_p99_ms"] for r in rows)
median_get_p99 = statistics.median(r["get_p99_ms"] for r in rows)

with open(summary_txt, "w") as f:
    f.write(f"runs={len(rows)}\n")
    f.write(f"median_set_ops={median_set_ops:.3f}\n")
    f.write(f"median_get_ops={median_get_ops:.3f}\n")
    f.write(f"median_set_p99_ms={median_set_p99:.5f}\n")
    f.write(f"median_get_p99_ms={median_get_p99:.5f}\n")

def parse_threshold(raw: str) -> float:
    try:
        return float(raw)
    except ValueError:
        print(f"invalid threshold value: {raw}", file=sys.stderr)
        sys.exit(2)

min_set = parse_threshold(min_set_ops)
min_get = parse_threshold(min_get_ops)
max_set = parse_threshold(max_set_p99)
max_get = parse_threshold(max_get_p99)

errors = []
if min_set > 0 and median_set_ops < min_set:
    errors.append(f"median_set_ops {median_set_ops:.3f} < required {min_set:.3f}")
if min_get > 0 and median_get_ops < min_get:
    errors.append(f"median_get_ops {median_get_ops:.3f} < required {min_get:.3f}")
if max_set > 0 and median_set_p99 > max_set:
    errors.append(f"median_set_p99_ms {median_set_p99:.5f} > allowed {max_set:.5f}")
if max_get > 0 and median_get_p99 > max_get:
    errors.append(f"median_get_p99_ms {median_get_p99:.5f} > allowed {max_get:.5f}")

if errors:
    print("performance regression gate failed:", file=sys.stderr)
    for line in errors:
        print(f"- {line}", file=sys.stderr)
    sys.exit(1)
PY

cat "${SUMMARY_TXT}"
echo "runs_csv=${RESULTS_CSV}"
echo "summary=${SUMMARY_TXT}"
