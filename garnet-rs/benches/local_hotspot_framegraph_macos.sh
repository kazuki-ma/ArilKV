#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GARNET_RS_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVER_BIN_DEFAULT="${GARNET_RS_ROOT}/target/release/garnet-server"
MANIFEST_PATH="${GARNET_RS_ROOT}/Cargo.toml"

PORT="${PORT:-6389}"
THREADS="${THREADS:-8}"
CONNS="${CONNS:-16}"
REQ_PER_CLIENT="${REQ_PER_CLIENT:-30000}"
PIPELINE="${PIPELINE:-1}"
SIZE_RANGE="${SIZE_RANGE:-1-1024}"
SAMPLE_SECONDS="${SAMPLE_SECONDS:-15}"
FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-/tmp/FlameGraph}"
OUTDIR="${OUTDIR:-/tmp/garnet-hotspots-$(date +%Y%m%d-%H%M%S)}"
SERVER_BIN="${SERVER_BIN:-${SERVER_BIN_DEFAULT}}"
MEMTIER_BIN="${MEMTIER_BIN:-$(command -v memtier_benchmark || true)}"
HOST="${HOST:-127.0.0.1}"
HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS:-25}"
MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES:-1048576}"
STRING_STORE_SHARDS="${STRING_STORE_SHARDS:-2}"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

require_command() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "missing required command: ${cmd}" >&2
    exit 1
  fi
}

ensure_flamegraph_tools() {
  if [[ ! -d "${FLAMEGRAPH_DIR}" ]]; then
    git clone --depth 1 https://github.com/brendangregg/FlameGraph "${FLAMEGRAPH_DIR}"
  fi
  if [[ ! -f "${FLAMEGRAPH_DIR}/stackcollapse-sample.awk" ]] || [[ ! -f "${FLAMEGRAPH_DIR}/flamegraph.pl" ]]; then
    echo "FlameGraph tools not found in ${FLAMEGRAPH_DIR}" >&2
    exit 1
  fi
}

ensure_port_available() {
  if nc -z "${HOST}" "${PORT}" 2>/dev/null; then
    echo "port ${PORT} is already in use; stop the existing server or set PORT=<free-port>" >&2
    exit 1
  fi
}

ensure_server_binary() {
  if [[ ! -x "${SERVER_BIN}" ]]; then
    cargo build -p garnet-server --release --manifest-path "${MANIFEST_PATH}"
  fi
}

start_server() {
  local tag="$1"
  GARNET_BIND_ADDR="${HOST}:${PORT}" \
    GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS}" \
    GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES}" \
    GARNET_TSAVORITE_STRING_STORE_SHARDS="${STRING_STORE_SHARDS}" \
    "${SERVER_BIN}" >"${OUTDIR}/server-${tag}.log" 2>&1 &
  SERVER_PID=$!

  for _ in $(seq 1 200); do
    if nc -z "${HOST}" "${PORT}" 2>/dev/null; then
      return 0
    fi
    sleep 0.05
  done
  echo "server did not become ready on ${HOST}:${PORT}" >&2
  return 1
}

wait_for_ping_ready() {
  for _ in $(seq 1 200); do
    if printf '*1\r\n$4\r\nPING\r\n' | nc -w 1 "${HOST}" "${PORT}" 2>/dev/null | grep -q '^+PONG'; then
      return 0
    fi
    sleep 0.05
  done
  echo "server failed PING readiness check on ${HOST}:${PORT}" >&2
  return 1
}

stop_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  SERVER_PID=""
  sleep 0.3
}

validate_memtier_log() {
  local mode="$1"
  local log_file="$2"

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
  if [[ "${got_requests}" != "${REQ_PER_CLIENT}" ]]; then
    echo "unexpected request count in ${log_file}: expected ${REQ_PER_CLIENT}, got ${got_requests}" >&2
    exit 1
  fi

  local totals_ops sets_ops gets_ops run_avg_ops
  totals_ops="$(awk '/^Totals[[:space:]]/{ops=$2} END{print ops+0}' "${log_file}")"
  sets_ops="$(awk '/^Sets[[:space:]]/{print $2+0; exit}' "${log_file}")"
  gets_ops="$(awk '/^Gets[[:space:]]/{print $2+0; exit}' "${log_file}")"
  run_avg_ops="$(perl -ne 'if (/\[RUN #1.*avg:\s*([0-9.]+)\)\s*ops\/sec/) { $ops = $1; } END { print $ops if defined $ops; }' "${log_file}")"
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
  local mode="$1"
  local json_out="$2"
  local log_out="$3"
  local ratio="1:0"
  if [[ "${mode}" == "get" ]]; then
    ratio="0:1"
  fi

  "${MEMTIER_BIN}" \
    -s "${HOST}" \
    -p "${PORT}" \
    -c "${CONNS}" \
    -t "${THREADS}" \
    -n "${REQ_PER_CLIENT}" \
    --distinct-client-seed \
    --hide-histogram \
    --key-prefix "" \
    --ratio "${ratio}" \
    --data-size-range "${SIZE_RANGE}" \
    --pipeline "${PIPELINE}" \
    --json-out-file "${json_out}" \
    --print-percentiles 50,90,99,99.9,99.99 \
    --key-pattern=P:P \
    >"${log_out}" 2>&1

  validate_memtier_log "${mode}" "${log_out}"
}

run_sample_while_benchmark() {
  local server_pid="$1"
  local bench_pid="$2"
  local sample_out="$3"

  sample "${server_pid}" "${SAMPLE_SECONDS}" -mayDie -file "${sample_out}" >/dev/null 2>&1 &
  local sample_pid=$!

  local bench_status=0
  wait "${bench_pid}" || bench_status=$?

  if kill -0 "${sample_pid}" 2>/dev/null; then
    kill -INT "${sample_pid}" 2>/dev/null || true
  fi
  wait "${sample_pid}" 2>/dev/null || true

  if [[ "${bench_status}" -ne 0 ]]; then
    return "${bench_status}"
  fi
}

make_flamegraph() {
  local sample_txt="$1"
  local folded="$2"
  local svg="$3"
  local title="$4"

  awk -f "${FLAMEGRAPH_DIR}/stackcollapse-sample.awk" "${sample_txt}" >"${folded}"
  perl "${FLAMEGRAPH_DIR}/flamegraph.pl" --title "${title}" "${folded}" >"${svg}"
}

write_hotspots() {
  local folded="$1"
  local out_prefix="$2"

  awk '{
    count=$NF;
    line=$0;
    sub(/ [0-9]+$/, "", line);
    n=split(line, stack, ";");
    leaf=stack[n];
    leaf_sum[leaf]+=count;
    for (i=1; i<=n; i++) {
      incl_sum[stack[i]]+=count;
    }
  }
  END {
    for (name in leaf_sum) {
      print leaf_sum[name], name > "'"${out_prefix}"'.leaf.tmp";
    }
    for (name in incl_sum) {
      print incl_sum[name], name > "'"${out_prefix}"'.incl.tmp";
    }
  }' "${folded}"

  sort -nr "${out_prefix}.leaf.tmp" | head -n 20 >"${out_prefix}.leaf.top20.txt"
  sort -nr "${out_prefix}.incl.tmp" | head -n 20 >"${out_prefix}.incl.top20.txt"
  rm -f "${out_prefix}.leaf.tmp" "${out_prefix}.incl.tmp"
}

require_command sample
require_command nc
require_command git
require_command perl
if [[ -z "${MEMTIER_BIN}" ]]; then
  echo "missing required command: memtier_benchmark" >&2
  exit 1
fi

mkdir -p "${OUTDIR}"
ensure_port_available
ensure_flamegraph_tools
ensure_server_binary

# GET-only profiling: preload keys with SET first.
start_server "get"
wait_for_ping_ready
run_memtier "set" "${OUTDIR}/get-preload-set.json" "${OUTDIR}/get-preload-set.log"
run_memtier "get" "${OUTDIR}/get-run.json" "${OUTDIR}/get-run.log" &
GET_BENCH_PID=$!
sleep 1
run_sample_while_benchmark "${SERVER_PID}" "${GET_BENCH_PID}" "${OUTDIR}/get.sample.txt"
make_flamegraph "${OUTDIR}/get.sample.txt" "${OUTDIR}/get.folded" "${OUTDIR}/garnet-get.flame.svg" \
  "garnet-rs GET-only local"
write_hotspots "${OUTDIR}/get.folded" "${OUTDIR}/get"
stop_server

# SET-only profiling.
start_server "set"
wait_for_ping_ready
run_memtier "set" "${OUTDIR}/set-run.json" "${OUTDIR}/set-run.log" &
SET_BENCH_PID=$!
sleep 1
run_sample_while_benchmark "${SERVER_PID}" "${SET_BENCH_PID}" "${OUTDIR}/set.sample.txt"
make_flamegraph "${OUTDIR}/set.sample.txt" "${OUTDIR}/set.folded" "${OUTDIR}/garnet-set.flame.svg" \
  "garnet-rs SET-only local"
write_hotspots "${OUTDIR}/set.folded" "${OUTDIR}/set"
stop_server

{
  echo "outdir=${OUTDIR}"
  echo "string_store_shards=${STRING_STORE_SHARDS}"
  echo "get_svg=${OUTDIR}/garnet-get.flame.svg"
  echo "set_svg=${OUTDIR}/garnet-set.flame.svg"
  echo "get_leaf_top20=${OUTDIR}/get.leaf.top20.txt"
  echo "get_incl_top20=${OUTDIR}/get.incl.top20.txt"
  echo "set_leaf_top20=${OUTDIR}/set.leaf.top20.txt"
  echo "set_incl_top20=${OUTDIR}/set.incl.top20.txt"
} | tee "${OUTDIR}/SUMMARY.txt"
