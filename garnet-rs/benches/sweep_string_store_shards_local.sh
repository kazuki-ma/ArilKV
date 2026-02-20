#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVER_BIN="${SERVER_BIN:-${REPO_ROOT}/target/release/garnet-server}"
MANIFEST_PATH="${REPO_ROOT}/Cargo.toml"
MEMTIER_BIN="${MEMTIER_BIN:-$(command -v memtier_benchmark || true)}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-16389}"
THREADS="${THREADS:-8}"
CONNS="${CONNS:-16}"
REQUESTS="${REQUESTS:-20000}"
PIPELINE="${PIPELINE:-1}"
SIZE_RANGE="${SIZE_RANGE:-1-1024}"
SHARD_COUNTS="${SHARD_COUNTS:-1 2 4 8 16}"
HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS:-25}"
MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES:-1048576}"
OUTDIR="${OUTDIR:-/tmp/garnet-shard-sweep-$(date +%Y%m%d-%H%M%S)}"

cleanup() {
  pkill -f "garnet-rs/target/release/garnet-server" 2>/dev/null || true
  for _ in $(seq 1 100); do
    if ! nc -z "${HOST}" "${PORT}" 2>/dev/null; then
      return 0
    fi
    sleep 0.05
  done
}
trap cleanup EXIT

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

start_server() {
  local shards="$1"
  local log_file="$2"
  GARNET_BIND_ADDR="127.0.0.1:${PORT}" \
    GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS}" \
    GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES}" \
    GARNET_TSAVORITE_STRING_STORE_SHARDS="${shards}" \
    "${SERVER_BIN}" >"${log_file}" 2>&1 &
  local pid=$!

  for _ in $(seq 1 200); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      echo "server exited before becoming ready for shard count ${shards}" >&2
      return 1
    fi
    if nc -z 127.0.0.1 "${PORT}" 2>/dev/null; then
      echo "${pid}"
      return 0
    fi
    sleep 0.05
  done

  echo "server failed to start for shard count ${shards}" >&2
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

run_memtier() {
  local ratio="$1"
  local output="$2"
    "${MEMTIER_BIN}" \
    -s "${HOST}" \
    -p "${PORT}" \
    -t "${THREADS}" \
    -c "${CONNS}" \
    -n "${REQUESTS}" \
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

validate_run() {
  local file="$1"
  if grep -q 'Connection error:' "${file}"; then
    echo "connection errors detected in ${file}" >&2
    return 1
  fi
  local expected_threads expected_conns expected_requests
  expected_threads="$(awk '/Threads$/{print $1; exit}' "${file}")"
  expected_conns="$(awk '/Connections per thread$/{print $1; exit}' "${file}")"
  expected_requests="$(awk '/Requests per client$/{print $1; exit}' "${file}")"
  if [[ "${expected_threads}" != "${THREADS}" ]]; then
    echo "invalid thread count in ${file}: expected ${THREADS}, got ${expected_threads}" >&2
    return 1
  fi
  if [[ "${expected_conns}" != "${CONNS}" ]]; then
    echo "invalid connection count in ${file}: expected ${CONNS}, got ${expected_conns}" >&2
    return 1
  fi
  if [[ "${expected_requests}" != "${REQUESTS}" ]]; then
    echo "invalid request count in ${file}: expected ${REQUESTS}, got ${expected_requests}" >&2
    return 1
  fi
  if ! perl -ne '
    if (/\[RUN #1.*avg:\s*([0-9.]+)\)\s*ops\/sec/) { $ops = $1; }
    END { exit(!defined($ops) || $ops <= 0); }
  ' "${file}"; then
    echo "RUN avg ops/sec was non-positive in ${file}" >&2
    return 1
  fi
}

extract_ops_and_p99() {
  local file="$1"
  local row="$2"
  local ops
  local p99
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

mkdir -p "${OUTDIR}"
ensure_server_binary

RESULTS_CSV="${OUTDIR}/results.csv"
echo "shards,set_ops,set_p99_ms,get_ops,get_p99_ms" >"${RESULTS_CSV}"

for shards in ${SHARD_COUNTS}; do
  if [[ ! "${shards}" =~ ^[0-9]+$ ]] || (( shards < 1 )); then
    echo "invalid shard count: ${shards}" >&2
    exit 1
  fi

  cleanup
  if nc -z "${HOST}" "${PORT}" 2>/dev/null; then
    echo "port ${PORT} is already in use; set PORT=<free-port> and retry" >&2
    exit 1
  fi
  server_log="${OUTDIR}/server-shards-${shards}.log"
  server_pid="$(start_server "${shards}" "${server_log}")"
  wait_for_ping_ready
  sleep 1
  if ! kill -0 "${server_pid}" 2>/dev/null; then
    echo "server process ${server_pid} exited unexpectedly for shard ${shards}" >&2
    exit 1
  fi

  set_log="${OUTDIR}/set-shards-${shards}.log"
  preload_log="${OUTDIR}/preload-shards-${shards}.log"
  get_log="${OUTDIR}/get-shards-${shards}.log"

  run_memtier "1:0" "${set_log}"
  validate_run "${set_log}"
  if ! kill -0 "${server_pid}" 2>/dev/null; then
    echo "server process ${server_pid} exited during set run for shard ${shards}" >&2
    exit 1
  fi
  run_memtier "1:0" "${preload_log}"
  sleep 0.2
  run_memtier "0:1" "${get_log}"
  validate_run "${get_log}"

  set_metrics="$(extract_ops_and_p99 "${set_log}" "Sets")"
  get_metrics="$(extract_ops_and_p99 "${get_log}" "Gets")"
  echo "${shards},${set_metrics},${get_metrics}" >>"${RESULTS_CSV}"

  kill "${server_pid}" 2>/dev/null || true
  wait "${server_pid}" 2>/dev/null || true
done

cat "${RESULTS_CSV}"
