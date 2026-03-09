#!/usr/bin/env bash
set -euo pipefail

BASE_BIN="${BASE_BIN:?BASE_BIN is required}"
NEW_BIN="${NEW_BIN:?NEW_BIN is required}"
BASE_LABEL="${BASE_LABEL:-before}"
NEW_LABEL="${NEW_LABEL:-after}"
RUNS="${RUNS:-3}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-16520}"
THREADS="${THREADS:-1}"
CONNS="${CONNS:-2}"
REQUESTS="${REQUESTS:-8000}"
PRELOAD_REQUESTS="${PRELOAD_REQUESTS:-4096}"
PIPELINE="${PIPELINE:-1}"
SIZE_RANGE="${SIZE_RANGE:-16-128}"
KEY_MIN="${KEY_MIN:-1}"
KEY_MAX="${KEY_MAX:-4096}"
HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS:-25}"
MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES:-262144}"
OUTDIR="${OUTDIR:-/tmp/garnet-object-mutation-ab-$(date +%Y%m%d-%H%M%S)}"
MEMTIER_BIN="${MEMTIER_BIN:-$(command -v memtier_benchmark || true)}"
REDIS_CLI="${REDIS_CLI:-$(command -v redis-cli || true)}"

if [[ -z "${MEMTIER_BIN}" || -z "${REDIS_CLI}" ]]; then
  echo "memtier_benchmark and redis-cli are required" >&2
  exit 1
fi

if [[ ! -x "${BASE_BIN}" ]]; then
  echo "BASE_BIN not executable: ${BASE_BIN}" >&2
  exit 1
fi
if [[ ! -x "${NEW_BIN}" ]]; then
  echo "NEW_BIN not executable: ${NEW_BIN}" >&2
  exit 1
fi
if [[ "${BASE_LABEL}" == "${NEW_LABEL}" ]]; then
  echo "BASE_LABEL and NEW_LABEL must be different" >&2
  exit 1
fi

SERVER_PID=""

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

wait_port() {
  for _ in $(seq 1 200); do
    if nc -z "${HOST}" "${PORT}" 2>/dev/null; then
      return 0
    fi
    sleep 0.05
  done
  return 1
}

wait_ping() {
  for _ in $(seq 1 200); do
    if "${REDIS_CLI}" -h "${HOST}" -p "${PORT}" PING 2>/dev/null | grep -q '^PONG$'; then
      return 0
    fi
    sleep 0.05
  done
  return 1
}

validate_memtier_log() {
  local log_file="$1"
  if grep -q 'Connection error:' "${log_file}"; then
    echo "connection errors detected in ${log_file}" >&2
    exit 1
  fi
  if grep -q 'handle error response:' "${log_file}"; then
    echo "server error responses detected in ${log_file}" >&2
    exit 1
  fi
}

extract_row_ops_and_p99() {
  local file="$1"
  local row="$2"
  awk -v row="${row}" '
    tolower($1)==tolower(row) { print ($2+0)","($8+0); found=1; exit }
    END { if (!found) exit 1 }
  ' "${file}"
}

run_memtier_preload() {
  local output="$1"
  shift
  "${MEMTIER_BIN}" \
    -s "${HOST}" \
    -p "${PORT}" \
    -t 1 \
    -c 1 \
    -n "${PRELOAD_REQUESTS}" \
    --pipeline 1 \
    --data-size-range "${SIZE_RANGE}" \
    --hide-histogram \
    --key-prefix "" \
    --key-minimum "${KEY_MIN}" \
    --key-maximum "${KEY_MAX}" \
    --distinct-client-seed \
    "$@" \
    >"${output}" 2>&1
}

run_memtier_workload() {
  local output="$1"
  shift
  "${MEMTIER_BIN}" \
    -s "${HOST}" \
    -p "${PORT}" \
    -t "${THREADS}" \
    -c "${CONNS}" \
    -n "${REQUESTS}" \
    --pipeline "${PIPELINE}" \
    --data-size-range "${SIZE_RANGE}" \
    --hide-histogram \
    --key-prefix "" \
    --key-minimum "${KEY_MIN}" \
    --key-maximum "${KEY_MAX}" \
    --distinct-client-seed \
    --print-percentiles 50,90,99,99.9,99.99 \
    "$@" \
    >"${output}" 2>&1
}

start_server() {
  local bin="$1"
  local run_dir="$2"

  stop_server

  if nc -z "${HOST}" "${PORT}" 2>/dev/null; then
    echo "port ${PORT} already in use" >&2
    exit 1
  fi

  env \
    GARNET_BIND_ADDR="${HOST}:${PORT}" \
    GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS}" \
    GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES}" \
    "${bin}" >"${run_dir}/server.log" 2>&1 &
  SERVER_PID=$!

  wait_port || { echo "server failed to listen" >&2; exit 1; }
  wait_ping || { echo "server failed ping" >&2; exit 1; }
}

prep_hash() {
  local run_dir="$1"
  "${REDIS_CLI}" -h "${HOST}" -p "${PORT}" DEL objhash >/dev/null
  run_memtier_preload "${run_dir}/preload_hash.log" \
    --command "HSET objhash __key__ __data__" \
    --command-ratio 1 \
    --command-key-pattern P
  validate_memtier_log "${run_dir}/preload_hash.log"
}

prep_list() {
  local run_dir="$1"
  "${REDIS_CLI}" -h "${HOST}" -p "${PORT}" DEL objlist >/dev/null
  run_memtier_preload "${run_dir}/preload_list.log" \
    --command "LPUSH objlist __data__" \
    --command-ratio 1 \
    --command-key-pattern P
  validate_memtier_log "${run_dir}/preload_list.log"
}

prep_set() {
  local run_dir="$1"
  "${REDIS_CLI}" -h "${HOST}" -p "${PORT}" DEL objset >/dev/null
  run_memtier_preload "${run_dir}/preload_set.log" \
    --command "SADD objset __key__" \
    --command-ratio 1 \
    --command-key-pattern P
  validate_memtier_log "${run_dir}/preload_set.log"
}

prep_zset() {
  local run_dir="$1"
  "${REDIS_CLI}" -h "${HOST}" -p "${PORT}" DEL objzset >/dev/null
  run_memtier_preload "${run_dir}/preload_zset.log" \
    --command "ZADD objzset 1 __key__" \
    --command-ratio 1 \
    --command-key-pattern P
  validate_memtier_log "${run_dir}/preload_zset.log"
}

run_hash_workload() {
  local run_dir="$1"
  run_memtier_workload "${run_dir}/hash.log" \
    --command "HSET objhash __key__ __data__" \
    --command-ratio 1 \
    --command-key-pattern P \
    --command "HDEL objhash __key__" \
    --command-ratio 1 \
    --command-key-pattern P
  validate_memtier_log "${run_dir}/hash.log"
}

run_list_workload() {
  local run_dir="$1"
  run_memtier_workload "${run_dir}/list.log" \
    --command "LPUSH objlist __data__" \
    --command-ratio 1 \
    --command-key-pattern P \
    --command "LPOP objlist" \
    --command-ratio 1 \
    --command-key-pattern P
  validate_memtier_log "${run_dir}/list.log"
}

run_set_workload() {
  local run_dir="$1"
  run_memtier_workload "${run_dir}/set.log" \
    --command "SADD objset __key__" \
    --command-ratio 1 \
    --command-key-pattern P \
    --command "SREM objset __key__" \
    --command-ratio 1 \
    --command-key-pattern P
  validate_memtier_log "${run_dir}/set.log"
}

run_zset_workload() {
  local run_dir="$1"
  run_memtier_workload "${run_dir}/zset.log" \
    --command "ZADD objzset 1 __key__" \
    --command-ratio 1 \
    --command-key-pattern P \
    --command "ZREM objzset __key__" \
    --command-ratio 1 \
    --command-key-pattern P
  validate_memtier_log "${run_dir}/zset.log"
}

run_label() {
  local label="$1"
  local bin="$2"
  local label_out="${OUTDIR}/${label}"
  mkdir -p "${label_out}"

  local csv="${label_out}/runs.csv"
  echo "run,hash_hset_ops,hash_hset_p99_ms,hash_hdel_ops,hash_hdel_p99_ms,list_lpush_ops,list_lpush_p99_ms,list_lpop_ops,list_lpop_p99_ms,set_sadd_ops,set_sadd_p99_ms,set_srem_ops,set_srem_p99_ms,zset_zadd_ops,zset_zadd_p99_ms,zset_zrem_ops,zset_zrem_p99_ms" >"${csv}"

  for run_idx in $(seq 1 "${RUNS}"); do
    local run_dir="${label_out}/run-${run_idx}"
    mkdir -p "${run_dir}"

    start_server "${bin}" "${run_dir}"

    prep_hash "${run_dir}"
    run_hash_workload "${run_dir}"

    prep_list "${run_dir}"
    run_list_workload "${run_dir}"

    prep_set "${run_dir}"
    run_set_workload "${run_dir}"

    prep_zset "${run_dir}"
    run_zset_workload "${run_dir}"

    local hash_hset hash_hdel list_lpush list_lpop set_sadd set_srem zset_zadd zset_zrem
    hash_hset="$(extract_row_ops_and_p99 "${run_dir}/hash.log" "Hsets")"
    hash_hdel="$(extract_row_ops_and_p99 "${run_dir}/hash.log" "Hdels")"
    list_lpush="$(extract_row_ops_and_p99 "${run_dir}/list.log" "Lpushs")"
    list_lpop="$(extract_row_ops_and_p99 "${run_dir}/list.log" "Lpops")"
    set_sadd="$(extract_row_ops_and_p99 "${run_dir}/set.log" "Sadds")"
    set_srem="$(extract_row_ops_and_p99 "${run_dir}/set.log" "Srems")"
    zset_zadd="$(extract_row_ops_and_p99 "${run_dir}/zset.log" "Zadds")"
    zset_zrem="$(extract_row_ops_and_p99 "${run_dir}/zset.log" "Zrems")"

    echo "${run_idx},${hash_hset},${hash_hdel},${list_lpush},${list_lpop},${set_sadd},${set_srem},${zset_zadd},${zset_zrem}" >>"${csv}"

    stop_server
  done

  python3 - "${csv}" "${label_out}/summary.txt" <<'PY'
import csv
import statistics
import sys

csv_path, summary_path = sys.argv[1:3]
rows = []
with open(csv_path, newline='') as f:
    for row in csv.DictReader(f):
        rows.append({k: float(v) for k, v in row.items() if k != 'run'})

metrics = {k: statistics.median(r[k] for r in rows) for k in rows[0].keys()}
with open(summary_path, 'w') as f:
    f.write(f"runs={len(rows)}\n")
    for k in sorted(metrics.keys()):
        f.write(f"median_{k}={metrics[k]:.5f}\n")
PY
}

mkdir -p "${OUTDIR}"

run_label "${BASE_LABEL}" "${BASE_BIN}"
run_label "${NEW_LABEL}" "${NEW_BIN}"

python3 - "${OUTDIR}" "${BASE_LABEL}" "${NEW_LABEL}" <<'PY'
import pathlib
import sys

outdir = pathlib.Path(sys.argv[1])
base_label = sys.argv[2]
new_label = sys.argv[3]


def read_summary(path):
    values = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if '=' not in line:
                continue
            k, v = line.split('=', 1)
            try:
                values[k] = float(v)
            except ValueError:
                values[k] = v
    return values


def pct(new, old):
    if old == 0:
        return 0.0
    return (new - old) * 100.0 / old


base = read_summary(outdir / base_label / 'summary.txt')
new = read_summary(outdir / new_label / 'summary.txt')
keys = [
    'median_hash_hset_ops',
    'median_hash_hset_p99_ms',
    'median_hash_hdel_ops',
    'median_hash_hdel_p99_ms',
    'median_list_lpush_ops',
    'median_list_lpush_p99_ms',
    'median_list_lpop_ops',
    'median_list_lpop_p99_ms',
    'median_set_sadd_ops',
    'median_set_sadd_p99_ms',
    'median_set_srem_ops',
    'median_set_srem_p99_ms',
    'median_zset_zadd_ops',
    'median_zset_zadd_p99_ms',
    'median_zset_zrem_ops',
    'median_zset_zrem_p99_ms',
]

report = outdir / 'comparison.txt'
with open(report, 'w') as f:
    f.write('object_mutation_ab_comparison\n')
    f.write(f'base_label={base_label}\n')
    f.write(f'new_label={new_label}\n')
    for k in keys:
        f.write(f'{base_label}.{k}={base.get(k, 0):.5f}\n')
    for k in keys:
        f.write(f'{new_label}.{k}={new.get(k, 0):.5f}\n')
    for k in keys:
        f.write(f'delta.{k}_pct={pct(new.get(k, 0), base.get(k, 0)):.2f}\n')

print(report.read_text(), end='')
print(f'comparison_path={report}')
PY
