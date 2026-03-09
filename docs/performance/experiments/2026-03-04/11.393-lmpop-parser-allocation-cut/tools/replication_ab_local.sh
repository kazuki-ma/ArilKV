#!/usr/bin/env bash
set -euo pipefail

BASE_BIN="${BASE_BIN:?BASE_BIN is required}"
NEW_BIN="${NEW_BIN:?NEW_BIN is required}"
BASE_LABEL="${BASE_LABEL:-before}"
NEW_LABEL="${NEW_LABEL:-after}"
RUNS="${RUNS:-3}"
HOST="${HOST:-127.0.0.1}"
PRIMARY_PORT="${PRIMARY_PORT:-16510}"
REPLICA_PORT="${REPLICA_PORT:-16511}"
THREADS="${THREADS:-4}"
CONNS="${CONNS:-8}"
REQUESTS="${REQUESTS:-5000}"
PIPELINE="${PIPELINE:-1}"
SIZE_RANGE="${SIZE_RANGE:-1-1024}"
HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS:-25}"
MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES:-262144}"
OUTDIR="${OUTDIR:-/tmp/garnet-replication-ab-$(date +%Y%m%d-%H%M%S)}"
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

PRIMARY_PID=""
REPLICA_PID=""

stop_pair() {
  if [[ -n "${REPLICA_PID}" ]] && kill -0 "${REPLICA_PID}" 2>/dev/null; then
    kill "${REPLICA_PID}" 2>/dev/null || true
    wait "${REPLICA_PID}" 2>/dev/null || true
  fi
  REPLICA_PID=""
  if [[ -n "${PRIMARY_PID}" ]] && kill -0 "${PRIMARY_PID}" 2>/dev/null; then
    kill "${PRIMARY_PID}" 2>/dev/null || true
    wait "${PRIMARY_PID}" 2>/dev/null || true
  fi
  PRIMARY_PID=""
  sleep 0.2
}

cleanup() {
  stop_pair
}
trap cleanup EXIT

wait_port() {
  local port="$1"
  for _ in $(seq 1 200); do
    if nc -z "${HOST}" "${port}" 2>/dev/null; then
      return 0
    fi
    sleep 0.05
  done
  return 1
}

wait_ping() {
  local port="$1"
  for _ in $(seq 1 200); do
    if "${REDIS_CLI}" -h "${HOST}" -p "${port}" PING 2>/dev/null | grep -q '^PONG$'; then
      return 0
    fi
    sleep 0.05
  done
  return 1
}

wait_probe_replicated() {
  local key="$1"
  local expected="$2"
  for _ in $(seq 1 400); do
    local got
    got="$("${REDIS_CLI}" -h "${HOST}" -p "${REPLICA_PORT}" --raw GET "${key}" 2>/dev/null || true)"
    if [[ "${got}" == "${expected}" ]]; then
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
  awk -v row="${row}" '$1==row{print ($2+0)","($8+0); exit}' "${file}"
}

run_memtier() {
  local ratio="$1"
  local output="$2"
  "${MEMTIER_BIN}" \
    -s "${HOST}" \
    -p "${PRIMARY_PORT}" \
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

start_pair() {
  local bin="$1"
  local run_dir="$2"

  stop_pair

  if nc -z "${HOST}" "${PRIMARY_PORT}" 2>/dev/null; then
    echo "primary port ${PRIMARY_PORT} already in use" >&2
    exit 1
  fi
  if nc -z "${HOST}" "${REPLICA_PORT}" 2>/dev/null; then
    echo "replica port ${REPLICA_PORT} already in use" >&2
    exit 1
  fi

  env \
    GARNET_BIND_ADDR="${HOST}:${PRIMARY_PORT}" \
    GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS}" \
    GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES}" \
    "${bin}" >"${run_dir}/primary.log" 2>&1 &
  PRIMARY_PID=$!

  env \
    GARNET_BIND_ADDR="${HOST}:${REPLICA_PORT}" \
    GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS}" \
    GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES}" \
    "${bin}" >"${run_dir}/replica.log" 2>&1 &
  REPLICA_PID=$!

  wait_port "${PRIMARY_PORT}" || { echo "primary failed to listen" >&2; exit 1; }
  wait_port "${REPLICA_PORT}" || { echo "replica failed to listen" >&2; exit 1; }
  wait_ping "${PRIMARY_PORT}" || { echo "primary failed ping" >&2; exit 1; }
  wait_ping "${REPLICA_PORT}" || { echo "replica failed ping" >&2; exit 1; }

  "${REDIS_CLI}" -h "${HOST}" -p "${REPLICA_PORT}" REPLICAOF "${HOST}" "${PRIMARY_PORT}" >/dev/null
}

run_label() {
  local label="$1"
  local bin="$2"
  local label_out="${OUTDIR}/${label}"
  mkdir -p "${label_out}"
  local csv="${label_out}/runs.csv"
  echo "run,write_set_ops,write_set_p99_ms,mixed_set_ops,mixed_set_p99_ms,mixed_get_ops,mixed_get_p99_ms" >"${csv}"

  for run_idx in $(seq 1 "${RUNS}"); do
    local run_dir="${label_out}/run-${run_idx}"
    mkdir -p "${run_dir}"
    start_pair "${bin}" "${run_dir}"

    local probe_key="__repl_probe__${label}__${run_idx}"
    local probe_value="ok-${run_idx}"
    "${REDIS_CLI}" -h "${HOST}" -p "${PRIMARY_PORT}" SET "${probe_key}" "${probe_value}" >/dev/null
    wait_probe_replicated "${probe_key}" "${probe_value}" || { echo "replication probe failed" >&2; exit 1; }

    local write_log="${run_dir}/write.log"
    local mixed_log="${run_dir}/mixed.log"

    run_memtier "1:0" "${write_log}"
    validate_memtier_log "${write_log}"

    run_memtier "1:1" "${mixed_log}"
    validate_memtier_log "${mixed_log}"

    local write_set mixed_set mixed_get
    write_set="$(extract_row_ops_and_p99 "${write_log}" "Sets")"
    mixed_set="$(extract_row_ops_and_p99 "${mixed_log}" "Sets")"
    mixed_get="$(extract_row_ops_and_p99 "${mixed_log}" "Gets")"
    echo "${run_idx},${write_set},${mixed_set},${mixed_get}" >>"${csv}"

    stop_pair
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
    'median_write_set_ops',
    'median_write_set_p99_ms',
    'median_mixed_set_ops',
    'median_mixed_set_p99_ms',
    'median_mixed_get_ops',
    'median_mixed_get_p99_ms',
]

report = outdir / 'comparison.txt'
with open(report, 'w') as f:
    f.write('replication_ab_comparison\n')
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
