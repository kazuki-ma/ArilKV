#!/usr/bin/env bash
set -euo pipefail

BASE_BIN="${BASE_BIN:?BASE_BIN is required}"
NEW_BIN="${NEW_BIN:?NEW_BIN is required}"
BASE_LABEL="${BASE_LABEL:-before}"
NEW_LABEL="${NEW_LABEL:-after}"
RUNS="${RUNS:-3}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-16522}"
THREADS="${THREADS:-1}"
CONNS="${CONNS:-2}"
REQUESTS="${REQUESTS:-12000}"
PIPELINE="${PIPELINE:-1}"
SIZE_RANGE="${SIZE_RANGE:-16-128}"
HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS:-25}"
MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES:-262144}"
OUTDIR="${OUTDIR:-/tmp/garnet-hsetex-hgetex-ab-$(date +%Y%m%d-%H%M%S)}"
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

run_memtier_workload() {
  local output="$1"
  "${MEMTIER_BIN}" \
    -s "${HOST}" \
    -p "${PORT}" \
    -t "${THREADS}" \
    -c "${CONNS}" \
    -n "${REQUESTS}" \
    --pipeline "${PIPELINE}" \
    --data-size-range "${SIZE_RANGE}" \
    --hide-histogram \
    --distinct-client-seed \
    --print-percentiles 50,90,99,99.9,99.99 \
    --command "HSETEX objhash PX 60000 FIELDS 2 f1 __data__ f2 __data__" \
    --command-ratio 1 \
    --command "HGETEX objhash FIELDS 2 f1 f2" \
    --command-ratio 1 \
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

run_label() {
  local label="$1"
  local bin="$2"
  local label_out="${OUTDIR}/${label}"
  mkdir -p "${label_out}"

  local csv="${label_out}/runs.csv"
  echo "run,hsetex_ops,hsetex_p99_ms,hgetex_ops,hgetex_p99_ms" >"${csv}"

  for run_idx in $(seq 1 "${RUNS}"); do
    local run_dir="${label_out}/run-${run_idx}"
    mkdir -p "${run_dir}"

    start_server "${bin}" "${run_dir}"
    "${REDIS_CLI}" -h "${HOST}" -p "${PORT}" DEL objhash >/dev/null
    "${REDIS_CLI}" -h "${HOST}" -p "${PORT}" \
      HSETEX objhash PX 60000 FIELDS 2 f1 seed f2 seed >/dev/null

    run_memtier_workload "${run_dir}/hsetex_hgetex.log"
    validate_memtier_log "${run_dir}/hsetex_hgetex.log"

    local hsetex hgetex
    hsetex="$(extract_row_ops_and_p99 "${run_dir}/hsetex_hgetex.log" "Hsetexs")"
    hgetex="$(extract_row_ops_and_p99 "${run_dir}/hsetex_hgetex.log" "Hgetexs")"

    echo "${run_idx},${hsetex},${hgetex}" >>"${csv}"
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
    'median_hsetex_ops',
    'median_hsetex_p99_ms',
    'median_hgetex_ops',
    'median_hgetex_p99_ms',
]

lines = [
    'hsetex_hgetex_ab_comparison',
    f'base_label={base_label}',
    f'new_label={new_label}',
]
for key in keys:
    lines.append(f'{base_label}.{key}={base[key]:.5f}')
for key in keys:
    lines.append(f'{new_label}.{key}={new[key]:.5f}')
for key in keys:
    lines.append(f'delta.{key}_pct={pct(new[key], base[key]):.2f}')

comparison_path = outdir / 'comparison.txt'
comparison_path.write_text('\n'.join(lines) + '\n')
print('\n'.join(lines))
print(f'comparison_path={comparison_path}')
PY
