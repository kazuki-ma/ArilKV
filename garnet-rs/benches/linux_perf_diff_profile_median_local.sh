#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

RUNS="${RUNS:-3}"
OUTDIR_HOST="${OUTDIR_HOST:-${REPO_ROOT}/benches/results/linux-perf-diff-median-$(date +%Y%m%d-%H%M%S)}"

if ! [[ "${RUNS}" =~ ^[0-9]+$ ]] || [[ "${RUNS}" -lt 1 ]]; then
  echo "RUNS must be a positive integer (current: ${RUNS})" >&2
  exit 1
fi

RUNS_CSV="${OUTDIR_HOST}/runs.csv"
MEDIAN_CSV="${OUTDIR_HOST}/median_summary.csv"
SUMMARY_TXT="${OUTDIR_HOST}/summary.txt"

mkdir -p "${OUTDIR_HOST}"
echo "run,target,workload,ops_sec,avg_lat_ms,p99_ms,outdir" >"${RUNS_CSV}"

for run in $(seq 1 "${RUNS}"); do
  run_out="${OUTDIR_HOST}/run-${run}"
  run_log="${OUTDIR_HOST}/run-${run}.log"
  mkdir -p "${run_out}"
  echo "[linux-perf-median] run ${run}/${RUNS} outdir=${run_out}"

  OUTDIR_HOST="${run_out}" "${SCRIPT_DIR}/docker_linux_perf_diff_profile.sh" >"${run_log}" 2>&1

  for target in garnet dragonfly; do
    for workload in set get; do
      log_file="${run_out}/${target}/${workload}/memtier-${workload}.log"
      if [[ ! -f "${log_file}" ]]; then
        echo "missing memtier log: ${log_file}" >&2
        exit 1
      fi

      read -r ops avg p99 < <(awk '/^Totals[[:space:]]/ {print $2, $5, $8; exit}' "${log_file}")
      if [[ -z "${ops:-}" || -z "${avg:-}" || -z "${p99:-}" ]]; then
        echo "failed to parse Totals metrics from ${log_file}" >&2
        exit 1
      fi

      echo "${run},${target},${workload},${ops},${avg},${p99},${run_out}" >>"${RUNS_CSV}"
    done
  done
done

median_value() {
  local target="$1"
  local workload="$2"
  local column="$3"
  local values
  mapfile -t values < <(awk -F, -v target="${target}" -v workload="${workload}" -v column="${column}" \
    '$2 == target && $3 == workload {print $column}' "${RUNS_CSV}" | sort -n)

  local count="${#values[@]}"
  if [[ "${count}" -eq 0 ]]; then
    echo "0"
    return
  fi

  if (( count % 2 == 1 )); then
    echo "${values[$((count / 2))]}"
    return
  fi

  local left="${values[$((count / 2 - 1))]}"
  local right="${values[$((count / 2))]}"
  awk -v left="${left}" -v right="${right}" 'BEGIN { printf "%.6f", (left + right) / 2.0 }'
}

echo "target,workload,median_ops_sec,median_avg_lat_ms,median_p99_ms" >"${MEDIAN_CSV}"
for target in garnet dragonfly; do
  for workload in set get; do
    median_ops="$(median_value "${target}" "${workload}" 4)"
    median_avg="$(median_value "${target}" "${workload}" 5)"
    median_p99="$(median_value "${target}" "${workload}" 6)"
    echo "${target},${workload},${median_ops},${median_avg},${median_p99}" >>"${MEDIAN_CSV}"
  done
done

garnet_set="$(awk -F, '$1 == "garnet" && $2 == "set" {print $3; exit}' "${MEDIAN_CSV}")"
dragonfly_set="$(awk -F, '$1 == "dragonfly" && $2 == "set" {print $3; exit}' "${MEDIAN_CSV}")"
garnet_get="$(awk -F, '$1 == "garnet" && $2 == "get" {print $3; exit}' "${MEDIAN_CSV}")"
dragonfly_get="$(awk -F, '$1 == "dragonfly" && $2 == "get" {print $3; exit}' "${MEDIAN_CSV}")"

ratio_set="$(awk -v d="${dragonfly_set}" -v g="${garnet_set}" 'BEGIN { if (g == 0) print "inf"; else printf "%.3f", d / g }')"
ratio_get="$(awk -v d="${dragonfly_get}" -v g="${garnet_get}" 'BEGIN { if (g == 0) print "inf"; else printf "%.3f", d / g }')"

cat >"${SUMMARY_TXT}" <<EOF
runs=${RUNS}
outdir=${OUTDIR_HOST}
runs_csv=${RUNS_CSV}
median_csv=${MEDIAN_CSV}

median dragonfly/garnet throughput ratio:
  set=${ratio_set}
  get=${ratio_get}
EOF

echo "linux perf median summary completed"
echo "outdir=${OUTDIR_HOST}"
echo "runs_csv=${RUNS_CSV}"
echo "median_csv=${MEDIAN_CSV}"
echo "summary=${SUMMARY_TXT}"
