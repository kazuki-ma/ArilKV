#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SWEEP_SCRIPT="${SCRIPT_DIR}/sweep_string_store_shards_local.sh"

WORKLOAD_MATRIX="${WORKLOAD_MATRIX:-w1:1:4:20000 w2:4:8:20000 w3:8:16:20000}"
OWNER_THREAD_COUNTS="${OWNER_THREAD_COUNTS:-0 16}"
SHARD_COUNTS="${SHARD_COUNTS:-1 2 4 8 16}"
PIPELINE="${PIPELINE:-1}"
SIZE_RANGE="${SIZE_RANGE:-1-1024}"
HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS:-25}"
MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES:-1048576}"
PORT="${PORT:-16389}"
OUTDIR="${OUTDIR:-/tmp/garnet-shard-policy-matrix-$(date +%Y%m%d-%H%M%S)}"

if [[ ! -x "${SWEEP_SCRIPT}" ]]; then
    echo "missing executable sweep script: ${SWEEP_SCRIPT}" >&2
    exit 1
fi

mkdir -p "${OUTDIR}"
MATRIX_CSV="${OUTDIR}/matrix-results.csv"
SUMMARY_CSV="${OUTDIR}/summary-by-owner.csv"
RECOMMENDATION_TXT="${OUTDIR}/recommendations.txt"

echo "workload,owner_threads,threads,conns,requests,shards,set_ops,set_p99_ms,get_ops,get_p99_ms,geo_mean_ops,avg_p99_ms" >"${MATRIX_CSV}"

for owner_threads in ${OWNER_THREAD_COUNTS}; do
    if [[ ! "${owner_threads}" =~ ^[0-9]+$ ]]; then
        echo "invalid OWNER_THREAD_COUNTS value: ${owner_threads}" >&2
        exit 1
    fi

    for workload in ${WORKLOAD_MATRIX}; do
        IFS=':' read -r workload_name threads conns requests <<<"${workload}"
        if [[ -z "${workload_name}" || -z "${threads}" || -z "${conns}" || -z "${requests}" ]]; then
            echo "invalid WORKLOAD_MATRIX entry: ${workload}" >&2
            exit 1
        fi
        if [[ ! "${threads}" =~ ^[0-9]+$ ]] || [[ ! "${conns}" =~ ^[0-9]+$ ]] || [[ ! "${requests}" =~ ^[0-9]+$ ]]; then
            echo "invalid numeric values in WORKLOAD_MATRIX entry: ${workload}" >&2
            exit 1
        fi

        run_dir="${OUTDIR}/${workload_name}-owner${owner_threads}"
        if (( owner_threads == 0 )); then
            OUTDIR="${run_dir}" \
            THREADS="${threads}" \
            CONNS="${conns}" \
            REQUESTS="${requests}" \
            SHARD_COUNTS="${SHARD_COUNTS}" \
            PIPELINE="${PIPELINE}" \
            SIZE_RANGE="${SIZE_RANGE}" \
            HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS}" \
            MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES}" \
            PORT="${PORT}" \
            "${SWEEP_SCRIPT}"
        else
            OUTDIR="${run_dir}" \
            THREADS="${threads}" \
            CONNS="${conns}" \
            REQUESTS="${requests}" \
            SHARD_COUNTS="${SHARD_COUNTS}" \
            PIPELINE="${PIPELINE}" \
            SIZE_RANGE="${SIZE_RANGE}" \
            HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS}" \
            MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES}" \
            PORT="${PORT}" \
            GARNET_STRING_OWNER_THREADS="${owner_threads}" \
            "${SWEEP_SCRIPT}"
        fi

        awk -F',' \
            -v workload="${workload_name}" \
            -v owner="${owner_threads}" \
            -v threads="${threads}" \
            -v conns="${conns}" \
            -v requests="${requests}" \
            'NR > 1 {
                set_ops = $2 + 0;
                set_p99 = $3 + 0;
                get_ops = $4 + 0;
                get_p99 = $5 + 0;
                geo = sqrt(set_ops * get_ops);
                avg_p99 = (set_p99 + get_p99) / 2.0;
                printf "%s,%s,%s,%s,%s,%s,%.0f,%.5f,%.0f,%.5f,%.3f,%.5f\n",
                    workload, owner, threads, conns, requests, $1, set_ops, set_p99, get_ops, get_p99, geo, avg_p99;
            }' "${run_dir}/results.csv" >>"${MATRIX_CSV}"
    done
done

python3 - "${MATRIX_CSV}" "${SUMMARY_CSV}" "${RECOMMENDATION_TXT}" <<'PY'
import csv
import statistics
import sys
from collections import defaultdict

matrix_csv, summary_csv, recommendation_txt = sys.argv[1:4]

grouped_geo = defaultdict(list)
grouped_p99 = defaultdict(list)
owners = set()
shards = set()

with open(matrix_csv, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        owner = int(row["owner_threads"])
        shard = int(row["shards"])
        owners.add(owner)
        shards.add(shard)
        grouped_geo[(owner, shard)].append(float(row["geo_mean_ops"]))
        grouped_p99[(owner, shard)].append(float(row["avg_p99_ms"]))

with open(summary_csv, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["owner_threads", "shards", "median_geo_mean_ops", "median_avg_p99_ms"])
    for owner in sorted(owners):
        for shard in sorted(shards):
            geo_values = grouped_geo.get((owner, shard), [])
            p99_values = grouped_p99.get((owner, shard), [])
            if not geo_values:
                continue
            writer.writerow(
                [
                    owner,
                    shard,
                    f"{statistics.median(geo_values):.3f}",
                    f"{statistics.median(p99_values):.5f}",
                ]
            )

with open(recommendation_txt, "w") as f:
    f.write("String-store shard recommendations by owner-thread mode\n")
    f.write("Selection criteria: maximize median geo-mean(GET ops, SET ops) across workloads.\n\n")
    for owner in sorted(owners):
        candidates = []
        for shard in sorted(shards):
            geo_values = grouped_geo.get((owner, shard), [])
            p99_values = grouped_p99.get((owner, shard), [])
            if not geo_values:
                continue
            median_geo = statistics.median(geo_values)
            median_p99 = statistics.median(p99_values)
            candidates.append((median_geo, -median_p99, shard, median_p99))
        if not candidates:
            continue
        best = max(candidates)
        f.write(
            f"owner_threads={owner}: recommended_shards={best[2]} "
            f"(median_geo_mean_ops={best[0]:.3f}, median_avg_p99_ms={best[3]:.5f})\n"
        )
PY

echo "matrix_csv=${MATRIX_CSV}"
echo "summary_csv=${SUMMARY_CSV}"
echo "recommendations=${RECOMMENDATION_TXT}"
