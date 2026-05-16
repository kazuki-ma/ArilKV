#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
GATE_SCRIPT="${SCRIPT_DIR}/perf_regression_gate_local.sh"

BASE_BIN="${BASE_BIN:-}"
NEW_BIN="${NEW_BIN:-}"
BASE_LABEL="${BASE_LABEL:-base}"
NEW_LABEL="${NEW_LABEL:-new}"
OUTDIR="${OUTDIR:-/tmp/garnet-binary-ab-$(date +%Y%m%d-%H%M%S)}"

RUNS="${RUNS:-3}"
THREADS="${THREADS:-4}"
CONNS="${CONNS:-8}"
REQUESTS="${REQUESTS:-5000}"
PRELOAD_REQUESTS="${PRELOAD_REQUESTS:-${REQUESTS}}"
PIPELINE="${PIPELINE:-1}"
SIZE_RANGE="${SIZE_RANGE:-1-1024}"
PORT="${PORT:-16389}"
HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS:-25}"
MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES:-262144}"
STRING_STORE_SHARDS="${STRING_STORE_SHARDS:-}"

if [[ ! -x "${GATE_SCRIPT}" ]]; then
    echo "missing executable perf gate script: ${GATE_SCRIPT}" >&2
    exit 1
fi

if [[ -z "${BASE_BIN}" || -z "${NEW_BIN}" ]]; then
    cat >&2 <<'EOF'
BASE_BIN and NEW_BIN are required.
Example:
  BASE_BIN=/tmp/garnet-base NEW_BIN=/tmp/garnet-new \
  RUNS=3 THREADS=8 CONNS=16 REQUESTS=5000 \
  ./benches/binary_ab_local.sh
EOF
    exit 1
fi

if [[ ! -x "${BASE_BIN}" ]]; then
    echo "BASE_BIN is not executable: ${BASE_BIN}" >&2
    exit 1
fi
if [[ ! -x "${NEW_BIN}" ]]; then
    echo "NEW_BIN is not executable: ${NEW_BIN}" >&2
    exit 1
fi
if [[ "${BASE_LABEL}" == "${NEW_LABEL}" ]]; then
    echo "BASE_LABEL and NEW_LABEL must be different" >&2
    exit 1
fi

mkdir -p "${OUTDIR}"

run_gate() {
    local label="$1"
    local server_bin="$2"
    local run_out="${OUTDIR}/${label}"
    mkdir -p "${run_out}"

    local -a gate_env
    gate_env=(
        "OUTDIR=${run_out}"
        "SERVER_BIN=${server_bin}"
        "RUNS=${RUNS}"
        "THREADS=${THREADS}"
        "CONNS=${CONNS}"
        "REQUESTS=${REQUESTS}"
        "PRELOAD_REQUESTS=${PRELOAD_REQUESTS}"
        "PIPELINE=${PIPELINE}"
        "SIZE_RANGE=${SIZE_RANGE}"
        "PORT=${PORT}"
        "HASH_INDEX_SIZE_BITS=${HASH_INDEX_SIZE_BITS}"
        "MAX_IN_MEMORY_PAGES=${MAX_IN_MEMORY_PAGES}"
        "MIN_MEDIAN_SET_OPS=0"
        "MIN_MEDIAN_GET_OPS=0"
        "MAX_MEDIAN_SET_P99_MS=0"
        "MAX_MEDIAN_GET_P99_MS=0"
    )
    if [[ -n "${STRING_STORE_SHARDS}" ]]; then
        gate_env+=("STRING_STORE_SHARDS=${STRING_STORE_SHARDS}")
    fi
    env "${gate_env[@]}" "${GATE_SCRIPT}" >/dev/null
}

echo "Running ${BASE_LABEL} benchmark gate..."
run_gate "${BASE_LABEL}" "${BASE_BIN}"

echo "Running ${NEW_LABEL} benchmark gate..."
run_gate "${NEW_LABEL}" "${NEW_BIN}"

python3 - "${OUTDIR}" "${BASE_LABEL}" "${NEW_LABEL}" <<'PY'
import pathlib
import sys

outdir = pathlib.Path(sys.argv[1])
base_label = sys.argv[2]
new_label = sys.argv[3]


def read_summary(path: pathlib.Path):
    values = {}
    with path.open() as f:
        for line in f:
            line = line.strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            try:
                values[key] = float(value)
            except ValueError:
                values[key] = value
    return values


def pct(new, old):
    if old == 0:
        return 0.0
    return (new - old) * 100.0 / old


base = read_summary(outdir / base_label / "summary.txt")
new = read_summary(outdir / new_label / "summary.txt")

report_path = outdir / "comparison.txt"
with report_path.open("w") as f:
    f.write("binary_ab_comparison\n")
    f.write(f"base_label={base_label}\n")
    f.write(f"new_label={new_label}\n")
    f.write(f"{base_label}.median_set_ops={base.get('median_set_ops', 0):.3f}\n")
    f.write(f"{base_label}.median_get_ops={base.get('median_get_ops', 0):.3f}\n")
    f.write(f"{base_label}.median_set_p99_ms={base.get('median_set_p99_ms', 0):.5f}\n")
    f.write(f"{base_label}.median_get_p99_ms={base.get('median_get_p99_ms', 0):.5f}\n")
    f.write(f"{new_label}.median_set_ops={new.get('median_set_ops', 0):.3f}\n")
    f.write(f"{new_label}.median_get_ops={new.get('median_get_ops', 0):.3f}\n")
    f.write(f"{new_label}.median_set_p99_ms={new.get('median_set_p99_ms', 0):.5f}\n")
    f.write(f"{new_label}.median_get_p99_ms={new.get('median_get_p99_ms', 0):.5f}\n")
    f.write(
        f"delta.set_ops_pct={pct(new.get('median_set_ops', 0), base.get('median_set_ops', 0)):.2f}\n"
    )
    f.write(
        f"delta.get_ops_pct={pct(new.get('median_get_ops', 0), base.get('median_get_ops', 0)):.2f}\n"
    )
    f.write(
        f"delta.set_p99_ms_pct={pct(new.get('median_set_p99_ms', 0), base.get('median_set_p99_ms', 0)):.2f}\n"
    )
    f.write(
        f"delta.get_p99_ms_pct={pct(new.get('median_get_p99_ms', 0), base.get('median_get_p99_ms', 0)):.2f}\n"
    )

print(report_path.read_text(), end="")
print(f"comparison_path={report_path}")
PY
