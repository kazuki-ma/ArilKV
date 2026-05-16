#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MANIFEST_PATH="${REPO_ROOT}/Cargo.toml"
GATE_SCRIPT="${SCRIPT_DIR}/perf_regression_gate_local.sh"

DEFAULT_TARGET_DIR="${DEFAULT_TARGET_DIR:-${REPO_ROOT}/target}"
MIMALLOC_TARGET_DIR="${MIMALLOC_TARGET_DIR:-${REPO_ROOT}/target/mimalloc}"
OUTDIR="${OUTDIR:-/tmp/garnet-allocator-ab-$(date +%Y%m%d-%H%M%S)}"

RUNS="${RUNS:-3}"
THREADS="${THREADS:-4}"
CONNS="${CONNS:-8}"
REQUESTS="${REQUESTS:-5000}"
PRELOAD_REQUESTS="${PRELOAD_REQUESTS:-${REQUESTS}}"
PIPELINE="${PIPELINE:-1}"
SIZE_RANGE="${SIZE_RANGE:-1-1024}"
PORT="${PORT:-16389}"
HASH_INDEX_SIZE_BITS="${HASH_INDEX_SIZE_BITS:-25}"
MAX_IN_MEMORY_PAGES="${MAX_IN_MEMORY_PAGES:-1048576}"
STRING_STORE_SHARDS="${STRING_STORE_SHARDS:-}"

if [[ ! -x "${GATE_SCRIPT}" ]]; then
    echo "missing executable perf gate script: ${GATE_SCRIPT}" >&2
    exit 1
fi

mkdir -p "${OUTDIR}"

echo "Building default allocator binary..."
cargo build -p garnet-server --release --manifest-path "${MANIFEST_PATH}" >/dev/null
DEFAULT_BIN="${DEFAULT_TARGET_DIR}/release/garnet-server"
if [[ ! -x "${DEFAULT_BIN}" ]]; then
    echo "default allocator binary not found: ${DEFAULT_BIN}" >&2
    exit 1
fi

echo "Building mimalloc allocator binary..."
CARGO_TARGET_DIR="${MIMALLOC_TARGET_DIR}" \
    cargo build -p garnet-server --release --features mimalloc --manifest-path "${MANIFEST_PATH}" >/dev/null
MIMALLOC_BIN="${MIMALLOC_TARGET_DIR}/release/garnet-server"
if [[ ! -x "${MIMALLOC_BIN}" ]]; then
    echo "mimalloc allocator binary not found: ${MIMALLOC_BIN}" >&2
    exit 1
fi

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

echo "Running default allocator benchmark gate..."
run_gate "default" "${DEFAULT_BIN}"

echo "Running mimalloc allocator benchmark gate..."
run_gate "mimalloc" "${MIMALLOC_BIN}"

python3 - "${OUTDIR}" <<'PY'
import pathlib
import sys

outdir = pathlib.Path(sys.argv[1])

def read_summary(path: pathlib.Path):
    values = {}
    with path.open() as f:
        for line in f:
            line = line.strip()
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            try:
                values[k] = float(v)
            except ValueError:
                values[k] = v
    return values

default_summary = read_summary(outdir / "default" / "summary.txt")
mimalloc_summary = read_summary(outdir / "mimalloc" / "summary.txt")

def pct(new, old):
    if old == 0:
        return 0.0
    return (new - old) * 100.0 / old

report_path = outdir / "comparison.txt"
with report_path.open("w") as f:
    f.write("allocator_ab_comparison\n")
    f.write(f"default.median_set_ops={default_summary.get('median_set_ops', 0):.3f}\n")
    f.write(f"default.median_get_ops={default_summary.get('median_get_ops', 0):.3f}\n")
    f.write(f"default.median_set_p99_ms={default_summary.get('median_set_p99_ms', 0):.5f}\n")
    f.write(f"default.median_get_p99_ms={default_summary.get('median_get_p99_ms', 0):.5f}\n")
    f.write(f"mimalloc.median_set_ops={mimalloc_summary.get('median_set_ops', 0):.3f}\n")
    f.write(f"mimalloc.median_get_ops={mimalloc_summary.get('median_get_ops', 0):.3f}\n")
    f.write(f"mimalloc.median_set_p99_ms={mimalloc_summary.get('median_set_p99_ms', 0):.5f}\n")
    f.write(f"mimalloc.median_get_p99_ms={mimalloc_summary.get('median_get_p99_ms', 0):.5f}\n")
    f.write(
        f"delta.set_ops_pct={pct(mimalloc_summary.get('median_set_ops', 0), default_summary.get('median_set_ops', 0)):.2f}\n"
    )
    f.write(
        f"delta.get_ops_pct={pct(mimalloc_summary.get('median_get_ops', 0), default_summary.get('median_get_ops', 0)):.2f}\n"
    )
    f.write(
        f"delta.set_p99_ms_pct={pct(mimalloc_summary.get('median_set_p99_ms', 0), default_summary.get('median_set_p99_ms', 0)):.2f}\n"
    )
    f.write(
        f"delta.get_p99_ms_pct={pct(mimalloc_summary.get('median_get_p99_ms', 0), default_summary.get('median_get_p99_ms', 0)):.2f}\n"
    )

print(report_path.read_text(), end="")
print(f"comparison_path={report_path}")
PY
