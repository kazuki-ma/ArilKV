#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"

DOCKER_IMAGE="${DOCKER_IMAGE:-rust:latest}"
DRAGONFLY_VERSION="${DRAGONFLY_VERSION:-v1.36.0}"

THREADS="${THREADS:-8}"
CONNS="${CONNS:-16}"
REQUESTS="${REQUESTS:-5000}"
PRELOAD_REQUESTS="${PRELOAD_REQUESTS:-${REQUESTS}}"
PIPELINE="${PIPELINE:-1}"
SIZE_RANGE="${SIZE_RANGE:-1-1024}"
PORT_BASE="${PORT_BASE:-16389}"
PERF_FREQ="${PERF_FREQ:-99}"
TARGETS="${TARGETS:-garnet dragonfly}"
WORKLOADS="${WORKLOADS:-set get}"
SERVER_CPU_SET="${SERVER_CPU_SET:-}"
CLIENT_CPU_SET="${CLIENT_CPU_SET:-}"
GARNET_TSAVORITE_STRING_STORE_SHARDS="${GARNET_TSAVORITE_STRING_STORE_SHARDS:-2}"
GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES="${GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES:-262144}"
GARNET_STRING_OWNER_THREADS="${GARNET_STRING_OWNER_THREADS:-}"
TOKIO_WORKER_THREADS="${TOKIO_WORKER_THREADS:-}"
GARNET_OWNER_THREAD_PINNING="${GARNET_OWNER_THREAD_PINNING:-}"
GARNET_OWNER_THREAD_CPU_SET="${GARNET_OWNER_THREAD_CPU_SET:-}"
GARNET_OWNER_EXECUTION_INLINE="${GARNET_OWNER_EXECUTION_INLINE:-}"
DRAGONFLY_PROACTOR_THREADS="${DRAGONFLY_PROACTOR_THREADS:-}"
DRAGONFLY_CONN_IO_THREADS="${DRAGONFLY_CONN_IO_THREADS:-}"

OUTDIR_HOST="${OUTDIR_HOST:-${REPO_ROOT}/benches/results/linux-perf-diff-docker-$(date +%Y%m%d-%H%M%S)}"
OUTDIR_BASENAME="$(basename "${OUTDIR_HOST}")"
OUTDIR_PARENT_HOST="$(cd "$(dirname "${OUTDIR_HOST}")" && pwd)"
OUTDIR_CONTAINER="/out-host/${OUTDIR_BASENAME}"

if ! command -v docker >/dev/null 2>&1; then
    echo "missing required command: docker" >&2
    exit 1
fi

docker info >/dev/null 2>&1 || {
    echo "docker daemon is unavailable; see benches/DOCKER_TROUBLESHOOTING_LOCAL.md" >&2
    exit 1
}

mkdir -p "${OUTDIR_HOST}"

DOCKER_RUN_ARGS=(
    --rm
    --privileged
    --security-opt seccomp=unconfined
    -v "${WORKSPACE_ROOT}:/work"
    -v "${OUTDIR_PARENT_HOST}:/out-host"
    -w /work
)

add_required_env() {
    DOCKER_RUN_ARGS+=(-e "$1=$2")
}

add_optional_env() {
    if [[ -n "$2" ]]; then
        DOCKER_RUN_ARGS+=(-e "$1=$2")
    fi
}

add_required_env THREADS "${THREADS}"
add_required_env CONNS "${CONNS}"
add_required_env REQUESTS "${REQUESTS}"
add_required_env PRELOAD_REQUESTS "${PRELOAD_REQUESTS}"
add_required_env PIPELINE "${PIPELINE}"
add_required_env SIZE_RANGE "${SIZE_RANGE}"
add_required_env PORT_BASE "${PORT_BASE}"
add_required_env PERF_FREQ "${PERF_FREQ}"
add_required_env TARGETS "${TARGETS}"
add_required_env WORKLOADS "${WORKLOADS}"
add_required_env GARNET_TSAVORITE_STRING_STORE_SHARDS "${GARNET_TSAVORITE_STRING_STORE_SHARDS}"
add_required_env GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES "${GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES}"
add_required_env OUTDIR "${OUTDIR_CONTAINER}"

add_optional_env SERVER_CPU_SET "${SERVER_CPU_SET}"
add_optional_env CLIENT_CPU_SET "${CLIENT_CPU_SET}"
add_optional_env GARNET_STRING_OWNER_THREADS "${GARNET_STRING_OWNER_THREADS}"
add_optional_env TOKIO_WORKER_THREADS "${TOKIO_WORKER_THREADS}"
add_optional_env GARNET_OWNER_THREAD_PINNING "${GARNET_OWNER_THREAD_PINNING}"
add_optional_env GARNET_OWNER_THREAD_CPU_SET "${GARNET_OWNER_THREAD_CPU_SET}"
add_optional_env GARNET_OWNER_EXECUTION_INLINE "${GARNET_OWNER_EXECUTION_INLINE}"
add_optional_env DRAGONFLY_PROACTOR_THREADS "${DRAGONFLY_PROACTOR_THREADS}"
add_optional_env DRAGONFLY_CONN_IO_THREADS "${DRAGONFLY_CONN_IO_THREADS}"

docker run "${DOCKER_RUN_ARGS[@]}" \
    "${DOCKER_IMAGE}" bash -lc "
set -euo pipefail
export RUSTUP_HOME=/usr/local/rustup
export CARGO_HOME=/usr/local/cargo
export PATH=/usr/local/cargo/bin:\$PATH
export DEBIAN_FRONTEND=noninteractive

apt-get update >/dev/null
apt-get install -y \
    linux-perf netcat-openbsd util-linux make autoconf automake libtool \
    pkg-config libevent-dev libssl-dev zlib1g-dev git curl ca-certificates \
    >/dev/null

if [[ ! -x /tmp/memtier-src/memtier_benchmark ]]; then
    rm -rf /tmp/memtier-src
    git clone --depth 1 https://github.com/RedisLabs/memtier_benchmark.git /tmp/memtier-src >/dev/null
    cd /tmp/memtier-src
    autoreconf -ivf >/dev/null
    ./configure >/dev/null
    make -j\"\$(nproc)\" >/dev/null
fi

arch=\"\$(uname -m)\"
if [[ \"\${arch}\" == \"aarch64\" ]]; then
    dfly_asset=\"dragonfly-aarch64.tar.gz\"
    dfly_name=\"dragonfly-aarch64\"
else
    dfly_asset=\"dragonfly-x86_64.tar.gz\"
    dfly_name=\"dragonfly-x86_64\"
fi

curl -L \"https://github.com/dragonflydb/dragonfly/releases/download/${DRAGONFLY_VERSION}/\${dfly_asset}\" \
    -o /tmp/dragonfly.tar.gz >/dev/null
rm -rf /tmp/dragonfly
mkdir -p /tmp/dragonfly
tar -xzf /tmp/dragonfly.tar.gz -C /tmp/dragonfly
chmod +x \"/tmp/dragonfly/\${dfly_name}\"

cd /work/garnet-rs
export CARGO_TARGET_DIR=/tmp/garnet-target-linux
export MEMTIER_BIN=/tmp/memtier-src/memtier_benchmark
export DRAGONFLY_BIN=\"/tmp/dragonfly/\${dfly_name}\"
export GARNET_BIN=/tmp/garnet-target-linux/release/garnet-server
./benches/linux_perf_diff_profile.sh
"

echo "dockerized linux perf differential profiling completed"
echo "outdir=${OUTDIR_HOST}"
