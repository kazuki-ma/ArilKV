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

OUTDIR_HOST="${OUTDIR_HOST:-${REPO_ROOT}/benches/results/linux-perf-diff-docker-$(date +%Y%m%d-%H%M%S)}"
OUTDIR_CONTAINER="/work/garnet-rs/benches/results/$(basename "${OUTDIR_HOST}")"

if ! command -v docker >/dev/null 2>&1; then
    echo "missing required command: docker" >&2
    exit 1
fi

docker info >/dev/null 2>&1 || {
    echo "docker daemon is unavailable; see benches/DOCKER_TROUBLESHOOTING_LOCAL.md" >&2
    exit 1
}

mkdir -p "${OUTDIR_HOST}"

docker run --rm --privileged --security-opt seccomp=unconfined \
    -v "${WORKSPACE_ROOT}:/work" \
    -w /work \
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
MEMTIER_BIN=/tmp/memtier-src/memtier_benchmark \
DRAGONFLY_BIN=\"/tmp/dragonfly/\${dfly_name}\" \
THREADS='${THREADS}' \
CONNS='${CONNS}' \
REQUESTS='${REQUESTS}' \
PRELOAD_REQUESTS='${PRELOAD_REQUESTS}' \
PIPELINE='${PIPELINE}' \
SIZE_RANGE='${SIZE_RANGE}' \
PORT_BASE='${PORT_BASE}' \
PERF_FREQ='${PERF_FREQ}' \
TARGETS='${TARGETS}' \
WORKLOADS='${WORKLOADS}' \
SERVER_CPU_SET='${SERVER_CPU_SET}' \
CLIENT_CPU_SET='${CLIENT_CPU_SET}' \
GARNET_TSAVORITE_STRING_STORE_SHARDS='${GARNET_TSAVORITE_STRING_STORE_SHARDS}' \
GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES='${GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES}' \
OUTDIR='${OUTDIR_CONTAINER}' \
./benches/linux_perf_diff_profile.sh
"

echo "dockerized linux perf differential profiling completed"
echo "outdir=${OUTDIR_HOST}"
