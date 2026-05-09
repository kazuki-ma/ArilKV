#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

RUST_TOOLCHAIN="${RUST_TOOLCHAIN:-1.95.0}"
RUSTFMT_NIGHTLY="${RUSTFMT_NIGHTLY:-nightly-2026-02-24}"
RUN_SMOKE="${RUN_SMOKE:-1}"
INSTALL_BREW_DEPS="${INSTALL_BREW_DEPS:-1}"
INSTALL_LOCAL_VALKEY="${INSTALL_LOCAL_VALKEY:-0}"
SMOKE_OUTDIR="${SMOKE_OUTDIR:-${REPO_ROOT}/benches/results/perf-compare-smoke-$(date +%Y%m%d-%H%M%S)}"

require_cmd() {
    local command="$1"
    if ! command -v "${command}" >/dev/null 2>&1; then
        echo "missing required command: ${command}" >&2
        return 1
    fi
}

install_brew_package_if_missing() {
    local package="$1"
    local command="$2"

    if command -v "${command}" >/dev/null 2>&1; then
        return 0
    fi
    if [[ "${INSTALL_BREW_DEPS}" != "1" ]]; then
        echo "missing ${command}; install Homebrew package ${package}" >&2
        return 1
    fi
    require_cmd brew
    brew install "${package}"
}

case "$(uname -s)" in
    Darwin)
        if [[ "${INSTALL_BREW_DEPS}" == "1" ]]; then
            require_cmd brew
            install_brew_package_if_missing rustup rustup
            install_brew_package_if_missing redis redis-cli
            install_brew_package_if_missing jq jq
            install_brew_package_if_missing memtier_benchmark memtier_benchmark
            if [[ "${INSTALL_LOCAL_VALKEY}" == "1" ]]; then
                install_brew_package_if_missing valkey valkey-server
            fi
        fi
        ;;
    Linux)
        echo "Linux detected; install memtier_benchmark, valkey-server, docker, jq, and rustup with your distro package manager if missing."
        ;;
    *)
        echo "unsupported OS: $(uname -s)" >&2
        exit 1
        ;;
esac

require_cmd rustup
require_cmd docker
require_cmd jq
require_cmd redis-cli

rustup toolchain install "${RUST_TOOLCHAIN}" --component rustfmt --component clippy
rustup toolchain install "${RUSTFMT_NIGHTLY}" --component rustfmt

docker info >/dev/null 2>&1 || {
    echo "docker daemon is unavailable; start Docker Desktop or dockerd first" >&2
    exit 1
}

(
    cd "${REPO_ROOT}"
    make fmt-check
    make check
    rustup run "${RUST_TOOLCHAIN}" cargo build -p garnet-server --release
)

if [[ "${RUN_SMOKE}" == "1" ]]; then
    (
        cd "${REPO_ROOT}"
        CAPTURE_PERF=0 \
        THREADS=1 \
        CONNS=1 \
        REQUESTS=50 \
        PRELOAD_REQUESTS=50 \
        PIPELINE=1 \
        SIZE_RANGE=1-32 \
        TARGETS="${TARGETS:-garnet dragonfly valkey}" \
        WORKLOADS="${WORKLOADS:-set get}" \
        OUTDIR_HOST="${SMOKE_OUTDIR}" \
            ./benches/docker_linux_perf_diff_profile.sh
    )
fi

echo "performance comparison environment is ready"
echo "smoke_outdir=${SMOKE_OUTDIR}"
