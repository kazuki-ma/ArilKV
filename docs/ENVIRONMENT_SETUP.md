# Local Environment Setup

These commands set up a macOS developer machine for the current repository-root
Rust workspace.

## Required tools

```bash
brew install rustup redis jq memtier_benchmark
```

Docker is required for Docker-backed interop and benchmark scripts. Install
Docker Desktop, then verify:

```bash
docker info
```

The comparison harness builds Valkey inside Docker. Installing Homebrew
`valkey` locally is optional and conflicts with Homebrew `redis` because both
formulae install `redis-*` binaries.

## Rust toolchains

```bash
rustup toolchain install 1.95.0 --component rustfmt --component clippy
rustup toolchain install nightly-2026-02-24 --component rustfmt
```

The `Makefile` invokes Rust through `rustup run`, so it does not depend on
whether Homebrew's `cargo` or rustup shims appear first in `PATH`.

## Redis upstream test repo

External compatibility scripts expect the Redis repository here by default:

```bash
mkdir -p /Users/kazuki-matsuda/dev/src/github.com/redis
cd /Users/kazuki-matsuda/dev/src/github.com/redis
git clone https://github.com/redis/redis.git
```

Override with `REDIS_REPO_ROOT=/path/to/redis` if needed.

## Core checks

Run from the repository root:

```bash
make fmt-check
make check
make test-server
make test
```

## Interop smoke checks

```bash
tests/interop/build_command_status_matrix.sh
tests/interop/build_command_maturity_matrix.sh
REDIS_RUNTEXT_MODE=subset tests/interop/redis_runtest_external_subset.sh
COMPAT_PROBE_MODE=subset tests/interop/build_compatibility_report.sh
```

## Performance comparison setup

```bash
benches/setup_perf_compare_env.sh
```

That script installs/checks the local toolchain, builds `garnet-server` in
release mode, verifies Docker availability, and runs a small Docker-backed smoke
comparison for Garnet, Dragonfly, and Valkey.

Run the full Linux-container comparison with:

```bash
THREADS=8 CONNS=16 REQUESTS=50000 PRELOAD_REQUESTS=50000 PIPELINE=1 \
  TARGETS="garnet dragonfly valkey" \
  benches/docker_linux_perf_diff_profile.sh
```

Garnet uses inline owner execution by default. This is the preferred
single-process performance path and avoids the older listener-to-owner
cross-thread handoff. Set `GARNET_OWNER_EXECUTION_INLINE=0` only when
intentionally measuring that legacy pooled-owner path.

For a quick smoke after a fresh checkout:

```bash
CAPTURE_PERF=0 THREADS=1 CONNS=1 REQUESTS=50 PRELOAD_REQUESTS=50 \
  TARGETS="garnet dragonfly valkey" \
  benches/docker_linux_perf_diff_profile.sh
```

The default pinned comparison versions are Dragonfly `v1.38.1` and Valkey
`9.0.4`. Override with `DRAGONFLY_VERSION=...` and `VALKEY_VERSION=...`.
