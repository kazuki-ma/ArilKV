# Interoperability Test Scripts

This directory contains reproducible shell scripts for command coverage and
cluster compatibility checks across `garnet-rs`, Redis, and Dragonfly.

## Scripts

- `command_coverage_audit.sh`
  - Compares `COMMAND` surfaces for:
    - Redis (`redis:7.2-alpine`)
    - Dragonfly (`docker.dragonflydb.io/dragonflydb/dragonfly:v1.36.0`)
    - local `garnet-rs` server
  - Produces sorted command lists and diff files under
    `garnet-rs/tests/interop/results/...`.

- `cluster_capability_matrix.sh`
  - Runs a capability matrix:
    - Redis 3-node cluster bootstrap via `redis-cli --cluster create`
    - Dragonfly `cluster_mode=emulated` single-node cluster surface
    - Dragonfly `cluster_mode=yes` multi-node bootstrap attempt
    - `garnet-rs` multi-port `MOVED` routing surface
  - Writes a CSV summary and per-case logs under
    `garnet-rs/tests/interop/results/...`.

## Usage

```bash
cd garnet-rs/tests/interop
chmod +x command_coverage_audit.sh cluster_capability_matrix.sh

./command_coverage_audit.sh
./cluster_capability_matrix.sh
```

## Current interpretation

- `garnet-rs` currently exposes a focused command subset and does not expose
  Redis `CLUSTER` command management verbs.
- Redis cluster bootstrap is expected to succeed.
- Dragonfly currently supports:
  - `cluster_mode=emulated` for a Redis-cluster-compatible single-node surface
  - but not redis-cli multi-node bootstrap in this script's flow
