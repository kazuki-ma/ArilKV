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

- `replication_capability_matrix.sh`
  - Runs replication capability checks:
    - Redis <-> Redis baseline with master switching:
      - phase1: master->replica `SET`/`GET`
      - switch: promote replica to master, reattach old master as replica
      - phase2: switched-master->replica `SET`/`GET`
    - Redis master -> Garnet replica attempt
    - Garnet master -> Redis replica attempt
  - Writes a CSV summary and per-case logs under
    `garnet-rs/tests/interop/results/...`.

- `redis_runtest_external_subset.sh`
  - Runs Redis official `runtest` in external-server mode against local Garnet:
    - `--host/--port --singledb --force-resp3`
    - targeted subset for current implemented commands (`MGET/MSET/INCRBY/DECRBY/EXISTS`)
  - Includes a direct `redis-cli TYPE` probe (`string/hash/none`) in the same run.
  - Writes a CSV summary and per-case logs under
    `garnet-rs/tests/interop/results/...`.

- `build_command_status_matrix.sh`
  - Generates a full Redis-command status matrix for Garnet (all commands):
    - `SUPPORTED_DECLARED`
    - `NOT_IMPLEMENTED`
    - `GARNET_EXTENSION`
  - Writes:
    - `docs/compatibility/redis-command-status.csv`
    - `docs/compatibility/redis-command-status-summary.md`

## Usage

```bash
cd garnet-rs/tests/interop
chmod +x command_coverage_audit.sh cluster_capability_matrix.sh
chmod +x replication_capability_matrix.sh
chmod +x redis_runtest_external_subset.sh
chmod +x build_command_status_matrix.sh

./command_coverage_audit.sh
./cluster_capability_matrix.sh
./replication_capability_matrix.sh
./redis_runtest_external_subset.sh
./build_command_status_matrix.sh
```

## Required Flow For Command Edits

When Redis command behavior is added/changed, run these in order:

```bash
cd garnet-rs
cargo test -p garnet-server -- --nocapture

cd tests/interop
REDIS_REPO_ROOT=/Users/kazuki-matsuda/dev/src/github.com/redis/redis \
./redis_runtest_external_subset.sh
./build_command_status_matrix.sh
```

If command status changed, include these generated files in the commit:

- `docs/compatibility/redis-command-status.csv`
- `docs/compatibility/redis-command-status-summary.md`

Recommended add-on checks:

```bash
./command_coverage_audit.sh
./replication_capability_matrix.sh
```

## Patterns To Re-check

- `redis_runtest_external_subset.sh` is a focused subset, not full Redis compatibility.
  - keep feature-level unit tests in `garnet-server` as the primary correctness gate.
- `build_command_status_matrix.sh` updates canonical status files under `docs/compatibility/`.
  - run it whenever `CommandId`/`COMMAND` surface changes.
- Always verify test count lines in command output, not only exit code.
  - the expected test-case counts are part of regression safety.

## Current interpretation

- `garnet-rs` currently exposes a focused command subset and does not expose
  Redis `CLUSTER` command management verbs.
- Redis cluster bootstrap is expected to succeed.
- Dragonfly currently supports:
  - `cluster_mode=emulated` for a Redis-cluster-compatible single-node surface
  - but not redis-cli multi-node bootstrap in this script's flow
