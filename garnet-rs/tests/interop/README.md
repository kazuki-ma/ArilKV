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

- `cluster_wait_failover_gap_probe.sh`
  - Runs a focused gap probe for high-cost compatibility commands:
    - Redis reference behavior for `WAIT` / `WAITAOF` in primary+replica mode
    - Redis 3-node cluster bootstrap reference behavior
    - Garnet standalone observations for `WAIT` / `WAITAOF` / `MIGRATE` / `FAILOVER`
    - Garnet multi-port cluster observations for `CLUSTER*`, `READONLY/READWRITE`, and `MOVED`
  - Uses companion compose file:
    - `docker-compose.cluster-wait-failover.yml`
  - Writes a CSV summary and per-case logs under
    `garnet-rs/tests/interop/results/...`.

- `redis_runtest_external_subset.sh`
  - Runs Redis official `runtest` in external-server mode against local Garnet:
    - default DB mode is `--singledb` (`RUNTEXT_SINGLEDB=1`)
    - set `RUNTEXT_SINGLEDB=0` to exercise MultiDB-capable Garnet without the singledb skip profile
    - default mode is **full** (no `--single` / `--only` / `--tags` filters)
    - full mode applies `--timeout 120` by default (override with `RUNTEXT_TIMEOUT_SECONDS`)
    - full mode keeps `unit/querybuf`, `unit/scripting`, `unit/other`, and
      `integration/redis-cli` in-band by default; the old exact-skip tail is now
      only available as an opt-in debugging escape hatch
    - optional isolated reruns remain available for focused diagnosis:
      `RUNTEXT_RUN_QUERYBUF_ISOLATED=1`,
      `RUNTEXT_RUN_SCRIPTING_ISOLATED=1`,
      `RUNTEXT_RUN_OTHER_ISOLATED=1`,
      `RUNTEXT_RUN_SCRIPTING_NOREPLICAS_TEST_ISOLATED=1`,
      `RUNTEXT_RUN_OTHER_PIPELINE_STRESSER_ISOLATED=1`,
      `RUNTEXT_RUN_REDIS_CLI_CONNECTING_AS_REPLICA_TEST_ISOLATED=1`
    - isolated `unit/other` still uses a longer per-unit timeout when that
      debugging path is enabled
      (`RUNTEXT_ISOLATED_OTHER_TIMEOUT_SECONDS=180`) because the inline
      `PIPELINING stresser` is slower under Tcl external mode than the Rust
      exact regression
    - isolated `unit/querybuf` runs after a server restart, and post-runtest probes also
      auto-restart if reset cannot recover a healthy `PING`
    - developer shortcut: `RUNTEXT_RUN_ONLY_ISOLATED_UNIT=<unit/path>` runs just one isolated
      unit path through the same restart/probe wrapper
    - set `RUNTEXT_ENABLE_LARGE_MEMORY=1` to pass upstream `--large-memory`
      into the same harness, with longer default timeouts and a higher
      Tsavorite page budget
    - optional compatibility-smoke mode: `REDIS_RUNTEXT_MODE=subset`
  - Validates runtest stdout counts in all modes:
    - parses `[ok]` / `[err]` / `[ignore]` counts from log
    - extracts failed tests to `failed-tests.txt` when present
    - marks run `FAIL` if exit is non-zero or parsed error count is non-zero
  - Includes a direct `redis-cli TYPE` probe (`string/hash/none`) in the same run.
  - Writes a CSV summary and per-case logs under
    `garnet-rs/tests/interop/results/...`.

- `redis_runtest_external_large_memory.sh`
  - Dedicated manual/CI lane for upstream `large-memory` cases:
    - enables `RUNTEXT_ENABLE_LARGE_MEMORY=1`
    - defaults to `RUNTEXT_EXTRA_ARGS='--tags large-memory'`
    - increases timeouts (`900s` per test, `14400s` wall) and
      `GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES=524288`
    - callers can still override `RUNTEXT_EXTRA_ARGS` for a narrower smoke run
  - Keeps the default `redis_runtest_external_subset.sh` profile lean while
    still providing a checked-in path for the 4GB-class upstream scenarios.

- `build_command_status_matrix.sh`
  - Generates a full Redis-command status matrix for Garnet (all commands):
    - `SUPPORTED_DECLARED`
    - `NOT_IMPLEMENTED`
    - `GARNET_EXTENSION`
  - Writes:
    - `docs/compatibility/redis-command-status.csv`
    - `docs/compatibility/redis-command-status-summary.md`

- `build_command_maturity_matrix.sh`
  - Joins command declaration matrix with behavior maturity status from
    `docs/compatibility/command-implementation-status.yaml`.
  - Maturity enum:
    - `FULL`
    - `PARTIAL_MINIMAL`
    - `DISABLED`
  - Writes:
    - `docs/compatibility/redis-command-maturity.csv`
    - `docs/compatibility/redis-command-maturity-summary.md`

- `build_compatibility_report.sh`
  - Full auto-generated compatibility report workflow:
    1. runs `build_command_status_matrix.sh`
    2. runs `build_command_maturity_matrix.sh`
    3. runs `redis_runtest_external_subset.sh` (default `full` probe mode)
    4. merges all outputs into a single report
  - Writes:
    - `docs/compatibility/compatibility-report.md`

## Usage

```bash
cd garnet-rs/tests/interop
chmod +x command_coverage_audit.sh cluster_capability_matrix.sh
chmod +x replication_capability_matrix.sh
chmod +x cluster_wait_failover_gap_probe.sh
chmod +x redis_runtest_external_subset.sh
chmod +x redis_runtest_external_large_memory.sh
chmod +x build_command_status_matrix.sh
chmod +x build_command_maturity_matrix.sh
chmod +x build_compatibility_report.sh

./command_coverage_audit.sh
./cluster_capability_matrix.sh
./replication_capability_matrix.sh
./cluster_wait_failover_gap_probe.sh
./redis_runtest_external_subset.sh
./redis_runtest_external_large_memory.sh
./build_command_status_matrix.sh
./build_command_maturity_matrix.sh
./build_compatibility_report.sh

# optional: quick smoke mode (instead of default full probe)
COMPAT_PROBE_MODE=subset ./build_compatibility_report.sh

# dedicated large-memory lane
./redis_runtest_external_large_memory.sh

# safe plumbing smoke for the large-memory lane without allocating 4GB values
RUNTEXT_EXTRA_ARGS='--single unit/type/string --only "SET and GET an item"' \
./redis_runtest_external_large_memory.sh
```

## Required Flow For Command Edits

When Redis command behavior is added/changed, run these in order:

```bash
cd garnet-rs
cargo test -p garnet-server -- --nocapture

cd tests/interop
REDIS_REPO_ROOT=/Users/kazuki-matsuda/dev/src/github.com/redis/redis \
./build_compatibility_report.sh

# recommended for cleaner full-baseline compatibility signal
# (interop runner enables scripting by default; override with GARNET_SCRIPTING_ENABLED=0 if needed)
REDIS_REPO_ROOT=/Users/kazuki-matsuda/dev/src/github.com/redis/redis \
GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES=4096 \
./build_compatibility_report.sh
```

If command status changed, include these generated files in the commit:

- `docs/compatibility/redis-command-status.csv`
- `docs/compatibility/redis-command-status-summary.md`
- `docs/compatibility/redis-command-maturity.csv`
- `docs/compatibility/redis-command-maturity-summary.md`
- `docs/compatibility/compatibility-report.md`

Recommended add-on checks:

```bash
./command_coverage_audit.sh
./replication_capability_matrix.sh
./cluster_wait_failover_gap_probe.sh
```

## Patterns To Re-check

- `redis_runtest_external_subset.sh` is a focused subset, not full Redis compatibility.
  - default mode is full external runtest; subset mode is optional for faster smoke checks.
  - full mode defaults `RUNTEXT_TIMEOUT_SECONDS=120` to prevent indefinite single-test stalls.
  - isolated `unit/other` has its own longer timeout budget because the Tcl
    pipelining stress test needs more than the generic 120s in some environments.
  - keep feature-level unit tests in `garnet-server` as the primary correctness gate.
- `build_command_status_matrix.sh` updates canonical status files under `docs/compatibility/`.
  - run it whenever `CommandId`/`COMMAND` surface changes.
- `build_command_maturity_matrix.sh` requires one YAML entry per declared command.
  - if a command is intentionally minimal/disabled, encode it in
    `docs/compatibility/command-implementation-status.yaml`.
- `build_compatibility_report.sh` is the canonical report generator.
  - it merges declaration status, maturity comments, and external probe results.
- Always verify test count lines in command output, not only exit code.
  - the expected test-case counts are part of regression safety.

## Current interpretation

- `garnet-rs` currently exposes a focused command subset and does not expose
  Redis `CLUSTER` command management verbs.
- Redis cluster bootstrap is expected to succeed.
- Dragonfly currently supports:
  - `cluster_mode=emulated` for a Redis-cluster-compatible single-node surface
  - but not redis-cli multi-node bootstrap in this script's flow
