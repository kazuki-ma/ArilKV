# AGENT.md (request_lifecycle)

## First Step

Read `garnet-rs/crates/garnet-server/src/request_lifecycle/README.md` before editing files in this directory.

## Scope

This file applies to command implementation files under:

- `garnet-rs/crates/garnet-server/src/request_lifecycle/`
- related command wiring in:
  - `garnet-rs/crates/garnet-server/src/command_spec.rs`
  - `garnet-rs/crates/garnet-server/src/command_dispatch.rs`

## Required Checks For Command Changes

When you add or modify Redis command behavior, run the following before commit.

1. Core server tests:

```bash
cd garnet-rs
cargo test -p garnet-server -- --nocapture
```

2. Redis official external compatibility subset:

```bash
cd garnet-rs/tests/interop
REDIS_REPO_ROOT=/Users/kazuki-matsuda/dev/src/github.com/redis/redis \
./redis_runtest_external_subset.sh
```

3. Command status matrix refresh:

```bash
cd garnet-rs/tests/interop
./build_command_status_matrix.sh
```

## Required Artifacts To Commit

If command surface or status changed, include updated files:

- `docs/compatibility/redis-command-status.csv`
- `docs/compatibility/redis-command-status-summary.md`

If compatibility script output changed materially, include the latest result dir path in the iteration notes in `TODO_AND_STATUS.md`.

## Useful Optional Checks

- Command-surface diff across Redis/Dragonfly/Garnet:

```bash
cd garnet-rs/tests/interop
./command_coverage_audit.sh
```

- Replication interop matrix:

```bash
cd garnet-rs/tests/interop
./replication_capability_matrix.sh
```

## Command/Test Template

When adding a new command, start from:

- `garnet-rs/crates/garnet-server/src/request_lifecycle/COMMAND_AND_TEST_TEMPLATE.md`
