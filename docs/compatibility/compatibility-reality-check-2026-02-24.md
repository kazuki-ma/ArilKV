# Compatibility Reality Check (2026-02-24)

## Purpose

Clarify the difference between:

1. command declaration parity (`COMMAND` surface),
2. behavioral parity (real Redis semantics), and
3. external-suite coverage currently executed in this repo.

This is a transparency snapshot, not a criticism of the current phased strategy.

## Current Snapshot

- `redis-command-status.csv` currently reports:
  - `SUPPORTED_DECLARED=241`
  - `NOT_IMPLEMENTED=0`
  - This means names are declared in `COMMAND`, not full semantics parity.
- Current external Redis check in CI/local workflow is a focused subset:
  - `unit/type/string` selected tests (`6`)
  - `unit/type/incr` selected tests (`4`)
  - `unit/keyspace` selected tests (`2`)
  - plus one `TYPE` probe
  - script: `garnet-rs/tests/interop/redis_runtest_external_subset.sh`

## Why `SUPPORTED_DECLARED=241/241` Is Not Enough

`build_command_status_matrix.sh` compares `COMMAND` output only:

- Redis commands list from `COMMAND`
- Garnet commands list from `COMMAND`
- category assignment by presence/absence only (`SUPPORTED_DECLARED`/`NOT_IMPLEMENTED`/`GARNET_EXTENSION`)

It does not validate semantic completeness for each command.

## Known Intentional Non-Full Surfaces (As Of 2026-02-24)

## A. Explicitly Disabled (validated parse/surface, execution disabled)

- `MIGRATE` -> `ERR MIGRATE is disabled in this server`
  - `garnet-rs/crates/garnet-server/src/request_lifecycle/server_commands.rs:311`
- `SHUTDOWN` -> `ERR SHUTDOWN is disabled in this server`
  - `garnet-rs/crates/garnet-server/src/request_lifecycle/server_commands.rs:1292`
- `FCALL`/`FCALL_RO` are implemented behind `GARNET_SCRIPTING_ENABLED` for functions loaded via `FUNCTION LOAD`.
  - `FCALL_RO` still rejects non-read-only registrations (`no-writes` enforcement).
  - `garnet-rs/crates/garnet-server/src/request_lifecycle/scripting.rs`

## A2. Feature-Gated Scripting Surface

- `EVAL`, `EVAL_RO`, `EVALSHA`, `EVALSHA_RO` are implemented but guarded by `GARNET_SCRIPTING_ENABLED` (default off).
- when disabled, these commands return `ERR scripting is disabled in this server`.
- when enabled, execution uses `mlua` (Lua 5.1 vendored) with `KEYS`/`ARGV` and `redis.call`/`redis.pcall`.
- `EVALSHA*` returns `NOSCRIPT` when SHA is missing.
- mutating `EVALSHA` replication is normalized to `EVAL` payloads to avoid replica-side `NOSCRIPT` on script-cache divergence.
- runtime hardening controls are env-driven:
  - `GARNET_SCRIPTING_MAX_SCRIPT_BYTES`
  - `GARNET_SCRIPTING_CACHE_MAX_ENTRIES`
  - `GARNET_SCRIPTING_MAX_MEMORY_BYTES`
  - `GARNET_SCRIPTING_MAX_EXECUTION_MILLIS`

## B. Cluster-Support-Disabled Paths

- `READONLY`, `READWRITE`, unsupported `CLUSTER` subcommands, `FAILOVER`
  - return cluster-support-disabled errors
  - `garnet-rs/crates/garnet-server/src/request_lifecycle/server_commands.rs:479`
  - `garnet-rs/crates/garnet-server/src/request_lifecycle/server_commands.rs:1108`
  - `garnet-rs/crates/garnet-server/src/request_lifecycle/server_commands.rs:1122`

## C. Minimal Compatibility Surfaces (intentionally narrow behavior)

- `PUBSUB` family: minimal ack/introspection shape implementation
  - `PUBLISH`/`SPUBLISH` integer response path and minimal subscribe/unsubscribe acks
  - `garnet-rs/crates/garnet-server/src/request_lifecycle/server_commands.rs:1205`
  - `garnet-rs/crates/garnet-server/src/request_lifecycle/tests.rs:5824`
- `CLIENT` family: minimal `CLIENT LIST` surface used for dependent tests
  - `garnet-rs/crates/garnet-server/src/request_lifecycle/server_commands.rs:341`
- admin telemetry commands (`LATENCY`/`MODULE`/`SLOWLOG`) introduced with minimal practical semantics
  - see tracker note for 11.83 in `TODO_AND_STATUS.md`

## D. Scripting Admin Surface Is Partial

- `SCRIPT` supports `FLUSH`, `LOAD`, `EXISTS`, and minimal `HELP`/`DEBUG`/`KILL` behavior (with cache-limit eviction and INFO observability fields for scripting runtime/cache counters).
- `FUNCTION` supports `HELP`/`LIST`/`DELETE`/`STATS`/`FLUSH` and `LOAD [REPLACE]` with `redis.register_function`.
- `FCALL` and `FCALL_RO` are available behind `GARNET_SCRIPTING_ENABLED` (`FCALL_RO` enforces `no-writes`).
- remaining gaps: deeper Redis function-library semantics (RESP3 shape parity, persistence/replication edge-cases, and full admin behavior).
- implementation path:
  - `garnet-rs/crates/garnet-server/src/request_lifecycle/scripting.rs`

## What External Checks Currently Miss

- Current subset script does not exercise:
  - Lua execution semantics,
  - pub/sub delivery semantics,
  - full cluster admin semantics,
  - full ACL/module/latency/slowlog behavior,
  - most command option edge-cases.

This is expected because the subset script is intentionally narrow and fast.

## Recommended Next Visibility Upgrade

Add a behavior-level maturity matrix next to `redis-command-status.csv`, e.g.:

- `FULL`
- `PARTIAL_MINIMAL`
- `DISABLED`
- `UNKNOWN_UNAUDITED`

Then enforce policy in interop workflow so declaration parity and behavior parity are visible separately.
