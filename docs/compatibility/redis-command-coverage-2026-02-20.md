# Redis Command Coverage & Compatibility TODO (garnet-rs)

Date: 2026-02-20

## Scope

This document answers:

1. How much Redis command coverage exists today in `garnet-rs`?
2. Is command abstraction "add command once" today?
3. What TODO should be prioritized (Lua/EVAL family excluded)?
4. How to validate compatibility against Redis/Dragonfly quickly?

## Measured command coverage snapshot

Measured with:

- Redis: `redis:7.2-alpine` + `redis-cli --json COMMAND`
- Dragonfly: `docker.dragonflydb.io/dragonflydb/dragonfly:v1.36.0` + `redis-cli --json COMMAND`
- Garnet: local `garnet-rs` + `redis-cli --json COMMAND`

Results:

- Redis commands: `241`
- Dragonfly commands: `288`
- Garnet commands: `42`
- Redis commands missing in Garnet: `199`
- Coverage vs Redis baseline: `17.43%` (`42 / 241`)

Current Garnet command set (`42`):

`ASKING, COMMAND, DBSIZE, DECR, DEL, DISCARD, ECHO, EXEC, EXPIRE, GET, HDEL, HGET, HGETALL, HSET, INCR, INFO, LPOP, LPUSH, LRANGE, MULTI, PERSIST, PEXPIRE, PING, PSYNC, PTTL, REPLCONF, REPLICAOF, RPOP, RPUSH, SADD, SET, SISMEMBER, SMEMBERS, SREM, SYNC, TTL, UNWATCH, WATCH, ZADD, ZRANGE, ZREM, ZSCORE`

## Coverage interpretation

- Basic string/object structures are present (string/hash/list/set/zset core ops).
- Transaction basics are present (`MULTI/EXEC/DISCARD/WATCH/UNWATCH`).
- A large part of Redis standard surface is still missing.
- Most missing commands are not Lua-related; even excluding Lua, gap remains large.

High-impact missing (non-Lua) examples:

- String/key ops: `EXISTS, TYPE, MGET, MSET, GETEX, GETDEL, GETRANGE, SETRANGE, APPEND, INCRBY, DECRBY`
- Hash/list/set/zset expansion: `HKEYS, HLEN, HMGET, LLEN, LINDEX, LTRIM, SCARD, SRANDMEMBER, ZCARD, ZRANK, ZREVRANGE`
- Cursor iteration: `SCAN, HSCAN, SSCAN, ZSCAN`
- Server/config/auth: `AUTH, ACL, CLIENT, CONFIG, FLUSHDB/FLUSHALL, LATENCY`
- Cluster command surface: `CLUSTER *` (management/configuration verbs)

## Command abstraction review (today)

Current state is improved but still not fully "add command in one place".

A new command currently needs changes in multiple files/branches:

- `garnet-rs/crates/garnet-server/src/command_dispatch.rs`
  - `CommandId` enum
  - command-name dispatch matching
- `garnet-rs/crates/garnet-server/src/command_spec.rs`
  - centralized metadata for:
    - command id -> uppercase command name mapping
    - `COMMAND` response exposure
    - key-access pattern (`none` / `first-key` / `all-keys-from-arg1`)
    - owner-thread routability
    - replication mutating classification
- `garnet-rs/crates/garnet-server/src/request_lifecycle.rs`
  - `RequestProcessor::execute` match arm
  - command arity/handler implementation
  - command handlers are still centralized (partial extraction completed):
    - `request_lifecycle/server_commands.rs`
    - `request_lifecycle/string_commands.rs`
    - `request_lifecycle/hash_commands.rs`
    - `request_lifecycle/list_commands.rs`
    - `request_lifecycle/set_commands.rs`
    - `request_lifecycle/zset_commands.rs`
- `garnet-rs/crates/garnet-server/src/lib.rs`
  - transaction-loop handling (if special behavior)
  - now uses `command_spec` for:
    - cluster key-routing policy
    - owner-thread routing eligibility
    - transaction slot extraction policy
    - replication mutating checks
    - transaction control-class classification (`ASKING/MULTI/EXEC/DISCARD/WATCH/UNWATCH`)
    - transaction/control-command arity policy checks
  - connection helper responsibilities split into dedicated modules:
    - `connection_protocol.rs` (RESP/error framing + ASCII command helpers)
    - `connection_routing.rs` (owner-thread routing + cluster slot routing checks)
    - `connection_transaction.rs` (transaction queue/state lifecycle)
  - large inline unit-test module has been moved to `garnet-rs/crates/garnet-server/src/tests.rs` to keep production-path code review focused.
- `garnet-rs/crates/garnet-server/src/request_lifecycle.rs`
  - inline unit tests have been moved to `garnet-rs/crates/garnet-server/src/request_lifecycle/tests.rs` for faster production-code review.
  - error handling and storage error mapping have been split to `garnet-rs/crates/garnet-server/src/request_lifecycle/errors.rs`.
  - stored/object value codec and numeric parser utilities have been split to `garnet-rs/crates/garnet-server/src/request_lifecycle/value_codec.rs`.
  - RESP formatting and ASCII command option helpers have been split to `garnet-rs/crates/garnet-server/src/request_lifecycle/resp.rs`.
  - Tsavorite session callback implementations have been split to `garnet-rs/crates/garnet-server/src/request_lifecycle/session_functions.rs`.
  - object-store access and object payload persistence helpers have been split to `garnet-rs/crates/garnet-server/src/request_lifecycle/object_store.rs`.
  - migration export/import and slot-migration helpers have been split to `garnet-rs/crates/garnet-server/src/request_lifecycle/migration.rs`.

Conclusion:

- `CommandSpec` centralization is in place for key policies and mutating/routing classification.
- Handler dispatch and arity validation are still split between `command_dispatch.rs` + per-handler logic.
- `RequestProcessor::execute` now uses explicit `CommandId` arms (no wildcard), so newly added `CommandId` variants trigger compile-time exhaustiveness checks.
- The architecture is not yet a full registry-driven command engine.

## TODO (prioritized, non-Lua focus)

1. Introduce central command metadata registry (`CommandSpec`):
   - name/id, arity policy, key-extraction policy, routing flags, transaction behavior class.
2. Use registry to derive:
   - `COMMAND` output
   - cluster routability checks
   - owner-thread routability checks
   - transaction slot extraction policy.
   - Status: **partially complete** (`COMMAND`, key extraction/routing, replication mutating checks, and transaction-control/arity policy checks are now derived from `command_spec`).
3. Add compatibility-focused command batches:
   - Phase C1: `EXISTS/TYPE/MGET/MSET/INCRBY/DECRBY/GETEX/GETDEL`
   - Phase C2: `SCAN/HSCAN/SSCAN/ZSCAN`
   - Phase C3: object-structure parity expansion (`H*`, `L*`, `S*`, `Z*` missing subset).
4. Add `CLUSTER` command surface compatibility layer:
   - at least `CLUSTER INFO/NODES/SLOTS/KEYSLOT` read path first.
5. Keep Lua family (`EVAL`, `EVALSHA`, `FUNCTION`) intentionally deferred.

## Test-tooling status and improvement

Implemented in this change:

- `garnet-rs/crates/garnet-server/src/testkit.rs`
  - string command line -> RESP frame conversion
  - in-memory `RequestProcessor` execution
  - direct RESP assertion flow for tests
- `request_lifecycle` test now includes command-line harness usage:
  - `command_line_testkit_executes_against_in_memory_processor`

This provides the requested "mocked memory state + string-based command verification" path for unit tests.

## Interoperability validation scripts

Added:

- `garnet-rs/tests/interop/command_coverage_audit.sh`
- `garnet-rs/tests/interop/cluster_capability_matrix.sh`
- `garnet-rs/tests/interop/README.md`

These scripts make Redis/Dragonfly/Garnet compatibility checks reproducible and
explicitly classify unsupported cluster bootstrap cases.

## Replication interop snapshot

Replication compatibility is tracked via:

- `garnet-rs/tests/interop/replication_capability_matrix.sh`

Latest run snapshot (`garnet-rs/tests/interop/results/replication-capability-20260221-063054`):

- Redis <-> Redis: `PASS`
  - master->replica `SET/GET` verified
  - master switch performed (`REPLICAOF NO ONE` + reattach old master as replica)
  - switched-master->replica `SET/GET` verified
- Redis master -> Garnet replica: `PASS`
  - Garnet accepts `REPLICAOF` and replicates `SET/GET` from Redis master.
- Garnet master -> Redis replica: `PASS`
  - Redis replica reaches `master_link_status=up` and receives probe writes from Garnet master.
