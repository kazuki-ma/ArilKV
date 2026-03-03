# Redis Command Maturity Summary

- Status source: `docs/compatibility/redis-command-status.csv`
- Implementation source: `docs/compatibility/command-implementation-status.yaml`
- Maturity matrix: `docs/compatibility/redis-command-maturity.csv`

- Supported declared commands: `241`
- `FULL`: `204`
- `PARTIAL_MINIMAL`: `32`
- `DISABLED`: `5`
- Full implementation ratio over declared commands: `84.65%`

## Non-Full Commands

| Command | Maturity | Comment |
|---|---|---|
| `ACL` | `PARTIAL_MINIMAL` | Minimal ACL subcommand surface only. |
| `BGREWRITEAOF` | `PARTIAL_MINIMAL` | Compatibility response surface only; full AOF rewrite lifecycle semantics are not implemented. |
| `BGSAVE` | `PARTIAL_MINIMAL` | Compatibility response surface only; does not provide full Redis background-save lifecycle behavior. |
| `CLIENT` | `PARTIAL_MINIMAL` | Narrow subset implementation including minimal CLIENT LIST behavior for dependent tests. |
| `CLUSTER` | `PARTIAL_MINIMAL` | Selected subcommands are implemented; unsupported subcommands are cluster-support-disabled. |
| `EVAL` | `PARTIAL_MINIMAL` | Implemented behind GARNET_SCRIPTING_ENABLED; executes Lua via mlua (Lua 5.1 vendored) with KEYS/ARGV and redis.call/pcall bridge. |
| `EVAL_RO` | `PARTIAL_MINIMAL` | Implemented behind GARNET_SCRIPTING_ENABLED; write commands are rejected from script context. |
| `EVALSHA` | `PARTIAL_MINIMAL` | Implemented behind GARNET_SCRIPTING_ENABLED; returns NOSCRIPT on missing SHA and executes cached script body. Replication normalizes mutating EVALSHA to EVAL payloads. |
| `EVALSHA_RO` | `PARTIAL_MINIMAL` | Implemented behind GARNET_SCRIPTING_ENABLED; same as EVALSHA with write-command rejection inside redis.call/pcall. |
| `FAILOVER` | `DISABLED` | - |
| `FCALL` | `PARTIAL_MINIMAL` | Implemented behind GARNET_SCRIPTING_ENABLED for functions registered via FUNCTION LOAD. |
| `FCALL_RO` | `PARTIAL_MINIMAL` | Implemented behind GARNET_SCRIPTING_ENABLED for functions registered via FUNCTION LOAD with no-writes flag. |
| `FUNCTION` | `PARTIAL_MINIMAL` | Supports HELP/LIST/DELETE/STATS/KILL/DUMP/RESTORE/FLUSH [ASYNC|SYNC] plus LOAD [REPLACE] with redis.register_function parsing; full Redis FUNCTION management semantics are not implemented. |
| `LATENCY` | `PARTIAL_MINIMAL` | Minimal admin and introspection compatibility surface. |
| `MIGRATE` | `DISABLED` | - |
| `MODULE` | `PARTIAL_MINIMAL` | Minimal admin and introspection compatibility surface. |
| `MONITOR` | `PARTIAL_MINIMAL` | Compatibility ACK response surface only. |
| `MOVE` | `PARTIAL_MINIMAL` | single-db mode only; MOVE to non-zero DB is unsupported. |
| `PSUBSCRIBE` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `PUBLISH` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `PUBSUB` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `PUNSUBSCRIBE` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `READONLY` | `DISABLED` | - |
| `READWRITE` | `DISABLED` | - |
| `ROLE` | `PARTIAL_MINIMAL` | Returns fixed master-role shape; full role-state semantics are not implemented. |
| `SCRIPT` | `PARTIAL_MINIMAL` | Supports FLUSH/LOAD/EXISTS plus HELP/DEBUG/KILL minimal behavior (runtime hardening env knobs included); but full Redis script-admin semantics are not implemented. |
| `SELECT` | `PARTIAL_MINIMAL` | single-db mode only; SELECT supports database 0. |
| `SHUTDOWN` | `DISABLED` | - |
| `SLOWLOG` | `PARTIAL_MINIMAL` | Minimal admin and introspection compatibility surface. |
| `SPUBLISH` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `SSUBSCRIBE` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `SUBSCRIBE` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `SUNSUBSCRIBE` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `SWAPDB` | `PARTIAL_MINIMAL` | single-db mode only; SWAPDB supports 0<->0 only. |
| `UNSUBSCRIBE` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `WAIT` | `PARTIAL_MINIMAL` | Minimal compatibility behavior; full replication and durability semantics are not implemented. |
| `WAITAOF` | `PARTIAL_MINIMAL` | Minimal compatibility behavior; full replication and durability semantics are not implemented. |
