# Redis Command Maturity Summary

- Generated at: 2026-02-24T05:12:30Z
- Status source: `docs/compatibility/redis-command-status.csv`
- Implementation source: `docs/compatibility/command-implementation-status.yaml`
- Maturity matrix: `docs/compatibility/redis-command-maturity.csv`

- Supported declared commands: `241`
- `FULL`: `207`
- `PARTIAL_MINIMAL`: `23`
- `DISABLED`: `11`
- Full implementation ratio over declared commands: `85.89%`

## Non-Full Commands

| Command | Maturity | Comment |
|---|---|---|
| `ACL` | `PARTIAL_MINIMAL` | Minimal ACL subcommand surface only. |
| `BGREWRITEAOF` | `PARTIAL_MINIMAL` | Compatibility response surface only; full AOF rewrite lifecycle semantics are not implemented. |
| `BGSAVE` | `PARTIAL_MINIMAL` | Compatibility response surface only; does not provide full Redis background-save lifecycle behavior. |
| `CLIENT` | `PARTIAL_MINIMAL` | Narrow subset implementation including minimal CLIENT LIST behavior for dependent tests. |
| `CLUSTER` | `PARTIAL_MINIMAL` | Selected subcommands are implemented; unsupported subcommands are cluster-support-disabled. |
| `EVAL` | `DISABLED` | - |
| `EVALSHA` | `DISABLED` | - |
| `EVALSHA_RO` | `DISABLED` | - |
| `EVAL_RO` | `DISABLED` | - |
| `FAILOVER` | `DISABLED` | - |
| `FCALL` | `DISABLED` | - |
| `FCALL_RO` | `DISABLED` | - |
| `FUNCTION` | `PARTIAL_MINIMAL` | Currently limited to FLUSH-compatible behavior. |
| `LATENCY` | `PARTIAL_MINIMAL` | Minimal admin and introspection compatibility surface. |
| `MIGRATE` | `DISABLED` | - |
| `MODULE` | `PARTIAL_MINIMAL` | Minimal admin and introspection compatibility surface. |
| `MONITOR` | `PARTIAL_MINIMAL` | Compatibility ACK response surface only. |
| `PSUBSCRIBE` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `PUBLISH` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `PUBSUB` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `PUNSUBSCRIBE` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `READONLY` | `DISABLED` | - |
| `READWRITE` | `DISABLED` | - |
| `ROLE` | `PARTIAL_MINIMAL` | Returns fixed master-role shape; full role-state semantics are not implemented. |
| `SCRIPT` | `PARTIAL_MINIMAL` | Currently limited to FLUSH-compatible behavior. |
| `SHUTDOWN` | `DISABLED` | - |
| `SLOWLOG` | `PARTIAL_MINIMAL` | Minimal admin and introspection compatibility surface. |
| `SPUBLISH` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `SSUBSCRIBE` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `SUBSCRIBE` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `SUNSUBSCRIBE` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `UNSUBSCRIBE` | `PARTIAL_MINIMAL` | Minimal deterministic pubsub compatibility shapes; not full runtime parity. |
| `WAIT` | `PARTIAL_MINIMAL` | Minimal compatibility behavior; full replication and durability semantics are not implemented. |
| `WAITAOF` | `PARTIAL_MINIMAL` | Minimal compatibility behavior; full replication and durability semantics are not implemented. |
