# Redis Command Maturity Summary

- Status source: `docs/compatibility/redis-command-status.csv`
- Implementation source: `docs/compatibility/command-implementation-status.yaml`
- Maturity matrix: `docs/compatibility/redis-command-maturity.csv`

- Supported declared commands: `241`
- `FULL`: `227`
- `PARTIAL_MINIMAL`: `9`
- `DISABLED`: `5`
- Full implementation ratio over declared commands: `94.19%`

## Non-Full Commands

| Command | Maturity | Comment |
|---|---|---|
| `BGREWRITEAOF` | `PARTIAL_MINIMAL` | Compatibility response surface only; full AOF rewrite lifecycle semantics are not implemented. |
| `BGSAVE` | `PARTIAL_MINIMAL` | Compatibility response surface only; does not provide full Redis background-save lifecycle behavior. |
| `CLUSTER` | `PARTIAL_MINIMAL` | Core subcommands implemented (HELP INFO KEYSLOT MYID NODES SLOTS SHARDS RESET SAVECONFIG COUNTKEYSINSLOT GETKEYSINSLOT); cluster-management subcommands (SETSLOT ADDSLOTS etc.) disabled. |
| `FAILOVER` | `DISABLED` | - |
| `MIGRATE` | `DISABLED` | - |
| `MOVE` | `PARTIAL_MINIMAL` | single-db mode only; MOVE to non-zero DB is unsupported. |
| `READONLY` | `DISABLED` | - |
| `READWRITE` | `DISABLED` | - |
| `ROLE` | `PARTIAL_MINIMAL` | Returns fixed master-role shape; full role-state semantics are not implemented. |
| `SELECT` | `PARTIAL_MINIMAL` | single-db mode only; SELECT supports database 0. |
| `SHUTDOWN` | `DISABLED` | - |
| `SWAPDB` | `PARTIAL_MINIMAL` | single-db mode only; SWAPDB supports 0<->0 only. |
| `WAIT` | `PARTIAL_MINIMAL` | Minimal compatibility behavior; full replication and durability semantics are not implemented. |
| `WAITAOF` | `PARTIAL_MINIMAL` | Minimal compatibility behavior; full replication and durability semantics are not implemented. |
