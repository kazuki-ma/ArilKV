# Redis Command Maturity Summary

- Status source: `docs/compatibility/redis-command-status.csv`
- Implementation source: `docs/compatibility/command-implementation-status.yaml`
- Maturity matrix: `docs/compatibility/redis-command-maturity.csv`

- Supported declared commands: `259`
- `FULL`: `227`
- `PARTIAL_MINIMAL`: `18`
- `DISABLED`: `14`
- Full implementation ratio over declared commands: `87.64%`

## Non-Full Commands

| Command | Maturity | Comment |
|---|---|---|
| `ACL` | `PARTIAL_MINIMAL` | Minimal ACL subcommand surface only. |
| `BGREWRITEAOF` | `PARTIAL_MINIMAL` | Compatibility response surface only; full AOF rewrite lifecycle semantics are not implemented. |
| `BGSAVE` | `PARTIAL_MINIMAL` | Compatibility response surface only; does not provide full Redis background-save lifecycle behavior. |
| `CLUSTER` | `PARTIAL_MINIMAL` | Selected subcommands are implemented; unsupported subcommands are cluster-support-disabled. |
| `DELEX` | `DISABLED` | Declared extension surface; runtime behavior is not implemented. |
| `DIGEST` | `PARTIAL_MINIMAL` | Compatibility digest surface is limited to DEBUG DIGEST-VALUE. |
| `FAILOVER` | `DISABLED` | - |
| `HEXPIRE` | `PARTIAL_MINIMAL` | Hash-field expiration subset implemented; full command-family parity is not complete. |
| `HEXPIREAT` | `DISABLED` | Declared for compatibility matrix only. |
| `HEXPIRETIME` | `DISABLED` | Declared for compatibility matrix only. |
| `HGETDEL` | `PARTIAL_MINIMAL` | Hash-field expiration extension surface implemented with partial parity. |
| `HGETEX` | `PARTIAL_MINIMAL` | Hash-field expiration extension surface implemented with partial parity. |
| `HPERSIST` | `DISABLED` | Declared for compatibility matrix only. |
| `HPEXPIRE` | `PARTIAL_MINIMAL` | Hash-field expiration subset implemented; full command-family parity is not complete. |
| `HPEXPIREAT` | `PARTIAL_MINIMAL` | Hash-field expiration subset implemented; full command-family parity is not complete. |
| `HPEXPIRETIME` | `DISABLED` | Declared for compatibility matrix only. |
| `HPTTL` | `DISABLED` | Declared for compatibility matrix only. |
| `HSETEX` | `PARTIAL_MINIMAL` | Hash-field expiration extension surface implemented with partial parity. |
| `HTTL` | `DISABLED` | Declared for compatibility matrix only. |
| `MIGRATE` | `DISABLED` | - |
| `MODULE` | `PARTIAL_MINIMAL` | Minimal admin and introspection compatibility surface. |
| `MOVE` | `PARTIAL_MINIMAL` | single-db mode only; MOVE to non-zero DB is unsupported. |
| `MSETEX` | `DISABLED` | Declared extension surface; runtime behavior is not implemented. |
| `READONLY` | `DISABLED` | - |
| `READWRITE` | `DISABLED` | - |
| `ROLE` | `PARTIAL_MINIMAL` | Returns fixed master-role shape; full role-state semantics are not implemented. |
| `SELECT` | `PARTIAL_MINIMAL` | single-db mode only; SELECT supports database 0. |
| `SHUTDOWN` | `DISABLED` | - |
| `SWAPDB` | `PARTIAL_MINIMAL` | single-db mode only; SWAPDB supports 0<->0 only. |
| `WAIT` | `PARTIAL_MINIMAL` | Minimal compatibility behavior; full replication and durability semantics are not implemented. |
| `WAITAOF` | `PARTIAL_MINIMAL` | Minimal compatibility behavior; full replication and durability semantics are not implemented. |
| `XCFGSET` | `DISABLED` | Declared extension surface; runtime behavior is not implemented. |
