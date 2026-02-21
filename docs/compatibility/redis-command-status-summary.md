# Redis Command Status Summary

- Generated at: 2026-02-21T23:20:06Z
- Source Redis image: `redis:7.2-alpine`
- Redis command count: `241`
- Garnet declared command count: `163`
- Supported (declared): `163`
- Not implemented: `78`
- Garnet extensions: `0`
- Coverage vs Redis baseline: `67.63%`

## Files

- Matrix CSV: `docs/compatibility/redis-command-status.csv`
- This summary: `docs/compatibility/redis-command-status-summary.md`

## Status Semantics

- `SUPPORTED_DECLARED`: command appears in both Redis and Garnet `COMMAND` output.
- `NOT_IMPLEMENTED`: command appears in Redis `COMMAND` output but not in Garnet.
- `GARNET_EXTENSION`: command appears in Garnet `COMMAND` output but not in Redis.

## Update Command

```bash
cd garnet-rs/tests/interop
./build_command_status_matrix.sh
```
