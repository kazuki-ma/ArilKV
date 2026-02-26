# Redis Command Status Summary

- Redis baseline source: `local-json:/Users/kazuki-matsuda/dev/src/github.com/redis/redis/src/commands`
- Redis command count: `263`
- Garnet declared command count: `241`
- Supported (declared): `241`
- Not implemented: `22`
- Garnet extensions: `0`
- Coverage vs Redis baseline: `91.63%`

## Files

- Matrix CSV: `docs/compatibility/redis-command-status.csv`
- This summary: `docs/compatibility/redis-command-status-summary.md`

## Status Semantics

- `SUPPORTED_DECLARED`: command appears in both Redis baseline and Garnet `COMMAND` output.
- `NOT_IMPLEMENTED`: command appears in Redis baseline but not in Garnet.
- `GARNET_EXTENSION`: command appears in Garnet `COMMAND` output but not in Redis baseline.

## Update Command

```bash
cd garnet-rs/tests/interop
./build_command_status_matrix.sh
```
