# Redis Command Status Summary

- Redis baseline source: `local-json:/Users/kazuki-matsuda/dev/src/github.com/redis/redis/src/commands`
- Redis command count: `267`
- Garnet declared command count: `259`
- Supported (declared): `259`
- Not implemented: `8`
- Garnet extensions: `0`
- Coverage vs Redis baseline: `97.00%`

## Files

- Matrix CSV: `docs/compatibility/redis-command-status.csv`
- This summary: `docs/compatibility/redis-command-status-summary.md`

## Status Semantics

- `SUPPORTED_DECLARED`: command appears in both Redis baseline and Garnet `COMMAND` output.
- `NOT_IMPLEMENTED`: command appears in Redis baseline but not in Garnet.
- `GARNET_EXTENSION`: command appears in Garnet `COMMAND` output but not in Redis baseline.

## Update Command

```bash
cd tests/interop
./build_command_status_matrix.sh
```
