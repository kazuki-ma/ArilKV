# garnet-server `src/` Notes

This directory contains server runtime wiring and command metadata.

## Read This Before Command Edits

When adding Redis command behavior, update all of the following:

1. `command_spec.rs`
   - add `CommandId`
   - add `COMMAND_SPECS` entry
   - set arity/key-access metadata
2. `command_dispatch.rs`
   - ensure command name dispatch is covered in tests
3. `request_lifecycle.rs`
   - route `CommandId` to the handler
4. `request_lifecycle/tests.rs`
   - add response-shape tests and error tests

## Patterns That Needed Re-checking

- RESP shape matters as much as logic:
  - pop-family commands can return bulk-string null (`$-1`) or null-array (`*-1`) depending on command.
- Commands with `numkeys` and/or trailing options (`timeout`, `COUNT`) do not fit `KeyAccessPattern::AllKeysFromArg1`.
  - In that case, prefer `KeyAccessPattern::None` until key-spec metadata is expanded.
- Blocking commands are intentionally implemented via owner-thread polling.
  - Execute on owner thread immediately.
  - If the command returns empty-blocking response (`$-1` / `*-1`), sleep briefly and retry until timeout/deadline.
  - No put-side wakeup registration is used for these commands in this phase (zero-overhead writes).

## Required Validation Path

```bash
cd .
cargo test -p garnet-server -- --nocapture

cd tests/interop
REDIS_REPO_ROOT=/Users/kazuki-matsuda/dev/src/github.com/redis/redis \
./redis_runtest_external_subset.sh
./build_command_status_matrix.sh
```
