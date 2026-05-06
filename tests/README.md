# garnet-rs tests

This directory contains test runners and interoperability checks for the
Rust server implementation.

## Directory Map

- `interop/README.md`
  - command-surface, compatibility subset, and cluster/replication scripts.

## Patterns To Re-check When Adding Commands

- Unit tests are the primary correctness gate.
  - add command-level behavior tests in `garnet-server` first.
- Interop subset is a compatibility smoke test, not full Redis conformance.
  - always run it after command-surface changes.
- Do not accept pass/fail by exit code alone.
  - verify reported test-case counts and absence of failure lines in stdout.
- Keep command status files synchronized when command catalog changes.
  - regenerate matrix and commit updated status CSV/summary.

## Required Flow For Command Changes

```bash
cd .
cargo test -p garnet-server -- --nocapture

cd tests/interop
REDIS_REPO_ROOT=/Users/kazuki-matsuda/dev/src/github.com/redis/redis \
./redis_runtest_external_subset.sh
./build_command_status_matrix.sh
```
