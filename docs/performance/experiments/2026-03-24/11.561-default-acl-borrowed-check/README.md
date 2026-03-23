# 11.561 default ACL borrowed check

- status: accepted
- before_commit: `0dc6f4fee38dd4e3a0fcf6c382c073fc4d319c6c`
- after_commit: unavailable; working-tree accepted change before commit
- revert_commit: not applicable

## Goal

Remove the remaining per-command user clone from the unrestricted default-ACL fast path.

## Change

- added `ServerMetrics::client_is_authenticated_default_user(...)`
- changed `acl_authorize_client_command(...)` to use the borrowed default-user check first when `default_acl_user_unrestricted` is enabled
- kept the existing `client_auth_state(...)` clone path only for non-default or restricted cases
- left denial/error behavior unchanged

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server default_acl_user_unrestricted_fast_path_tracks_profile_updates -- --nocapture`
- `cargo test -p garnet-server acl_dryrun_and_cluster_saveconfig_countkeysinslot_getkeysinslot -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`

Verified counts:

- `710 passed; 0 failed; 16 ignored`
- plus `26 passed`
- plus `1 passed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.561-r2 \
BASE_BIN=/tmp/garnet-11.561-baseline-51856/garnet-rs/target/release/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=acl_default_path_clone \
NEW_LABEL=acl_default_path_borrowed_check \
RUNS=5 \
THREADS=1 \
CONNS=4 \
REQUESTS=30000 \
PRELOAD_REQUESTS=30000 \
PIPELINE=1 \
SIZE_RANGE=1-256 \
OWNER_THREADS=1 \
GARNET_OWNER_THREAD_PINNING=1 \
GARNET_OWNER_THREAD_CPU_SET=0 \
GARNET_OWNER_EXECUTION_INLINE=1 \
./garnet-rs/benches/binary_ab_local.sh
```

## Result

This was a small but clean keep.

First run:

- `SET ops 130901 -> 133217` (`+1.77%`)
- `GET ops 156277 -> 160803` (`+2.90%`)
- `SET p99 0.071 -> 0.071 ms` (`0.00%`)
- `GET p99 0.071 -> 0.063 ms` (`-11.27%`)

Accepted rerun:

- `SET ops 120554 -> 124898` (`+3.60%`)
- `GET ops 144245 -> 148533` (`+2.97%`)
- `SET p99 0.079 -> 0.079 ms` (`0.00%`)
- `GET p99 0.071 -> 0.063 ms` (`-11.27%`)

The change is deliberately small, but it removes a real allocation from the plain default-user ACL path and stays directionally positive across the rerun.

## Artifacts

- `first-comparison.txt`
- `first-before-summary.txt`
- `first-before-runs.csv`
- `first-after-summary.txt`
- `first-after-runs.csv`
- `comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `diff.patch`
