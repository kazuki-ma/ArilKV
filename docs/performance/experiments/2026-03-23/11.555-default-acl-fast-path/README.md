# 11.555 default ACL fast path

- status: accepted
- before_commit: `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- after_commit: unavailable; working-tree-only accepted change on top of `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- revert_commit: not applicable

## Goal

Remove avoidable ACL work from the default owner-inline hot path when the authenticated client is still using the unrestricted default user.

## Change

- added `AclSelectorProfile::is_unrestricted_runtime()` and `AclUserProfile::is_unrestricted_runtime()`
- added a cached `default_acl_user_unrestricted: AtomicBool` on `RequestProcessor`
- update that cache whenever ACL users are replaced or the default user profile changes
- added `ServerMetrics::client_auth_state(...)` so ACL authorization reads `authenticated + user` under one client-metrics lock instead of two
- changed `acl_authorize_client_command(...)` to return early for authenticated clients on the unrestricted default user
- added exact regression `default_acl_user_unrestricted_fast_path_tracks_profile_updates`

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server acl_ -- --nocapture`

Full gate:

- `make fmt-check`
- `make test-server`
- verified counts: `708 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260323-11.555-r2 \
BASE_BIN=/tmp/garnet-11.555-baseline/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=acl_full_auth_path \
NEW_LABEL=acl_default_fast_path \
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

The first owner-inline `RUNS=5` sample was already good (`first-comparison.txt`), and the identical rerun confirmed the direction:

- `SET ops 77011 -> 81836` (`+6.27%`)
- `GET ops 78947 -> 87654` (`+11.03%`)
- `SET p99 0.159 -> 0.159 ms` (`0.00%`)
- `GET p99 0.159 -> 0.151 ms` (`-5.03%`)

This is a clear keep. In the default benchmark shape, Garnet was still paying for full ACL user/profile lookup and selector evaluation on every command even though the active user was the unrestricted default superuser.

## Artifacts

- `comparison.txt`
- `first-comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `diff.patch`
