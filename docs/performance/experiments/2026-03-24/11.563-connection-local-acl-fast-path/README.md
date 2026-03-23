# 11.563 connection-local ACL fast path

- status: accepted
- before_commit: `445d9be74d3bf5d3d3882b7739d226f48b687a4e`
- after_commit: unavailable; accepted change in current working tree
- revert_commit: not applicable

## Goal

Remove the per-command `ServerMetrics.clients` mutex snapshot from the plain ACL authorization path by mirroring the current connection's authenticated state locally.

## Change

- added `authenticated` and `authenticated_user` to `ClientConnectionState`
- initialized and reset that state from the server's default-user startup semantics instead of re-querying metrics on every command
- added `sync_connection_auth_state_from_metrics(...)` and used it only on successful `AUTH` and `HELLO ... AUTH` side effects, so OIDC and password auth both keep the exact authenticated principal
- replaced `acl_authorize_client_command(...)` with `acl_authorize_connection_command(...)`, which authorizes from the connection-local auth snapshot instead of taking the global client-metrics lock
- threaded the same borrowed/local auth state through blocking-command ACL checks and queued transaction execution
- added exact regression `auth_side_effects_sync_connection_local_auth_state_from_metrics`

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server auth_side_effects_sync_connection_local_auth_state_from_metrics -- --nocapture`
- `cargo test -p garnet-server reset_clears_multi_and_authenticated_state -- --nocapture`
- `cargo test -p garnet-server acl_auth_password_rotation_and_hello_auth_match_external_scenarios -- --nocapture`
- `cargo test -p garnet-server acl_current_connection_and_hello_chain_match_external_scenarios -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`
- verified counts: `711 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark commands

Short owner-inline reruns:

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.563-r1 \
BASE_BIN=/private/tmp/garnet-11.563-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.563-current-target/release/garnet-server \
BASE_LABEL=metrics_acl_lock_path \
NEW_LABEL=connection_local_acl_fast_path \
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

Stabilization rerun:

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.563-r4-long \
BASE_BIN=/private/tmp/garnet-11.563-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.563-current-target/release/garnet-server \
BASE_LABEL=metrics_acl_lock_path \
NEW_LABEL=connection_local_acl_fast_path \
RUNS=5 \
THREADS=1 \
CONNS=4 \
REQUESTS=50000 \
PRELOAD_REQUESTS=50000 \
PIPELINE=1 \
SIZE_RANGE=1-256 \
OWNER_THREADS=1 \
GARNET_OWNER_THREAD_PINNING=1 \
GARNET_OWNER_THREAD_CPU_SET=0 \
GARNET_OWNER_EXECUTION_INLINE=1 \
./garnet-rs/benches/binary_ab_local.sh
```

## Result

The short `REQUESTS=30000` reruns were noisy:

- run 1: `SET +16.26%`, `GET +3.94%`, `SET p99 -16.84%`, `GET p99 flat`
- run 2: `SET -18.54%`, `GET -4.36%`, `SET p99 +93.20%`, `GET p99 flat`
- run 3: `SET +15.63%`, `GET -1.83%`, `SET p99 -25.20%`, `GET p99 +11.27%`

The longer stabilization run was clean and matched the intended lock-removal win:

- `SET ops 108992 -> 124147` (`+13.90%`)
- `GET ops 136829 -> 144001` (`+5.24%`)
- `SET p99 0.087 -> 0.079 ms` (`-9.20%`)
- `GET p99 0.095 -> 0.071 ms` (`-25.26%`)

Keep rationale: the code change removes a real unconditional mutexed client snapshot from every ACL-checked command, the semantics stayed green on the exact AUTH/HELLO/RESET regressions, and the longer run converged cleanly in the expected direction even though the shorter reruns were host-noisy.

## Artifacts

- `first-comparison.txt`
- `second-comparison.txt`
- `third-comparison.txt`
- `long-comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `diff.patch`
