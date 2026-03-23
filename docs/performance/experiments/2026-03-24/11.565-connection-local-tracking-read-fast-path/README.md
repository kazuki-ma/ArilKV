# 11.565 connection-local tracking-read fast path

- status: accepted
- before_commit: `848ab84b38e0e645db7dc1731f6b43f836ebd417`
- after_commit: unavailable; accepted change in current working tree
- revert_commit: not applicable

## Goal

Remove the unconditional `tracking_clients` atomic load from the plain read path by deciding once per connection whether read tracking is actually active for the current request.

## Change

- added `ClientConnectionState::should_track_reads()` so the connection path can derive read-tracking activity from local `CLIENT TRACKING` / `CLIENT CACHING` state
- threaded that derived `client_tracks_reads` flag through owner-routing, blocking execution, and transaction execution into `RequestProcessor`
- added `execute_with_client_tracking_context_and_effects_in_db(...)` and `execute_with_client_tracking_no_touch_in_transaction_and_effects_in_db(...)` so the connection path can opt into read tracking without changing existing default call sites
- changed `execute_in_current_context(...)` to preserve the connection-level read-tracking decision and only disable it for mutating commands
- removed the now-redundant `has_tracking_clients()` atomic check from `track_read_key_for_current_client(...)`
- added exact TCP regression `client_tracking_optin_only_tracks_reads_after_caching_yes`

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server client_tracking_optin_only_tracks_reads_after_caching_yes -- --nocapture`
- `cargo test -p garnet-server tracking_gets_notification_of_expired_keys_like_external_scenario -- --nocapture`
- `cargo test -p garnet-server execute_owned_frame_args_via_processor_matches_direct_execution -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`
- verified counts: `712 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark commands

Long owner-inline reruns:

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.565-r1-long \
BASE_BIN=/private/tmp/garnet-11.565-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.565-current-target/release/garnet-server \
BASE_LABEL=tracking_read_atomic_path \
NEW_LABEL=connection_local_tracking_reads \
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

The second and third reruns used the same command with `OUTDIR=/tmp/garnet-binary-ab-20260324-11.565-r2-long` and `...-r3-long`.

## Result

The three identical long reruns did not converge to one perfectly clean number:

- run 1: `SET +7.66%`, `GET +3.20%`, `SET p99 -9.20%`, `GET p99 flat`
- run 2: `SET +1.95%`, `GET -6.47%`, `SET p99 -10.13%`, `GET p99 +11.27%`
- run 3: `SET +4.98%`, `GET +0.34%`, `SET p99 +11.27%`, `GET p99 -10.13%`

Keep rationale:

- the code removes a real unconditional atomic from every non-mutating tracked-read probe
- exact semantics for `OPTIN`, `CLIENT CACHING YES`, and existing `BCAST` invalidation behavior stayed green
- two of the three long reruns remained net positive on throughput, and the read-side p99 direction improved or stayed flat in two of the three runs
- the change is small, localized, and keeps the connection/runtime boundary cleaner than the previous global check

## Artifacts

- `first-comparison.txt`
- `second-comparison.txt`
- `third-comparison.txt`
- `first-before-summary.txt`
- `first-before-runs.csv`
- `first-after-summary.txt`
- `first-after-runs.csv`
- `second-before-summary.txt`
- `second-before-runs.csv`
- `second-after-summary.txt`
- `second-after-runs.csv`
- `third-before-summary.txt`
- `third-before-runs.csv`
- `third-after-summary.txt`
- `third-after-runs.csv`
- `diff.patch`
