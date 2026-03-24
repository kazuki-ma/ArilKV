# 11.571 reserve bulk string capacity

- status: accepted
- before_commit: `18702fef2cb4deec73563c1a1625370d561de4e8`
- after_commit: `6d042940a2c693cebf6cd839e2439628d7a74714`
- revert_commit: not applicable

## Goal

Turn the fresh Linux perf evidence for plain `GET` into a small targeted fix. The current owner-inline profile showed `realloc` under `garnet_server::request_lifecycle::resp::append_bulk_string`, so this slice pre-reserves the exact RESP bulk-string payload size before appending.

## Perf evidence

Fresh Linux-side perf on current `HEAD` under owner-inline `THREADS=1 CONNS=4 REQUESTS=50000 PIPELINE=1 SIZE_RANGE=1-256` showed:

- `GET`: `__kernel_clock_gettime` `13.33%` at the top, but also a concrete userland allocation chain:
  - `realloc -> alloc::raw_vec::RawVecInner::reserve -> append_bulk_string`
- `GET` also still showed `HashIndex::find_tag_address` and `record_key_access`, so the response buffer growth was one of the cleanest low-risk wins available from the current profile.

## Change

- added `ascii_len_usize(...)` in `resp.rs`
- changed `append_bulk_string(...)` to `reserve(...)` exactly enough space for `$<len>\r\n<payload>\r\n` before extending the output buffer

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
- `cargo test -p garnet-server execute_owned_frame_args_via_processor_matches_direct_execution -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`
- verified counts: `712 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark commands

Current binary:

```bash
CARGO_TARGET_DIR=/private/tmp/garnet-11.571-current-target cargo build -p garnet-server --release
```

Owner-inline `PIPELINE=1`:

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.571-p1 \
BASE_BIN=/private/tmp/garnet-11.567-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.571-current-target/release/garnet-server \
BASE_LABEL=baseline_resp_growth \
NEW_LABEL=reserve_bulk_string \
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

Owner-inline `PIPELINE=16` first run:

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.571-p16 \
BASE_BIN=/private/tmp/garnet-11.567-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.571-current-target/release/garnet-server \
BASE_LABEL=baseline_resp_growth \
NEW_LABEL=reserve_bulk_string \
RUNS=5 \
THREADS=1 \
CONNS=4 \
REQUESTS=50000 \
PRELOAD_REQUESTS=50000 \
PIPELINE=16 \
SIZE_RANGE=1-256 \
OWNER_THREADS=1 \
GARNET_OWNER_THREAD_PINNING=1 \
GARNET_OWNER_THREAD_CPU_SET=0 \
GARNET_OWNER_EXECUTION_INLINE=1 \
./garnet-rs/benches/binary_ab_local.sh
```

Owner-inline `PIPELINE=16` rerun:

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.571-p16-r2 \
BASE_BIN=/private/tmp/garnet-11.567-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.571-current-target/release/garnet-server \
BASE_LABEL=baseline_resp_growth \
NEW_LABEL=reserve_bulk_string \
RUNS=5 \
THREADS=1 \
CONNS=4 \
REQUESTS=50000 \
PRELOAD_REQUESTS=50000 \
PIPELINE=16 \
SIZE_RANGE=1-256 \
OWNER_THREADS=1 \
GARNET_OWNER_THREAD_PINNING=1 \
GARNET_OWNER_THREAD_CPU_SET=0 \
GARNET_OWNER_EXECUTION_INLINE=1 \
./garnet-rs/benches/binary_ab_local.sh
```

## Result

- `PIPELINE=1`: `SET +7.13%`, `GET +3.78%`, `SET p99 -18.39%`, `GET p99 -9.20%`
- `PIPELINE=16` first run: `SET -6.03%`, `GET +10.99%`, `SET p99 +3.59%`, `GET p99 -8.74%`
- `PIPELINE=16` rerun: `SET +2.72%`, `GET +3.28%`, `SET p99 -6.48%`, `GET p99 -8.74%`

## Interpretation

- The `PIPELINE=1` result is already a clean win on both throughput and p99.
- The first `PIPELINE=16` run showed the expected `GET` gain but an implausible `SET` regression for a GET-side response helper, so it was rerun before acceptance.
- The identical rerun confirmed the change as net positive on both `SET` and `GET`, with the clearest benefit remaining on the read path that motivated the fix.

## Artifacts

- `comparison-p1.txt`
- `comparison-p16-first.txt`
- `comparison-p16-rerun.txt`
- `summary.txt`
- `diff.patch`
