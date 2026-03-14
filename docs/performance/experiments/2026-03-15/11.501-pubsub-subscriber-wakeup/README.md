# 11.501 pubsub subscriber wakeup smoke benchmark

- Date: 2026-03-15
- Purpose: verify that subscriber-only pubsub wakeups do not regress the non-subscribed connection hot path after adding per-client pending-message notifiers.
- Before commit: `f32fb61368`
- After commit: `working-tree (iteration 752)`
- Harness: `garnet-rs/benches/binary_ab_local.sh`
- Command:
  - `OUTDIR=/tmp/garnet-binary-ab-pubsub-wakeup-r3b-20260315-081217 BASE_BIN=/tmp/garnet-bench-base-XrUlyY/garnet-rs/target/release/garnet-server NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server RUNS=3 THREADS=2 CONNS=4 REQUESTS=3000 PRELOAD_REQUESTS=3000 PIPELINE=1 HASH_INDEX_SIZE_BITS=16 MAX_IN_MEMORY_PAGES=262144 ./benches/binary_ab_local.sh`
- Result summary:
  - base median set ops: `75963`
  - base median get ops: `50297`
  - base median set p99 ms: `0.295`
  - base median get p99 ms: `0.599`
  - new median set ops: `82326`
  - new median get ops: `101833`
  - new median set p99 ms: `0.207`
  - new median get p99 ms: `0.143`
- Interpretation: no hot-path regression was observed after restricting the wakeup path to clients with active pubsub subscriptions.
