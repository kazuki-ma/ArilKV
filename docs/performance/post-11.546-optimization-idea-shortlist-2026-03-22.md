# Post-11.546 Optimization Idea Shortlist

- Date: 2026-03-22
- Context: after `11.546` the owner-inline single-thread Docker/Linux comparison has Garnet ahead of Dragonfly again, so the remaining big wins are more structural than the recent low-risk cleanup slices.

## Most promising

### 1. Direct read-to-RESP path for string reads

- Today `GET`-like paths still do `record -> Vec<u8> -> response_out`.
- `read_string_value(...)` materializes an owned `Vec<u8>` at [string_store.rs](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/crates/garnet-server/src/request_lifecycle/string_store.rs#L242).
- The storage read path also materializes `Self::Value` before the command uses it at [read_operation.rs](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/crates/tsavorite/src/read_operation.rs#L100).
- RESP emission then copies again in [resp.rs](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/crates/garnet-server/src/request_lifecycle/resp.rs#L50).
- There is already a callback-bounded borrowed `peek(...)` API in [read_operation.rs](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/crates/tsavorite/src/read_operation.rs#L185).
- Best next design: add a borrowed `read_with_decoded_value(...)` or `append_string_value_as_resp_bulk(...)` path so `GET`, `GETRANGE`, `GETEX`, and similar commands append directly into the response buffer while still inside the borrowed callback scope.
- Expected upside: medium-to-large on read-heavy workloads.
- Risk: medium. Needs careful expiry/WRONGTYPE/tracking semantics and likely a clean parallel API rather than mutating the existing generic read ABI.

### 2. Kill global key-access mutexes on the hot path

- `record_key_access(...)` takes a process-wide mutex on every `GET`/`SET` touch at [request_lifecycle.rs](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/crates/garnet-server/src/request_lifecycle.rs#L6033).
- That state is stored in `DbKeyMapState` behind `key_lru_access_millis.lock()` rather than per-shard structures.
- Best next design: move LRU/LFU touch accounting to per-shard state, or stage updates in thread-local/per-owner buffers and flush lazily.
- Expected upside: medium in owner-inline, potentially large in multi-thread owner-pool mode.
- Risk: medium-to-high because `OBJECT IDLETIME` and related introspection semantics can drift if touch publication is delayed.

### 3. Add a real zero-work fast path for WATCH/client-tracking invalidation when unused

- Every write currently hits `bump_watch_version(...)` at [string_store.rs](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/crates/garnet-server/src/request_lifecycle/string_store.rs#L1476), which immediately calls tracking invalidation.
- That flows into [request_lifecycle.rs](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/crates/garnet-server/src/request_lifecycle.rs#L4005), which locks `pubsub_state`, clones RESP version metadata, then locks `tracking_state` and walks clients.
- Best next design: maintain cheap global counters for `watching clients` and `tracking clients`, then short-circuit the whole invalidation path when both are zero.
- Expected upside: medium-to-large on write-heavy plain workloads if the current no-tracking/no-watch common case still pays hidden overhead.
- Risk: medium. The fast path must not break transactional watch invalidation or RESP3 tracking redirect edge cases.

## High variance but potentially huge

### 4. Rework owner-thread routing in pooled mode

- Earlier flamegraphs in the pooled profile were dominated by wakeup/sync overhead rather than value decode work.
- The owner-thread pool is selected in [connection_handler.rs](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/crates/garnet-server/src/connection_handler.rs#L6048).
- Best next design candidates:
  - sticky connection-to-owner routing,
  - batching multiple routed commands per wakeup,
  - or a tighter owner-local execution queue that reduces cross-thread wakeups.
- Expected upside: very large, especially in the 2-thread and multi-connection profiles where the pooled path previously lagged.
- Risk: very high. This is an architectural change and could affect fairness, blocking commands, replication interactions, and ordering guarantees.

### 5. Split hot-path metadata into strict hot and lazy cold lanes

- Recent wins came from removing work that was semantically correct but unnecessarily eager: `MONITOR`, commandstats name allocation, last-command buffer churn.
- The same pattern likely still exists in client activity updates, key touch bookkeeping, and optional observability surfaces.
- Best next design: define an explicit hot-path contract where only correctness-critical state is updated synchronously; everything else gets per-thread counters or periodic flush.
- Expected upside: medium if done surgically, large if it lets pooled mode shed several tiny mutexed side effects at once.
- Risk: high because it is easy to accidentally weaken introspection semantics in small ways that only show up in compatibility tests.

## Probably worth trying only after the above

### 6. Borrowed-read helpers for compare-and-inspect commands beyond `SET`

- `peek(...)` is now used for `SET IFEQ/IFNE`-style checks, but many commands still force owned reads through `read_string_value(...)`.
- Extending borrowed helpers to `GETBIT`, `BITCOUNT`, `STRLEN`, or range-like read-only inspections could trim more allocations.
- Expected upside: small-to-medium per command, but broad if applied across the read-heavy command surface.
- Risk: medium because the API stays clean, but the aggregate benefit may be lower than the structural items above.

## Recommendation

- If optimizing for the next clear throughput jump, start with:
  1. tracking/watch no-subscriber fast path,
  2. direct read-to-RESP path,
  3. per-shard/lazy key-touch accounting.
- If optimizing for the biggest possible upside rather than the safest win, the real bet is pooled-mode owner routing.
