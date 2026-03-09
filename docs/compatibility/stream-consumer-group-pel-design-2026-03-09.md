# Stream Consumer-Group / PEL Compatibility Design

Date: 2026-03-09

## 1. Goal

Close the remaining high-cost compatibility gaps in Redis/Valkey stream
consumer-group behavior, centered on:

- `XGROUP`
- `XREADGROUP`
- `XPENDING`
- `XACK`
- `XCLAIM`
- `XAUTOCLAIM`
- `XINFO ... FULL`
- blocked stream reprocessing / unblock semantics
- stream deletion / trimming interactions with pending entries

This document answers four questions:

1. Is full compatibility possible with the current owner-thread model?
2. What is missing in the current `StreamObject` model?
3. What state model should replace it?
4. What test / TLA+ work must precede the implementation slices?

## 2. Decision Summary

Full compatibility is feasible with the current per-key owner-thread execution
model.

No global lock redesign is required.

However, the current stream consumer-group state model is too small to reach
full compatibility. The existing shape:

- stores stream entries
- stores per-group `last_id`
- stores only the set of consumer names per group

is not sufficient for:

- consumer-group pending entries list (PEL)
- history reads (`XREADGROUP ... ID != ">"`)
- `XPENDING`
- `XACK`
- `XCLAIM` / `XAUTOCLAIM`
- `XINFO` consumer idle/active fields
- delete/trim behavior while pending references still exist
- blocked waiter reprocessing on the same stream key

The recommended direction is:

1. add a clean consumer-group / PEL state model
2. keep the current owner-thread serialization contract
3. do not optimize for pre-change internal payload compatibility
4. add exact external TCP tests before each behavior fix
5. add TLA+ models for blocked stream reprocessing and PEL ownership

## 3. Current Reality

### 3.1 Code Facts

Current `StreamObject`:

- `entries: BTreeMap<StreamId, Vec<(Vec<u8>, Vec<u8>)>>`
- `groups: BTreeMap<Vec<u8>, StreamId>`
- `group_consumers: BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>>`

Current behavior is therefore necessarily partial:

- `XREADGROUP` new-delivery path is approximated by scanning entries after the
  group's `last_id`
- `XREADGROUP` history path is not modeled as consumer-local PEL history
- `XPENDING` is mostly stubbed
- `XACK` always returns `0`
- `XCLAIM` / `XAUTOCLAIM` parse some options but do not manipulate ownership
- `XGROUP DESTROY` removes group names, but there is no pending-reference model
- blocked stream waiter readiness is still mostly key-level rather than
  waiter-predicate-level

### 3.2 External Result Facts

After iteration 592, the latest full external report is:

- result dir:
  - `garnet-rs/tests/interop/results/compatibility-report-20260309-024354/redis-runtest-external-full`
- summary:
  - `ok=1349 err=120 timeout=1 ignore=159`

Relevant improvements already achieved:

- `Blocking XREAD: key deleted` -> `[ok]`
- `Blocking XREADGROUP for stream key that has clients blocked on list` -> `[ok]`
- `Blocking XREADGROUP: key deleted` -> `[ok]`
- `Blocking XREADGROUP: flushed DB` -> `[ok]`

Remaining stream-cgroup failures now cluster into three classes:

1. parser / surface gaps
2. missing consumer-group / PEL semantics
3. blocked stream reprocessing semantics

## 4. Failure Taxonomy

### 4.1 Parser / Surface Gaps

These do not require the new PEL model, but they block progress in
`unit/type/stream-cgroups` if left unfixed.

Examples:

- `XREAD and XREADGROUP against wrong parameter`
  - currently generic `ERR syntax error`
  - expected Redis-style unbalanced stream/ID error text
- `Blocking XREADGROUP for stream key that has clients blocked on stream - avoid endless loop`
  - currently fails earlier because `XADD MAXLEN 5000 * ...` surface is missing
- `XGROUP CREATE: with ENTRIESREAD parameter`
  - current validation is too weak
- `XGROUP CREATE: automatic stream creation fails without MKSTREAM`
  - current behavior is too permissive
- `XGROUP DESTROY correctly manage min_cgroup_last_id cache`
  - currently hits unknown stream-side commands (`XDELEX` family gaps)

### 4.2 Consumer-Group / PEL State Gaps

These are impossible to implement correctly with the current minimal
`StreamObject`.

Examples:

- `Blocking XREADGROUP will ignore BLOCK if ID is not >`
- `XREADGROUP will return only new elements`
- `XREADGROUP can read the history of the elements we own`
- `XPENDING ...`
- `XACK ...`
- `XCLAIM ...`
- `XAUTOCLAIM ...`
- `XINFO FULL output`
- `XGROUP DESTROY removes all consumer group references`
- `XREADGROUP from PEL does not change dirty`

### 4.3 Blocking / Reprocessing Gaps

These are concurrency-sensitive and need both a richer waiter model and a
better state model.

Examples:

- `Blocking XREADGROUP for stream key that has clients blocked on stream - avoid endless loop`
- `Blocking XREADGROUP for stream key that has clients blocked on stream - reprocessing command`
- `XGROUP DESTROY should unblock XREADGROUP with -NOGROUP`

## 5. Feasibility and Constraints

### 5.1 What Is Feasible Without Core Concurrency Redesign

All remaining stream consumer-group semantics are single-key semantics.

Because the same key is already serialized on the same owner thread, we can
implement:

- group state mutation
- pending-entry reassignment
- blocked waiter wakeup / reprocessing
- destroy/delete/type-change wakeups

without introducing:

- cross-key locking
- global stream coordinators
- cross-thread ownership transfer for the same stream key

### 5.2 What Must Change

The current internal stream payload format must change.

This is the key design decision: do a clean internal stream consumer-group
state redesign instead of trying to stretch the current `groups + consumer set`
layout.

### 5.3 Explicit Non-Goal

Do not preserve pre-change internal stream payload compatibility as a primary
goal.

For this work, the right baseline is post-apply code behavior. If a temporary
decode-upgrade path is cheap, it can be added, but the design should not be
constrained by old internal payload layout.

## 6. Proposed State Model

### 6.1 Domain Types

Introduce typed wrappers in the new consumer-group layer instead of continuing
to thread raw `Vec<u8>` through internal APIs:

- `StreamGroupName`
- `StreamConsumerName`

These can remain thin wrappers over bytes, but internal APIs should stop
passing anonymous raw names where behavior depends on their role.

### 6.2 Stream-Level State

Proposed shape:

```rust
struct StreamObject {
    entries: BTreeMap<StreamId, StreamEntryRecord>,
    groups: BTreeMap<StreamGroupName, StreamGroupState>,
    last_generated_id: StreamId,
    max_deleted_entry_id: StreamId,
    entries_added: u64,
}

struct StreamEntryRecord {
    fields: Vec<(Vec<u8>, Vec<u8>)>,
}
```

Notes:

- `entries` remain keyed by `StreamId`
- `last_generated_id`, `max_deleted_entry_id`, and `entries_added` are needed
  for `XINFO` and deletion/trimming semantics
- do not add per-entry ack/ref caches in the first correctness slice
  - derive from group state on demand first
  - optimize later only with benchmark evidence

### 6.3 Group State

```rust
struct StreamGroupState {
    last_delivered_id: StreamId,
    entries_read: Option<u64>,
    pel: BTreeMap<StreamId, StreamPendingEntry>,
    consumers: BTreeMap<StreamConsumerName, StreamConsumerState>,
}

struct StreamPendingEntry {
    consumer: StreamConsumerName,
    last_delivery_unix_millis: u64,
    delivery_count: u64,
}

struct StreamConsumerState {
    pending_ids: BTreeSet<StreamId>,
    seen_unix_millis: u64,
    active_unix_millis: Option<u64>,
}
```

Rationale:

- group-global PEL owns the delivery metadata
- consumer-local pending set only stores membership
- the same pending entry metadata is not duplicated per consumer
- this matches the cleanest form of the Valkey model without copying its
  storage details verbatim

### 6.4 Deleted / Trimmed Entries

Pending references can outlive the actual stream entry payload.

Therefore:

- `entries` may not contain an ID that is still present in a group's PEL
- history reads and claim operations must tolerate PEL IDs whose entry payload
  was deleted or trimmed
- first correctness version should model this by:
  - leaving the PEL entry intact
  - allowing lookup of the actual payload to return missing
  - returning the Redis-compatible missing-entry behavior for history/claim

This is sufficient for correctness.

If later profiling shows deletion-heavy scans are expensive, a derived
stream-level summary can be added then.

### 6.5 State Invariants

The redesigned state model should enforce these invariants at all times:

- for one group, every pending ID appears at most once in `group.pel`
- every `group.pel[id].consumer` must exist in `group.consumers`
- the union of all `consumer.pending_ids` must match the key set of
  `group.pel`
- one pending ID belongs to exactly one consumer within the group
- `last_delivered_id` is monotonic for new-delivery reads and `SETID`
- `entries_read`, when present, is monotonic except when explicitly reset by
  Redis-compatible `SETID` semantics
- history reads (`XREADGROUP ... ID != ">"`) do not mutate PEL ownership and
  do not advance `last_delivered_id`
- `XGROUP DESTROY` removes group, consumer, and PEL state atomically from the
  owner thread point of view

These invariants should drive both Rust assertions/helpers and the TLA+ models.

## 7. Command Semantics Mapping

### 7.1 `XGROUP`

Required behavior:

- `CREATE`
  - fail on duplicate group with `BUSYGROUP`
  - require existing stream unless `MKSTREAM`
  - validate `ENTRIESREAD`
- `SETID`
  - update `last_delivered_id`
  - update `entries_read` when explicitly supplied
- `DESTROY`
  - remove the entire group state, including PEL and consumers
  - wake blocked `XREADGROUP` waiters on that group with `NOGROUP`
- `CREATECONSUMER`
  - create if missing, report created/not-created count
- `DELCONSUMER`
  - remove consumer and return number of pending entries that were associated
    with it

### 7.2 `XREADGROUP` with `ID == ">"`

Required behavior:

- this is the new-delivery path
- only new entries after `group.last_delivered_id` are eligible
- when an entry is delivered:
  - `last_delivered_id` advances
  - `entries_read` updates
  - if not `NOACK`, the ID enters:
    - group PEL
    - consumer pending set
  - consumer `seen_unix_millis` and `active_unix_millis` update
- if `BLOCK` is given and there is no eligible new entry:
  - the command blocks

### 7.3 `XREADGROUP` with `ID != ">"`

This is not a new-delivery scan.

It is a history read from the requesting consumer's own PEL only.

Required behavior:

- `BLOCK` is ignored
- return entries from the requesting consumer's PEL in ID order
- if a pending ID exists but the underlying entry payload is gone:
  - return Redis-compatible missing-entry shape
  - do not silently invent a new scan over stream entries

This is the main semantic gap behind:

- `Blocking XREADGROUP will ignore BLOCK if ID is not >`
- several history / pending / claim failures

### 7.4 `XPENDING`

Required behavior:

- summary form:
  - total pending count
  - first pending ID
  - last pending ID
  - per-consumer pending counts
- range form:
  - iterate either group PEL or one consumer PEL
  - include idle time and delivery count
  - support `IDLE`

### 7.5 `XACK`

Required behavior:

- remove IDs from:
  - group PEL
  - owning consumer pending set
- return number of IDs actually acknowledged
- invalid ID syntax must fail before partial mutation

### 7.6 `XCLAIM`

Required behavior:

- move ownership of existing pending IDs to a target consumer
- update delivery time / retry count according to options
- optional `FORCE`
  - only if the entry exists in the stream
  - create pending metadata even if not already pending
- optional `JUSTID`
- optional `LASTID`
  - advance group last-delivered state for replication-equivalent semantics

### 7.7 `XAUTOCLAIM`

Required behavior:

- iterate group PEL from a start cursor
- move idle-enough pending IDs to a target consumer
- return:
  - next cursor
  - claimed entries or IDs
  - deleted IDs if applicable
- `COUNT` validation must match Redis

### 7.8 `XINFO ... FULL`

Needed state:

- `last_generated_id`
- `max_deleted_entry_id`
- `entries_added`
- per-group PEL counts
- per-consumer pending count
- per-consumer `seen-time`, `active-time`, `idle`

### 7.9 `XDELEX ACKED` / Trimming / `XDEL`

Required behavior for compatibility:

- deletion must respect whether any group still references the ID
- `ACKED` decisions depend on:
  - no remaining PEL references
  - group last-delivered constraints
- first correctness slice should compute these by scanning group state
  rather than maintaining a fragile eager cache

## 8. Blocking / Reprocessing Design

### 8.1 Problem

The current generic blocking queue is keyed primarily by `(RedisKey, class)`.
That was enough to fix cross-class contamination, but it is still too coarse
for stream consumer groups.

For streams, readiness is not only a function of:

- key existence
- key type
- stream non-emptiness

It also depends on:

- command kind (`XREAD` vs `XREADGROUP`)
- group identity
- consumer identity
- read mode (`">"` vs history)
- preserved `$` / last-delivered snapshots

### 8.2 Proposed Design

Keep the existing owner-thread polling loop, but add stream-specific blocked
waiter metadata:

```rust
struct BlockedStreamWaiter {
    client_id: ClientId,
    key: RedisKey,
    command: BlockedStreamCommand,
    deadline: Option<Instant>,
}

enum BlockedStreamCommand {
    Xread {
        cursors: Vec<StreamReadCursor>,
        count: usize,
    },
    Xreadgroup {
        group: StreamGroupName,
        consumer: StreamConsumerName,
        mode: XreadgroupMode,
        noack: bool,
        count: usize,
    },
}
```

Where `XreadgroupMode` distinguishes:

- `NewEntries`
- `HistoryFrom(StreamId)`

Because a single stream key is already serialized on one owner thread, this
waiter registry can remain owner-local to that key. It does not require a new
shared lock or a cross-thread wake coordinator for correctness.

### 8.3 Wake Rules

Wake/reprocess reasons:

- `XADD`
- `DEL`
- type change (`SET`, `RENAME` to wrong type, transaction rewrite)
- `FLUSHDB` / `FLUSHALL`
- `XGROUP DESTROY`

Important rule:

- stream waiter readiness must be evaluated per waiter predicate, not by a
  generic "stream has entries" condition

### 8.4 Fairness Rule

For multiple blocked `XREADGROUP ... ">"` waiters on the same stream key:

- only the queue head should be reprocessed first
- if it consumes the new entry, followers remain blocked
- followers must not spin in an endless wake/requeue loop

This is the behavior behind Redis tests:

- `... avoid endless loop`
- `... reprocessing command`

## 9. Serialization Plan

Recommended approach:

- introduce a new canonical stream payload codec that encodes the richer state
- keep stream codec logic centralized in `value_codec` / object-store helpers
- do not contort the new model to fit the old payload shape
- make the new codec explicitly versioned rather than relying on trailing-field
  heuristics once PEL/consumer metadata is added

If a legacy decode path is needed temporarily, keep it as:

- old payload -> upgrade to the new in-memory model
- re-save only in the new format
- no long-term dual-write / dual-read complexity

## 10. Implementation Plan

### P0. Parser / Surface Normalization

Fix stream command surface mismatches that currently short-circuit suites:

- `XREAD` / `XREADGROUP` unbalanced argument errors
- `XGROUP CREATE` duplicate / `MKSTREAM` / `ENTRIESREAD`
- `XADD MAXLEN` option parsing needed by blocked reprocessing tests
- missing stream-side surface commands that block later verification

### P1. Consumer-Group State Redesign

Replace the current minimal `groups + consumer set` model with:

- group last-delivered state
- group PEL
- consumer state
- stream metadata fields needed by `XINFO`

### P2. `XREADGROUP` / `XPENDING` / `XACK`

Implement:

- `">"` delivery semantics
- history reads from consumer PEL
- summary and range `XPENDING`
- real `XACK`

This should eliminate most of the high-volume stream-cgroup failures.

### P3. `XCLAIM` / `XAUTOCLAIM`

Implement:

- pending ownership transfer
- retry / idle metadata updates
- `FORCE`, `JUSTID`, `LASTID`
- cursor semantics for `XAUTOCLAIM`

### P4. Blocked Stream Reprocessing

Implement the richer blocked waiter model and reprocessing rules for:

- same-key same-group waiter chains
- destroy/delete/type-change wake reasons
- no-endless-loop behavior

### P5. `XINFO FULL` / Deletion / Trimming

Finish:

- `XINFO FULL`
- `XDELEX ACKED`
- trim/delete interactions with still-pending IDs

## 11. Test Strategy

### 11.1 Required Rule

Before fixing any external failure in this area:

- add the exact external scenario as a Rust TCP integration test

Do not start with a reduced or in-memory-only approximation.

### 11.2 Rust Test Layers

1. TCP integration tests in `garnet-rs/crates/garnet-server/src/tests.rs`
   - exact external scenarios
2. request-lifecycle tests
   - parser / pure command semantics
3. codec tests
   - stream payload encode/decode / upgrade behavior

### 11.3 External Validation

For every slice:

1. targeted Rust tests
2. `make test-server`
3. targeted external:
   - `RUNTEXT_EXTRA_ARGS='--single unit/type/stream-cgroups'`
4. full compatibility workflow:
   - `garnet-rs/tests/interop/build_compatibility_report.sh`

No new Docker Compose setup is required for this stream-cgroups work; the
existing standalone TCP server and Redis external suite are sufficient.

### 11.4 Benchmark Plan

This work is correctness-first, but it touches a hot mutation/read path.

After each major data-model slice:

- benchmark at least:
  - `XADD`
  - `XREADGROUP ... >`
  - `XACK`
  - `XPENDING`

Do not introduce speculative caches before the exact TCP regressions exist and
before profiling shows a real hotspot.

## 12. TLA+ Plan

Add new models before the concurrency-sensitive slices land.

### 12.1 `StreamPelOwnership.tla`

Safety targets:

- a pending ID belongs to at most one consumer in a group
- group PEL and consumer pending sets stay consistent
- `XACK` removes ownership exactly once

### 12.2 `StreamBlockedReprocessing.tla`

Safety / liveness targets:

- head waiter reprocesses first
- followers do not starve behind wrong wake conditions
- a wake for one waiter does not force endless reprocessing of others

### 12.3 `StreamGroupDestroyWake.tla`

Safety targets:

- `XGROUP DESTROY` wakes blocked `XREADGROUP` waiters with `NOGROUP`
- destroy does not leave stale wait registrations behind

## 13. Recommendation

Proceed in this order:

1. parser / surface normalization
2. consumer-group / PEL state redesign
3. `XREADGROUP` history + `XPENDING` + `XACK`
4. `XCLAIM` / `XAUTOCLAIM`
5. blocked stream reprocessing
6. `XINFO FULL` / deletion / trimming

This order keeps suites debuggable:

- parser gaps stop hiding later stateful bugs
- PEL work unlocks most remaining compatibility
- blocked reprocessing becomes tractable once waiter predicates can be
  evaluated against real group state

## 14. Bottom Line

There is no fundamental incompatibility with the current threading model.

The blocker is not concurrency architecture; it is that the current stream
consumer-group data model is too weak.

The correct next step is a clean consumer-group / PEL redesign, not another
round of small blocking-path patches.
