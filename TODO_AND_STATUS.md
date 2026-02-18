# TODO & Status Tracker — garnet-rs

> **Last Updated**: 2026-02-18
> **Current Phase**: Phase 1 — Core Primitives
> **Current Iteration**: 7

---

## Status Legend

| Status | Meaning |
|--------|---------|
| `TODO` | Not started |
| `IN_PROGRESS` | Currently being worked on |
| `DONE` | Completed and verified (`cargo check` + `cargo test` pass) |
| `BLOCKED` | Cannot proceed — see notes |
| `DEFERRED` | Intentionally postponed |

---

## Phase 0: Project Scaffolding

| # | Task | Status | Notes |
|---|------|--------|-------|
| 0.1 | Create `garnet-rs/Cargo.toml` workspace root with all crate members | DONE | Added workspace root with all five crate members and resolver 2. |
| 0.2 | Create `garnet-rs/crates/garnet-common/` crate with empty `lib.rs` | DONE | Created crate manifest and placeholder `src/lib.rs`. |
| 0.3 | Create `garnet-rs/crates/tsavorite/` crate with empty `lib.rs` | DONE | Created crate manifest and placeholder `src/lib.rs`. |
| 0.4 | Create `garnet-rs/crates/garnet-server/` crate with empty `lib.rs` + `main.rs` | DONE | Created crate manifest, placeholder `src/lib.rs`, and no-op `src/main.rs`. |
| 0.5 | Create `garnet-rs/crates/garnet-cluster/` crate with empty `lib.rs` | DONE | Created crate manifest and placeholder `src/lib.rs`. |
| 0.6 | Create `garnet-rs/crates/garnet-client/` crate with empty `lib.rs` | DONE | Created crate manifest and placeholder `src/lib.rs`. |
| 0.7 | Create `garnet-rs/benches/` and `garnet-rs/tests/` directories with placeholder files | DONE | Added directories with tracked `README.md` placeholder files. |
| 0.8 | Create `rust-toolchain.toml` at repo root (edition 2021, stable or nightly as needed) | DONE | Added stable toolchain config with `rustfmt` and `clippy` components. |
| 0.9 | Add workspace dependencies in root `Cargo.toml` | DONE | Added suggested shared dependencies under `[workspace.dependencies]`. |
| 0.10 | Verify `cargo check` and `cargo test` pass on empty workspace | DONE | Verified from `garnet-rs/`: both commands succeeded. |

---

## Phase 1: Core Primitives (`garnet-common` + `tsavorite` foundations)

**Read before starting**: docs/reimplementation/02, 04, 06, 15

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 1.1 | Implement `SpanByte` — variable-length byte sequence with 4-byte length prefix | DONE | 0.10 | Implemented in `garnet-common` as `SpanByte` packed header + `SpanByteRef`/`SpanByteRefMut` zero-copy views with safe `as_slice()` / `as_mut_slice()` wrappers and header bit validation. |
| 1.2 | Implement `RecordInfo` — 64-bit packed bitfield | DONE | 0.10 | Implemented as `#[repr(C)]` 8-byte packed header in `tsavorite` with previous-address masking, flag accessors/mutators, closed/scan semantics, and size/bitfield unit tests. |
| 1.3 | Implement `LightEpoch` — epoch protection with cache-line-padded table | DONE | 0.10 | Implemented `LightEpoch` with `#[repr(C, align(64))] EpochEntry`, per-thread pin/unpin guard API, global/safe epoch tracking, and deferred drain callbacks. |
| 1.4 | Implement `HashBucket` — 64-byte cache-line-aligned bucket with 7 entries + overflow | DONE | 0.10 | Implemented `#[repr(C, align(64))] HashBucket` with 7 data entries + overflow/latch word, overflow address helpers, and size/alignment tests (`64` bytes). |
| 1.5 | Implement `HashBucketEntry` — 8-byte packed entry (tag, address, tentative, pending) | DONE | 0.10 | Implemented atomic packed entry with tag/address/flag packing helpers, read-cache bit support, and CAS update APIs over `AtomicU64`. |
| 1.6 | Unit tests for all Phase 1 types | DONE | 1.1-1.5 | Added size/alignment and bitfield/behavior tests across Phase 1 primitives, including `proptest` roundtrip coverage for SpanByte serialization. |
| 1.7 | Benchmark Phase 1 hot-path operations | TODO | 1.6 | `criterion` benchmarks for RecordInfo bit ops, HashBucketEntry CAS, epoch pin/unpin. |

---

## Phase 2: Hybrid Log Allocator (`tsavorite`)

**Read before starting**: docs/reimplementation/02, 06

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 2.1 | Implement page management — allocate/flush/evict pages, logical address space (page + offset) | TODO | 1.6 | See doc 02. Page = contiguous byte buffer. LogicalAddress = page_index << page_size_bits | offset. |
| 2.2 | Implement log address pointers — TailAddress, ReadOnlyAddress, SafeReadOnlyAddress, HeadAddress, SafeHeadAddress, BeginAddress | TODO | 2.1 | See doc 02. AtomicU64 for each. Monotonically increasing. Fuzzy region between ReadOnly and SafeReadOnly. |
| 2.3 | Implement lock-free tail allocation — `AtomicU64::fetch_add` for log append | TODO | 2.2 | See doc 02. CAS loop for page boundary crossing. Seal current page, allocate new page, retry. |
| 2.4 | Implement record format — RecordInfo + key SpanByte + value SpanByte, variable-length records | TODO | 1.1, 1.2, 2.1 | See doc 06. Record = RecordInfo (8B) + key length (4B) + key data + padding + value length (4B) + value data + padding. 8-byte alignment. |
| 2.5 | Implement page I/O — read/write pages to storage device abstraction | TODO | 2.1 | See doc 02. `IDevice` trait with `ReadAsync`/`WriteAsync`. Start with in-memory device for testing. |
| 2.6 | Implement page eviction and read-from-disk path | TODO | 2.2, 2.5 | See doc 02. When address < HeadAddress, page is on disk. Async read + callback. |
| 2.7 | Unit tests for hybrid log allocator | TODO | 2.1-2.6 | Allocation correctness, page boundary crossing, concurrent allocation stress test. |

---

## Phase 3: Hash Index (`tsavorite`)

**Read before starting**: docs/reimplementation/03, 06

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 3.1 | Implement hash index structure — power-of-2 bucket array, tag-based matching | TODO | 1.4, 1.5 | See doc 03. `num_buckets = 2^index_size_bits`. Hash → bucket_index (lower bits) + tag (upper 14 bits). |
| 3.2 | Implement `FindTag` / `FindOrCreateTag` — CAS-based entry insertion | TODO | 3.1 | See doc 03. Scan bucket entries, match tag. CAS for tentative insert. Overflow chain traversal. |
| 3.3 | Implement overflow bucket allocation — fixed-page allocator for overflow chains | TODO | 3.1 | See doc 03. Pre-allocated overflow page pool. Thread-local free lists for lock-free allocation. |
| 3.4 | Implement transient S/X locking — shared/exclusive latch on bucket overflow entry | TODO | 3.1 | See doc 03 §transient locking. Lock word in overflow pointer slot. Readers take S, writers take X. |
| 3.5 | Unit tests for hash index | TODO | 3.1-3.4 | Concurrent insert/lookup, overflow chain correctness, tag collision handling. Use `loom` for concurrency testing. |

---

## Phase 4: CRUD Operations (`tsavorite`)

**Read before starting**: docs/reimplementation/06 (especially §2.12), 10

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 4.1 | Define `ISessionFunctions` trait — generic callback interface | TODO | 2.4 | See doc 10. `Reader`, `Writer`, `Comparer` associated types. `ConcurrentReader`, `SingleReader`, `SingleWriter`, `ConcurrentWriter`, `InPlaceUpdater`, `CopyUpdater` callbacks. |
| 4.2 | Implement `Read` operation — hash lookup → chain traversal → reader callbacks | TODO | 3.2, 4.1 | See doc 06 §2.12 + doc 09. Address >= SafeReadOnlyAddress → ConcurrentReader. Address >= HeadAddress but < SafeReadOnlyAddress → RETRY_LATER. Address < HeadAddress → async I/O path. |
| 4.3 | Implement `Upsert` operation — hash lookup → IPU or RCU → CAS into chain | TODO | 3.2, 4.1 | See doc 06. Mutable region → in-place update (IPU). Read-only or not found → record copy update (RCU), append to tail, CAS hash entry. |
| 4.4 | Implement `RMW` (Read-Modify-Write) — with fuzzy region RETRY_LATER | TODO | 4.2, 4.3 | See doc 06. Mutable → InPlaceUpdater. ReadOnly/Fuzzy → RETRY_LATER (critical: fuzzy region between ReadOnlyAddress and SafeReadOnlyAddress). OnDisk → CopyUpdater + append. |
| 4.5 | Implement `Delete` — tombstone creation | TODO | 4.3 | See doc 06. Create tombstone record (RecordInfo.Tombstone = true). |
| 4.6 | Implement `TsavoriteKV<K, V>` facade — public API wrapping all operations | TODO | 4.2-4.5 | Generic over key/value types. Session management. Epoch integration. |
| 4.7 | Unit tests for CRUD operations | TODO | 4.2-4.5 | Single-threaded correctness, concurrent read/write stress, tombstone handling, fuzzy region retry. |
| 4.8 | Integration test: end-to-end key-value store | TODO | 4.6 | Insert N keys, read all back, delete half, verify. Concurrent version. |

---

## Phase 5: Network Layer (`garnet-server`, `garnet-common`)

**Read before starting**: docs/reimplementation/07, 08, 09

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 5.1 | Implement TCP accept loop and connection handler | TODO | 0.10 | See doc 07. tokio-based TCP listener. Per-connection task. |
| 5.2 | Implement `LimitedFixedBufferPool` — tiered pool with power-of-2 size classes | TODO | 0.10 | See doc 07 §2.5. Size classes: 256B, 512B, ..., 1MB. Pre-allocated buffers. Thread-safe checkout/return. |
| 5.3 | Implement RESP protocol parser — zero-copy `&[u8]` based | TODO | 0.10 | See doc 08. Parse `*count\r\n$len\r\n...` format. Return `&[u8]` slices into input buffer. No allocation. |
| 5.4 | Implement `ArgSlice` — 12-byte pointer+length reference to RESP argument | TODO | 5.3 | See doc 08. `ptr: *const u8` + `length: i32` = 12 bytes. Points into receive buffer. |
| 5.5 | Implement command dispatch — `match` on command byte pattern | TODO | 5.3 | See doc 09. Fast path: match first bytes for GET/SET/DEL/INCR. Fallback: full command table lookup. |
| 5.6 | Implement request lifecycle — parse → dispatch → storage op → response write → send | TODO | 5.1-5.5, 4.6 | See doc 09. Full pipeline from TCP receive to TCP send. |
| 5.7 | Unit tests for RESP parser | TODO | 5.3 | Valid/invalid RESP parsing. Partial message handling. Fuzz with `cargo-fuzz`. |
| 5.8 | Unit tests for buffer pool | TODO | 5.2 | Allocation/deallocation, pool exhaustion, size class selection. |

---

## Phase 6: Basic Redis Commands

**Read before starting**: docs/reimplementation/09, 10

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 6.1 | Implement `GET` command — full path from TCP to response | TODO | 5.6 | See doc 09. RESP parse → hash lookup → ConcurrentReader → RESP bulk string response. |
| 6.2 | Implement `SET` command — upsert path | TODO | 5.6 | See doc 09. RESP parse → Upsert → `+OK\r\n` response. Handle EX/PX/NX/XX options. |
| 6.3 | Implement `DEL` command — delete path | TODO | 5.6 | Delete → tombstone → `:1\r\n` or `:0\r\n`. |
| 6.4 | Implement `INCR`/`DECR` commands — RMW path with in-place update | TODO | 5.6 | RMW → InPlaceUpdater (parse int, add 1, write back). Handle non-integer error. |
| 6.5 | Implement `EXPIRE`/`TTL`/`PEXPIRE`/`PTTL` — expiration metadata | TODO | 6.2 | RecordInfo.HasExpiration flag + expiration timestamp in record. Background expiry scan. |
| 6.6 | Implement `PING`/`ECHO`/`INFO`/`DBSIZE`/`COMMAND` — utility commands | TODO | 5.6 | Simple response generation. No storage interaction for PING/ECHO. |
| 6.7 | Integration test: redis-cli compatibility | TODO | 6.1-6.6 | Start server, connect with `redis-cli`, run GET/SET/DEL/INCR/PING. |
| 6.8 | Benchmark: GET/SET throughput and latency | TODO | 6.1, 6.2 | `criterion` or custom benchmark. Target: GET < 10µs p50, > 1M ops/sec single core. |

---

## Phase 7: Object Store & Data Structures

**Read before starting**: docs/reimplementation/11

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 7.1 | Implement object store — second `TsavoriteKV` instance for complex types | TODO | 4.6 | See doc 11. Key = SpanByte, Value = serialized object (type tag + data). |
| 7.2 | Implement Hash — HGET/HSET/HDEL/HGETALL | TODO | 7.1 | Internal HashMap per key. RMW for mutations. |
| 7.3 | Implement List — LPUSH/RPUSH/LPOP/RPOP/LRANGE | TODO | 7.1 | VecDeque or doubly-linked list. |
| 7.4 | Implement Set — SADD/SREM/SMEMBERS/SISMEMBER | TODO | 7.1 | Internal HashSet per key. |
| 7.5 | Implement SortedSet — ZADD/ZREM/ZRANGE/ZSCORE | TODO | 7.1 | Skip list or BTreeMap for ordered access. |
| 7.6 | Unit tests for all data structures | TODO | 7.2-7.5 | CRUD correctness, edge cases, serialization roundtrip. |

---

## Phase 8: Checkpointing & AOF

**Read before starting**: docs/reimplementation/05, 14

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 8.1 | Implement checkpoint state machine — fold-over and snapshot modes | TODO | 4.6, 1.3 | See doc 05. State transitions: REST → PREPARE → IN_PROGRESS → WAIT_FLUSH → PERSISTENCE_CALLBACK → REST. Epoch-based coordination. |
| 8.2 | Implement AOF writer — append-only log with TsavoriteLog | TODO | 2.5 | See doc 14. Sequential log. Each entry: header + serialized operation. Flush policy (every N ops or every M ms). |
| 8.3 | Implement AOF replay — recovery from AOF | TODO | 8.2 | See doc 14. Read AOF entries, replay operations against store. Idempotency handling. |
| 8.4 | Implement checkpoint + AOF coordination | TODO | 8.1, 8.2 | See doc 14. Checkpoint truncates AOF. Recovery = restore checkpoint + replay AOF tail. |
| 8.5 | Integration test: crash recovery | TODO | 8.4 | Insert data → checkpoint → more inserts → simulate crash → recover → verify all data. |

---

## Phase 9: Transactions

**Read before starting**: docs/reimplementation/12

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 9.1 | Implement MULTI/EXEC/DISCARD — command queuing and atomic replay | TODO | 6.1-6.4 | See doc 12. Queue commands during MULTI. Execute atomically on EXEC. Sorted lock acquisition for deadlock freedom. |
| 9.2 | Implement WATCH — optimistic locking via WatchVersionMap | TODO | 9.1 | See doc 12. Track key versions at WATCH time. Abort EXEC if any watched key changed. Note: GarnetWatchApi is read-only (IGarnetReadApi only). |
| 9.3 | Unit tests for transactions | TODO | 9.1, 9.2 | MULTI/EXEC correctness, WATCH conflict detection, DISCARD behavior. |

---

## Phase 10: Cluster

**Read before starting**: docs/reimplementation/13, 14

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 10.1 | Implement cluster config — 16384 hash slots, immutable copy-on-write config | TODO | 6.6 | See doc 13. ClusterConfig is immutable, replaced atomically. Worker array indexed by slot → node mapping. |
| 10.2 | Implement gossip protocol — failure-budget sampling | TODO | 10.1 | See doc 13. `count` = failure budget (NOT success target). Decrement on failure, increment on success. Random node selection. |
| 10.3 | Implement replication — primary-replica sync via checkpoint + AOF | TODO | 8.4, 10.1 | See doc 13 + 14. Full sync = send checkpoint + AOF tail. Incremental = stream AOF. |
| 10.4 | Implement slot migration — MOVED/ASK redirections | TODO | 10.1 | See doc 13. MOVED for completed migrations. ASK for in-progress migrations. |
| 10.5 | Integration test: multi-node cluster | TODO | 10.1-10.4 | Start 3 nodes, verify slot routing, test failover. |

---

## Design Decisions Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-02-18 | Use a pure virtual workspace at `garnet-rs/Cargo.toml` with five member crates and placeholder sources. | Keeps Phase 0 scaffolding minimal while enabling immediate workspace-wide `cargo check`/`cargo test`. |
| 2026-02-18 | Pin `rust-toolchain.toml` to `stable` with `rustfmt` and `clippy`. | Matches Phase 0 requirement and keeps early iterations portable before Linux-specific `io_uring` decisions are needed. |
| 2026-02-18 | Model SpanByte as a packed 4-byte `#[repr(C)]` header plus borrowed serialized views (`SpanByteRef`, `SpanByteRefMut`) instead of a pointer-bearing struct. | Preserves on-disk/in-log layout semantics while avoiding unsafe pointer ownership in early Rust scaffolding. |
| 2026-02-18 | Keep RecordInfo’s canonical Tsavorite bit layout (including `Modified`/`VectorSet`) and place `HasExpiration` in a reclaimed leftover bit for planned Garnet metadata. | Preserves C# parity for existing bits while satisfying planned expiration flag support from the tracker. |
| 2026-02-18 | Implement LightEpoch with thread-local slot/depth bookkeeping and RAII `EpochGuard` instead of explicit suspend/resume API on callers. | Matches Rust ownership ergonomics (`let guard = epoch.pin()`) while retaining cache-line-padded entry table and deferred drain semantics. |
| 2026-02-18 | Implement HashBucketEntry as `AtomicU64`-backed packed word with standalone pack/unpack helpers plus CAS entrypoint. | Keeps hot-path updates lock-free and allows direct bit-level compatibility with Tsavorite hash index encoding. |
| 2026-02-18 | Represent HashBucket overflow entry as a single atomic word carrying both overflow address bits and latch bits. | Mirrors Tsavorite’s compact overflow+latch encoding while keeping 64-byte one-cache-line bucket size. |
| 2026-02-18 | Use property-based tests (`proptest`) for SpanByte serialized roundtrip behavior in addition to deterministic unit tests. | Catches edge-case payload combinations while preserving explicit regression tests for layout/flag rules. |

---

## Doc Gaps Discovered

| Doc | Gap Description | Impact | Workaround |
|-----|----------------|--------|------------|
| (none yet) | | | |

---

## Iteration Log

| Iteration | Date | Task(s) Worked | Outcome | Notes |
|-----------|------|---------------|---------|-------|
| 1 | 2026-02-18 | 0.1-0.10 | DONE | Phase 0 scaffolding completed; workspace compiles/tests cleanly and is ready for Phase 1 (`SpanByte`). |
| 2 | 2026-02-18 | 1.1 | DONE | Added SpanByte header parsing/serialization, metadata-bit handling, safe payload slice wrappers, and unit tests in `garnet-common`. |
| 3 | 2026-02-18 | 1.2 | DONE | Added 8-byte `RecordInfo` with bitfield APIs and tests (`size_of == 8`, previous-address masking, lifecycle flag behavior). |
| 4 | 2026-02-18 | 1.3 | DONE | Added `LightEpoch` and `EpochGuard` with 64-byte aligned epoch entries, per-thread pin/unpin behavior, and drain callback execution. |
| 5 | 2026-02-18 | 1.5 | DONE | Added `HashBucketEntry` with 14-bit tag + 48-bit address encoding, tentative/pending/read-cache handling, and CAS-oriented APIs/tests. |
| 6 | 2026-02-18 | 1.4 | DONE | Added 64-byte `HashBucket` layout with seven data entries, overflow/latch atomic word, and validation tests. |
| 7 | 2026-02-18 | 1.6 | DONE | Completed Phase 1 unit-test coverage, including proptest roundtrip checks for SpanByte and behavior tests for RecordInfo/LightEpoch/HashBucketEntry/HashBucket. |
