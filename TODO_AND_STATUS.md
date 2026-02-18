# TODO & Status Tracker — garnet-rs

> **Last Updated**: 2026-02-18
> **Current Phase**: Phase 5 — Network Layer
> **Current Iteration**: 34

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
| 1.7 | Benchmark Phase 1 hot-path operations | DONE | 1.6 | Added Criterion benchmark target (`phase1_hotpath`) in `tsavorite` for RecordInfo bit ops, HashBucketEntry CAS, and LightEpoch pin/unpin; benchmark target builds successfully. |

---

## Phase 2: Hybrid Log Allocator (`tsavorite`)

**Read before starting**: docs/reimplementation/02, 06

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 2.1 | Implement page management — allocate/flush/evict pages, logical address space (page + offset) | DONE | 1.6 | Added `hybrid_log::PageManager` and `PageAddressSpace` with logical address encode/decode, page allocation, flush/eviction checks, and page-local read/write APIs plus lifecycle tests. |
| 2.2 | Implement log address pointers — TailAddress, ReadOnlyAddress, SafeReadOnlyAddress, HeadAddress, SafeHeadAddress, BeginAddress | DONE | 2.1 | Added `LogAddressPointers` with `AtomicU64` fields for all six pointers, monotonic shift APIs, tail `fetch_add`, and snapshot/test coverage for non-regressing pointer movement. |
| 2.3 | Implement lock-free tail allocation — `AtomicU64::fetch_add` for log append | DONE | 2.2 | Added `TailAllocator` with fetch-add reservation fast path, page-turn handling (`RetryNow`/`RetryLater`), sealed-page tracking, and page preallocation for next/next+1 pages. |
| 2.4 | Implement record format — RecordInfo + key SpanByte + value SpanByte, variable-length records | DONE | 1.1, 1.2, 2.1 | Added `hybrid_log::record_format` with size/layout computation, aligned serialization (`write_record`), and parse helpers for RecordInfo/key/value SpanByte views. |
| 2.5 | Implement page I/O — read/write pages to storage device abstraction | DONE | 2.1 | Added `PageDevice` trait (`read_async`/`write_async`), `InMemoryPageDevice`, and `flush_page_to_device` / `load_page_from_device` helpers with roundtrip + error-path tests. |
| 2.6 | Implement page eviction and read-from-disk path | DONE | 2.2, 2.5 | Added `page_residency` helpers for monotonic `HeadAddress` shift + flush/evict, plus disk fallback read path (`read_with_callback`) that loads pages when `address < HeadAddress`. |
| 2.7 | Unit tests for hybrid log allocator | DONE | 2.1-2.6 | Added allocator stress coverage: multi-page non-overlap checks and concurrent `TailAllocator` allocation uniqueness tests, alongside page residency read/eviction tests from 2.6. |

---

## Phase 3: Hash Index (`tsavorite`)

**Read before starting**: docs/reimplementation/03, 06

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 3.1 | Implement hash index structure — power-of-2 bucket array, tag-based matching | DONE | 1.4, 1.5 | Added `HashIndex` with power-of-2 bucket allocation (`size_bits`), `hash -> (bucket_index, tag)` mapping, primary-bucket tag scan, and coverage tests. |
| 3.2 | Implement `FindTag` / `FindOrCreateTag` — CAS-based entry insertion | DONE | 3.1 | Added `HashIndex::find_tag` and CAS-based `find_or_create_tag` with tentative-slot install, duplicate-slot check, truncated-entry reclaim, and explicit `BucketFullNeedsOverflow` handoff for overflow path. |
| 3.3 | Implement overflow bucket allocation — fixed-page allocator for overflow chains | DONE | 3.1 | Added `OverflowBucketAllocator` with 64K-bucket fixed pages, lock-free free-list reuse (`SegQueue`), and integrated overflow-chain allocation/linking in `HashIndex::find_or_create_tag`. |
| 3.4 | Implement transient S/X locking — shared/exclusive latch on bucket overflow entry | DONE | 3.1 | Added `HashBucket` transient latch APIs: `try_acquire_shared_latch`, `try_acquire_exclusive_latch`, `try_promote_latch`, and corresponding release operations with bounded spin/drain behavior. |
| 3.5 | Unit tests for hash index | DONE | 3.1-3.4 | Added concurrent same-tag/different-tag insertion tests, overflow-chain reachability checks, and collision-path regression coverage for `find_or_create_tag` + `find_tag`. |

---

## Phase 4: CRUD Operations (`tsavorite`)

**Read before starting**: docs/reimplementation/06 (especially §2.12), 10

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 4.1 | Define `ISessionFunctions` trait — generic callback interface | DONE | 2.4 | Added `session_functions` module with `ISessionFunctions` trait, operation info structs (`ReadInfo`, `UpsertInfo`, `RmwInfo`), `WriteReason`, and callback signatures for read/upsert/RMW paths. |
| 4.2 | Implement `Read` operation — hash lookup → chain traversal → reader callbacks | DONE | 3.2, 4.1 | Added `read_operation::read` with hash-entry address lookup, previous-address chain traversal, key match filtering, region-aware reader callback dispatch (`ConcurrentReader` vs `SingleReader`), closed/tombstone handling, and disk-resident page loading path. |
| 4.3 | Implement `Upsert` operation — hash lookup → IPU or RCU → CAS into chain | DONE | 3.2, 4.1 | Added `upsert_operation::upsert` with tag-head lookup, key-chain search, mutable-region IPU (same-size fast path), tail copy-update/insert append, and hash-entry address CAS replacement. |
| 4.4 | Implement `RMW` (Read-Modify-Write) — with fuzzy region RETRY_LATER | DONE | 4.2, 4.3 | Added `rmw_operation::rmw` with key-chain search, fuzzy-region `RetryLater` handling (`safe_read_only <= addr < read_only`), mutable-region IPU, and copy-update append + hash-head CAS fallback. |
| 4.5 | Implement `Delete` — tombstone creation | DONE | 4.3 | Added `delete_operation::delete` with mutable-region in-place tombstone path, immutable append-tombstone path (`previous_address` linked), and hash-head CAS update plus not-found/retry handling tests. |
| 4.6 | Implement `TsavoriteKV<K, V>` facade — public API wrapping all operations | DONE | 4.2-4.5 | Added `tsavorite_kv` module with generic `TsavoriteKV<K, V, D>`, `TsavoriteSession`, config/init error types, per-operation epoch pinning, hash-based key routing, pointer/head management helpers, and wrappers for read/upsert/rmw/delete. |
| 4.7 | Unit tests for CRUD operations | DONE | 4.2-4.5 | Added facade-level CRUD unit tests for single-thread roundtrip, fuzzy-region `RetryLater`, tombstone visibility, and multi-thread read/write stress through session API. |
| 4.8 | Integration test: end-to-end key-value store | DONE | 4.6 | Added `tsavorite/tests/crud_integration.rs` with end-to-end insert/read/delete verification for N keys and a concurrent insert/read/delete variant. |

---

## Phase 5: Network Layer (`garnet-server`, `garnet-common`)

**Read before starting**: docs/reimplementation/07, 08, 09

| # | Task | Status | Depends On | Notes |
|---|------|--------|------------|-------|
| 5.1 | Implement TCP accept loop and connection handler | DONE | 0.10 | Added async TCP server in `garnet-server` using `tokio::net::TcpListener`, per-connection spawned handler tasks, shutdown-aware accept loop, connection/byte metrics, and unit tests for accept+shutdown behavior. |
| 5.2 | Implement `LimitedFixedBufferPool` — tiered pool with power-of-2 size classes | DONE | 0.10 | Added `limited_fixed_buffer_pool` module with power-of-two level mapping, bounded per-level queues (`ArrayQueue`), secure clear-on-return behavior, overflow-drop semantics, and concurrent unit tests. |
| 5.3 | Implement RESP protocol parser — zero-copy `&[u8]` based | DONE | 0.10 | Added `garnet-common::resp` parser (`parse_resp_command`) for `*<n>\r\n$<len>\r\n...` frames, returning zero-copy argument slices via caller-provided output storage, with partial-frame/error handling tests. |
| 5.4 | Implement `ArgSlice` — 12-byte pointer+length reference to RESP argument | DONE | 5.3 | Added packed 12-byte `ArgSlice` in `garnet-common` with pointer/length accessors, conversion from byte slices, unsafe view reconstruction, layout tests, and RESP parser output support (`parse_resp_command_arg_slices`). |
| 5.5 | Implement command dispatch — `match` on command byte pattern | DONE | 5.3 | Added `garnet-server::command_dispatch` with case-insensitive fast-path byte checks for GET/SET/DEL/INCR and fallback lookup for additional command names, including ArgSlice-based dispatch entrypoints and tests. |
| 5.6 | Implement request lifecycle — parse → dispatch → storage op → response write → send | DONE | 5.1-5.5, 4.6 | Added `request_lifecycle` module with command execution over shared `TsavoriteKV`, RESP response builders, and connection-loop integration that parses frames, dispatches commands, executes storage ops, writes responses, and handles partial frames/protocol errors. |
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
| 2026-02-18 | Place Phase 1 hot-path Criterion benchmarks under `tsavorite/benches` instead of workspace root. | The workspace root is virtual (no root package), so benchmark targets must live in an actual crate. |
| 2026-02-18 | Introduce a dedicated `PageAddressSpace` helper for logical address encoding/decoding and reuse it through `PageManager`. | Keeps address math centralized and explicit for later Tail/Head pointer work in Phase 2. |
| 2026-02-18 | Centralize region-boundary pointers in `LogAddressPointers` and expose only monotonic shift operations. | Enforces one-way address movement invariants by construction and simplifies future checkpoint/fuzzy-region logic. |
| 2026-02-18 | Use a `TryLock`-gated page-turn path in `TailAllocator` and return `RetryNow` when another thread is currently handling boundary crossing. | Preserves lock-free fast path while avoiding blocking contention in boundary-turn slow path. |
| 2026-02-18 | Depend on `garnet-common` from `tsavorite` for SpanByte parsing/serialization in record layout helpers. | Avoids duplicate length-prefix parsing logic and keeps SpanByte wire-format semantics centralized. |
| 2026-02-18 | Model page-device interaction as synchronous trait calls with async-compatible method names (`read_async`/`write_async`) in Phase 2. | Keeps early allocator logic deterministic/testable while preserving an API shape that can later map to actual async I/O backends. |
| 2026-02-18 | Implement the on-disk read path as a callback-based helper (`read_with_callback`) that synchronously ensures residency before invoking the completion callback. | Preserves the future async completion shape while keeping Phase 2 tests deterministic and easy to validate. |
| 2026-02-18 | Stress-test `TailAllocator` with multi-threaded allocation loops that tolerate `RetryNow` and assert unique logical addresses. | Validates lock-free reservation semantics under contention without introducing unstable timing-dependent assertions. |
| 2026-02-18 | Start hash-index implementation with a fixed-size `HashIndex` table (`Box<[HashBucket]>`) and explicit hash split helpers before CAS insertion logic. | Establishes deterministic bucket/tag addressing invariants first, reducing complexity when adding concurrent `FindOrCreateTag` flows in the next step. |
| 2026-02-18 | Keep `FindOrCreateTag` scoped to primary-bucket CAS insertion for now and return an explicit `BucketFullNeedsOverflow` signal when no free slot exists. | Allows validation of tentative-insert correctness immediately while isolating overflow allocator complexity into task 3.3. |
| 2026-02-18 | Use a dedicated `OverflowBucketAllocator` with fixed-size page chunks and a concurrent free-list (`SegQueue`) for overflow bucket reuse. | Preserves stable logical-address mapping for overflow chains while avoiding per-bucket heap allocation churn under collisions. |
| 2026-02-18 | Implement bucket latching directly on the overflow word using bounded CAS spin loops and explicit reader-drain limits. | Mirrors Tsavorite’s transient lock semantics while preventing unbounded waits during exclusive acquisition/promotion. |
| 2026-02-18 | Validate hash-index collision paths with multi-threaded `find_or_create_tag` tests that force overflow-chain growth and concurrent duplicate-tag races. | Provides practical contention coverage ahead of CRUD integration without yet introducing `loom` model-checking overhead. |
| 2026-02-18 | Define `ISessionFunctions` in Rust as a trait with associated marker types (`Reader`/`Writer`/`Comparer`) plus explicit read/upsert/RMW callback contracts. | Establishes a stable generic callback surface before wiring operation pipelines in Phase 4. |
| 2026-02-18 | Introduce a `HybridLogReadAdapter` bridge trait so generic `ISessionFunctions` implementations can consume raw log bytes without coupling read-path logic to a concrete key/value type. | Keeps read traversal generic while allowing zero-copy-ish log parsing to stay inside storage-engine modules. |
| 2026-02-18 | Implement Upsert as a two-stage path: attempt mutable in-place rewrite when size is unchanged, otherwise append a new record and CAS-swap the hash-entry head address. | Preserves core Tsavorite mutation semantics while keeping the first Rust implementation tractable before richer in-place growth/filler handling. |
| 2026-02-18 | Model RMW as a dedicated operation module that explicitly checks the address-based fuzzy window before deciding IPU vs copy-update. | Preserves lost-update prevention semantics from Tsavorite while reusing the same append/CAS primitives introduced for Upsert. |
| 2026-02-18 | Implement Delete with a dual path: mutable-region in-place tombstone rewrite when callback output keeps value size stable, otherwise append a tombstone record and CAS-swap hash head. | Preserves Tsavorite delete semantics while minimizing extra appends in mutable regions and retaining lock-free head update behavior in immutable regions. |
| 2026-02-18 | Add a `TsavoriteKV<K, V, D>` facade with `TsavoriteSession` that binds callbacks and computes key hashes internally while pinning LightEpoch per operation. | Provides a practical public API boundary above low-level operation modules without sacrificing current callback-driven semantics or epoch coordination. |
| 2026-02-18 | Validate CRUD through both unit-level facade tests and crate-level integration tests (`tests/crud_integration.rs`) including concurrent workflows. | Improves confidence that composed read/upsert/rmw/delete behavior stays correct beyond isolated operation-module tests. |
| 2026-02-18 | Implement the initial network server as a shutdown-aware Tokio accept loop (`run_with_shutdown`) with per-connection task spawning and lightweight connection metrics. | Delivers Phase 5.1 functionality with testable lifecycle behavior before layering RESP parsing and command dispatch in later steps. |
| 2026-02-18 | Model `LimitedFixedBufferPool` as fixed power-of-two levels backed by bounded `crossbeam_queue::ArrayQueue<Vec<u8>>` and clear buffers before reuse. | Matches Garnet’s tiered reuse semantics while keeping checkout/return lock-free and preventing stale data leakage across connections. |
| 2026-02-18 | Implement RESP command parsing as a zero-copy routine that writes argument `&[u8]` slices into caller-provided storage instead of allocating a vector per request. | Preserves parse-time zero-copy semantics and enables partial-frame handling without introducing heap churn in the hot receive path. |
| 2026-02-18 | Represent `ArgSlice` as a packed 12-byte pointer+length record and expose an unsafe `as_slice` view API for caller-controlled lifetime reconstruction. | Matches the documented compact layout target while keeping unsafe boundaries explicit at the conversion point. |
| 2026-02-18 | Implement command-name dispatch with fixed-length ASCII fast-path checks before falling back to a slower lookup branch. | Keeps hot command routing (`GET`/`SET`/`DEL`/`INCR`) branch-light while retaining extensibility for less frequent commands. |
| 2026-02-18 | Build request lifecycle around a shared `RequestProcessor` that combines RESP parsing, command dispatch, Tsavorite-backed storage callbacks, and inline RESP encoding within the connection loop. | Delivers an end-to-end receive-to-send pipeline in Phase 5 while keeping parser/dispatch/storage responsibilities modular and testable. |

---

## Doc Gaps Discovered

| Doc | Gap Description | Impact | Workaround |
|-----|----------------|--------|------------|
| 02-tsavorite-hybrid-log | Exact concurrent correction protocol for overflowing `fetch_add` reservations (how to avoid transient over-reservation without losing monotonicity) is only described at high level. | Medium: current Rust `TailAllocator` may pessimistically return retries in high contention page-turn windows. | Implemented conservative retry-based page-turn with monotonic tail shifts; flagged for refinement when deeper allocator invariants are documented. |

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
| 8 | 2026-02-18 | 1.7 | DONE | Added and validated Criterion benchmarks for core Phase 1 operations (`cargo bench -p tsavorite --bench phase1_hotpath --no-run`). |
| 9 | 2026-02-18 | 2.1 | DONE | Added initial hybrid-log page manager with logical-address mapping plus allocate/flush/evict lifecycle and bounds-checked page I/O helpers. |
| 10 | 2026-02-18 | 2.2 | DONE | Added atomic log-region pointer set (`Tail/RO/SafeRO/Head/SafeHead/Begin`) with monotonic updates and snapshot helpers. |
| 11 | 2026-02-18 | 2.3 | DONE | Added lock-free style tail allocation with retry statuses, page-turn sealing, and boundary-crossing tests. |
| 12 | 2026-02-18 | 2.4 | DONE | Added record layout math and serialization/parsing for `RecordInfo + SpanByte key + SpanByte value` with 8-byte alignment guarantees. |
| 13 | 2026-02-18 | 2.5 | DONE | Added page I/O abstraction (`PageDevice`), in-memory device implementation, and flush/load helper flow with roundtrip and error-path tests. |
| 14 | 2026-02-18 | 2.6 | DONE | Added head-shift eviction helper with flush-before-evict behavior and an `address < HeadAddress` disk fallback read path with callback completion semantics. |
| 15 | 2026-02-18 | 2.7 | DONE | Expanded hybrid-log allocator tests with multi-page allocation integrity and concurrent tail-allocation stress uniqueness checks. |
| 16 | 2026-02-18 | 3.1 | DONE | Added initial hash-index structure (`HashIndex`) with power-of-two bucket array, bucket/tag extraction, and primary-bucket non-tentative tag matching tests. |
| 17 | 2026-02-18 | 3.2 | DONE | Added `FindTag`/`FindOrCreateTag` behavior on primary buckets using CAS tentative insertion, duplicate-slot resolution, and stale-entry reclamation tests. |
| 18 | 2026-02-18 | 3.3 | DONE | Added fixed-page overflow allocator and wired overflow-bucket chain allocation into hash-index insertion when primary buckets are full. |
| 19 | 2026-02-18 | 3.4 | DONE | Added transient shared/exclusive/promote latch operations on hash-bucket overflow entries with unit tests for lock-state transitions. |
| 20 | 2026-02-18 | 3.5 | DONE | Added concurrent hash-index tests covering same-tag convergence, overflow-chain collision inserts, and post-insert lookup correctness. |
| 21 | 2026-02-18 | 4.1 | DONE | Added the first Rust `ISessionFunctions` trait surface and operation-info structs, with a concrete test implementation validating callback invocation semantics. |
| 22 | 2026-02-18 | 4.2 | DONE | Added initial read operation implementation with hash-index address lookup, record-chain traversal, region-based reader callback selection, and disk fallback coverage tests. |
| 23 | 2026-02-18 | 4.3 | DONE | Added initial Upsert operation with mutable-region in-place update path, tail append copy-update path, and hash-head CAS update tests. |
| 24 | 2026-02-18 | 4.4 | DONE | Added RMW operation implementation with fuzzy-region retry behavior, mutable in-place updater path, and immutable copy-update append path with CAS head replacement. |
| 25 | 2026-02-18 | 4.5 | DONE | Added Delete operation with mutable in-place tombstoning, immutable tombstone append + hash-head CAS replacement, and targeted tests for in-place/append/not-found flows. |
| 26 | 2026-02-18 | 4.6 | DONE | Added generic `TsavoriteKV<K, V, D>` + `TsavoriteSession` facade with epoch pin integration and operation wrappers for read/upsert/rmw/delete. |
| 27 | 2026-02-18 | 4.7 | DONE | Added facade-level CRUD unit tests covering single-thread correctness, fuzzy-region retry, tombstone behavior, and concurrent read/write stress. |
| 28 | 2026-02-18 | 4.8 | DONE | Added end-to-end integration tests for insert/read/delete verification across N keys, including a concurrent variant. |
| 29 | 2026-02-18 | 5.1 | DONE | Added `garnet-server` TCP accept loop + per-connection handlers with shutdown control, runtime metrics, and Tokio unit tests. |
| 30 | 2026-02-18 | 5.2 | DONE | Added `LimitedFixedBufferPool` with tiered size classes, bounded return queues, clear-on-return security behavior, and concurrent correctness tests. |
| 31 | 2026-02-18 | 5.3 | DONE | Added zero-copy RESP parser in `garnet-common` for array+bulk command frames with robust incomplete/invalid-input handling tests. |
| 32 | 2026-02-18 | 5.4 | DONE | Added 12-byte `ArgSlice` representation and wired RESP parsing to produce pointer+length argument references without payload copies. |
| 33 | 2026-02-18 | 5.5 | DONE | Added command dispatch module with hot-path byte-pattern matching and fallback command lookup, including ArgSlice-based command extraction tests. |
| 34 | 2026-02-18 | 5.6 | DONE | Added parse→dispatch→storage→response pipeline and integrated it into the TCP connection loop with partial-frame handling and protocol error responses. |
