# Codex Global Instruction: Garnet Rust Reimplementation

## Overview

You are an autonomous Rust implementation agent working on `garnet-rs`, a high-performance Redis-compatible cache store reimplemented in Rust from Microsoft's C# Garnet. You run in an infinite loop: read this instruction, do work, update status, exit, get restarted.

## Execution Loop

Every time you start:

1. **Read this file** (`CODEX_GLOBAL_INSTRUCTION.md`)
2. **Read status** (`TODO_AND_STATUS.md`) to understand current state
3. **Pick the next unblocked task** (highest priority, status=TODO or IN_PROGRESS)
4. **Read relevant reimplementation docs** before writing code
5. **Implement** (write Rust code, tests, etc.)
6. **Update `TODO_AND_STATUS.md`** — mark completed tasks DONE, update IN_PROGRESS, add new tasks discovered during implementation
7. **Run `cargo check` and `cargo test`** to verify your work compiles and passes
8. **Git commit** your changes with a descriptive message
9. **Exit** (you will be restarted with the same args)

**IMPORTANT RULES**:
- Do ONE focused task per iteration. Do not try to implement everything at once.
- Always leave the codebase in a compiling state (`cargo check` must pass).
- Always update `TODO_AND_STATUS.md` before exiting.
- If a task is too large for one iteration, break it into subtasks in TODO_AND_STATUS.md and mark progress.
- If you're blocked, document WHY in TODO_AND_STATUS.md and move to the next unblocked task.

---

## Project Structure

```
/  (this git repo — Microsoft Garnet C# source)
├── CODEX_GLOBAL_INSTRUCTION.md      ← You are here
├── TODO_AND_STATUS.md               ← Your work tracker (read/write every iteration)
├── garnet-rs/                       ← Rust implementation (you write code here)
│   ├── Cargo.toml                   ← Workspace root
│   ├── crates/
│   │   ├── tsavorite/               ← Storage engine (hybrid log, hash index, epoch)
│   │   │   ├── Cargo.toml
│   │   │   └── src/
│   │   ├── garnet-server/           ← Server binary (TCP, RESP, sessions)
│   │   │   ├── Cargo.toml
│   │   │   └── src/
│   │   ├── garnet-common/           ← Shared types (SpanByte, ArgSlice, buffer pool)
│   │   │   ├── Cargo.toml
│   │   │   └── src/
│   │   ├── garnet-cluster/          ← Cluster support (gossip, replication, slots)
│   │   │   ├── Cargo.toml
│   │   │   └── src/
│   │   └── garnet-client/           ← Client library
│   │       ├── Cargo.toml
│   │       └── src/
│   ├── benches/                     ← Benchmarks
│   └── tests/                       ← Integration tests
├── docs/reimplementation/           ← Design documents (READ ONLY — do not modify)
│   ├── 00-index.md                  ← Master index, glossary, reading order
│   ├── 01-architecture-overview.md
│   ├── 02-tsavorite-hybrid-log.md
│   ├── 03-tsavorite-hash-index.md
│   ├── 04-tsavorite-epoch-and-concurrency.md
│   ├── 05-tsavorite-checkpointing.md
│   ├── 06-tsavorite-record-management.md
│   ├── 07-network-layer.md
│   ├── 08-resp-parser.md
│   ├── 09-request-lifecycle.md
│   ├── 10-storage-session-functions.md
│   ├── 11-data-structures-objects.md
│   ├── 12-transactions-and-scripting.md
│   ├── 13-cluster-and-replication.md
│   ├── 14-aof-and-durability.md
│   ├── 15-performance-engineering.md
│   ├── 16-feasibility-rust-vs-kotlin.md
│   └── 17-jvm-performance-gap-analysis.md
└── libs/                            ← Original C# source (reference — read when docs insufficient)
```

---

## Architecture Principles

These are non-negotiable design decisions for the Rust implementation:

### 1. Zero-Allocation Hot Path
- No heap allocation on GET/SET/INCR hot path
- Use `&[u8]` slices, stack-allocated structs, arena allocators
- Buffer pools for network I/O (pre-allocated, recycled)

### 2. Monomorphization via Generics
- Mirror C#'s generic monomorphization: `TsavoriteKV<K, V, F, A>` with concrete type parameters
- Use `#[inline]` aggressively on trait implementations
- Session functions as generic parameters, not trait objects

### 3. Epoch-Based Concurrency
- Implement LightEpoch with cache-line-padded per-thread entries
- `#[repr(C, align(64))]` for epoch table entries
- Guard-based API: `let guard = epoch.pin();` — borrow checker enforces protection scope

### 4. Cache-Line Alignment
- `#[repr(C, align(64))]` for HashBucket
- `#[repr(C)]` with explicit field ordering for RecordInfo, SpanByte
- Benchmark cache-miss rates

### 5. Unsafe Encapsulation
- `unsafe` is expected (~40-50% of storage engine code)
- Encapsulate behind safe APIs: `Page`, `RecordRef`, `BucketRef`, `EpochGuard`
- Every `unsafe` block must have a `// SAFETY:` comment explaining the invariant

### 6. I/O Model
- Thread-per-core using `io_uring` (via `tokio-uring` or `monoio`) on Linux
- Fallback to `mio`/`epoll` for portability
- No `async` in the storage engine hot path (epoch protection doesn't compose with task migration)

---

## Implementation Order (Phases)

Follow this order. Each phase builds on the previous.

### Phase 0: Project Scaffolding
- Cargo workspace setup
- Crate structure with empty `lib.rs` files
- CI basics (cargo check, cargo test, cargo clippy)
- `rust-toolchain.toml` (nightly for `io_uring` features if needed)

### Phase 1: Core Primitives (`garnet-common` + `tsavorite` foundations)
Read: docs 02, 04, 06, 15

1. **SpanByte** — Variable-length byte sequence with 4-byte length prefix
2. **RecordInfo** — 64-bit packed bitfield (valid, tombstone, dirty, sealed, filler, in-new-version, has-etag, has-expiration + previous-address)
3. **LightEpoch** — Epoch protection with cache-line-padded table, acquire/release/drain
4. **HashBucket** — 64-byte cache-line-aligned bucket with 7 entries + overflow pointer
5. **HashBucketEntry** — 8-byte packed entry (tag, address, tentative, pending)

### Phase 2: Hybrid Log Allocator (`tsavorite`)
Read: docs 02, 06

1. **Page management** — Allocate/flush/evict pages, address space (page + offset)
2. **Log regions** — TailAddress, ReadOnlyAddress, SafeReadOnlyAddress, HeadAddress, BeginAddress
3. **Lock-free tail allocation** — `AtomicU64::fetch_add` for log append
4. **Record format** — RecordInfo + key SpanByte + value SpanByte, variable-length records
5. **Page I/O** — Read/write pages to storage device abstraction

### Phase 3: Hash Index (`tsavorite`)
Read: docs 03, 06

1. **Hash index structure** — Power-of-2 bucket array, tag-based matching
2. **FindTag / FindOrCreateTag** — CAS-based entry insertion
3. **Overflow bucket allocation** — Fixed-page allocator for overflow chains
4. **Transient locking** — Shared/exclusive latch on bucket overflow entry

### Phase 4: CRUD Operations (`tsavorite`)
Read: docs 06 (especially Section 2.12), 10

1. **Read** — Hash lookup → chain traversal → ConcurrentReader/SingleReader callbacks
2. **Upsert** — Hash lookup → IPU or RCU → CAS into chain
3. **RMW** — Read-modify-write with fuzzy region RETRY_LATER
4. **Delete** — Tombstone creation
5. **CPR consistency checks** — Version comparison for checkpoint coordination
6. **ISessionFunctions trait** — Generic callback interface

### Phase 5: Network Layer (`garnet-server`, `garnet-common`)
Read: docs 07, 08, 09

1. **TCP server** — Accept loop, connection handler
2. **Buffer pool** — Tiered pool with power-of-2 size classes
3. **RESP parser** — Zero-copy `&[u8]` based parser, ArgSlice
4. **Command dispatch** — `match` on command enum
5. **Request lifecycle** — Parse → dispatch → storage op → response write → send

### Phase 6: Basic Redis Commands
Read: docs 09, 10

1. **GET** — Full path from TCP to response
2. **SET** — Upsert path
3. **DEL** — Delete path
4. **INCR/DECR** — RMW path with in-place update
5. **EXPIRE/TTL** — Expiration metadata
6. **PING/ECHO/INFO** — Utility commands

### Phase 7: Object Store & Data Structures
Read: doc 11

1. **Object store** — Second TsavoriteKV instance for complex types
2. **Hash** — HGET/HSET/HDEL/HGETALL
3. **List** — LPUSH/RPUSH/LPOP/RPOP/LRANGE
4. **Set** — SADD/SREM/SMEMBERS/SISMEMBER
5. **SortedSet** — ZADD/ZREM/ZRANGE/ZSCORE

### Phase 8: Checkpointing & AOF
Read: docs 05, 14

1. **Checkpoint state machine** — Fold-over/snapshot
2. **AOF writer** — Append-only log with TsavoriteLog
3. **AOF replay** — Recovery from AOF
4. **Checkpoint + AOF coordination**

### Phase 9: Transactions
Read: doc 12

1. **MULTI/EXEC/DISCARD** — Command queuing and replay
2. **WATCH** — Optimistic locking via WatchVersionMap
3. **Sorted lock acquisition** — Deadlock-free locking

### Phase 10: Cluster
Read: docs 13, 14

1. **Cluster config** — 16384 hash slots, immutable copy-on-write
2. **Gossip protocol** — Failure-budget sampling
3. **Replication** — Primary-replica sync via checkpoint + AOF
4. **Slot migration** — MOVED/ASK redirections

---

## Coding Standards

### Rust Style
- Use `rustfmt` defaults
- `clippy` must pass with no warnings
- Minimum Rust edition: 2021
- Prefer `thiserror` for error types
- Prefer `tracing` for logging (not `log`)

### Testing
- Unit tests in each module (`#[cfg(test)] mod tests`)
- Integration tests in `garnet-rs/tests/`
- Property-based tests with `proptest` for data structures
- Fuzz testing with `cargo-fuzz` for parser and record handling

### Performance
- Benchmark critical paths with `criterion`
- Profile with `perf` / `flamegraph`
- Track allocations with `dhat` or `heaptrack`
- Target: zero allocations per GET/SET on hot path

### Safety
- Every `unsafe` block has a `// SAFETY:` comment
- Minimize `unsafe` surface: wrap in safe abstractions
- Use `miri` for undefined behavior detection in tests
- Use `loom` for concurrency testing of lock-free structures

### Documentation
- `///` doc comments on all public APIs
- Module-level `//!` docs explaining purpose and design decisions
- Reference the reimplementation doc number: `/// See [Doc 02 Section 2.3] for the address space model`

---

## Reference Lookup Guide

When implementing a component, read the relevant doc FIRST:

| Component | Primary Doc | Secondary Docs | C# Reference (when docs insufficient) |
|-----------|-------------|----------------|---------------------------------------|
| SpanByte | 06 §2.4 | 02 §2.8 | `libs/storage/Tsavorite/cs/src/core/VarLen/SpanByte.cs` |
| RecordInfo | 06 §2.1 | 15 §2.4 | `libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs` |
| Epoch | 04 | 15 §2.6 | `libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs` |
| Hybrid Log | 02 | 06 §2.12 | `libs/storage/Tsavorite/cs/src/core/Allocator/` |
| Hash Index | 03 | 06 §2.12 | `libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs` |
| CRUD Ops | 06 §2.12 | 05 §2.4 | `libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Internal*.cs` |
| TCP Server | 07 | 01 §2.4 | `libs/server/Servers/GarnetServerTcp.cs` |
| RESP Parser | 08 | 09 §2.1 | `libs/common/RespReadUtils.cs` |
| Buffer Pool | 07 §2.5 | 15 §2.8 | `libs/common/Memory/LimitedFixedBufferPool.cs` |
| Session Funcs | 10 | 06 §2.12 | `libs/server/Storage/Functions/MainStore/` |
| Objects | 11 | — | `libs/server/Objects/` |
| Transactions | 12 | 03 §2.4 | `libs/server/Transaction/` |
| Cluster | 13 | 14 | `libs/cluster/` |
| AOF | 14 | 13 §2.6 | `libs/server/AOF/` |
| Checkpointing | 05 | 04 | `libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/` |
| Perf Patterns | 15 | all | (cross-cutting) |

---

## Key Performance Targets

| Metric | Target | How to Verify |
|--------|--------|---------------|
| GET latency (p50) | < 10 us | `criterion` bench |
| GET throughput (single core) | > 1M ops/sec | `criterion` bench |
| Allocations per GET | 0 | `dhat` profiling |
| HashBucket size | exactly 64 bytes | `assert_eq!(std::mem::size_of::<HashBucket>(), 64)` |
| RecordInfo size | exactly 8 bytes | `assert_eq!(std::mem::size_of::<RecordInfo>(), 8)` |
| EpochEntry alignment | 64-byte aligned | `assert_eq!(std::mem::align_of::<EpochEntry>(), 64)` |

---

## Dependencies (Suggested)

```toml
# Cargo.toml workspace dependencies
[workspace.dependencies]
crossbeam-epoch = "0.9"
crossbeam-utils = "0.8"
parking_lot = "0.12"
bytes = "1"
tokio = { version = "1", features = ["full"] }
mio = "1"
tracing = "0.1"
tracing-subscriber = "0.3"
thiserror = "2"
criterion = "0.5"
proptest = "1"
```

Adjust versions as needed. Prefer minimal dependencies.

---

## When Docs Are Insufficient

If the reimplementation docs don't cover something you need:

1. Check the C# source in `libs/` (read-only reference)
2. Add a note in `TODO_AND_STATUS.md` under "Doc Gaps Discovered"
3. Make a reasonable design decision and document it
4. Mark it for future review

---

## Communication Protocol

You communicate exclusively through `TODO_AND_STATUS.md`. Update it every iteration with:
- What you completed
- What's in progress
- What's blocked and why
- Any design decisions you made
- Any doc gaps you discovered
