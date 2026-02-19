# Feasibility Assessment: Rust x Storage Engine
## Assessor: assess-rust-sys

### Coverage Rating: B+

The documentation is remarkably thorough for the core storage engine subsystems. Bit layouts, memory structures, concurrency protocols, and operation flows are specified at a level of detail that would allow a competent Rust developer to begin implementation without reading C# source. The documents lose points primarily for (1) missing I/O device abstraction details, (2) incomplete recovery protocol specifics, and (3) absence of formal correctness invariants for the concurrency protocols. The Rust Mapping Notes sections in each document are a significant asset -- they demonstrate that the authors have already thought through Rust-specific concerns.

---

### Dimension Scores

| Dimension | Score (1-10) | Notes |
|-----------|-------------|-------|
| Memory Management Mapping | 9 | Hybrid log circular buffer, pinned pages, and native pointer arrays are described in detail. Rust's ownership model maps well: pages are `Box<[u8]>` owned by the allocator, with epoch protection governing safe access. The only gap is the exact protocol for page handoff between the flush pipeline and the eviction path -- the docs describe both but not the synchronization handshake between them in complete detail. |
| Concurrency Model Mapping | 8 | Epoch-based reclamation is described with full table layout, acquire/release protocol, drain list mechanics, and the subtle SuspendDrain edge case. The two-probe hashing, semaphore fallback, and instance tracking are all documented. Missing: formal memory ordering requirements (the doc notes x86 TSO dependency but doesn't specify which stores need which barriers). For ARM-targeted Rust builds, this matters. |
| Generic Monomorphization | 10 | C#'s struct constraint + generic type parameter pattern (`TAllocator`, `TStoreFunctions`) is explicitly documented as enabling JIT monomorphization. Rust does this natively and better via trait bounds + monomorphization. No gaps -- Rust's generics are strictly more powerful here. The `IAllocator` interface maps to a Rust trait, and the `SpanByteAllocator` wrapper struct pattern maps to a concrete implementation. |
| Lock-free Data Structures | 9 | CAS-based hash bucket insertion with tentative bits, the packed PageOffset trick for lock-free tail allocation, RecordInfo atomic sealing -- all are documented with bit layouts and CAS protocols. `std::sync::atomic` provides `AtomicU64` with `compare_exchange` which is sufficient. The reader-writer latch in the overflow entry is fully specified. Minor gap: the exact retry protocol for failed CAS operations on hash entries is described but some edge cases (e.g., concurrent resize + insertion) reference the state machine without full detail. |
| Unsafe Code Requirements | 7 | Significant `unsafe` will be required: raw pointer arithmetic into page buffers, `AtomicU64` operations on packed bitfields, manual memory layout for records, and mmap/direct-I/O interactions. However, the docs provide enough detail to encapsulate unsafety behind safe abstractions: a `Page` type that owns its buffer, a `RecordRef` type that borrows from a page with lifetime tied to epoch protection, and an `AllocatorHandle` that enforces the region-based access rules. The filler mechanism and SpanByte layout require careful `unsafe` implementations. The docs do not explicitly enumerate all pointer aliasing scenarios, which matters for Rust's stricter aliasing rules. |
| Cache-line Alignment | 10 | Every cache-line-aligned structure is explicitly documented: HashBucket (64 bytes), epoch table entries (64 bytes via `StructLayout(Size=64)`), and hash table sector alignment. Rust's `#[repr(C, align(64))]` is a direct equivalent. `crossbeam-utils::CachePadded<T>` is noted in the Rust mapping sections. No gaps. |
| I/O and Storage | 6 | This is the weakest area. The docs describe *what* happens (async page flush, sector-aligned direct I/O, snapshot files, delta logs) but not the device abstraction layer in detail. The `IDevice` interface is referenced but not documented. Segment management, file naming conventions, multi-segment log layout, and the async I/O completion callback chain are not specified. A reimplementor would need to design the I/O layer from first principles or read the source. Rust crates exist (`io-uring`, `glommio`, `tokio-uring`) but the mapping to Tsavorite's device model needs more specification. |
| Performance Parity | 9 | Rust can match or exceed C# performance for every documented subsystem. Lock-free atomics are identical at the hardware level. Monomorphization is native. No GC pauses. Direct memory control eliminates pinning overhead. The only risk is the I/O path: Rust's async I/O ecosystem is less mature than C#'s on Windows (where Garnet primarily runs), and `io_uring` on Linux requires kernel 5.1+. For the pure in-memory hot path, Rust should be faster due to zero-cost abstractions and absence of GC. |

---

### Subsystem Analysis

#### Document 02: Hybrid Log Allocator

**Maps cleanly to Rust:**
- Circular buffer of pages: `Vec<Option<Box<[u8; PAGE_SIZE]>>>` or `Vec<Option<AlignedPage>>`
- Logical address encoding (page + offset): simple bit arithmetic, no language-specific concerns
- Lock-free tail allocation via `AtomicU64::fetch_add` on packed PageOffset -- this is a direct translation
- Page overflow pool: `crossbeam::queue::ArrayQueue<Box<[u8]>>` as suggested in the doc
- Address region management (TailAddress, ReadOnlyAddress, HeadAddress, etc.): simple atomic variables
- Record layout with SpanByte: manual byte buffer offset arithmetic, natural in Rust

**Requires creative solutions:**
- Page lifecycle management: In C#, pages are GC-pinned arrays. In Rust, pages would be heap-allocated aligned buffers. The challenge is ensuring no dangling references when a page is evicted. Rust's borrow checker cannot statically enforce epoch-based protection, so a `PageGuard` type (similar to `crossbeam-epoch::Guard`) that ties page access to an active epoch would be needed.
- The `NeedToWait` / page readiness protocol during page turns: The doc describes the high-level flow but the exact synchronization between the allocating thread and the flush completion callback needs careful design in Rust's async model.
- Mutable fraction enforcement: The concept of "in-place update if address >= ReadOnlyAddress" requires runtime region checks. Rust cannot encode this statically, so an `enum RecordAccess { Mutable(&mut [u8]), ReadOnly(&[u8]) }` pattern would be needed.

**Missing from docs:**
- Device abstraction (`IDevice` interface): page read/write API, segment management, device factory
- Async I/O completion chain: how flush completions propagate to `FlushedUntilAddress` advancement
- Error handling: what happens on I/O errors during flush? Page retry? Store shutdown?
- ReadCache integration: mentioned but deferred; a reimplementor would need this for production use

#### Document 03: Hash Index

**Maps cleanly to Rust:**
- Hash bucket: `#[repr(C, align(64))] struct HashBucket { entries: [AtomicU64; 8] }` -- perfect fit
- HashBucketEntry bitfield: accessor methods on `u64` with shifts and masks
- Tag extraction and matching: pure bit arithmetic
- CAS-based insertion with tentative bit protocol: `AtomicU64::compare_exchange`
- Reader-writer latch in overflow entry: `AtomicU64` with careful ordering
- Overflow bucket chain traversal: pointer following via allocator

**Requires creative solutions:**
- Overflow bucket allocator (`MallocFixedPageSize`): The doc describes it as a two-level paged allocator. In Rust, this needs a custom slab allocator with careful lifetime management. Overflow buckets must remain valid until the next index resize (epoch-protected). A `typed-arena` or custom implementation is needed.
- Online index resizing with two concurrent table versions: The split-status tracking and progressive migration is well-documented, but the Rust implementation must handle two simultaneously valid table pointers safely. An `RwLock<IndexState>` or epoch-protected version swap would work.

**Missing from docs:**
- Overflow bucket reclamation protocol during resize: the doc mentions buckets are only freed during resize but doesn't detail the deallocation path
- Exact memory ordering requirements for FindTag (read-only scan): Can this use `Ordering::Relaxed` for entry reads, or is `Acquire` required?
- Hash function specification: `IStoreFunctions.GetHashCode64` is referenced but the actual hash function used by Garnet is not specified

#### Document 04: Epoch Protection & Concurrency

**Maps cleanly to Rust:**
- Epoch table with cache-padded entries: `Vec<CachePadded<EpochEntry>>` using `crossbeam-utils`
- Thread-local entry indices: `thread_local!` macro or `#[thread_local]` on nightly
- Atomic CAS for entry acquisition: `AtomicI32::compare_exchange` for threadId, `AtomicI64` for localCurrentEpoch
- SafeToReclaimEpoch computation: simple scan over the table
- Drain list with CAS-based slot claiming: `AtomicI64` for epoch + `UnsafeCell<Option<Box<dyn FnOnce()>>>` for actions
- Semaphore-based backoff: `std::sync::Condvar` or `parking_lot::Condvar`

**Requires creative solutions:**
- The `[ThreadStatic]` pattern for metadata (threadId, two probe offsets, instance entries): Rust's `thread_local!` has slightly different semantics (destructor order, initialization cost). For hot-path performance, `#[thread_local]` (nightly) is preferred but requires unsafe access. A wrapper type that lazily initializes on first access would work.
- The SuspendDrain protocol (last-thread-out): The doc provides pseudo-code but the full fence (`Thread.MemoryBarrier`) maps to `std::sync::atomic::fence(SeqCst)`. The recursive-avoidance (calling `Release` not `Suspend`) must be carefully implemented.
- Multiple LightEpoch instances (up to 16): The instance tracking with static CAS-allocated IDs maps to a global `AtomicU32` bitset.

**Missing from docs:**
- Memory ordering requirements on non-x86 architectures: The doc acknowledges this gap ("code relies on x86 TSO") but doesn't specify which operations need stronger ordering on ARM/RISC-V. A Rust reimplementation targeting ARM (e.g., AWS Graviton) must audit every atomic operation.
- Interaction with async runtimes: The doc notes that epoch protection is thread-bound and incompatible with async/await task migration. For a Rust implementation using `tokio`, this means epoch protection must be acquired/released around every `.await` point, or a thread-per-core model (`glommio`) must be used.
- Performance characteristics under contention: What happens when >128 threads compete for epoch table entries? The semaphore path is described but its latency impact is not.

#### Document 05: Checkpointing & Recovery

**Maps cleanly to Rust:**
- SystemState as `AtomicU64` with phase/version bitfields
- State machine trait hierarchy: `IStateMachineTask` -> Rust trait, `StateMachineBase` -> trait object or enum dispatch
- Version change state machine (REST -> PREPARE -> IN_PROGRESS -> REST)
- Epoch-based phase coordination: reuses the epoch system from doc 04
- Checkpoint metadata serialization: simple struct fields

**Requires creative solutions:**
- Async state machine loop: The doc describes `ProcessWaitingListAsync` but doesn't detail the async I/O wait mechanism. In Rust, this would use `tokio::spawn` + `tokio::sync::Semaphore` or `async-channel` for completion notification.
- Fuzzy region handling during recovery: Records in `[startLogicalAddress, finalLogicalAddress)` may have `InNewVersion` set. The undo logic requires scanning and conditionally reverting records. In Rust, this is a log scan with mutable page access -- straightforward but must handle partial pages.
- Delta log format: The incremental snapshot writes `[8-byte address][4-byte size][record bytes]` entries. Rust needs a streaming writer/reader for this format.

**Missing from docs:**
- Recovery protocol completeness: Section 2.11 provides a high-level overview but lacks detail on how the hash index is rebuilt from a log scan (when no index checkpoint exists), how partial/corrupt checkpoints are detected, and how recovery handles interrupted incremental snapshots.
- Device/file management for checkpoint files: Where are checkpoint files stored? Naming convention? Cleanup of old checkpoints?
- StreamingSnapshotCheckpointSM: Mentioned as existing but not documented. This is relevant for replication scenarios.
- Multi-store coordination: Garnet uses two stores; their checkpoint coordination is flagged as an open question.
- Error recovery: What happens if a checkpoint partially fails (e.g., disk full during snapshot write)?

#### Document 06: Record Management

**Maps cleanly to Rust:**
- RecordInfo as `AtomicU64` with bitfield accessors: `#[repr(C)] struct RecordInfo(AtomicU64)` with methods for each flag
- SpanByte in serialized form: `&[u8]` with 4-byte length prefix -- natural in Rust
- Record layout with 8-byte alignment: manual offset computation on byte slices
- Filler mechanism: extra int after value area, straightforward byte-level manipulation
- Tombstone management: flag bit in RecordInfo
- Revivification free pool: per-bin concurrent queues of (address, size) tuples
- End-to-end CRUD operation flows: fully documented with decision trees for mutable/readonly/disk regions

**Requires creative solutions:**
- Safe record access: Accessing record fields requires raw pointer arithmetic into page buffers. In Rust, a `RecordRef<'a>` type that borrows from a `&'a [u8]` page slice would encapsulate the unsafe offset calculations. The lifetime `'a` should be tied to the epoch guard to prevent use-after-eviction.
- In-place update safety: Mutable region records can be updated concurrently. The CAS on RecordInfo (sealing) prevents races, but the actual value write is not atomic for variable-length data. The doc describes this implicitly (seal prevents concurrent access) but a Rust implementation must ensure the value write is not observed partially (which it won't be, since readers check the seal bit first).
- Compaction's temporary store: The scan-based compaction creates a temporary TsavoriteKV instance. In Rust, this means instantiating a full store temporarily, or using a simpler `HashMap` for deduplication.

**Missing from docs:**
- SpanByte unserialized form interaction with Rust: The pointer-based form is less relevant but the doc doesn't specify when unserialized SpanBytes appear in the log (answer: they shouldn't, but this should be explicitly stated).
- Cross-record atomicity: The doc flags this as an open question. For Garnet's RENAME command, this is a real concern.
- ETag and VectorSet bit semantics: These are Garnet-specific but their exact payload format is not documented.
- Record chain integrity under concurrent seal + delete: If thread A seals a record for RCU and thread B concurrently deletes it, what happens? The interaction between seal and tombstone bits needs specification.

---

### Missing Information

The following gaps would block or significantly complicate a production-quality Rust reimplementation:

1. **I/O Device Abstraction (Critical)**: The `IDevice` interface, segment management, file layout, and async I/O completion model are not documented. This is the single largest gap. A reimplementor must either read the source or design an I/O layer from scratch.

2. **Memory Ordering Specification (High)**: The epoch system and lock-free structures rely on x86 TSO. For portable Rust (targeting ARM), every atomic operation needs explicit ordering annotation. The docs should specify required orderings.

3. **Recovery Protocol Details (High)**: Hash index rebuild from log scan, corrupt checkpoint detection, and interrupted checkpoint cleanup are not fully specified.

4. **ReadCache Subsystem (Medium)**: Referenced throughout (ReadCache bit in addresses, read cache eviction callbacks) but not documented. A production reimplementation would need this.

5. **Lock Table / Transaction Isolation (Medium)**: Referenced in the resize state machine (lock table migration) but not documented. Essential for understanding multi-key transaction support.

6. **Error Handling Patterns (Medium)**: What happens on I/O failure, allocation failure, or epoch table exhaustion beyond the semaphore backoff? The error propagation model is not documented.

7. **Session/ClientSession Abstraction (Low-Medium)**: The `ISessionFunctions` callbacks are referenced heavily but the session lifecycle (creation, disposal, thread affinity) is not documented in these files.

---

### Risk Analysis

**Top risks for a Rust reimplementation of the storage engine, ordered by severity:**

1. **Unsafe Code Surface Area (High Risk)**: The storage engine fundamentally operates on raw byte buffers with pointer arithmetic. Estimated 30-40% of core code will require `unsafe` blocks. While this can be encapsulated behind safe APIs, bugs in `unsafe` code can cause UB that Rust's safety guarantees cannot catch. Extensive fuzzing (`cargo fuzz`, `miri`) is essential.

2. **Epoch-Lifetime Bridging (High Risk)**: Rust's borrow checker cannot understand epoch-based protection. A page reference obtained within an epoch-protected region is valid only until `Suspend()`, but the compiler cannot enforce this. Either (a) use a guard-based API (like `crossbeam-epoch`) that ties lifetimes to epoch guards, or (b) use raw pointers with manual correctness reasoning. Option (a) is safer but adds API complexity; option (b) is more flexible but error-prone.

3. **I/O Layer Maturity (Medium-High Risk)**: Garnet runs primarily on Windows with IOCP. Rust's async I/O is strongest on Linux (`io_uring`). A cross-platform I/O layer matching Tsavorite's device abstraction is a significant engineering effort. Consider `tokio` for portability or `io-uring` crate for Linux-specific performance.

4. **Concurrency Bug Regression (Medium Risk)**: The lock-free algorithms (tag insertion with tentative bits, packed PageOffset allocation, reader-writer latch) are subtle. Translating from C# to Rust risks introducing ordering bugs, especially because C#'s memory model differs from Rust's (C# on x86 provides stronger guarantees than Rust's default `Ordering::Relaxed`). Every CAS operation must be carefully annotated with the correct `Ordering`.

5. **Feature Parity vs. Simplification (Medium Risk)**: The docs describe features like revivification, incremental snapshots, and online index resizing that add significant complexity. A phased approach (core log + index first, then checkpoint, then revivification) is advisable, but the interaction between features means late-added features may require rework.

6. **Testing Against C# Reference (Low-Medium Risk)**: Without a formal specification, the C# implementation IS the specification. Edge cases in behavior (e.g., concurrent seal + delete, epoch table exhaustion, compaction during checkpoint) can only be validated against the C# code or through extensive property-based testing.

---

### Recommended Rust Crates

| Subsystem | Crate | Purpose |
|-----------|-------|---------|
| **Cache-line Padding** | `crossbeam-utils` | `CachePadded<T>` for 64-byte alignment on epoch table entries |
| **Lock-free Queues** | `crossbeam-queue` | `ArrayQueue` for overflow page pool, `SegQueue` for revivification free list |
| **Epoch-based Reclamation** | Custom (preferred) or `crossbeam-epoch` | Tsavorite's epoch system is simpler and more specialized than crossbeam-epoch; a custom implementation following doc 04 is recommended. crossbeam-epoch can serve as a reference. |
| **Atomic Operations** | `std::sync::atomic` | `AtomicU64`, `AtomicI64`, `AtomicU32` for all CAS-based structures |
| **Aligned Allocation** | `std::alloc::Layout` | Sector-aligned page allocation with `Layout::from_size_align(page_size, sector_size)` |
| **Memory-mapped I/O** | `memmap2` | mmap-based page access for read-only log regions |
| **Direct I/O (Linux)** | `io-uring` | Async direct I/O via Linux io_uring for page flush and read |
| **Direct I/O (Cross-platform)** | `tokio` + `tokio-uring` (Linux) | Async I/O runtime; `tokio-uring` for io_uring integration |
| **Hashing** | `ahash` or `xxhash-rust` | 64-bit hash function for key hashing (Garnet uses its own but these are drop-in candidates) |
| **Thread-local Storage** | `thread_local` (std) | Per-thread epoch metadata; consider nightly `#[thread_local]` for hot paths |
| **Async Synchronization** | `tokio::sync` | `Semaphore`, `Notify` for state machine coordination and epoch table backoff |
| **Serialization** | `bincode` or manual | Checkpoint metadata serialization |
| **Testing / Fuzzing** | `cargo-fuzz`, `loom` | Fuzz testing for unsafe code; `loom` for concurrency model checking of lock-free algorithms |
| **Slab Allocation** | `slab` or custom | Fixed-page allocator for overflow hash buckets |
| **Byte-level Manipulation** | `zerocopy` | Safe transmutation for record access patterns; reduces manual unsafe for bitcasting |
| **Parking / Condvar** | `parking_lot` | Faster mutex/condvar implementation for non-hot-path synchronization |

---

### Summary

The documentation is strong enough to begin a Rust reimplementation of the Tsavorite storage engine. The hybrid log, hash index, epoch system, checkpoint state machine, and record management are all documented at a level that enables implementation without source code access. The primary gaps are in the I/O device layer, memory ordering specifics for non-x86, and recovery edge cases.

Rust is an excellent target language for this subsystem. The core data structures (cache-line-aligned hash buckets, lock-free atomic operations, epoch-based protection, zero-copy byte buffer management) are natural in Rust and should achieve performance parity or better compared to C#. The main engineering challenges are (1) managing the significant unsafe code surface area, (2) bridging epoch-based memory safety with Rust's borrow checker, and (3) building a robust cross-platform I/O layer.

A recommended implementation order would be:
1. Epoch system (foundation for everything)
2. Hybrid log allocator (pages, allocation, flush)
3. Hash index (buckets, CAS insertion, overflow chain)
4. Record management (RecordInfo, SpanByte, CRUD operations)
5. Checkpointing (fold-over first, then snapshot, then incremental)
6. Compaction and revivification (optimization layer)
