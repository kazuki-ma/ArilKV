# 16 -- Feasibility Assessment: Rust vs Kotlin/JVM

## 1. Purpose

This document synthesizes the Rust and Kotlin/JVM mapping notes from all 15 reimplementation documents (01-15) into a structured feasibility comparison across 10 dimensions. Each dimension is scored on a 1-5 scale for both languages (5 = strong fit, 1 = poor fit) with justification drawn from the specific technical requirements identified in the source analysis.

## 2. Scoring Dimensions

### D1: Memory Layout and Zero-Copy (Weight: Critical)

**Garnet requirement**: The system relies heavily on explicit memory layouts -- `SpanByte` with 4-byte length prefix, `RecordInfo` as a 64-bit bitfield, `HashSlot` as a 3-byte packed struct, `AofHeader` as a 16-byte explicit-layout struct, `TxnKeyEntry` as a 10-byte explicit-layout struct. These are accessed via raw pointer arithmetic (`*(AofHeader*)ptr`, `Unsafe.AsRef<SpanByte>(ptr)`). The hybrid log stores records as contiguous bytes with inline metadata.

**Rust (5/5)**: Native support. `#[repr(C, packed)]` and `#[repr(C, align(64))]` give exact control over struct layout. Raw pointer arithmetic (`*const u8`, `offset()`) is available in `unsafe` blocks. `bytemuck` and `zerocopy` crates provide safe zero-copy casts between byte slices and structs. `&[u8]` slices with explicit length prefixes map directly to `SpanByte`. Documents 01, 02, 06, 08, 14 all note this as a natural fit.

**Kotlin/JVM (2/5)**: The JVM has no value types (pre-Valhalla), no struct layout control, no pointer arithmetic. Every workaround adds complexity: `ByteBuffer.allocateDirect()` for off-heap memory, `sun.misc.Unsafe` for raw access, `MemorySegment` (Panama, JDK 21+) for safer off-heap access. Documents 01, 03, 06 note that `SpanByte`, `RecordInfo`, and `HashBucket` must all become offset-into-buffer abstractions rather than native types. The entire hot path is dominated by indirect memory access.

**Per-request allocation comparison**: C# Garnet achieves 0 heap allocations on the GET hot path -- `ArgSlice`, `SpanByte`, `SpanByteAndMemory`, and `RawStringInput` are all stack-allocated structs. A Kotlin/JVM implementation has an irreducible floor of 2-3 heap allocations per GET (~64-96 bytes), even with aggressive optimization (object pooling, primitive encoding, Netty `Recycler`). Realistically, 3-5 allocations (~96-160 bytes) per GET. This per-request allocation pressure drives GC overhead and is a fundamental JVM limitation.

### D2: Concurrency Primitives (Weight: Critical)

**Garnet requirement**: Epoch-based protection (doc 04), lock-free CAS on 64-bit packed values (docs 03, 06), `Interlocked.CompareExchange` on config references (doc 13), `AtomicLong` arrays for watch version maps (doc 12), `volatile` reads on config references, `SpinLock` for short critical sections (doc 07), cache-line-padded per-thread epoch entries.

**Rust (5/5)**: `std::sync::atomic` types map directly to C#'s `Interlocked`. `crossbeam-epoch` provides epoch-based reclamation. `crossbeam-utils::CachePadded<T>` provides cache-line padding. `parking_lot::Mutex` provides fast spin-then-park locks. `#[repr(C, align(64))]` ensures cache-line alignment. `thread_local!` replaces `[ThreadStatic]`.

**Kotlin/JVM (3/5)**: `AtomicLong`, `AtomicLongArray`, `VarHandle` provide CAS operations. `@Contended` annotation provides some false-sharing mitigation. `ThreadLocal<T>` replaces `[ThreadStatic]` but is slower due to hash-table lookup (doc 04). No cache-line alignment guarantee for on-heap data; must use `Unsafe.allocateMemory` for aligned off-heap. Epoch protection must be implemented manually with padded arrays (doc 04). Workable but requires more manual effort and has higher overhead.

### D3: Generics and Monomorphization (Weight: High)

**Garnet requirement**: Tsavorite uses C# generics with struct constraints (`where TKey : struct`) and static abstract interface methods. The JIT monomorphizes each `<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>` combination, eliminating virtual dispatch on the hot path (doc 15). The `ISessionFunctions` callback interface is invoked millions of times per second.

**Rust (5/5)**: Rust generics are monomorphized at compile time -- this is identical to C#'s behavior. `#[inline]` on trait implementations ensures inlining. The `ISessionFunctions` trait maps directly. Documents 01 and 15 highlight this as a key advantage.

**Kotlin/JVM (2/5)**: JVM type erasure prevents monomorphization entirely. Generic types are erased to `Object`, adding boxing overhead for primitive types and preventing inlining of generic code. Workarounds: code generation (KSP, annotation processors), manual specialization, or accepting the virtual dispatch cost (doc 01, 15). This is the single biggest structural disadvantage for a JVM reimplementation.

### D4: Async I/O and Networking (Weight: High)

**Garnet requirement**: The network layer uses `SocketAsyncEventArgs` (SAEA) for async socket I/O with buffer pinning (doc 07). The server uses a thread-per-session model for RESP processing. Tsavorite uses async page I/O with completion callbacks. AOF commit is async. Gossip uses async TCP connections.

**Rust (4/5)**: `tokio` with `io_uring` backend (`tokio-uring`) or `monoio` for thread-per-core designs. `io_uring` natively supports buffer pools (doc 07). Async file I/O via `tokio::fs` or direct `io_uring` submissions. The challenge is that Rust's async model uses cooperative scheduling, which differs from Garnet's thread-per-session model. Epoch protection does not compose well with `async/await` task migration across threads.

**Kotlin/JVM (3.5/5)**: Netty provides a battle-tested async networking framework with `EpollEventLoop`, `PooledByteBufAllocator`, write watermarks, and TLS support (doc 07). Kotlin coroutines with `delay()` for gossip timers (doc 13). `AsynchronousFileChannel` or `io_uring` via JNI for async file I/O. Netty's `ChunkedWriteHandler` maps to AOF streaming (doc 13). The JVM networking ecosystem is mature and well-optimized. However, Garnet's design assumes the I/O layer passes raw byte pointers directly to the parser and storage layer (zero-copy non-TLS path, doc 07). On the JVM, Netty's `ByteBuf` must be converted to `ByteBuffer` or offset-based access patterns, adding a translation layer. The TLS state machine (doc 07 Section 2.5) with its three states maps more naturally to Rust's async/await than to Netty's pipeline model.

**TLS note**: For TLS-heavy workloads specifically, Netty + BoringSSL (via `netty-tcnative`) is likely faster than .NET's managed `SslStream` and Rust's `rustls`. This is a narrow Kotlin/JVM advantage in TLS-dominant deployments.

### D5: Storage Engine Core (Hybrid Log + Hash Index) (Weight: Critical)

**Garnet requirement**: The hybrid log (doc 02) is a circular buffer of pages with lock-free tail allocation, page-level async I/O, sector-aligned writes. The hash index (doc 03) is a cache-line-aligned bucket array with 8-entry buckets, overflow chaining, and CAS-based entry manipulation. Record management (doc 06) involves `RecordInfo` bitfield manipulation, in-place updates, copy-on-write, revivification. Checkpointing (doc 05) uses a state machine with epoch-coordinated phase transitions.

**Rust (4.5/5)**: All core operations (page allocation via `mmap`, lock-free tail via `AtomicU64::fetch_add`, cache-line-aligned buckets via `#[repr(C, align(64))]`, CAS on `AtomicU64` entries, epoch-based page eviction) map directly. The revivification free-record pool maps to `crossbeam::queue::SegQueue` per-bin. The state machine maps to an async loop. The 0.5-point deduction acknowledges implementation complexity: the operation flows (doc 06 Section 2.12) reveal that every CRUD operation involves transient bucket locking, CPR consistency checks, read-cache fast paths, fuzzy region retry logic for RMW, and record chain traversal with validity filtering. These are intricate correctness invariants that `unsafe` code must maintain.

**Unsafe surface area estimate**: Based on the storage engine documentation (docs 02-06), approximately 40-50% of the Rust storage engine code would contain `unsafe` blocks. By subsystem: hybrid log allocator ~60%, hash index ~70%, epoch system ~50%, record management ~40%, checkpointing ~20%. However, the unsafe surface can be organized into encapsulated abstractions (`Page`, `RecordRef`, `BucketRef`, `EpochGuard`), allowing operation-level code (InternalRead, InternalUpsert, etc.) to be written in safe Rust.

**Kotlin/JVM (2/5)**: This is the hardest layer. The hybrid log requires: off-heap page allocation (`ByteBuffer.allocateDirect` or `Unsafe`), manual offset arithmetic for record access, `VarHandle` for atomic operations on off-heap memory. The hash index requires: off-heap 64-byte-aligned buckets (manual alignment via `Unsafe`), CAS on `long` values at computed offsets. No language-level support for any of these patterns -- everything is manual. Doc 03 notes the need to store buckets as `long[]` with stride 8. Doc 06 notes no value types for `RecordInfo`. GC cannot help with off-heap memory management.

### D6: Data Structures (Object Store Types) (Weight: Medium)

**Garnet requirement**: Hash, List, Set, SortedSet objects with in-memory data structures, per-element expiration (Hash, SortedSet), serialization state machine, size tracking, and `Operate()` dispatch (doc 11).

**Rust (4/5)**: Standard library collections (`HashMap`, `HashSet`, `BTreeSet`, `VecDeque`, `LinkedList`) cover most needs. `OrderedFloat` for NaN-safe SortedSet ordering. The serialization state machine uses `AtomicU32` with `compare_exchange`. The enum-with-variants pattern is more natural than class hierarchy for a closed type set. Challenge: byte-array keys require custom hash/eq implementations.

**Kotlin/JVM (4/5)**: Standard library collections map directly (`HashMap`, `HashSet`, `TreeSet`, `LinkedList`). Java's `PriorityQueue` is a min-heap by default, mapping directly to the expiration queue. The sealed class hierarchy provides type-safe dispatch. `AtomicInteger` with `compareAndSet` for serialization state. The JVM's GC naturally manages heap-allocated objects. Challenge: `ByteArray` lacks structural equality, requiring wrapper classes.

### D7: Transaction System (Weight: High)

**Garnet requirement**: Sorted lock acquisition protocol with `TxnKeyEntry` 10-byte structs (doc 12), `WatchVersionMap` with atomic long array, buffer-replay mechanism, `LockableContext` from Tsavorite, stored procedure API with three phases.

**Rust (4/5)**: `Vec<TxnKeyEntry>` with `sort_unstable_by` for lock ordering. `Vec<AtomicI64>` for watch map. Buffer replay via `&[u8]` slice with saved offset. Trait-based stored procedure API. The main challenge is implementing `LockableContext` -- Tsavorite's key locking is tightly integrated with the hash table and epoch system.

**Kotlin/JVM (3/5)**: `AtomicLongArray` for watch map. `ByteBuffer` replay. `ReentrantReadWriteLock` or Guava `Striped<Lock>` for lockable contexts. The JVM overhead is modest here since transactions are not the extreme hot path (they are heavier operations by nature).

### D8: AOF and Durability (Weight: Medium)

**Garnet requirement**: Append-only log with `TsavoriteLog` (concurrent append, page-based allocation, async commit), 16-byte `AofHeader` with zero-copy reading, fuzzy region handling during checkpoints, transaction replay coordination (doc 14).

**Rust (4/5)**: Custom log with `mmap` or `io_uring`-based I/O. `#[repr(C, packed)]` for `AofHeader`. `bytemuck::from_bytes` for zero-copy parsing. `HashMap<i32, Vec<(AofHeader, Vec<u8>)>>` for transaction grouping.

**Kotlin/JVM (3/5)**: `MappedByteBuffer` or Chronicle Queue for high-performance append-only logging. `ByteBuffer` positional reads for header parsing. `HashMap<Int, MutableList<ByteArray>>` for transaction grouping. Cannot replicate the exact `AofHeader` layout, but offset-based `ByteBuffer` access is equivalent.

### D9: Cluster and Replication (Weight: Medium)

**Garnet requirement**: Copy-on-write immutable config with CAS publishing (doc 13), gossip protocol with failure-budget sampling, checkpoint transfer, AOF streaming, failover state machines.

**Rust (4/5)**: `arc_swap::ArcSwap<ClusterConfig>` for lock-free CAS. `DashMap` for ban list. `tokio` TCP streams for replication. Async gossip loop with `tokio::select!`.

**Kotlin/JVM (4/5)**: `AtomicReference<ClusterConfig>` with `compareAndSet`. Kotlin `copy()` for immutable config updates. Kotlin coroutines for gossip timer. Netty for replication I/O.

### D10: Ecosystem and Operational Maturity (Weight: Medium)

**Garnet requirement**: Production-quality server with monitoring, configuration, TLS, authentication, custom command extensibility, cluster management.

**Rust (3/5)**: Strong systems programming ecosystem but less mature for application-server patterns. TLS via `rustls`. Configuration via `clap` + `serde`. Monitoring via `tracing` + `prometheus` crates. Custom command plugins require dynamic loading (`libloading`) or static compilation. The Rust ecosystem for Redis-like servers exists (mini-redis, etc.) but is less battle-tested than JVM alternatives.

**Kotlin/JVM (5/5)**: Mature ecosystem for server applications. TLS via Netty/BoringSSL. Configuration via Spring/Ktor. Monitoring via Micrometer/Prometheus. Rich plugin systems. Extensive operational tooling (JMX, jconsole, async-profiler). The JVM has decades of production server experience.

## 3. Summary Scorecard

| Dimension | Rust | Kotlin/JVM | Delta |
|-----------|------|------------|-------|
| D1: Memory Layout & Zero-Copy | 5 | 2 | +3 Rust |
| D2: Concurrency Primitives | 5 | 3 | +2 Rust |
| D3: Generics & Monomorphization | 5 | 2 | +3 Rust |
| D4: Async I/O & Networking | 4 | 3.5 | +0.5 Rust |
| D5: Storage Engine Core | 4.5 | 2 | +2.5 Rust |
| D6: Data Structures | 4 | 4 | 0 |
| D7: Transaction System | 4 | 3 | +1 Rust |
| D8: AOF & Durability | 4 | 3 | +1 Rust |
| D9: Cluster & Replication | 4 | 4 | 0 |
| D10: Ecosystem & Operations | 3 | 5 | +2 Kotlin |
| **Weighted Total** | **42.5** | **31.5** | **+11 Rust** |

## 4. Key Findings

### 4.1 Rust Advantages

1. **Storage engine is a natural fit**: The three critical dimensions (D1, D3, D5) where Garnet's architecture is most demanding all strongly favor Rust. The hybrid log, hash index, epoch system, and session function callbacks all rely on patterns that Rust supports natively and the JVM fundamentally cannot.

2. **Performance parity is achievable**: Rust can match or exceed C#'s performance for the core storage engine. C#'s `Interlocked`, `SpanByte`, `[FieldOffset]`, `GC.AllocateArray(pinned: true)` all have direct Rust equivalents that are at least as performant.

3. **Safety with escape hatches**: The storage engine core will require `unsafe` Rust (~40-50% of storage engine code) for raw pointer arithmetic, `mmap` page management, and atomic operations on packed bitfields. However, the unsafe surface can be encapsulated behind safe abstractions (`Page`, `RecordRef`, `BucketRef`, `EpochGuard`), allowing operation-level code to be written in safe Rust.

### 4.2 Kotlin/JVM Advantages

1. **Higher-level components are comfortable**: Cluster management, gossip protocol, data structures, and stored procedure APIs all map naturally to JVM patterns. The sealed class hierarchy, coroutines, and rich collection library are well-suited.

2. **Operational maturity**: The JVM ecosystem for monitoring, debugging, profiling, and operating production servers is unmatched. Tools like async-profiler, JFR, JMX, and Netty's extensive documentation provide immediate operational capability.

3. **Faster initial development**: For the application layer (documents 10-14), JVM development would likely be faster due to garbage collection, richer libraries, and more accessible concurrency primitives.

### 4.3 Kotlin/JVM Risks

1. **The "off-heap everything" problem**: Reimplementing the storage engine on the JVM requires putting almost all hot-path data off-heap to avoid GC overhead and achieve the required memory layouts. This effectively means writing C-like code through Java APIs (`Unsafe`, `ByteBuffer`, `VarHandle`), negating most JVM advantages while retaining its limitations (no monomorphization, no cache-line alignment, no pointer arithmetic).

2. **Monomorphization gap**: The inability to specialize generic code is a fundamental performance barrier for the Tsavorite hot path. Every `ISessionFunctions` callback invocation goes through virtual dispatch on the JVM. Given that these callbacks are invoked per-operation (millions/sec), this overhead compounds.

3. **Project Valhalla dependency**: Many JVM limitations (no value types, no flat data structures) are targeted by Project Valhalla. However, Valhalla's timeline is uncertain, and a reimplementation should not depend on unreleased JVM features.

### 4.4 Hybrid Approach Consideration

A hybrid architecture could leverage both languages:
- **Rust**: Storage engine core (hybrid log, hash index, epoch system, record management, session functions), RESP parser, and network handler -- documents 02-10, 15.
- **Kotlin/JVM**: Higher-level application concerns (cluster management, stored procedure hosting, configuration, monitoring, object store data structures) -- documents 11-14.
- **Interface**: FFI via JNI or a shared-nothing message-passing protocol.

The RESP parser and network handler should remain in Rust, not Kotlin. The parsing layer is tightly coupled to buffer management (zero-copy `ArgSlice` -> `SpanByte` -> storage) and splitting it at the Kotlin/Rust boundary would require serializing key/value data across FFI, adding the very copies that Garnet's architecture is designed to avoid. The FFI boundary should be at the session level: batch of parsed commands -> Rust storage engine -> batch of responses. Even at this level, the FFI cost is non-trivial -- every key-value operation crosses the boundary, and there is no natural batching point except at the TCP receive level (which is already how Garnet works). The boundary requires serializing/deserializing `ArgSlice` arrays and `SpanByteAndMemory` outputs, which is a substantial engineering challenge.

## 5. Recommendation

**Rust is the recommended primary language** for a full reimplementation. The three most architecturally demanding subsystems (hybrid log, hash index, session function callbacks) all require low-level memory control that Rust provides natively. A JVM reimplementation would require extensive `Unsafe` usage that eliminates the JVM's safety advantages while retaining its performance limitations.

For organizations with strong JVM expertise and limited Rust experience, a **Kotlin application layer with a Rust storage engine** (the hybrid approach) is a viable alternative that balances development velocity with performance requirements.

## 6. Effort Estimates

Rough engineering effort estimates for a full reimplementation (excluding DiskANN/VectorSet):

**Rust full-stack**: 38-54 engineer-months. The storage engine (docs 02-06) accounts for ~40% of effort due to unsafe code complexity and correctness invariants. Budget 30% more time than initially estimated for the storage engine -- the operation flows in doc 06 Section 2.12 reveal correctness invariants (CPR checks, fuzzy region retry, read-cache interaction, transient locking) that are easy to get wrong.

**Kotlin/JVM full-stack**: 31-47 engineer-months. Faster initial development for the application layer, but the storage engine requires extensive off-heap programming that negates JVM productivity advantages. Performance tuning to reach acceptable throughput may add significant additional time.

**Hybrid (Rust storage + Kotlin app)**: 35-50 engineer-months. FFI boundary design and testing adds overhead, but each subsystem is implemented in its natural language.

These estimates assume experienced engineers familiar with the target language and systems programming. Testing infrastructure (property-based tests, fuzz testing, stress tests) should be budgeted separately at ~15-20% of implementation time.

## 7. JVM-Specific Operational Considerations

### GC Collector Selection

A Kotlin/JVM reimplementation requires a low-pause garbage collector. **G1 is not acceptable** for this workload profile (millions of ops/sec with 2-5 allocations per operation). Required collectors:

- **ZGC** (JDK 15+): Sub-millisecond pauses, handles high allocation rates. Recommended for production.
- **Shenandoah** (Red Hat): Similar sub-ms pause characteristics. Alternative to ZGC.

Without ZGC/Shenandoah, p99 latency will be dominated by GC pauses rather than actual operation cost.

### Warmup Characteristics

Rust has no warmup (compiled ahead of time). The JVM JIT requires warmup to reach peak performance -- tiered compilation reaches steady state over the first minutes of operation. For a server that must handle traffic immediately after start, this is a practical concern. C#'s tiered compilation reaches peak faster than the JVM's C2 compiler.

### Memory Overhead

Rust can match C#'s memory layout exactly. The JVM adds ~16 bytes per object header + alignment padding. For a system storing millions of small records, this compounds to significant overhead. Off-heap storage avoids this but requires manual memory management.

## 8. Testing and Correctness Gaps

The 10-dimension scoring framework covers development and performance but omits testing infrastructure. Garnet has complex correctness invariants (epoch safety, CPR consistency, fuzzy region filtering, transaction isolation) that a reimplementation must verify extensively.

- **Rust**: `loom` for model-checking concurrency protocols, `miri` for undefined behavior detection in unsafe code, `cargo-fuzz` for input fuzzing. The type system prevents some bug classes but cannot encode epoch invariants.
- **Kotlin/JVM**: `jqwik` for property-based testing, mature fuzzing and profiling ecosystem (`async-profiler`, JFR). Better debugging and profiling ergonomics (JMX, jconsole) vs. Rust's `gdb`/`lldb`/`perf`.

Investment in testing is critical regardless of language: property-based tests for epoch safety, fuzz testing for RESP parsing, stress tests for concurrent record operations.

## 9. Open Questions

1. **Unsafe surface area**: Estimated at ~40-50% of the storage engine code (see D5 for per-subsystem breakdown). The unsafe surface can be encapsulated behind safe abstractions (`Page`, `RecordRef`, `BucketRef`, `EpochGuard`), but bugs in the unsafe core (incorrect offset arithmetic, missing barriers in latch protocol) would be undetectable by the compiler. Mitigation: `loom` for concurrency testing, `miri` for UB detection, `cargo-fuzz` for input fuzzing. Does this volume of unsafe code negate Rust's safety story for this specific project?
2. **FFI overhead**: If a hybrid approach is chosen, what is the per-operation FFI cost? Can it be amortized via batching?
3. **Valhalla timeline**: If Project Valhalla delivers value types within the reimplementation timeframe, does this change the JVM feasibility for the storage engine?
4. **Async/epoch interaction**: Both languages struggle with epoch protection in async contexts (task migration across threads). What is the best pattern for each?
5. **Build and CI complexity**: Rust's compilation times are longer than C#/JVM. How does this affect development velocity for a large codebase?
6. **DiskANN/Vector store**: The vector store (third Tsavorite instance) adds another performance-critical subsystem. Does this further favor Rust or is it equivalent across languages?
