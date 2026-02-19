# Feasibility Assessment: Kotlin/JVM x Storage Engine
## Assessor: assess-kotlin-sys

### Coverage Rating: B+

The documentation is thorough and actionable for reimplementation. Each document includes dedicated Kotlin/JVM mapping notes, explicit bit layouts, and performance-critical design rationale. The gap between "documented" and "implementable" is small for the data structures, but larger for the concurrency/memory model interactions where JVM semantics diverge fundamentally from C#/.NET.

---

### Dimension Scores

| Dimension | Score (1-10) | Notes |
|-----------|-------------|-------|
| Type Erasure Impact | 3 | Severe. C# monomorphizes `AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator>` -- the JIT inlines all record access through the concrete `SpanByteAllocator` struct wrapper. JVM erases generics, so every call through a generic allocator interface is a virtual dispatch. The docs explicitly note "the JIT compiler can inline all record access methods and eliminate virtual dispatch overhead" (doc 02, Section 1). On the JVM, you must either (a) hand-specialize the hot paths (losing genericity), (b) use code generation (annotation processors / bytecode rewriting), or (c) accept ~15-30% overhead from megamorphic call sites. |
| Value Type Gap | 2 | Critical. The docs describe at least 6 value types that are layout-critical: `RecordInfo` (8 bytes, atomic CAS), `SpanByte` (4-byte header + inline payload), `HashBucketEntry` (8 bytes packed long), `HashBucket` (64 bytes = 1 cache line), `PageOffset` (8 bytes, overlapping int+long layout), `EpochEntry` (64 bytes, cache-line-padded). On the JVM, none of these can be stack-allocated or densely packed in arrays. Each would need to be either (a) a primitive long with manual bit extraction, or (b) an offset into an off-heap buffer. The `PageOffset` union trick (Interlocked.Add on overlapping int/long -- doc 02, Section 2.5) is especially problematic: JVM has no union types, so you must use long arithmetic with explicit shift/mask for the page and offset components, losing the natural carry behavior. |
| Unsafe Memory Access | 3 | Significant overhead. C# uses `unsafe` pointers freely: `nativePointers[pageIndex] + offset` for physical address lookup (doc 02, Section 2.2), pointer arithmetic into `HashBucket*` for tag scanning (doc 03, Section 2.5), direct struct field access via `FieldOffset` for epoch entries (doc 04, Section 2.2). JVM alternatives: (a) `sun.misc.Unsafe` -- functional but internal API, may be restricted; (b) `MemorySegment` (Panama, JDK 21+) -- bounds-checked by default, ~5-15ns overhead per access depending on JIT optimization; (c) `ByteBuffer` -- additional indirection and bounds checks. For the hash index scan path (7 entries per bucket, 1 cache line), adding bounds checks per entry access is costly. |
| Cache-line Alignment | 2 | Very limited. The docs are explicit about cache-line alignment requirements: `HashBucket` is exactly 64 bytes (doc 03, Section 2.3), epoch table entries are 64 bytes with `[StructLayout(Size = 64)]` (doc 04, Section 2.2), the hash table itself is sector-aligned via pinned arrays (doc 03, Section 2.9). JVM provides `@Contended` (JDK 8+), but it only adds padding around fields in objects -- it cannot control the absolute alignment of array elements. For off-heap memory, `MemorySegment.allocateNative(layout, align)` can provide alignment, but then all access goes through the Panama API. There is no way to get a cache-line-aligned array of structs on the JVM heap. |
| Lock-free Atomics | 7 | Mostly adequate. `VarHandle` (JDK 9+) provides `compareAndSet`, `getAndAdd`, `getAcquire`, `setRelease` on arrays, byte buffers, and fields. `AtomicLongArray` works for the hash bucket entries. The main gap is the `PageOffset` union trick -- `Interlocked.Add` on a packed page+offset where overflow carries naturally (doc 02, Section 2.5). On JVM, you must use `AtomicLong.addAndGet()` and manually decompose the result, which is functionally correct but adds branch overhead. The `Interlocked.CompareExchange` on epoch table entries (doc 04, Section 2.5) maps directly to `VarHandle.compareAndSet`. The latch protocol in `HashBucket` (doc 03, Section 2.4) -- CAS on packed shared/exclusive bits in a long -- also maps cleanly to `VarHandle` CAS on a `long[]`. |
| Pinned Memory | 3 | Major complexity. C# uses `GC.AllocateArray<byte>(size, pinned: true)` for both log pages (doc 02, Section 2.2) and the hash table (doc 03, Section 2.9). Pinned arrays are on-heap but immovable, enabling native pointer access with no GC interference. JVM alternatives: (a) `ByteBuffer.allocateDirect()` -- limited to 2GB per buffer (int-indexed), requires `Cleaner` or manual deallocation, not subject to GC compaction; (b) `Unsafe.allocateMemory()` -- no size limit, but fully manual lifecycle; (c) `MemorySegment.allocateNative()` -- cleaner API, auto-closeable, JDK 21+. For Tsavorite's pages (typically 16MB-32MB each), DirectByteBuffer works, but the 2GB limit means the hash table (which can be very large) may need multiple segments or Unsafe allocation. The sector-alignment requirement for direct I/O (doc 02, Section 2.2) adds another layer -- `posix_memalign` equivalent must be obtained via JNI or the Panama linker. |
| ThreadStatic | 5 | Moderate impact. C# `[ThreadStatic]` (doc 04, Section 2.4) provides O(1) access to per-thread metadata with no hash lookup. JVM `ThreadLocal<T>` uses a `ThreadLocalMap` with linear probing -- typically O(1) amortized but with higher constant factor (~10-20ns vs ~1-2ns for ThreadStatic). The epoch system is on the critical path of every operation (`Resume`/`Suspend` bracket every store call -- doc 04, Section 2.6). With 16 `LightEpoch` instances possible (doc 04, Section 2.3), each thread needs its `InstanceIndexBuffer` (16 ints). On JVM, this is a single `ThreadLocal<IntArray>` access, which is acceptable. The real concern is the hot-path `Acquire`/`Release` cycle where `ThreadLocal.get()` is called twice (once for metadata, once for the entry reference). A workaround: cache the epoch entry index in a `long` field on the session object, avoiding ThreadLocal after initial setup. |
| Performance Ceiling | 4 | Realistic ceiling: ~40-60% of C# Garnet throughput. See detailed analysis below. |

---

### Fundamental JVM Limitations

#### Hybrid Log Allocator (doc 02)

| C# Feature | JVM Equivalent | Performance Cost |
|-------------|---------------|-----------------|
| `GC.AllocateArray(pinned: true)` | `DirectByteBuffer` / `Unsafe.allocateMemory` | +5-10% overhead for all page access due to bounds checks or API indirection |
| `nativePointers[]` pointer array for O(1) address lookup | `long[]` of base addresses + offset arithmetic | Functionally equivalent, but every `GetPhysicalAddress` requires a method call to read from the ByteBuffer rather than a pointer dereference |
| `Interlocked.Add` on `PageOffset` union | `AtomicLong.addAndGet()` + shift/mask decomposition | +2-5ns per allocation due to decomposition; the natural page-carry trick is lost |
| Monomorphized `AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator>` | Erased generics; must hand-specialize or use code generation | +15-30% on the entire record access path if not specialized |
| `Span<byte>` / pointer slicing for record access | `ByteBuffer.slice()` or offset+length pairs | Allocation pressure from `slice()`, or manual offset tracking |

#### Hash Index (doc 03)

| C# Feature | JVM Equivalent | Performance Cost |
|-------------|---------------|-----------------|
| `[StructLayout(Size=64)]` for cache-line buckets | Off-heap `MemorySegment` with stride 64 | Lose hardware prefetch hinting; off-heap access adds bounds check overhead |
| `HashBucket*` pointer arithmetic for bucket scan | `long[]` indexing with stride 8 (8 longs per bucket) | Adds index multiplication; mitigated by JIT strength reduction |
| `unsafe` raw pointer for `FindTag` inner loop | `VarHandle.get(long[], index)` or `Unsafe.getLong(base, offset)` | `VarHandle`: +3-5ns per entry due to bounds/type checks. `Unsafe`: comparable to C# but unsupported API |
| Pinned table allocation with sector alignment | `Unsafe.allocateMemory` + manual alignment | Manual lifecycle management; risk of memory leaks |

#### Epoch System (doc 04)

| C# Feature | JVM Equivalent | Performance Cost |
|-------------|---------------|-----------------|
| `[ThreadStatic]` for per-thread metadata | `ThreadLocal<T>` | +10-15ns per access on hot path (Resume/Suspend) |
| `[StructLayout(Size=64)]` epoch entries | `long[]` with stride 8 (8 longs per entry) | Waste 56 bytes per entry; on-heap array may not be cache-line-aligned |
| `Interlocked.CompareExchange` on entry fields | `VarHandle.compareAndSet` on `long[]` element | Functionally equivalent; minor overhead from VarHandle indirection |
| `Thread.MemoryBarrier()` full fence | `VarHandle.fullFence()` | Equivalent on x86; may be slightly more expensive on ARM due to JVM's conservative barrier emission |

#### Checkpointing (doc 05)

| C# Feature | JVM Equivalent | Performance Cost |
|-------------|---------------|-----------------|
| `SystemState` packed 64-bit struct | `AtomicLong` with bitwise ops | Clean mapping; no significant overhead |
| Async I/O with `WriteAsync` | `AsynchronousFileChannel` or coroutine-wrapped NIO | Comparable performance; Kotlin coroutines map well to async state machine |
| `Action` delegates in drain list | `Runnable` / lambda in `AtomicReferenceArray` | Minor GC pressure from lambda allocation; mitigated by caching |

#### Record Management (doc 06)

| C# Feature | JVM Equivalent | Performance Cost |
|-------------|---------------|-----------------|
| `RecordInfo` as value type with bitfield accessors | `long` primitive with manual bit ops | Clean mapping when stored in off-heap buffer |
| `SpanByte` with `FieldOffset` union (length + pointer overlay) | Offset+length pair into ByteBuffer | Lose the union trick; need separate serialized/unserialized code paths |
| `StructLayout(Explicit)` for overlapping fields | No JVM equivalent | Must use explicit shift/mask for all packed structures |
| In-place CAS on `RecordInfo` word | `Unsafe.compareAndSwapLong` on off-heap address | Equivalent if using Unsafe; MemorySegment CAS has additional overhead |

---

### Workaround Analysis

#### 1. Type Erasure -- Hand-specialization
**Workaround**: Create concrete `SpanByteAllocator` and `SpanByteStore` classes without generic parameters. Inline the hot paths manually. Use Kotlin inline functions where applicable.
**Overhead**: Loss of genericity (cannot support multiple key/value types without code duplication). Development complexity increases 2-3x for the allocator layer.

#### 2. Value Types -- Off-heap memory with manual layout
**Workaround**: Allocate all critical data structures (pages, hash table, epoch table) off-heap via `Unsafe.allocateMemory` or `MemorySegment`. Access via computed offsets with `Unsafe.getLong`/`putLong`.
**Overhead**: All access requires explicit offset calculation. No compiler help for layout correctness. Memory leaks if lifecycle not managed carefully. Approximately +5-15% throughput overhead from API indirection.

#### 3. PageOffset Union Trick
**Workaround**: Use `AtomicLong.addAndGet()`. Extract page = `(int)(result >>> 32)`, offset = `(int)(result & 0xFFFFFFFFL)`. Detect page overflow by checking if offset > pageSize after the add.
**Overhead**: The page-carry is no longer implicit -- need explicit branch after every allocation. Adds ~2-3 instructions to the allocation hot path. Functionally correct.

#### 4. Cache-line Alignment
**Workaround**: For on-heap arrays, use `@Contended` on wrapper classes (limited to JDK internal use or `--add-opens`). For off-heap, allocate with `MemorySegment.allocateNative(layout.withBitAlignment(512))` (64 bytes = 512 bits).
**Overhead**: Off-heap access adds API overhead. On-heap `@Contended` requires JVM flags and doesn't work on array elements. Most practical approach: off-heap for hash table and epoch table, accept the access overhead.

#### 5. ThreadStatic Replacement
**Workaround**: Cache epoch entry index in the session/client object. Only call `ThreadLocal.get()` on first access per session acquire, then use the cached index.
**Overhead**: Requires session objects to be thread-affine. If sessions migrate between threads (coroutine dispatcher), the cache must be invalidated. Adds ~1 branch per operation.

#### 6. Pinned Memory for Direct I/O
**Workaround**: Use `Unsafe.allocateMemory` for page buffers with manual sector alignment. Wrap in a `Closeable` lifecycle manager. For I/O, use JNI to call `pwrite`/`pread` with `O_DIRECT`, or use the Panama Foreign Linker to call POSIX APIs directly.
**Overhead**: Significant development complexity. JNI calls add ~50-100ns overhead per I/O operation. The Panama linker is cleaner but requires JDK 21+.

---

### Performance Ceiling Estimate

**Relative to Redis (single-threaded)**:
Garnet claims ~10-100x Redis throughput for many workloads due to multi-threaded operation, in-place updates, and efficient memory management. A Kotlin/JVM reimplementation of the storage engine would likely achieve **3-15x Redis**, depending on workload:
- **Read-heavy, in-memory**: ~8-15x Redis (hash index scan and epoch overhead are the bottlenecks, but multi-threading scales well)
- **Write-heavy, in-memory**: ~3-8x Redis (type erasure overhead on allocation path, ThreadLocal overhead on epoch acquire/release, loss of in-place PageOffset trick)
- **Mixed with disk tiering**: ~5-12x Redis (disk I/O dominates; JVM overhead is amortized)

**Relative to C# Garnet**:
- **Optimistic estimate (with Unsafe + hand-specialization)**: 55-70% of Garnet throughput
- **Realistic estimate (with MemorySegment + idiomatic Kotlin)**: 40-55% of Garnet throughput
- **Conservative estimate (pure on-heap, no Unsafe)**: 25-35% of Garnet throughput

**Key bottlenecks in order of impact**:
1. **Type erasure / virtual dispatch** on allocator hot paths: ~15-25% overhead
2. **Off-heap access overhead** for pages, hash table, epoch table: ~10-15%
3. **ThreadLocal vs ThreadStatic** on epoch acquire/release: ~5-10%
4. **Loss of struct value types** causing GC pressure for temporary objects: ~5-10%
5. **Bounds checking** on all array/buffer access: ~3-5%

---

### Missing Information

1. **Direct I/O details**: The docs mention sector-aligned pages for direct I/O (doc 02) but don't specify the I/O syscall interface (`pwrite`/`pread`, `io_uring`, `WriteFile` with `FILE_FLAG_NO_BUFFERING`). A Kotlin reimplementation needs to know which I/O model to target.

2. **Memory ordering requirements**: The docs note reliance on x86 TSO (doc 04, Section 7.3) but don't enumerate which stores/loads depend on TSO vs explicit barriers. The JVM's memory model (JSR-133) is weaker than TSO in some respects (plain reads/writes have no ordering), so all epoch and RecordInfo accesses need VarHandle acquire/release semantics -- but which ones?

3. **Read cache integration**: The ReadCache bit appears in both HashBucketEntry (doc 03) and RecordInfo (doc 06), but read cache allocation, eviction, and chain traversal are not documented. This is a significant subsystem for a reimplementation.

4. **Object log serialization**: The docs mention `GenericAllocator` and an `ObjectLogDevice` (doc 02, Section 7.3) but don't detail the serialization protocol. Garnet's object store uses this path.

5. **Lock table and transactions**: Doc 03 mentions that index resize needs a "transaction barrier" and lock table migration, but the lock table structure and transaction protocol are not documented.

6. **Garnet-specific RecordInfo bits**: `ETag` (bit 58), `VectorSet` (bit 63) are noted as application-level metadata (doc 06, Section 7.2). How these interact with checkpointing, compaction, and revivification is unclear.

7. **Overflow bucket lifecycle**: Doc 03 notes that overflow buckets are never freed during normal operation. The interaction between overflow buckets and index checkpointing/recovery needs clarification.

8. **Thread pool sizing**: The docs assume C#'s ThreadPool with work-stealing. How does the epoch system interact with Kotlin coroutine dispatchers (which use a fixed thread pool)?

---

### Valhalla Impact

Project Valhalla (value types for JVM) would significantly change the assessment if it ships with full feature parity:

**What Valhalla provides**:
- `value class` (inline classes with identity-free semantics): stack allocation, flat array layouts, no object headers
- `IdentityFree` objects that can be scalarized by the JIT
- Specialized generics (potentially): generic instantiation without boxing

**Impact on each dimension**:

| Dimension | Current Score | Post-Valhalla Score | Change |
|-----------|-------------|-------------------|--------|
| Type Erasure | 3 | 6-7 | Specialized generics eliminate boxing; JIT can inline through value-typed allocator wrappers |
| Value Type Gap | 2 | 7-8 | `RecordInfo`, `HashBucketEntry`, `PageOffset`, `SpanByte` become true value types with flat array storage |
| Unsafe Memory Access | 3 | 4-5 | Value types in flat arrays reduce need for off-heap tricks; but direct I/O still needs off-heap buffers |
| Cache-line Alignment | 2 | 4-5 | Flat arrays of value types improve locality but explicit alignment control is still limited |
| Lock-free Atomics | 7 | 8 | Atomic operations on value-typed fields become cleaner with VarHandle improvements |
| Pinned Memory | 3 | 4 | Flat value arrays reduce GC pressure but still need off-heap for direct I/O pages |
| ThreadStatic | 5 | 5 | Valhalla doesn't address thread-local storage |
| Performance Ceiling | 4 | 6-7 | Estimated 65-80% of Garnet throughput with Valhalla + hand-specialization |

**Overall Valhalla verdict**: Would raise the performance ceiling from ~55% to ~75% of C# Garnet. The remaining gap comes from:
- No `pinned` allocation equivalent (GC can still move heap objects, even value types in arrays)
- No `StructLayout(Explicit)` for overlapping fields
- ThreadLocal overhead remains
- Direct I/O still requires off-heap buffers and JNI/Panama

**Valhalla timeline risk**: As of early 2026, Valhalla value types have preview status in JDK 23+ but specialized generics are not yet shipped. Building a production storage engine on preview features is risky. A pragmatic approach: design the off-heap layout now, add Valhalla value types as an optimization layer when they stabilize.

---

### Summary Verdict

A Kotlin/JVM reimplementation of Tsavorite is **technically feasible but will require extensive use of `sun.misc.Unsafe` or Panama MemorySegment for competitive performance**. The documentation is sufficient to implement all subsystems, but the fundamental JVM limitations around value types, pinned memory, and monomorphized generics create a persistent performance gap of 30-45% versus C# Garnet.

The storage engine is the **hardest part** of Garnet to port to the JVM because it is the layer most dependent on C#-specific low-level features. The RESP protocol layer, command processing, and cluster management would port more naturally.

**Recommendation**: If targeting Kotlin/JVM, consider a hybrid approach -- use the JVM for the command processing and cluster layers, but call into a native (Rust or C++) storage engine via JNI/Panama for the hot path. This preserves Kotlin's strengths (coroutines, ecosystem, developer productivity) while avoiding the JVM's weaknesses for low-level memory management.
