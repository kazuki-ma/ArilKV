# Feasibility Assessment: Kotlin/JVM x Server/Network
## Assessor: assess-kotlin-app

### Coverage Rating: B+

The four documents (07-network-layer, 08-resp-parser, 09-request-lifecycle, 12-transactions-and-scripting) provide excellent coverage of the server/network layer. The request lifecycle document is particularly valuable, tracing GET/SET/INCR end-to-end. The Kotlin/JVM mapping notes in each document are present but terse -- they acknowledge the challenges without always quantifying the cost. Missing: explicit latency numbers, benchmark comparisons, and GC tuning guidance for high-throughput scenarios.

---

### Dimension Scores

| Dimension | Score (1-10) | Notes |
|-----------|-------------|-------|
| Network I/O (SAEA -> Netty) | 7 | Netty's epoll transport is genuinely competitive. The SAEA accept loop (burst accept without returning to thread pool) maps well to Netty's boss/worker model. Netty's `ChannelOption.TCP_NODELAY`, write watermarks, and `EpollEventLoop` are direct equivalents. This is the strongest dimension. |
| Zero-Copy Parsing | 3 | Garnet's `byte*` pointer parsing with `ArgSlice` (12-byte struct, pointer+length into pinned receive buffer) is fundamentally incompatible with the JVM's memory model. `ByteBuffer.slice()` creates a new object per slice. Panama `MemorySegment` adds bounds-checking overhead. Every `ArgSlice` equivalent on JVM is an object allocation. |
| Buffer Management | 6 | Netty's `PooledByteBufAllocator` provides a similar tiered pooling strategy with thread-local caches (arena-based, size classes). It is production-proven and arguably more mature than `LimitedFixedBufferPool`. However, Garnet's pinned arrays with raw `byte*` pointers have zero indirection; Netty's `ByteBuf` adds at least one vtable dispatch per access. |
| Command Dispatch | 6 | Kotlin `when` compiles to `tableswitch`/`lookupswitch` bytecode, which the JIT can optimize to a jump table. However, Garnet's C# switch over a `ushort` enum with ~200+ cases compiles to a dense jump table directly. The JVM's `lookupswitch` for sparse enums is O(log n). A `HashMap<String, Handler>` lookup adds hashing + boxing overhead. Netty's `AsciiString` for command matching avoids `String` allocation but still involves hash computation. |
| Transaction Support | 5 | Kotlin coroutines offer no meaningful advantage for MULTI/EXEC. The transaction model is fundamentally synchronous: lock keys in sorted order, replay commands, commit. Coroutines help with async I/O but the transaction execution phase is CPU-bound with locks held. The `WatchVersionMap` maps cleanly to `AtomicLongArray`. The sorted lock protocol (10-byte `TxnKeyEntry` with sort+lock) would use boxed objects on JVM, adding GC pressure. |
| GC Pressure | 2 | This is the critical weakness. Garnet achieves zero allocations on the hot path: `ArgSlice` is a 12-byte struct (not heap-allocated), `SessionParseState` uses a pinned array allocated once per session, response writing goes directly to pinned send buffers via `byte*` pointers. On JVM, EVERY equivalent operation allocates: `ArgSlice` data class = heap object, `ByteBuffer.slice()` = heap object, boxed integers for parsing, `String` for error messages. Conservatively 5-15 object allocations per simple GET vs. zero in C#. |
| Inlining Limits | 4 | Garnet uses `AggressiveInlining` on `RespWriteUtils.TryWrite*`, `RespReadUtils.TryRead*`, and parsing primitives. The C# JIT honors these hints, inlining small methods into command handlers. The JVM JIT makes autonomous inlining decisions based on call-site frequency and method size. HotSpot's C2 compiler generally inlines well for hot paths, but: (a) the JVM has a hard bytecode size limit for inlining (~325 bytes default), (b) virtual dispatch on `ByteBuf` methods prevents some inlining, (c) the JIT needs warmup time whereas Garnet's AOT/tiered compilation is faster to reach peak. |
| Realistic Performance | 4 | See detailed estimate below. |

---

### Netty as Bright Spot

Netty is the one area where the JVM can genuinely approach Garnet's C# performance, and in some configurations may even match it:

**Where Netty matches or approaches SAEA:**
1. **Epoll-based event loop**: Netty's `EpollEventLoopGroup` uses direct epoll syscalls via JNI, bypassing the NIO selector overhead. This is functionally equivalent to SAEA's completion-port / epoll model on Linux.
2. **Connection accept**: Netty's boss group handles accept bursts efficiently. The `SO_REUSEPORT` option enables multiple accept threads, potentially better than Garnet's single-SAEA accept loop under extreme connection rates.
3. **Write batching**: Netty's `channelRead()` / `channelReadComplete()` pattern naturally supports Garnet's pipelining model -- accumulate responses during `channelRead`, flush on `channelReadComplete`. This is a clean mapping of Garnet's "process all commands then Send()" pattern.
4. **Backpressure**: Netty's write watermark (`WRITE_BUFFER_WATER_MARK`) provides equivalent functionality to Garnet's `throttleCount` / `ThrottleMax` mechanism, with arguably better ergonomics.
5. **TLS**: Netty + BoringSSL (via `netty-tcnative`) can outperform .NET's `SslStream` because it uses OpenSSL's optimized C implementation directly. This could be a genuine advantage.
6. **Direct buffers**: `ByteBuffer.allocateDirect()` and Netty's `UnpooledUnsafeDirectByteBuf` provide off-heap memory that avoids GC scanning, partially addressing the GC pressure concern for network buffers.

**Where Netty falls short:**
1. **Synchronous completion fast path**: Garnet's SAEA loops check `ReceiveAsync` returning `false` for synchronous completion and loop without returning to the thread pool. Netty's event loop model always returns to the event loop between operations, adding one extra context switch per receive in the fast case.
2. **Buffer pointer access**: Garnet passes `byte*` directly from the receive buffer to the parser. Netty requires `ByteBuf.memoryAddress()` (only for direct buffers) plus bounds checking, or copying to a byte array.
3. **Send buffer reuse**: Garnet's `LightConcurrentStack<GarnetSaeaBuffer>` with `SpinLock` is a lock-free-ish stack optimized for the specific send pattern. Netty's buffer recycler is more general-purpose with higher overhead.

**Net assessment**: Netty can achieve 80-90% of Garnet's raw network I/O performance. The gap is in the per-operation overhead (vtable dispatch, bounds checking, event loop indirection) rather than fundamental throughput limits.

---

### JVM Pain Points

1. **No value types (structs)**: This is the single biggest limitation. Garnet's `ArgSlice` (12 bytes, stack/inline allocated), `TxnKeyEntry` (10 bytes, pinned array), and `SpanByte` (pointer+length, stack allocated) are all value types that avoid heap allocation. On the JVM, every equivalent is a heap-allocated object with a 12-16 byte header. Valhalla (Project Valhalla / value types for Java) is still not production-ready as of early 2026. Kotlin inline classes only work for single-field wrappers, not multi-field structs like ArgSlice.

2. **No pinned memory / raw pointers**: Garnet's zero-copy path relies on `GC.AllocateArray<byte>(size, pinned: true)` + `Unsafe.AsPointer` to get stable `byte*` pointers into the receive buffer. The JVM has no equivalent. Panama's `MemorySegment` provides off-heap memory access but with bounds-checking overhead on every access. `sun.misc.Unsafe` provides raw access but is being deprecated/restricted.

3. **No `AggressiveInlining` control**: The JVM JIT decides inlining autonomously. While HotSpot's C2 compiler is excellent at inlining hot methods, it cannot be forced. Garnet's `RespReadUtils` methods are explicitly marked for inlining -- on the JVM, the equivalent methods might or might not be inlined depending on call-site frequency, code size, and JIT tier.

4. **Bounds checking cannot be eliminated for pointer-based parsing**: Garnet's parser does `*ptr == '$'` then `ptr++` -- zero overhead. JVM equivalent: `buffer.get(position)` with bounds check, then `position++`. HotSpot can sometimes eliminate bounds checks in tight loops, but not for the irregular access patterns in RESP parsing.

5. **SIMD/bit-twiddling limitations**: Garnet's `MakeUpperCase` reads two `ulong` values from a pointer and uses bit manipulation to check 8 bytes at once. On the JVM, this requires either `Unsafe.getLong()` (deprecated path) or `MemorySegment.get(JAVA_LONG, offset)` (Panama). The Vector API (Project Panama) could provide SIMD, but it is still in incubator status.

6. **Object header overhead**: Every JVM object has a 12-16 byte header (mark word + klass pointer + potential padding). A 12-byte `ArgSlice` on C# becomes a 28-32 byte object on JVM. For `SessionParseState` with 5 ArgSlices: C# = 60 bytes (one cache line); JVM = 5 * 32 = 160 bytes + array overhead = ~200 bytes (3+ cache lines).

---

### Allocation Analysis

**C# Garnet hot path (GET command):**
- Per-request allocations: **0**
- `ArgSlice` array: pre-allocated, pinned, reused across requests
- Response buffer: pooled via `LightConcurrentStack`, reused
- Receive buffer: pooled via `LimitedFixedBufferPool`, reused
- Command parsing: pointer arithmetic on pinned buffers, no allocation
- `SpanByteAndMemory` output: stack-allocated struct pointing at send buffer
- Total per-GET GC pressure: **0 bytes**

**Kotlin/JVM equivalent (GET command), best-case with careful optimization:**
- `ArgSlice` data class for key: 1 allocation (~32 bytes)
- `ByteBuffer.slice()` or position/limit manipulation: 0-1 allocations
- Command string comparison (if using `String`): 1 allocation (~40 bytes)
- Response `ByteBuffer` management: 0 (if using Netty pooled `ByteBuf`)
- Boxing for integer parsing results: 0-1 (if using primitive specialization)
- Lambda/closure for command dispatch (if functional style): 0-1
- **Best case: 1-3 allocations, ~50-100 bytes per GET**
- **Typical case: 3-8 allocations, ~150-300 bytes per GET**

**Kotlin/JVM equivalent (SET command):**
- Same as GET plus:
- `RawStringInput` equivalent: 1 allocation
- Key `ArgSlice`: 1 allocation
- Value `ArgSlice`: 1 allocation
- **Best case: 3-5 allocations, ~100-200 bytes per SET**

**Kotlin/JVM equivalent (MULTI/EXEC with 5 commands):**
- `TxnKeyEntry` per key: 5 allocations (vs. pinned struct array in C#)
- Sorting requires `Comparator` allocation (or pre-allocated)
- Watch version reads: 0 extra (AtomicLongArray is fine)
- Per-command parsing during replay: same as individual commands
- **Best case: 15-25 allocations, ~500-1000 bytes per transaction**

**GC impact at scale:**
At 1M ops/sec:
- C#: 0 allocations/sec from hot path -> no GC pressure
- Kotlin best case: 1-3M allocations/sec -> ~50-300 MB/sec allocation rate
- ZGC or Shenandoah can handle this but adds 1-3% CPU overhead and occasional pauses
- Young generation collection every ~100-500ms depending on heap size
- Each minor GC: 1-5ms pause (ZGC) or sub-ms (Shenandoah)

At 10M ops/sec (Garnet's target with pipelining):
- Kotlin: 10-30M allocations/sec -> 500MB-3GB/sec allocation rate
- This stresses even modern low-pause GCs significantly
- G1 would be unacceptable (10-50ms pauses); ZGC or Shenandoah required
- Allocation rate itself becomes a bottleneck (TLAB exhaustion, pointer bumping contention)

---

### Missing Information

1. **Latency percentiles**: The docs describe the architecture thoroughly but provide no p50/p99/p999 latency numbers. Without these, it is impossible to quantify the JVM GC pause impact against a concrete baseline.

2. **Throughput benchmarks**: No ops/sec numbers for GET/SET/INCR. The docs say the hot path is zero-allocation and pipelining amortizes syscalls, but quantitative targets are not stated.

3. **Memory footprint per connection**: The docs describe buffer sizes (128KB-1MB tiered pool) but not total memory per connection including session state, parse state, and Tsavorite context objects.

4. **Thread model details**: How many I/O threads does Garnet use? Is it one thread per core? This affects how Netty's event loop group should be configured.

5. **Scatter-gather GET details**: The async GET path (`NetworkGET_SG`, `AsyncProcessor`) is mentioned but not fully traced. For a Kotlin implementation, understanding whether coroutines could improve this path would require more detail on the async completion model.

6. **RESP3 parsing paths**: The docs focus on RESP2. RESP3 adds map, set, push types. The additional parsing complexity and its JVM mapping are not covered.

7. **Custom command registration**: The docs mention a jump-table dispatch for ~200+ built-in commands but do not detail how custom commands are registered and dispatched. A HashMap-based dispatch for custom commands may be acceptable even if built-in dispatch uses a jump table.

---

### Realistic Performance Estimate

**Baseline: Redis (single-threaded, C)**
- GET: ~100K-200K ops/sec per core (pipelined: ~1M ops/sec)
- Well-established, highly optimized

**Garnet (C#, multi-threaded):**
- Documented as significantly faster than Redis due to: multi-threading, zero-allocation hot path, SAEA-based I/O, Tsavorite's hybrid log, pipelining
- Estimated: 5-10x Redis on equivalent hardware (based on public Microsoft benchmarks)

**Kotlin/JVM reimplementation estimate:**

| Component | vs. C# Garnet | Reasoning |
|-----------|--------------|-----------|
| Network I/O | 80-90% | Netty epoll is competitive; slight overhead from event loop indirection |
| RESP parsing | 40-60% | Bounds checking, object allocation for ArgSlice, no pointer arithmetic |
| Command dispatch | 70-85% | `when` expression is close but not as tight as C# jump table; JIT warmup |
| Storage operations | 60-75% | Tsavorite must be reimplemented; off-heap storage with manual serialization adds overhead |
| Response writing | 50-70% | No direct `byte*` writing; `ByteBuf.write*()` adds bounds checks and vtable dispatch |
| Transaction locking | 70-85% | `ReentrantReadWriteLock` is mature; sorted lock protocol works the same; object overhead for `TxnKeyEntry` |
| Overall throughput | 50-65% of C# Garnet | GC pressure is the dominant factor at high throughput |

**Final estimate:**
- **Kotlin/JVM Garnet vs. Redis: approximately 2.5-4x Redis** (compared to Garnet's estimated 5-10x Redis)
- At low throughput (<100K ops/sec): closer to C# Garnet (GC is not a bottleneck)
- At high throughput (>1M ops/sec): gap widens due to allocation rate and GC pauses
- p99 latency: likely 2-5x worse than C# Garnet due to GC pauses (even with ZGC)
- With aggressive optimization (object pooling, off-heap buffers, Panama MemorySegment): could reach 60-70% of C# Garnet

**Honest bottom line**: The JVM's lack of value types and inability to do zero-allocation pointer-based parsing makes it fundamentally unable to match C#'s server/network performance. Netty I/O is the bright spot, but the per-request processing overhead (parsing, dispatch, response writing) accumulates. A Kotlin reimplementation would still handily beat Redis (multi-threading + better data structures), but would leave 30-50% of Garnet's performance on the table. The investment may be justified for JVM ecosystem integration, but performance parity with C# Garnet is not achievable without Project Valhalla value types and mature Panama FFI.
