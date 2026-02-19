# Performance Engineering

## 1. Overview

This document catalogs every performance optimization technique used in Garnet. These techniques collectively achieve the roughly 100x throughput improvement over Redis that Garnet demonstrates on standard benchmarks. The optimizations span memory layout, compiler hints, lock-free data structures, buffer management, generic specialization, and I/O system design. This is the "secret sauce" document -- a reimplementation must apply these techniques (or their equivalents) to achieve comparable performance. Cross-references to the detailed designs: hybrid log ([Doc 02](./02-tsavorite-hybrid-log.md)), hash index ([Doc 03](./03-tsavorite-hash-index.md)), epoch protection ([Doc 04](./04-tsavorite-epoch-and-concurrency.md)), record management ([Doc 06](./06-tsavorite-record-management.md)), network layer ([Doc 07](./07-network-layer.md)), RESP parser ([Doc 08](./08-resp-parser.md)), session functions ([Doc 10](./10-storage-session-functions.md)).

## 2. Detailed Design

### 2.1 Generic Monomorphization

**What**: C# generics, unlike Java generics, produce distinct native code for each type instantiation (for value types). Garnet exploits this by using deeply nested generic type parameters throughout the storage and session layers, ensuring the JIT produces specialized code with no virtual dispatch.

**Where**: The canonical example is the type aliases in `RespServerSession.cs` [Source: libs/server/Resp/RespServerSession.cs:L23-L40]:

```csharp
using BasicGarnetApi = GarnetApi<
    BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long,
        MainSessionFunctions,
        StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
        SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
    BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long,
        ObjectSessionFunctions,
        StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
        GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ...>>>,
    BasicContext<SpanByte, SpanByte, VectorInput, SpanByte, long,
        VectorSessionFunctions,
        StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
        SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>>;
```

Note the three `BasicContext` type parameters: main store (SpanByte KV), object store (byte[]/IGarnetObject), and vector store (SpanByte KV with VectorSessionFunctions/VectorInput). However, `GarnetApi` only stores two context instances at runtime (main and object); the `TVectorContext` is a phantom type parameter -- vector operations route through `StorageSession`/`VectorManager` directly rather than a stored context. The monomorphization benefit still applies to all three contexts at the type-alias level for compile-time checking. [Source: libs/server/Resp/RespServerSession.cs:L23-L40, libs/server/API/GarnetApi.cs:L29-L37]

This creates a fully specialized type that the JIT can inline through multiple layers: `GarnetApi -> BasicContext -> TsavoriteKV -> Allocator -> StoreFunctions`. Every method call in this chain becomes a direct call (or is inlined entirely) rather than a virtual dispatch.

**Why it matters**: Each virtual dispatch costs approximately 5-10ns due to indirect branch prediction misses. In a tight storage lookup loop that executes millions of times per second, eliminating virtual dispatch from the 5-10 method calls per operation saves 25-100ns per operation.

**The dispatch pattern** is also visible in command processing [Source: libs/server/Resp/RespServerSession.cs:L728-L793]:
```csharp
private bool ProcessBasicCommands<TGarnetApi>(RespCommand cmd, ref TGarnetApi storageApi)
    where TGarnetApi : IGarnetApi
```
The `ref TGarnetApi` parameter is a constrained generic. When called with `ref basicGarnetApi` (a concrete struct type), the JIT generates a specialized version with all API calls inlined.

### 2.2 Cache-Line Alignment

**What**: Critical data structures are explicitly sized and aligned to CPU cache lines (64 bytes) to prevent false sharing and optimize spatial locality.

**HashBucket** [Source: libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:L11]:
```csharp
[StructLayout(LayoutKind.Explicit, Size = Constants.kEntriesPerBucket * 8)]
internal unsafe struct HashBucket
{
    [FieldOffset(0)]
    public fixed long bucket_entries[Constants.kEntriesPerBucket];
}
```
`kEntriesPerBucket` is 8, so each bucket is 64 bytes = exactly one cache line. A hash lookup touches exactly one cache line for the primary bucket.

**LightEpoch Entry Table** [Source: libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:L88, L170-L176]:
```csharp
const int kCacheLineBytes = 64;

tableRaw = GC.AllocateArray<Entry>(kTableSize + 2, true);
p = (long)Unsafe.AsPointer(ref tableRaw[0]);
long p2 = (p + (kCacheLineBytes - 1)) & ~(kCacheLineBytes - 1);
tableAligned = (Entry*)p2;
```
The epoch table is manually aligned to 64-byte boundaries. Each `Entry` is explicitly sized to exactly 64 bytes via `[StructLayout(LayoutKind.Explicit, Size = kCacheLineBytes)]` [Source: libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:L719]. The struct contains `localCurrentEpoch` (8 bytes at offset 0) and `threadId` (4 bytes at offset 8), with the remaining 52 bytes as padding to fill the cache line. This ensures different threads' epoch entries never share a cache line (preventing false sharing).

**RecordInfo** [Source: libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:L16-L17]:
```csharp
[StructLayout(LayoutKind.Explicit, Size = 8)]
public struct RecordInfo
{
    [FieldOffset(0)]
    private long word;
}
```
Exactly 8 bytes, packing all metadata (valid, tombstone, sealed, dirty, ETag, previous address, version flags) into a single `long` that can be atomically read or CAS'd.

### 2.3 Bit-Packed Metadata

**RecordInfo bit layout** (64 bits total) [Source: libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:L14]:
```
[VectorSet][Modified][InNewVersion][Filler][Dirty][ETag][Sealed][Valid][Tombstone][LLLLLLL][RAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA]
```
- Bits 0-47: Previous address (48 bits = 256TB address space)
- Bits 48-54: Leftover/reserved (7 bits)
- Bit 55: Tombstone
- Bit 56: Valid
- Bit 57: Sealed
- Bit 58: ETag
- Bit 59: Dirty
- Bit 60: Filler
- Bit 61: InNewVersion
- Bit 62: Modified
- Bit 63: VectorSet

All metadata checks are single-instruction mask-and-compare operations on the same `long` word, avoiding any pointer indirection.

**SpanByte header** [Source: libs/storage/Tsavorite/cs/src/core/VarLen/SpanByte.cs:L21-L31]:
```csharp
[StructLayout(LayoutKind.Explicit, Pack = 4)]
public unsafe struct SpanByte
{
    [FieldOffset(0)] private int length;    // Lower 29 bits = length, upper 3 bits = flags
    [FieldOffset(4)] private IntPtr payload;
}
```
- Bit 31: Unserialized (1) or serialized (0)
- Bit 30: Extra metadata present
- Bit 29: Namespace present
- Bits 0-28: Payload length (max 512MB)

This packs length and metadata flags into a single 4-byte header. With `Pack = 4` and fields at offsets 0 and 4, the total `SpanByte` size is 4 (int) + 8 (IntPtr) = 12 bytes on 64-bit. Unlike `ArgSlice` which has explicit `[StructLayout(Size = 12)]` and `public const int Size = 12`, `SpanByte` relies on `Pack = 4` without an explicit `Size` attribute, so the size is determined by the runtime's struct packing rules. On .NET 8+ with `Pack = 4` this yields 12 bytes, but no `sizeof(SpanByte)` assertion exists in the codebase to guarantee this. Implementors should verify with a runtime check.

### 2.4 AggressiveInlining Strategy

Garnet uses `[MethodImpl(MethodImplOptions.AggressiveInlining)]` extensively. The pattern is consistent:

**Always inline** (in hot-path code):
- Buffer pool Get/Return [Source: libs/common/Memory/LimitedFixedBufferPool.cs:L85, L111]
- Parse state access methods [Source: libs/server/Resp/Parser/SessionParseState.cs:L72, L89, L102, etc.]
- Network sender operations [Source: libs/common/Networking/GarnetTcpNetworkSender.cs:L155, L173, L191, L200, L210]
- RESP read/write utilities [Source: libs/common/RespReadUtils.cs:L24, L44, L117, L149]
- RecordInfo accessors [Source: libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:L57, L72, L79]
- `SendAndReset` [Source: libs/server/Resp/RespServerSession.cs:L1254-L1255] (one of the most performance-critical inlining decisions, as it is called in retry loops for every buffer-overflow response write)

**Intentionally NOT inline** (`[MethodImpl(MethodImplOptions.NoInlining)]`):
- Error paths: `ThrowInvalidOperationException` [Source: libs/common/Networking/NetworkHandler.cs:L338]
- Rare paths: `ShrinkNetworkReceiveBuffer` [Source: libs/common/Networking/NetworkHandler.cs:L513]
- Exception throwers: `RespParsingException.Throw*` methods

The strategy is to inline all code that runs on every request, and explicitly prevent inlining of error/rare paths to keep the hot method's instruction cache footprint small.

### 2.5 Zero-Allocation Patterns

**1. Pinned arrays with pointer access**:
```csharp
rootBuffer = GC.AllocateArray<ArgSlice>(MinParams, true);    // pinned
bufferPtr = (ArgSlice*)Unsafe.AsPointer(ref rootBuffer[0]);
```
[Source: libs/server/Resp/Parser/SessionParseState.cs:L64-L65]

`GC.AllocateArray<T>(pinned: true)` allocates on the Pinned Object Heap (POH), which the GC never relocates. This allows stable `byte*` / `T*` pointers without `GCHandle`. Used for:
- Parse state argument buffers
- Network receive/send buffers (via `PoolEntry`)
- Epoch table entries

**2. SpanByteAndMemory dual-mode output**:
```csharp
var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
```
[Source: libs/server/Resp/BasicCommands.cs:L35]

The output is initialized pointing at the send buffer. If the result fits (the common case), it's written directly there with zero allocation. Only if the result overflows does it fall back to a heap-allocated `IMemoryOwner<byte>`.

**3. Struct-based callbacks**:
```csharp
public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<...>
```
[Source: libs/server/Storage/Functions/MainStore/MainSessionFunctions.cs:L11]

`MainSessionFunctions` is a `readonly struct`, not a class. When passed as a generic type parameter, the JIT inlines all its method calls. No interface dispatch, no heap allocation for the callback object.

**4. stackalloc and Span for temporary buffers**:
Used in various places for temporary formatting or conversion buffers that don't escape the call stack.

### 2.6 Memory Pooling

**LimitedFixedBufferPool** [Source: libs/common/Memory/LimitedFixedBufferPool.cs]:
- **Power-of-2 tiered**: Level `i` holds buffers of `minAllocationSize << i`
- **Bounded per-level**: `maxEntriesPerLevel` (default 16) prevents unbounded pool growth
- **Lock-free core**: Uses `ConcurrentQueue<PoolEntry>` with `Interlocked` size tracking
- **Reference counting for safe disposal**: `totalReferences` tracks outstanding allocations; `Dispose()` spins until all references are returned

**GarnetSaeaBuffer recycling** [Source: libs/common/Networking/GarnetTcpNetworkSender.cs:L33, L78]:
- Send buffers (SAEA + byte array) are cached in a `LightConcurrentStack` per connection
- Stack capacity is `2 * ThrottleMax` (default 16)
- On send completion, the buffer is pushed back to the stack rather than allocated anew

### 2.7 Thread-Local Storage and Per-Thread Epoch

**LightEpoch Thread-Static Metadata** [Source: libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:L50-L83]:

The `[ThreadStatic]` fields are declared inside a nested `private class Metadata` within `LightEpoch`, not as direct members of `LightEpoch`:
```csharp
// Inside LightEpoch:
private class Metadata
{
    [ThreadStatic] internal static int threadId;
    [ThreadStatic] internal static ushort startOffset1;
    [ThreadStatic] internal static ushort startOffset2;
    [ThreadStatic] internal static InstanceIndexBuffer Entries;
}
```

Rather than using `ThreadLocal<T>` (which has allocation and lock overhead), Garnet uses `[ThreadStatic]` static fields for thread-local epoch data. The nesting in `Metadata` groups these logically. The `Entries` field is an `InstanceIndexBuffer` -- a fixed-size struct that stores per-LightEpoch-instance table indices. This gives each (thread, epoch-instance) pair O(1) access to its epoch table entry without any heap allocation or dictionary lookup.

The epoch table itself is a cache-line-aligned array where each thread's entry is at a unique index, preventing false sharing between threads.

### 2.8 Lock-Free Data Structures

**HashBucket Latch Protocol** [Source: libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:L11-L169]:

The hash bucket uses the overflow pointer word (entry index `kOverflowBucketIndex = 7`) for latching, embedding shared/exclusive latch bits in the high bits of the overflow bucket address:
```
[bit 63: X latch (1 bit)][bits 62-48: S latch count (15 bits)][bits 47-0: overflow address (48 bits)]
```
Constants: `kSharedLatchBits = 63 - kAddressBits = 63 - 48 = 15`, `kSharedLatchBitOffset = 48`, `kExclusiveLatchBitOffset = 63`. [Source: libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:L15-L30]

- **Shared latch**: `Interlocked.CompareExchange` to increment shared count. Fails if exclusive bit is set or shared count is full (all 15 bits set). [Source: HashBucket.cs:L39-L58]
- **Exclusive latch**: CAS to set exclusive bit, then spin-wait for shared count to drain (up to `kMaxReaderLockDrainSpins = kMaxLockSpins * 10` iterations). If readers don't drain in time, the exclusive bit is released via CAS and the operation returns `false`. [Source: HashBucket.cs:L77-L110]
- **Latch promotion** (S->X): Retries up to `kMaxLockSpins` (default 10) iterations to CAS-set the X bit while simultaneously decrementing the S count: `new_word = (expected_word | kExclusiveLatchBitMask) - kSharedLatchIncrement`. On each iteration, if another exclusive holder is present, the CAS fails and the loop retries with `Thread.Yield()`. After a successful CAS, there is a spin-drain for remaining shared readers. If draining fails, the promotion is reversed (X bit cleared, S count re-incremented). Promotion is NOT guaranteed to succeed even after the CAS succeeds -- reader drain timeout causes rollback. [Source: HashBucket.cs:L113-L149]

All operations are non-blocking with bounded spin counts. If a latch attempt fails after `kMaxLockSpins` (default 10) iterations, the operation returns `false` and the caller retries at a higher level (with epoch refresh).

**Epoch-based memory reclamation** [Source: libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs]:
- `BumpCurrentEpoch()` atomically increments `CurrentEpoch`
- `Acquire()`/`Release()` write thread's current epoch to its table entry
- `SafeToReclaimEpoch` = minimum epoch across all threads' entries
- Deferred actions execute when their target epoch becomes safe to reclaim

### 2.9 Unsafe Code Patterns

**1. Pointer arithmetic for RESP parsing**:
```csharp
if (*(ushort*)ptr != MemoryMarshal.Read<ushort>("\r\n"u8))
    RespParsingException.ThrowUnexpectedToken(*ptr);
```
[Source: libs/server/Resp/RespServerSession.cs:L1168]

Reads 2 bytes as a `ushort` for single-instruction `\r\n` validation instead of two separate byte comparisons.

**2. Direct buffer manipulation**:
```csharp
Buffer.MemoryCopy(networkReceiveBufferPtr + networkReadHead, networkReceiveBufferPtr, bytesLeft, bytesLeft);
```
[Source: libs/common/Networking/NetworkHandler.cs:L536]

Uses `Buffer.MemoryCopy` (which maps to `memmove`) for buffer shifting, taking advantage of the CPU's optimized memory copy instructions.

**3. SIMD-like batch uppercase detection** [Source: libs/server/Resp/RespServerSession.cs:L690-L698]:
```csharp
var firstUlong = *(ulong*)(ptr + 4);
var firstAllUpper = (((firstUlong + (~0UL / 255 * (127 - 95))) | firstUlong) & (~0UL / 255 * 128)) == 0;
```
Processes 8 bytes at once to determine if any byte is lowercase, using the "determine if a word has a byte greater than n" bit hack. This avoids per-byte branching for the common case.

**4. Fixed buffer in struct for hash bucket**:
```csharp
[FieldOffset(0)]
public fixed long bucket_entries[Constants.kEntriesPerBucket];
```
[Source: libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:L33]

`fixed` arrays in structs are inline (no heap allocation, no array header overhead). Combined with `StructLayout(Explicit)`, this creates a flat, cache-line-sized memory block.

### 2.10 Batch Processing and Pipelining

**Network-level batching**: The receive handler processes ALL bytes received in a single `ProcessMessages` call, parsing and executing multiple commands before performing a single `Send()`. This amortizes:
- Epoch acquire/release overhead (once per batch)
- Send buffer allocation (one buffer for all responses)
- `SendAsync` syscall overhead (one syscall per batch)

**Scatter-gather GET** [Source: libs/server/Resp/BasicCommands.cs:L177-L216]:
For `EnableScatterGatherGet`, Garnet parses ahead to find multiple pending GET operations and issues them to Tsavorite via `storageApi.GET_WithPending(ref key, ref input, ref o, ctx, out var isPending)`. This method issues the read and returns immediately, indicating via `isPending` whether the operation requires disk I/O. The scatter-gather loop continues parsing subsequent GET commands and issuing reads. Once all GETs are parsed, completions are awaited and responses assembled. The context parameter (`ctx`) carries the command index so out-of-order completions can be placed at the correct position in the response. This improves I/O parallelism when records are on disk by overlapping multiple disk reads. [Source: libs/server/Resp/BasicCommands.cs:L195-L196]

**Async GET processing** [Source: libs/server/Resp/AsyncProcessor.cs:L48-L143]:
When `useAsync` is enabled for a session, pending GET operations are dispatched to a background `AsyncGetProcessor` task. The first async operation on a session creates the processor via `Task.Run(async () => await AsyncGetProcessor(...))`. The processor handles I/O completions by calling `storageApi.GET_CompletePending()`, takes over the network sender to send push-style responses (`>3\r\n...`), and uses a `SingleWaiterAutoResetEvent` to coordinate with the main session thread. Async responses are NOT guaranteed to be in order.

### 2.11 VectorSet Hot-Path Impact

The VectorSet feature adds a BIDIRECTIONAL type-mismatch check to `MainSessionFunctions.SingleReader` that runs on EVERY read operation [Source: libs/server/Storage/Functions/MainStore/ReadMethods.cs:L29-L45]:

1. If `RecordInfo.VectorSet` is set AND the command is NOT legal on VectorSet (L33-L37): cancels the operation (prevents string ops on vector data)
2. If `RecordInfo.VectorSet` is NOT set AND the command IS legal on VectorSet (L39-L43): cancels the operation (prevents vector ops on string data)

Both directions cancel via `readInfo.Action = ReadAction.CancelOperation`. This check reads the `RecordInfo.VectorSet` bit (bit 63) and calls `cmd.IsLegalOnVectorSet()`. While these are single-instruction operations, they are in the absolute hottest path of the system. The performance impact should be negligible due to the branch predictor (the common case is non-VectorSet keys with non-vector commands, so both checks predict "not taken"), but implementors should be aware this bidirectional check exists and cannot be removed for correctness.

### 2.12 SpinLock Usage

Garnet uses `System.Threading.SpinLock` in two distinct contexts with very different hold durations:
- `LightConcurrentStack` [Source: libs/common/Networking/LightConcurrentStack.cs:L16]: Protects the send buffer stack. Hold duration is sub-microsecond (push/pop a pointer).
- `GarnetTcpNetworkSender` [Source: libs/common/Networking/GarnetTcpNetworkSender.cs:L58]: Protects response object lifecycle. The lock is acquired in `EnterAndGetResponseObject` (L123) and NOT released until `ExitAndReturnResponseObject` (L150), spanning the entire `ProcessMessages` call. This is NOT a short critical section -- it serializes the main processing thread and the `AsyncProcessor` for the duration of a full request batch. See [Doc 09 Section 2.4](./09-request-lifecycle.md#24-timing-critical-sections) for the full hold-duration analysis.

`SpinLock` avoids the overhead of `Monitor.Enter` (which involves checking thin lock, inflation to OS mutex, etc.). For the `LightConcurrentStack` case, this is a clear win since the critical section is sub-microsecond. For the `GarnetTcpNetworkSender` case, the SpinLock choice means the `AsyncProcessor` will spin-wait when the main thread is processing -- this is acceptable because there is at most one waiter (the AsyncProcessor) and the design intentionally serializes these two paths.

### 2.13 ReadOptimizedLock and SingleWriterMultiReaderLock

For reader-heavy workloads, Garnet provides specialized lock types that optimize for the common case of no writers:
- `ReadOptimizedLock` [Source: libs/common/ReadOptimizedLock.cs]: Uses a `[ThreadStatic]` counter for reader tracking, avoiding contention on shared counters
- `SingleWriterMultiReaderLock` [Source: libs/common/SingleWriterMultiReaderLock.cs]: Compact reader-writer lock using `Interlocked` operations

### 2.14 Compiler and Runtime Hints

**`[MethodImpl(MethodImplOptions.NoInlining)]` on error paths**: Prevents the JIT from inlining exception-throwing methods into the hot path, keeping the hot method's code size small for better instruction cache behavior.

**`readonly struct` for callback types**: Tells the compiler that the struct's fields won't change, enabling additional optimizations like avoiding defensive copies.

**`GC.AllocateArray<T>(pinned: true)`**: Allocates on the Pinned Object Heap, avoiding the overhead of `GCHandle.Alloc(array, GCHandleType.Pinned)` and the associated GC tracking.

**`Unsafe.AsPointer` / `Unsafe.AsRef`**: Zero-cost cast between managed references and pointers, avoiding the overhead of fixed statements when the array is already pinned.

### 2.15 Network I/O Optimization

**TCP_NODELAY**: Set on all accepted connections [Source: libs/server/Servers/GarnetServerTcp.cs:L147]:
```csharp
e.AcceptSocket.NoDelay = true;
```
Disables Nagle's algorithm, ensuring responses are sent immediately without buffering.

**Synchronous completion fast paths**: Both `AcceptAsync` and `ReceiveAsync` check for synchronous completion (return value `false`) and loop without returning to the thread pool. This avoids thread pool scheduling latency for back-to-back operations.

**Send throttle**: Limits concurrent `SendAsync` operations per connection to `ThrottleMax` (default 8), preventing kernel socket buffer exhaustion and excessive memory usage from in-flight send buffers.

### 2.16 Data Structure Layout for Access Patterns

**SpanByte** is designed for the sequential access pattern of storage operations:
```
[4-byte length+flags][optional 8-byte metadata][payload bytes...]
```
The length is at the start for fast size checking. Metadata (expiry timestamp) immediately follows for expiry checks. Payload comes last for sequential read/write. [Source: libs/storage/Tsavorite/cs/src/core/VarLen/SpanByte.cs:L17-L22]

**Hash bucket entries** are 8 bytes each [Source: libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucketEntry.cs:L9-L11] (note: the source-code comment at L9 says `[1-bit tentative][15-bit TAG][48-bit address]` but the tag is actually 14 bits per `kTagSize = 14`, and the Pending bit at position 62 is omitted from the comment):
```
Logical bit layout (MSB to LSB):
[bit 63: Tentative (1 bit)][bit 62: Pending (1 bit)][bits 61-48: Tag (14 bits)][bits 47-0: Address (48 bits)]
```
Constants: `kTentativeBitShift = 63`, `kPendingBitShift = 62`, `kTagSize = 14`, `kTagShift = 62 - 14 = 48`, `kAddressBits = 48`. The ReadCache bit (`kReadCacheBitShift = 47`) is embedded WITHIN the 48-bit address field (bit 47), not a separate top-level field. [Source: libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Constants.cs:L24-L40]

The tag provides a 14-bit partial hash check that avoids following the address pointer for non-matching entries. 8 entries per bucket = 64 bytes = one cache line means a lookup checks up to 8 entries with one cache line fetch.

## 3. Source File Map

| File | Purpose | Key Lines |
|------|---------|-----------|
| `libs/server/Resp/RespServerSession.cs` | Monomorphized type aliases, batch processing | L23-L40, L572-L667 |
| `libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs` | Cache-line-aligned hash bucket with bit-packed latches | L11-L139 |
| `libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs` | 8-byte bit-packed record metadata | L13-L80 |
| `libs/storage/Tsavorite/cs/src/core/VarLen/SpanByte.cs` | Bit-packed variable-length byte with pointer | L21-L100 |
| `libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs` | Epoch protection with thread-static entries | L15-L220 |
| `libs/common/Memory/LimitedFixedBufferPool.cs` | Tiered pinned buffer pool | L19-L243 |
| `libs/common/Memory/PoolEntry.cs` | Pinned byte array allocation | L31-L37 |
| `libs/common/Networking/GarnetTcpNetworkSender.cs` | SpinLock-based send with throttle | L58, L155-L329 |
| `libs/common/Networking/LightConcurrentStack.cs` | SpinLock-based buffer cache | L10-L72 |
| `libs/server/Storage/Functions/MainStore/MainSessionFunctions.cs` | readonly struct callbacks | L11 |
| `libs/common/RespReadUtils.cs` | AggressiveInlining RESP parsing | L24-L150 |
| `libs/server/Resp/Parser/SessionParseState.cs` | Pinned ArgSlice buffer | L60-L66 |

## 4. Performance Notes

Summary of measured/estimated impact of each technique:

| Technique | Estimated Impact | Scope |
|-----------|-----------------|-------|
| Generic monomorphization | 25-100ns/op saved | Every storage operation |
| Cache-line hash buckets | 1 cache miss per lookup | Every key lookup |
| Pinned zero-alloc buffers | Eliminates GC pressure | Every request |
| AggressiveInlining | 2-5ns per avoided call | Every method call |
| Batch epoch acquisition | ~10ns amortized | Per-batch vs per-command |
| Pipelined response send | ~1 syscall per batch | Network I/O |
| SpinLock vs Monitor | ~50ns per lock/unlock saved | Per send operation |
| Bit-packed RecordInfo | 1 CAS for all metadata | Per record access |
| SIMD-like uppercase | ~5ns per command | Command parsing |
| VectorSet bit check | <1ns (branch predictor) | Every SingleReader call |
| Async GET processing | Amortizes disk I/O latency | Pending GET operations |
| waitForAofBlocking | Blocks send until AOF flush | Write durability mode |

## 5. Rust Mapping Notes

- **Monomorphization** [Difficulty: Low, Parity: Full]: Native in Rust. Use generics everywhere and the compiler will specialize automatically. Use `#[inline]` aggressively on trait implementations for the `ISessionFunctions` equivalent. LLVM's monomorphization is strictly better than C#'s JIT monomorphization because it happens at compile time with full optimization passes (LLVM -O2/-O3), whereas .NET's RyuJIT has a constrained optimization budget per method. Expect equal or better performance for the deeply nested generic callback chains.

- **Cache-line alignment** [Difficulty: Low, Parity: Full]: Use `#[repr(C, align(64))]` on critical structs (`LightEpoch::Entry` equivalent, hash bucket). `crossbeam-utils::CachePadded<T>` wraps any type with cache-line padding, mapping directly to the `[StructLayout(Size = kCacheLineBytes)]` pattern. Rust gives full control over struct layout, matching every `[FieldOffset]` and `[StructLayout]` attribute in Garnet.

- **Pinned memory** [Difficulty: Medium, Parity: Full]: Use `mmap` for large buffers (network receive/send pools, hybrid log pages) or `Box::into_raw()` for heap-allocated pinned data. Unlike C#'s `GC.AllocateArray(pinned: true)` which allocates on the Pinned Object Heap, Rust has no GC to relocate memory -- all heap allocations are inherently "pinned" at their address. The `Pin<Box<[u8]>>` wrapper is only needed to prevent moves via the type system, not to prevent GC relocation. This is structurally simpler than C#'s approach.

- **Lock-free atomics** [Difficulty: Low, Parity: Full]: Use `std::sync::atomic` with `Ordering::Relaxed`/`Acquire`/`Release` as appropriate. Rust's atomic model maps closely to C#'s `Interlocked`: `AtomicU64::compare_exchange` = `Interlocked.CompareExchange`, `AtomicU64::fetch_add` = `Interlocked.Add`. The `RecordInfo` 64-bit bitfield manipulation maps to `AtomicU64` with bit masks.

- **SpinLock** [Difficulty: Low, Parity: Full]: Use `parking_lot::Mutex` (which spins briefly before parking) for the send-path serialization lock. For the very short critical sections (e.g., `LightConcurrentStack` push/pop), a custom `SpinLock` using `AtomicBool` with `compare_exchange` is appropriate. The key design point is that Garnet's `GarnetTcpNetworkSender` SpinLock is held for the entire `ProcessMessages` duration -- `parking_lot::Mutex` correctly handles this by parking rather than spinning.

- **Bit packing** [Difficulty: Low, Parity: Full]: Use `bitflags` crate or manual bit manipulation with `u64`. The `RecordInfo` 64-bit packed bitfield maps to a `#[repr(transparent)] struct RecordInfo(AtomicU64)` with const bit masks for each field. Rust's const evaluation ensures masks are computed at compile time.

- **Zero-copy parsing** [Difficulty: Low, Parity: Full]: Parse `&[u8]` slices directly from the receive buffer. The RESP parser operates on borrowed slices with an advancing offset, identical to C#'s `byte*` pointer advancement. Use `memchr` crate for SIMD-accelerated `\r\n` scanning. Rust's borrow checker ensures parsed `ArgSlice` references cannot outlive the receive buffer, providing safety that C# lacks.

- **Buffer pool** [Difficulty: Medium, Parity: Full]: Implement with `crossbeam::ArrayQueue` per size class (4 levels), backed by `mmap`-allocated pinned memory. Each buffer is a `Box<[u8]>` created from `mmap`-allocated memory. The pool's `Get`/`Return` pattern maps to `try_pop()`/`push()`. If the queue is full on return, the buffer is dropped (same as C#'s behavior where unreturned buffers become GC garbage).

- **Performance summary**: All 8 performance techniques documented in this chapter have direct Rust equivalents at full parity. The main risk area is the `unsafe` surface required for raw pointer arithmetic in the hybrid log and hash index -- estimated at 15-25% of the storage engine code requiring `unsafe` blocks.

## 6. Kotlin/JVM Mapping Notes

- **No monomorphization** [Difficulty: High, Risk: Critical Performance Gap]: JVM type erasure prevents specialization. The deeply nested `TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>` generic chain is erased to raw types at runtime, adding virtual dispatch on every `ISessionFunctions` callback invocation. Workarounds:
  - Code generation (KSP or annotation processors) to produce specialized classes for the `SpanByte`+`SpanByte` hot path -- estimated 2-4 weeks of additional development
  - Use `sun.misc.Unsafe` for manual struct-like layouts in off-heap memory
  - Accept the virtual dispatch cost (estimated 20-40% throughput reduction for the storage hot path) and optimize other areas
  - This is the single largest structural disadvantage for JVM reimplementation and cannot be fully mitigated.

- **No cache-line alignment control** [Difficulty: High, Risk: False Sharing]: JVM doesn't expose memory layout for on-heap objects. Use `@Contended` annotation (JDK 9+) for false-sharing prevention on the epoch table entries and per-connection state. For off-heap data (hash buckets, hybrid log records), manually align with `Unsafe.allocateMemory` + offset rounding to 64-byte boundaries. The `@Contended` annotation adds 128 bytes of padding (two cache lines) per annotated field, which is wasteful but effective.

- **No value types (before Valhalla)** [Difficulty: High, Risk: GC Pressure]: All objects are heap-allocated with a 12-16 byte header. `SpanByte` (12 bytes in C#) becomes a 32+ byte heap object. `ArgSlice` (12 bytes in C#) similarly bloats. `RecordInfo` (8 bytes in C#) becomes a 24+ byte object. This overhead compounds on the hot path: 1M ops/sec * 3 objects/op = 3M objects/sec of allocation pressure. Use primitive arrays and index arithmetic to simulate flat data structures for the most critical paths. Project Valhalla (preview in JDK 23+) may eventually address this.

- **GC pressure** [Difficulty: High, Risk: Tail Latency]: Minimize object allocation on the hot path. Use `ThreadLocal<ByteBuffer>` for per-thread scratch buffers. Netty's `PooledByteBufAllocator` with thread-local caches reduces allocation overhead. Even with best practices, expect periodic GC pauses (10-50ms for G1, 1-5ms for ZGC) that will appear in p99/p99.9 latency. C#'s approach of pinned arrays on the POH avoids this entirely for hot-path buffers.

- **Inlining** [Difficulty: Medium]: JVM inlines automatically based on method size and call frequency. HotSpot's C2 compiler inlines methods under ~325 bytes of bytecode. Keep hot methods small and call them frequently to trigger inlining. The critical `ISessionFunctions` methods (`SingleReader`, `SingleWriter`, `InPlaceUpdater`) should be designed as small, focused methods. However, without monomorphization, the JIT cannot inline through virtual dispatch -- it can only perform guarded devirtualization (speculative inlining with a type check guard), which adds branch overhead.

- **SpinLock** [Difficulty: Low]: Use `java.util.concurrent.locks.StampedLock` for optimistic reads on configuration, or `Unsafe.compareAndSwapInt` for custom spin locks matching `System.Threading.SpinLock`. For the `GarnetTcpNetworkSender` serialization lock (held for entire `ProcessMessages`), use `ReentrantLock` which parks the thread when contended.

- **Epoch protection** [Difficulty: Medium]: Implement with padded `AtomicLong` arrays for per-thread epoch entries. Use `@Contended` on the `ThreadLocal` epoch entry to prevent false sharing. `ThreadLocal<T>` has ~15-30ns access overhead vs. C#'s `[ThreadStatic]` at ~1-2ns, due to hash-table lookup. This overhead is incurred twice per batch (acquire + release), adding ~30-60ns per batch. For high-throughput scenarios, consider `Thread.getId()` indexed into a direct array to avoid `ThreadLocal` overhead.

- **Performance summary**: The JVM reimplementation faces three compounding performance gaps: (1) no monomorphization (20-40% on storage hot path), (2) no value types (GC pressure + object header overhead), (3) no raw pointer access (bounds-checked memory access). Cumulatively, expect 40-60% lower single-thread throughput for the storage engine core. The gap is smaller (15-25%) for the networking and RESP parsing layers where Netty provides optimized implementations. The gap narrows further for I/O-bound workloads where disk latency dominates.

## 7. Open Questions

1. **NUMA awareness**: The current code does not appear to have explicit NUMA-aware allocation. On multi-socket systems, cross-NUMA memory access could add 50-100ns per cache miss. Does Garnet rely on the OS's first-touch policy for implicit NUMA placement?
2. **JIT warmup**: C# JIT compilation means the first executions of monomorphized paths are interpreted/compiled at runtime. What is the warmup period before peak throughput is reached? Does Garnet use ReadyToRun (R2R) ahead-of-time compilation?
3. **GC interaction**: With pinned arrays on the POH, how does the GC handle the resulting fragmentation? Are there periodic GC pauses visible in latency percentiles?
4. **Thread affinity**: There is no visible `Thread.SetProcessorAffinity` in the code. Is Garnet relying on OS scheduling, or is affinity set at a higher level (container cgroup, etc.)?
5. **io_uring**: The .NET SAEA model is ultimately backed by epoll on Linux. Has Garnet been tested with .NET's io_uring support (available in .NET 8+)? This could reduce syscall overhead further.
6. **Adaptive batching**: The current design processes all received bytes in one batch. Could adaptive batching (e.g., yielding after N commands to reduce tail latency) improve p99 latency under mixed workloads?
