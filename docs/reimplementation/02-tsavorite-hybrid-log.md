# Tsavorite Hybrid Log Allocator

## 1. Overview

The Hybrid Log Allocator is Tsavorite's central data structure for managing the record storage layer. It implements a **circular buffer of memory pages** that seamlessly tiers between RAM and SSD, allowing the key-value store to operate on datasets far larger than available memory while keeping the hot working set in RAM. Every read, write, upsert, and delete in Tsavorite ultimately goes through the allocator.

The "hybrid" in Hybrid Log refers to the dual nature of the log: recent records live in a mutable in-memory region where they can be updated in-place, while older records are progressively flushed to disk and become immutable. This design eliminates write amplification for hot keys (no need to write a new record for every update) while still providing persistence and large-capacity storage through SSD tiering.

The allocator is generic over key/value types via C# struct constraints and monomorphization (`AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator>`), enabling the JIT compiler to inline all record access methods and eliminate virtual dispatch overhead. [Source: AllocatorBase.cs:L17-L19]

## 2. Detailed Design

### 2.1 Logical Address Space

All addresses in Tsavorite are **48-bit logical addresses** within a single monotonically increasing address space. A logical address encodes both the page number and the offset within that page:

```
Logical Address (48 bits):
  [upper bits: page number][lower LogPageSizeBits bits: offset within page]

Page number = logicalAddress >> LogPageSizeBits
Offset      = logicalAddress & PageSizeMask
```

The first valid address is `Constants.kFirstValidAddress = 64`, reserving the initial bytes for special sentinel values: `kInvalidAddress = 0`, `kTempInvalidAddress = 1`, `kUnknownAddress = 2`. [Source: Constants.cs:L75-L78]

### 2.2 Circular Buffer Structure

The in-memory portion is a **circular buffer** of `BufferSize` pages, where `BufferSize = LogTotalSizeBytes / PageSize`. Both must be powers of two. A page's position in the circular buffer is:

```
bufferIndex = (logicalAddress >> LogPageSizeBits) & (BufferSize - 1)
```

For `SpanByteAllocator` (the primary allocator used by Garnet), each page is a pinned byte array allocated via `GC.AllocateArray<byte>(PageSize + 2 * sectorSize, pinned: true)`, sector-aligned for direct I/O. The pointer to the sector-aligned start is stored in a pinned `nativePointers` array for fast physical address lookup: [Source: SpanByteAllocatorImpl.cs:L16-L33]

```
Physical address = nativePointers[pageIndex] + offset
```

This two-level lookup (array of pointers to pages) keeps `GetPhysicalAddress` as a simple pointer dereference plus addition. [Source: SpanByteAllocatorImpl.cs:L240-L248]

### 2.3 Address Pointers and Regions

The allocator maintains six key address pointers that partition the logical address space into regions (see [Doc 06 Section 2.12](./06-tsavorite-record-management.md#212-end-to-end-operation-flows) for how operations use these boundaries). From tail to head:

```
  BeginAddress    HeadAddress   SafeHeadAddress  ReadOnlyAddress  SafeReadOnlyAddress  TailAddress
       |               |              |                |                |                |
       |<-- disk only ->|<--- read-only in memory ---->|<---------- mutable ----------->|
                        |                                                                |
                  lowest in-memory                                                  next alloc
```

| Pointer | Description |
|---------|-------------|
| `TailAddress` | Next allocation point. Derived from `TailPageOffset` (page + offset). [Source: AllocatorBase.cs:L791-L812] |
| `ReadOnlyAddress` | Boundary between mutable and read-only in-memory regions. Records at or above this can be updated in-place; records below require RCU. [Source: AllocatorBase.cs:L85] |
| `SafeReadOnlyAddress` | Lags behind ReadOnlyAddress; updated after all threads have acknowledged the ReadOnlyAddress shift. The Read path uses SafeReadOnlyAddress (not ReadOnlyAddress) as its mutable/immutable boundary, because reads in the fuzzy region between SafeReadOnlyAddress and ReadOnlyAddress are safe (reads don't modify). Write paths (Upsert, RMW, Delete) use ReadOnlyAddress as their IPU boundary. [Source: AllocatorBase.cs:L88] |
| `HeadAddress` | Lowest in-memory address. Records below this are only on disk. [Source: AllocatorBase.cs:L94] |
| `SafeHeadAddress` | Set by `OnPagesClosed` as the highest address of the range it is **starting** to close. It **leads** `ClosedUntilAddress` -- while the epoch is held, records above this address will not be evicted. [Source: AllocatorBase.cs:L97-L100] |
| `ClosedUntilAddress` | Tracks the actual progress of page closing. Catches up to `SafeHeadAddress` as pages in the range are closed. [Source: AllocatorBase.cs:L106-L109] |
| `FlushedUntilAddress` | All records below this are persisted to disk. [Source: AllocatorBase.cs:L103] |
| `BeginAddress` | Lowest valid address in the log (records below are truncated). [Source: AllocatorBase.cs:L112] |
| `PersistedBeginAddress` | Lowest valid address on disk -- updated when truncating the log. Distinct from `BeginAddress` and relevant for recovery. [Source: AllocatorBase.cs:L115] |

### 2.4 Lag Offsets and the Mutable Fraction

The distance between the tail and the read-only/head addresses is controlled by **lag offsets**:

```csharp
ReadOnlyAddressLagOffset = (long)(LogMutableFraction * headOffsetLagSize) << LogPageSizeBits;
HeadAddressLagOffset     = (long)headOffsetLagSize << LogPageSizeBits;
```

where `headOffsetLagSize = MaxEmptyPageCount - EmptyPageCount` (the number of usable in-memory pages). [Source: AllocatorBase.cs:L746-L749]

`LogMutableFraction` (default 0.9) determines what fraction of in-memory pages are mutable (updatable in-place). A value of 0.9 means 90% of in-memory pages are mutable, and the remaining 10% are read-only but still in memory (avoiding disk reads). This balances in-place update performance against the cost of flushing.

### 2.5 Tail Allocation (TryAllocate)

Record allocation is **lock-free** via `Interlocked.Add` on a packed `PageOffset` structure: [Source: AllocatorBase.cs:L986-L1017]

```csharp
[StructLayout(LayoutKind.Explicit)]
struct PageOffset {
    [FieldOffset(0)] public int Offset;       // lower 32 bits: offset within page
    [FieldOffset(4)] public int Page;          // upper 32 bits: page number
    [FieldOffset(0)] public long PageAndOffset; // combined 64-bit value for atomic operations
}
```

The key insight: because `Offset` occupies the lower 32 bits and `Page` the upper 32 bits, an `Interlocked.Add` that causes `Offset` to exceed `PageSize` naturally carries into `Page`, incrementing the page number. This makes the page-turn detection implicit in the arithmetic. [Source: PageUnit.cs]

1. Atomically add `numSlots` (record size in 8-byte units) to `TailPageOffset.PageAndOffset`.
2. If the new offset is within the page, return the logical address.
3. If `Offset > PageSize`, exactly one thread "owns" the page turn (the thread whose pre-increment offset was <= PageSize and post-increment was > PageSize).
4. The owning thread calls `HandlePageOverflow`, which:
   - Checks if the next page's buffer slot has been flushed (`NeedToWait`).
   - Allocates memory for the next page (and the page after for lookahead).
   - Issues `ShiftReadOnlyAddress` and `ShiftHeadAddress` to maintain lag offsets.
   - Updates `TailPageOffset` to the new page with offset = numSlots.

`TryAllocate` returns three distinct statuses: `SUCCESS` (allocation succeeded on current page), `RETRY_NOW` (page overflow occurred, another thread owns the page turn -- retry immediately), or `RETRY_LATER` (the next page's buffer slot has not been flushed yet -- the caller must wait). These return codes propagate up to the Internal* operation methods and are essential for correct retry handling. [Source: AllocatorBase.cs:L986-L1017]

### 2.6 Page Flush Mechanics

When `ReadOnlyAddress` advances, pages in the newly read-only region are asynchronously flushed to disk via `device.WriteAsync`. The flush pipeline works as follows:

1. `ShiftReadOnlyAddress(desiredAddress)` is called when the tail advances past the lag offset.
2. For each page transitioning to read-only, an async write is issued.
3. On I/O completion, `FlushedUntilAddress` is advanced.
4. When `FlushedUntilAddress` passes a page, `ShiftHeadAddress` can evict it from memory.

The `PageStatusIndicator` array tracks per-page status (dirty version, flush status). The `PendingFlush` array manages in-flight flush operations. [Source: AllocatorBase.cs:L139-L140]

### 2.7 Page Eviction

When `HeadAddress` advances past a page, `OnPagesClosed` is invoked, which:

1. Sets `SafeHeadAddress` to the highest address of the range it is starting to close (this **leads** the actual closing progress).
2. Iterates through the pages to close, advancing `ClosedUntilAddress` as each page is processed.
3. Uses epoch-based protection to ensure no thread is still accessing the evicted pages.
4. Frees the page memory (either returning to `overflowPagePool` for reuse or clearing the byte array). [Source: SpanByteAllocatorImpl.cs:L49-L63]

The `EmptyPageCount` parameter (configurable at runtime) controls how many buffer slots are kept empty to prevent the tail from catching up with unflushed pages. The invariant is: the tail must never wrap around and overwrite pages that haven't been flushed yet. [Source: AllocatorBase.cs:L729-L771]

### 2.8 Record Layout (SpanByte Allocator)

Within a page, records are laid out sequentially with the following structure (see [Doc 06 Section 2.5](./06-tsavorite-record-management.md#25-record-layout-in-the-hybrid-log) for full record layout details including RecordInfo and filler mechanism):

```
[RecordInfo (8 bytes)][Key (SpanByte, variable)][padding to 8-byte align][Value (SpanByte, variable)]
Total record aligned to kRecordAlignment (8 bytes)
```

- `RecordInfo` is always at the start (`physicalAddress + 0`). [Source: SpanByteAllocatorImpl.cs:L68]
- Key starts at `physicalAddress + RecordInfo.GetLength()` (8 bytes). [Source: SpanByteAllocatorImpl.cs:L74]
- Value starts at key offset + `RoundUp(key.TotalSize, 8)`. [Source: SpanByteAllocatorImpl.cs:L93]
- When `RecordInfo.HasFiller` is set, an extra `int` after the used value length stores the allocated-minus-used space, enabling in-place growth. [Source: SpanByteAllocatorImpl.cs:L117-L118]

### 2.9 Overflow Page Pool

Freed pages are cached in an `OverflowPool<PageUnit>` (capacity 4) for reuse, avoiding repeated large allocations. When `AllocatePage` is called, it first checks the pool before allocating new memory. [Source: SpanByteAllocatorImpl.cs:L21, L220-L237]

## 3. Source File Map

| File | Purpose | Key Lines |
|------|---------|-----------|
| `Allocator/AllocatorBase.cs` | Abstract base class with address management, allocation, flush pipeline | L17-L1049 |
| `Allocator/SpanByteAllocatorImpl.cs` | Concrete allocator for SpanByte key/value pairs | L13-L463 |
| `Allocator/SpanByteAllocator.cs` | Thin wrapper struct for inlining | Full file |
| `Allocator/BlittableAllocatorImpl.cs` | Fixed-size blittable key/value allocator | Full file |
| `Allocator/GenericAllocatorImpl.cs` | Managed object key/value allocator | Full file |
| `Allocator/AllocatorSettings.cs` | Settings struct passed to allocator factory | L12-L35 |
| `Allocator/PageUnit.cs` | Page memory unit for overflow pool | Full file |
| `Allocator/PendingFlushList.cs` | Tracks in-flight page flushes | Full file |
| `Allocator/IAllocator.cs` | Interface defining allocator contract | Full file |
| `Index/Common/LogSettings.cs` | User-facing configuration (page size bits, memory size bits, mutable fraction) | Full file |

## 4. Performance Notes

- **Lock-free allocation**: `Interlocked.Add` on a single 64-bit word gives O(1) append with no locks. Only the rare page-turn path involves retries.
- **Pinned memory + native pointers**: `GC.AllocateArray(pinned: true)` avoids GC compaction. Native pointer array enables single-indirection physical address lookup.
- **Sector-aligned pages**: Direct I/O avoids OS page cache overhead, enabling predictable SSD throughput.
- **Mutable fraction**: By keeping 90% of in-memory pages mutable, most updates are in-place (no allocation + copy), reducing write amplification to near zero for hot keys.
- **Overflow page pool**: Avoids the cost of large pinned array allocation on page turns.
- **Monomorphized generics**: The `TAllocator` struct type parameter enables the JIT to inline all allocator calls through the wrapper, eliminating virtual dispatch.

## 5. Rust Mapping Notes

- The circular buffer can be implemented as a `Vec<Option<Box<[u8]>>>` with `mmap`-aligned allocations or `std::alloc::Layout` with alignment.
- Lock-free tail allocation maps to `AtomicU64::fetch_add` on a packed page+offset value.
- Page I/O maps to `io_uring` or `libaio` for async direct I/O on Linux.
- The `SpanByte` record layout maps naturally to `&[u8]` slices with explicit length prefixes.
- Epoch protection (see doc 04) is required for safe page eviction; in Rust this interacts with lifetime management. Consider `crossbeam-epoch` or a custom epoch scheme.
- The overflow page pool is a simple `crossbeam::queue::ArrayQueue<Box<[u8]>>`.

## 6. Kotlin/JVM Mapping Notes

- **No pinned memory**: The JVM GC can move objects. Use `ByteBuffer.allocateDirect()` for off-heap pages, or `sun.misc.Unsafe` for raw allocation. Direct ByteBuffers are not movable.
- **No pointer arithmetic**: Use long offsets into `ByteBuffer` or `MemorySegment` (Panama API, JDK 21+).
- **Atomic operations**: `AtomicLong.addAndGet()` or `VarHandle` for the tail offset.
- **Type erasure**: The generic monomorphization advantage is lost. Use code generation or specialization frameworks to avoid boxing overhead on key/value access.
- **Page flush**: Java NIO `AsynchronousFileChannel` for async I/O, or `io_uring` via JNI/JNA.
- **Sector alignment**: `FileChannel.open` with `StandardOpenOption.DSYNC` or use `O_DIRECT` via JNI.

## 7. Open Questions

1. **Non-power-of-two memory sizes**: The `MinEmptyPageCount` mechanism handles this, but the exact interaction with `EmptyPageCount` changes at runtime needs further study.
2. **ReadCache integration**: The allocator can be instantiated as a read cache (with `EvictCallback`). The read cache allocation and eviction strategy deserves separate documentation.
3. **Object log device**: `GenericAllocator` uses a separate `ObjectLogDevice` for heap objects that cannot be stored inline. The serialization protocol for this path is complex and less relevant for Garnet (which uses SpanByte).
4. **Throttled checkpoint flush**: The `ThrottleCheckpointFlushDelayMs` parameter introduces delays during snapshot flushes to reduce I/O impact. The exact throttling strategy needs investigation.
5. **Page flush pipeline**: The `PageStatusIndicator` state machine, `PendingFlush` management, and flush completion callbacks (`FlushCallback`) form a complex subsystem that is described only at a high level in this document. A complete reimplementation requires detailed documentation of the per-page flush state transitions, partial-page flush handling for the last page, and the interaction between `ShiftReadOnlyAddress`/`ShiftHeadAddress` and epoch protection.
