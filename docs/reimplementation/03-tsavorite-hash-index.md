# Tsavorite Hash Index

## 1. Overview

The Tsavorite hash index is a **lock-free, cache-line-aligned hash table** that maps keys to their most recent record location in the hybrid log. It is the entry point for all key lookups: given a hash of the key, the index returns the logical address of the latest record, from which the full hash chain of older versions can be traversed via `RecordInfo.PreviousAddress`.

The index is designed for extreme concurrency. Each bucket is exactly 64 bytes (one cache line), entries use CAS-based insertion with tag-based matching to minimize false positives, and overflow buckets are allocated from a fixed-page allocator. The index supports online resizing by maintaining two versions and progressively splitting buckets.

## 2. Detailed Design

### 2.1 Hash Computation and Tag Extraction

A 64-bit hash of the key is computed using the `IStoreFunctions.GetHashCode64` method. From this hash:

- **Bucket index**: Lower bits masked by `state[version].size_mask` select the bucket.
- **Tag**: Upper 14 bits (`hash >> kHashTagShift`, where `kHashTagShift = 50`) serve as a compact fingerprint for fast in-bucket matching. [Source: Constants.cs:L35-L43]

```
64-bit hash value:
[14-bit tag (bits 63-50)][remaining bits used for bucket index (masked)]
```

The tag reduces false-positive key comparisons. With 14 bits, the probability of a tag collision within a bucket is ~1/16384 per entry.

### 2.2 HashBucketEntry Layout (8 bytes)

Each entry in the hash table is a single `long` (8 bytes). The source comment states the layout as `[1-bit tentative][15-bit TAG][48-bit address]`, but more precisely: [Source: HashBucketEntry.cs:L9-L10, Constants.cs:L24-L43]

```
Bit 63:     Tentative flag (1 bit)  - marks entries being inserted (CAS protocol)
Bit 62:     Pending flag (1 bit)    - marks entries with pending I/O operations
Bits 61-48: Tag (14 bits)           - compact fingerprint from hash for in-bucket matching
Bits 47-0:  Address (48 bits)       - logical address in hybrid log (bit 47 = ReadCache indicator)
```

The `Address` field is 48 bits total. The ReadCache bit at position 47 is **part of** the address -- it indicates whether the address points to the read cache log rather than the main log. The `Address` property getter masks all 48 bits: `word & kAddressMask`. [Source: HashBucketEntry.cs:L16-L19]

Constants: [Source: Constants.cs:L24-L43]
- `kAddressBits = 48`
- `kAddressMask = (1L << 48) - 1`
- `kTagSize = 14`
- `kTagShift = 48` (= 62 - kTagSize)
- `kTagPositionMask = kTagMask << kTagShift` (bits 61-48)
- `kTentativeBitMask = 1L << 63`
- `kPendingBitMask = 1L << 62`
- `kReadCacheBitMask = 1L << 47`

**Tentative bit**: A critical correctness mechanism for concurrent insertion. When a new tag is being inserted, it is first written as Tentative. If another thread concurrently inserts the same tag in a different slot, the duplicate is detected and removed before the Tentative bit is cleared. [Source: TsavoriteBase.cs:L175-L216]

**Pending bit**: Marks an entry that has an outstanding asynchronous I/O operation (e.g., a read from disk). This allows concurrent operations to detect that another thread is already fetching the record and avoid issuing duplicate I/O. [Source: Constants.cs:L28-L30]

Note: The Pending bit (62) overlaps the positional range where the source comment says "15-bit TAG." In practice, the `Tag` property uses `kTagPositionMask` which covers bits 61-48 (14 bits), and the Pending bit at position 62 is a separate flag that does not participate in tag matching.

### 2.3 HashBucket Layout (64 bytes)

A `HashBucket` is exactly 64 bytes = 1 cache line, containing 8 entries of 8 bytes each: [Source: HashBucket.cs:L11, Constants.cs:L19-L21]

```
Offset  Content
0x00    Entry[0]  (8 bytes) - regular entry
0x08    Entry[1]  (8 bytes) - regular entry
0x10    Entry[2]  (8 bytes) - regular entry
0x18    Entry[3]  (8 bytes) - regular entry
0x20    Entry[4]  (8 bytes) - regular entry
0x28    Entry[5]  (8 bytes) - regular entry
0x30    Entry[6]  (8 bytes) - regular entry
0x38    Entry[7]  (8 bytes) - overflow bucket pointer + latch bits
```

`kEntriesPerBucket = 8` (= 1 << kBitsPerBucket where kBitsPerBucket = 3). The last entry (`Entry[7]`, index `kOverflowBucketIndex = 7`) is dual-purpose: its lower 48 bits hold the address of an overflow bucket, while upper bits encode shared/exclusive latches. [Source: Constants.cs:L56]

### 2.4 Latch Encoding in Overflow Entry

The overflow entry (Entry[7]) packs latch state into the bits above the address: [Source: HashBucket.cs:L15-L30]

```
Bit 63:     Exclusive latch (1 bit)
Bits 62-48: Shared latch counter (15 bits, max 32767 concurrent readers)
Bits 47-0:  Overflow bucket address (48 bits)
```

```csharp
kSharedLatchBitOffset = kAddressBits;              // 48
kSharedLatchBits = 63 - kAddressBits;              // 15
kExclusiveLatchBitOffset = kSharedLatchBitOffset + kSharedLatchBits;  // 63
```

This is a compact reader-writer latch in a single atomic word, colocated with the overflow pointer. The latch protocol:

- **Shared (read) latch**: CAS to increment shared counter, but only if exclusive bit is clear. Spin exactly `kMaxLockSpins = 10` iterations (with `Thread.Yield()` between attempts). If all attempts fail (exclusive bit remains set), returns false -- the caller must retry the entire operation. [Source: HashBucket.cs:L39-L58]
- **Exclusive (write) latch**: CAS to set exclusive bit, then spin-wait for shared counter to drain to zero (up to `kMaxReaderLockDrainSpins = kMaxLockSpins * 10 = 100` iterations). If readers don't drain in time, release the exclusive bit and return false -- the caller must retry the entire operation. A reimplementation that assumes these operations always succeed would introduce deadlocks or missed updates. [Source: HashBucket.cs:L77-L110]
- **Promote (S to X)**: Atomically set exclusive bit and decrement shared counter in one CAS. Then drain remaining readers. [Source: HashBucket.cs:L113-L149]

### 2.5 Tag Lookup (FindTag) and Transient Locking

Transient bucket locking is the first step in all record operations (see [Doc 06 Section 2.12](./06-tsavorite-record-management.md#212-end-to-end-operation-flows) for how transient locks integrate with the full operation flow). `FindTag` searches for an existing non-tentative entry matching a given tag: [Source: TsavoriteBase.cs:L133-L172]

1. Compute bucket index from hash: `masked_entry_word = hash & state[version].size_mask`.
2. Get pointer to aligned bucket: `bucket = state[version].tableAligned + masked_entry_word`.
3. Linear scan entries 0..6 (skip overflow entry at index 7).
4. For each non-zero entry, compare the 14-bit tag. If tag matches and not Tentative, return success.
5. If no match, follow the overflow chain: read Entry[7]'s address bits, convert to physical pointer via `overflowBucketsAllocator.GetPhysicalAddress`, and repeat.

**Transient locking**: In practice, the Internal* operations do NOT call `FindTag` or `FindOrCreateTag` directly. They use combined methods that atomically pair tag lookup with bucket-level transient locking: `FindTagAndTryTransientSLock` (shared lock, for Read) and `FindOrCreateTagAndTryTransientXLock` (exclusive lock, for Upsert/RMW/Delete). Separating find-then-lock would introduce a TOCTOU race where the record could be modified between tag lookup and lock acquisition. The transient lock is held for the entire operation duration and released in a `finally` block. [Source: InternalRead.cs:L63, InternalUpsert.cs:L58]

### 2.6 Tag Insertion (FindOrCreateTag)

`FindOrCreateTag` finds an existing entry or creates a new one: [Source: TsavoriteBase.cs:L175-L216]

1. Call `FindTagOrFreeInternal` which simultaneously searches for the tag AND remembers the first free (zero) slot.
2. If found, return it.
3. If not found, create a Tentative entry in the free slot via CAS (word = 0 -> new entry with Tentative=true, Address=kTempInvalidAddress).
4. After successful CAS, call `FindOtherSlotForThisTagMaybeTentativeInternal` to check if another thread concurrently inserted the same tag in a different slot. If so, zero out our slot and retry.
5. If no duplicate, clear the Tentative bit (simple store, not CAS, since we own the slot).

The `FindTagOrFreeInternal` method also **reclaims stale entries**: if an entry's address is below `BeginAddress` (the log has been truncated past it), it CAS-zeros the entry to reclaim the slot. [Source: TsavoriteBase.cs:L246-L259]

### 2.7 Overflow Bucket Allocation

When all 7 data slots in a bucket chain are full, a new overflow bucket is allocated from `MallocFixedPageSize<HashBucket>`. This is a **two-level paged allocator** that allocates fixed-size chunks (pages of 65536 entries) and hands out individual entries. [Source: MallocFixedPageSize.cs:L60-L120]

The overflow bucket's address is CAS-ed into Entry[7] of the last bucket in the chain. If the CAS fails (another thread won), the allocation is freed and the search continues from the winner's bucket. [Source: TsavoriteBase.cs:L274-L298]

### 2.8 Index Resizing

The hash table supports online doubling via a state machine with phases `PREPARE_GROW` and `IN_PROGRESS_GROW`: [Source: StateTransitions.cs:L47-L51]

- Two `InternalHashTable` states are maintained: `state[0]` and `state[1]`. [Source: TsavoriteBase.cs:L30]
- `resizeInfo` tracks the current version (0 or 1) and resize status. [Source: StateTransitions.cs:L11-L22]
- During resize, the table is split into `kNumMergeChunks = 256` chunks. Each chunk is independently migrated by splitting bucket entries based on the new bit in the hash. [Source: Constants.cs:L68-L69]
- `splitStatus` tracks which chunks have been migrated. [Source: TsavoriteBase.cs:L33]
- A new transaction barrier prevents new transactions during `PREPARE_GROW` since the lock table needs migration. [Source: StateMachineDriver.cs:L102]

### 2.9 Table Memory Layout

The table is allocated as a pinned array of `HashBucket` structs, sector-aligned:

```csharp
size_bytes = size * sizeof(HashBucket);  // size * 64
aligned_size_bytes = sector_size + ((size_bytes + (sector_size - 1)) & ~(sector_size - 1));
state[version].tableRaw = GC.AllocateArray<HashBucket>(aligned_size_bytes / 64, pinned: true);
state[version].tableAligned = (HashBucket*)sectorAlignedPointer;
```
[Source: TsavoriteBase.cs:L110-L124]

The table size must be a power of two and fit in 32 bits. Each bucket is exactly one cache line, so scanning a bucket is a single cache line load.

## 3. Source File Map

| File | Purpose | Key Lines |
|------|---------|-----------|
| `Index/Tsavorite/HashBucket.cs` | HashBucket struct with latch operations | L11-L185 |
| `Index/Tsavorite/HashBucketEntry.cs` | 8-byte entry struct with tag/address/tentative fields | L12-L68 |
| `Index/Tsavorite/TsavoriteBase.cs` | Hash table initialization, FindTag, FindOrCreateTag, resizing | L11-L367 |
| `Index/Tsavorite/Constants.cs` | Bit layout constants (tag size, address bits, bucket size) | L7-L79 |
| `Allocator/MallocFixedPageSize.cs` | Fixed-page allocator for overflow buckets | L60-L120 |
| `Index/Checkpointing/IndexResizeSM.cs` | State machine for online index resizing | Full file |
| `Index/Checkpointing/IndexResizeSMTask.cs` | Tasks for index resize state transitions | Full file |

## 4. Performance Notes

- **Cache-line-aligned buckets**: Each bucket is exactly 64 bytes. A lookup requires loading at most one cache line for the primary bucket, plus one per overflow bucket. Most lookups (assuming good hash distribution and <7 entries per bucket) touch a single cache line.
- **Tag filtering**: The 14-bit tag eliminates 99.99% of false-positive key comparisons within a bucket, avoiding expensive key deserialization and comparison.
- **Lock-free reads**: `FindTag` requires no locking -- it only reads atomic words. Writes use CAS on individual 8-byte entries. The bucket latch is only needed for operations that modify the overflow chain structure.
- **Tentative insertion**: Avoids the ABA problem during concurrent tag insertion without requiring a bucket-level lock for the common case.
- **Pinned memory**: The table is pinned at allocation time, enabling unsafe pointer access with no GC pauses.
- **Minimal indirection**: For non-overflow cases, lookup is: compute hash -> mask -> add to base pointer -> scan 7 words. This is 2-3 instructions plus 1 cache line load.

## 5. Rust Mapping Notes

- The hash table can be a `Vec<HashBucket>` with `#[repr(C, align(64))]` on the bucket struct.
- Each entry is an `AtomicU64`. Tag extraction and CAS use `Ordering::Acquire/Release`.
- Overflow buckets can use a slab allocator (e.g., `typed-arena` or custom fixed-page allocator).
- The latch protocol maps directly to `AtomicU64` operations with careful ordering.
- Consider using `crossbeam-epoch` for safe memory reclamation of overflow buckets during resize.
- The `unsafe` keyword is needed for raw pointer arithmetic into the bucket array.

## 6. Kotlin/JVM Mapping Notes

- **No cache-line alignment guarantee**: The JVM doesn't guarantee struct alignment. Use `sun.misc.Unsafe` to allocate off-heap memory with explicit 64-byte alignment, or use `MemorySegment` with Panama (JDK 21+).
- **No value types for buckets**: Each bucket would need to be an offset into a `ByteBuffer` or `long[]` array rather than a struct. Consider storing the table as a single `long[]` where indices 0-7 form bucket 0, indices 8-15 form bucket 1, etc.
- **Atomic operations**: `VarHandle` or `AtomicLongArray` for CAS on individual entries.
- **Tag extraction**: Bit manipulation on `long` values works identically on JVM.
- **Overflow allocator**: Can use a `LongArray`-based pool with free list, similar to `MallocFixedPageSize`.

## 7. Open Questions

1. **Overflow bucket reclamation**: Overflow buckets are allocated but not freed during normal operation. They are only reclaimed during index resize. Under adversarial hash distributions, this could leak memory.
2. **Resize under load**: The resize state machine blocks new transactions during `PREPARE_GROW`. The duration and impact of this pause under high load needs measurement.
3. **Read cache entries**: Entries with the ReadCache bit set point to a separate read cache log. The interaction between read cache and main log entries in the same bucket needs clarification.
4. **Optimal table size**: The initial table size determines overflow frequency. Guidelines for sizing relative to expected key count are not documented.
5. **Index resizing detail**: The `SplitBuckets` procedure (invoked during `IN_PROGRESS_GROW` phase), the bucket splitting algorithm (redistributing entries based on the new hash bit), and overflow bucket redistribution during resize are documented only at a high level. A complete reimplementation requires detailed documentation of the per-chunk migration protocol and the interaction with concurrent operations during the resize.
