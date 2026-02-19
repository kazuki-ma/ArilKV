# Tsavorite Record Management

## 1. Overview

Record management in Tsavorite encompasses the physical layout of key-value records in the hybrid log, the metadata stored in each record's header (`RecordInfo`), the variable-length data format (`SpanByte`), and the mechanisms for creating, updating, deleting, and recycling records. Understanding this layer is essential for reimplementation because every operation -- read, upsert, RMW, delete -- interacts with records at this level.

Tsavorite supports two update strategies: **in-place update (IPU)** for mutable records and **read-copy-update (RCU)** for immutable records. It also features **record revivification**, a mechanism to reuse deleted record slots to reduce log growth, and **log compaction**, which reclaims space occupied by old or deleted records.

## 2. Detailed Design

### 2.1 RecordInfo Bit Layout (8 bytes / 64 bits)

`RecordInfo` is the header of every record, packed into a single `long` word. The bit layout from LSB to MSB: [Source: RecordInfo.cs:L14, L23-L48]

```
Bits 47-0:   PreviousAddress (48 bits) - logical address of previous version of this key
                                          (bit 47 = ReadCache indicator, same encoding as hash index entries)
Bits 54-48:  Leftover bits (7 bits) - reclaimed from former lock bits, currently unused
Bit 55:      Tombstone - record is a delete marker
Bit 56:      Valid - record is valid (not in the process of being created)
Bit 57:      Sealed - record is sealed (being modified, e.g., during revivification)
Bit 58:      ETag - record has an ETag in its value payload
Bit 59:      Dirty - record has been modified in-place since last checkpoint
Bit 60:      Filler - record has a filler length field (allocated > used value space)
Bit 61:      InNewVersion - record was created in the new version (during checkpoint)
Bit 62:      Modified - record has been modified (for change tracking)
Bit 63:      VectorSet - record is part of a vector set
```

Computed constants: [Source: RecordInfo.cs:L23-L48]
```
kPreviousAddressBits = 48
kPreviousAddressMaskInWord = (1L << 48) - 1
kLeftoverBitCount = 7
kTombstoneBitOffset = 48 + 7 = 55
kValidBitOffset = 56
kSealedBitOffset = 57
kEtagBitOffset = 58
kDirtyBitOffset = 59
kFillerBitOffset = 60
kInNewVersionBitOffset = 61
kModifiedBitOffset = 62
kVectorSetBitOffset = 63
```

### 2.2 RecordInfo Lifecycle

A record goes through these states:

1. **Allocation**: `WriteInfo` sets the record to Sealed + Invalid, stores `PreviousAddress`, and optionally sets `InNewVersion`. The record is Sealed+Invalid to prevent scans from seeing partially-written data. [Source: RecordInfo.cs:L58-L69]

2. **Activation**: After the record is fully written and CAS-ed into the hash index, `UnsealAndValidate()` atomically clears Sealed and sets Valid. [Source: RecordInfo.cs:L235]

3. **In-place update**: `SetDirtyAndModified()` marks the record for checkpoint delta tracking. [Source: RecordInfo.cs:L227]

4. **Sealing for RCU**: `TrySeal(invalidate: false)` CAS-sets the Sealed bit, preventing concurrent in-place updates while a new copy is being created. [Source: RecordInfo.cs:L109-L121]

5. **Deletion**: `SetTombstone()` marks the record as deleted. The record remains in the log; tombstone filtering is the responsibility of scan callers (see Section 2.10). [Source: RecordInfo.cs:L164]

6. **Invalidation**: `SealAndInvalidate()` or `SetInvalid()` marks records that should be skipped entirely (e.g., failed insertions). [Source: RecordInfo.cs:L231-L237]

### 2.3 Closed vs Open Records

A record is "closed" if it is not both Valid and non-Sealed: [Source: RecordInfo.cs:L80]
```csharp
static bool IsClosedWord(long word) => (word & (kValidBitMask | kSealedBitMask)) != kValidBitMask;
```

Closed records cannot be updated in-place. Operations on closed records return `RETRY_LATER` (the record may become open again, e.g., after revivification completes). [Source: RecordInfo.cs:L83-L91]

### 2.4 SpanByte: Variable-Length Data Format

`SpanByte` is the primary key and value type used by Garnet. It represents a variable-length byte sequence with an inline 4-byte length header: [Source: SpanByte.cs:L22-L23]

```
[4-byte header][payload bytes...]
```

**Header bits** (in the 4-byte `length` field): [Source: SpanByte.cs:L25-L31]
```
Bit 31: Unserialized flag (1 = pointer-based, 0 = inline/serialized)
Bit 30: ExtraMetadata flag (1 = 8-byte metadata prefix in payload)
Bit 29: Namespace flag (1 = 1-byte namespace prefix in payload)
Bits 28-0: Payload length (max ~512MB)
```

**Two forms**: [Source: SpanByte.cs:L36-L44]
- **Serialized** (Unserialized=0): The `payload` field is the start of inline data (stored contiguously after `length`). `TotalSize = sizeof(int) + Length`.
- **Unserialized** (Unserialized=1): The `payload` field is an `IntPtr` pointing to external pinned memory. Used for passing keys/values from network buffers.

```csharp
[FieldOffset(0)] private int length;    // 4 bytes: flags + payload length
[FieldOffset(4)] private IntPtr payload; // 4/8 bytes: inline data or pointer
```

**MetadataSize**: Computed via bit shifts: `((length & ExtraMetadataBitMask) >> 27) + ((length & NamespaceBitMask) >> 29)`. This yields 0 if neither flag set, 8 if ExtraMetadata only, 1 if Namespace only, or 9 if both flags are set. However, `Debug.Assert` statements in the code (SpanByte.cs:L149, L166) enforce "Don't use both extension for now", meaning both flags should not be set simultaneously. A reimplementation should either support MetadataSize=9 or explicitly forbid it, matching the current constraint. [Source: SpanByte.cs:L100]

### 2.5 Record Layout in the Hybrid Log

For SpanByte keys and values, a complete record in the log looks like:

```
Offset 0:                     RecordInfo (8 bytes)
Offset 8:                     Key SpanByte (4-byte length + payload)
Offset 8+RoundUp(key.TotalSize, 8): Value SpanByte (4-byte length + payload)
Total: RoundUp(entire record, 8)
```

The key is accessed at `physicalAddress + RecordInfo.GetLength()` (i.e., +8). [Source: SpanByteAllocatorImpl.cs:L74, L90]

The value is accessed at `keyOffset + RoundUp(key.TotalSize, kRecordAlignment)`. [Source: SpanByteAllocatorImpl.cs:L93]

All alignments use `kRecordAlignment = 8` to ensure atomic access to RecordInfo and SpanByte length fields. [Source: Constants.cs:L12]

### 2.6 Filler Mechanism for In-Place Growth

When a record is allocated with more space than the current value needs (e.g., anticipating future RMW growth), the extra space is tracked via the **Filler** mechanism: [Source: SpanByteAllocatorImpl.cs:L107-L122]

1. The `HasFiller` bit is set in RecordInfo.
2. An `int` is stored after the used value area (at `RoundUp(usedValueLength, sizeof(int))`) containing the extra allocated bytes.
3. `GetRecordSize` reads the filler to report the true allocated size:
   ```
   allocatedSize = RecordInfo + alignedKeySize + usedValueLength + extraValueLength
   ```

The `GetAndInitializeValue` method initializes a newly allocated value SpanByte to span the entire available space, so the `ISessionFunctions` can write the actual value and the filler is automatically computed from the difference. [Source: SpanByteAllocatorImpl.cs:L80-L87]

### 2.7 In-Place Update vs Read-Copy-Update

**In-Place Update (IPU)**: When a record is in the mutable region (address >= ReadOnlyAddress) and is open (Valid, not Sealed), the value can be updated directly. The `Dirty` and `Modified` bits are set. [Source: RecordInfo.cs:L227]

**Read-Copy-Update (RCU)**: When a record is in the read-only or on-disk region:
1. A new record is allocated at the tail of the log (initially Sealed + Invalid via `WriteInfo`).
2. The key and (modified) value are copied to the new record.
3. The new record's `PreviousAddress` points to the old record.
4. The hash index entry is CAS-updated to point to the new record via `CASRecordIntoChain`, which atomically unseals and validates the new record.
5. AFTER the CAS succeeds, the old record in the mutable region (if any) is sealed/invalidated. Note: the sealing of the old record happens AFTER the new record is installed, not before. For records already in the read-only region, sealing is unnecessary since they are already immutable. [Source: InternalUpsert.cs:L314-L395, RecordInfo.cs:L109-L121]

### 2.8 Record Revivification

Revivification reuses deleted (tombstoned) or sealed record slots to reduce log growth. Managed by `RevivificationManager`: [Source: RevivificationManager.cs:L9-L85]

**Free Record Pool** (`FreeRecordPool`): When a record is deleted in the mutable region, instead of just setting the Tombstone bit, the record's address and size are added to a size-segregated free list (bins). [Source: RevivificationManager.cs:L62-L67]

When a new record needs to be inserted:
1. Query the free pool for a record of sufficient size via `TryTake(recordSize, minAddress, out address)`.
2. `minAddress` is computed from `GetMinRevivifiableAddress`, which is `tailAddress - fraction * (tailAddress - readOnlyAddress)`. This prevents revivifying records too close to the read-only boundary. [Source: RevivificationManager.cs:L57-L58]
3. If found, the old record is reused: its RecordInfo is re-initialized, key/value are overwritten, and it is re-linked into the hash chain.

Key settings: [Source: RevivificationManager.cs:L33-L53]
- `revivifiableFraction`: Fraction of mutable region eligible for revivification (default = MutableFraction).
- `restoreDeletedRecordsIfBinIsFull`: If the free pool bin is full, restore the deleted record instead of discarding it.
- `useFreeRecordPoolForCTT`: Whether copy-to-tail operations can use the free pool.

### 2.9 Log Compaction

Compaction reclaims space from the cold region of the log by copying live records to the tail and advancing `BeginAddress`: [Source: TsavoriteCompaction.cs:L9-L163]

**Two strategies**:

1. **Lookup-based** (`CompactLookup`): Scan the region to compact, and for each non-tombstoned, non-deleted record, issue a `CompactionCopyToTail` operation that checks if the record is still the latest version. [Source: TsavoriteCompaction.cs:L36-L73]

2. **Scan-based** (`CompactScan`): Uses a temporary TsavoriteKV to deduplicate records. First, scan the compaction region into the temp KV (latest value per key). Then scan the immutable tail to remove keys that have newer versions. Finally, copy surviving records from the temp KV to the main store's tail. [Source: TsavoriteCompaction.cs:L75-L150]

After compaction, `Log.ShiftBeginAddress(untilAddress)` advances the begin address, and the physical log segments can be truncated.

### 2.10 Tombstone Records

Delete operations create tombstone records:
- The `Tombstone` bit is set in RecordInfo. [Source: RecordInfo.cs:L164]
- For SpanByte allocator, a tombstone record has minimal value allocation: just `sizeof(int)` for the SpanByte length header. [Source: SpanByteAllocatorImpl.cs:L135-L142]
- **Important**: `SkipOnScan` does NOT skip tombstoned records. `SkipOnScan` checks `IsClosedWord(word)`, which returns true only when the record is not (Valid AND not Sealed) -- i.e., Invalid or Sealed records are skipped. A tombstoned record that is Valid and not Sealed has `SkipOnScan = false`. Tombstone filtering must be handled by the scan caller or by the `ISessionFunctions` callback, not by the `SkipOnScan` property. [Source: RecordInfo.cs:L258, L80]
- The hash chain is maintained: the tombstone's `PreviousAddress` still points to older versions.
- Tombstones are cleaned up during compaction or revivified if revivification is enabled.

### 2.11 Disk Image Cleanup

When records are read from disk during recovery, `ClearBitsForDiskImages` removes transient bits that should not survive a restart: [Source: RecordInfo.cs:L73-L77]
```csharp
word &= ~(kDirtyBitMask | kSealedBitMask);
```
The `Dirty` bit is checkpoint-specific. The `Sealed` bit must be cleared because a sealed record from a crash may need to become the current record again if the RCU replacement was not persisted.

### 2.12 End-to-End Operation Flows

The core CRUD operations tie together the hash index (doc 03), hybrid log (doc 02), epoch protection (doc 04), and record management. Each operation uses an `OperationStackContext` to track hash index state and follows a common structure: acquire a transient bucket lock, traverse the hash chain to find the record, check CPR consistency (if checkpointing is active), then perform the operation based on which log region the record is in. All operations release the transient lock in a `finally` block. [Source: InternalRead.cs, InternalUpsert.cs, InternalRMW.cs, InternalDelete.cs, FindRecord.cs]

**Transient Bucket Locking** (see [Doc 03 Section 2.4](./03-tsavorite-hash-index.md#24-latch-encoding-in-overflow-entry) for latch bit encoding and [Doc 03 Section 2.5](./03-tsavorite-hash-index.md#25-tag-lookup-findtag-and-transient-locking) for the combined find-and-lock protocol): Every operation acquires a transient lock on the hash bucket before any record access. This prevents races between record insertion, elision, and revivification. Read uses a **shared** lock via `FindTagAndTryTransientSLock`; write operations (Upsert, RMW, Delete) use an **exclusive** lock via `FindOrCreateTagAndTryTransientXLock` or `FindTagAndTryTransientXLock`. The lock is held for the duration of the operation and released in `finally`. [Source: InternalRead.cs:L63, InternalUpsert.cs:L58, InternalRMW.cs:L62, InternalDelete.cs:L55]

**CPR Consistency Checks** (see [Doc 05 Section 2.4](./05-tsavorite-checkpointing.md#24-version-change-state-machine) for version change phases and [Doc 05 Section 2.5](./05-tsavorite-checkpointing.md#25-hybrid-log-checkpoint-hybridlogcheckpointsmtask) for the fuzzy region definition): During checkpointing (phase != REST), all operations perform CPR (Concurrent Prefix Recovery) checks after finding the record. A thread in version V encountering a V+1 record returns `CPR_SHIFT_DETECTED` (retry after epoch refresh). A thread in version V+1 encountering a V record forces RCU (`CreateNewRecord`) instead of IPU, ensuring the old-version record is preserved for recovery. Read's CPR check is at InternalRead.cs:L115-L116; write operations use `CheckCPRConsistencyUpsert`/`CheckCPRConsistencyRMW`/`CheckCPRConsistencyDelete`. [Source: InternalUpsert.cs:L87-L103, InternalRMW.cs:L93-L109, InternalDelete.cs:L87-L102]

**Read (`InternalRead`)**: [Source: InternalRead.cs]
1. `FindTagAndTryTransientSLock` -- acquire shared bucket lock and find the tag.
2. **ReadCache fast path**: If the hash entry points to the read cache (`stackCtx.hei.IsReadCache`), call `FindInReadCache` first. If found, use `SingleReader` on the read-cache record and return. [Source: InternalRead.cs:L81-L103]
3. If not in read cache, call `TryFindRecordForRead` which walks the hash chain down to `HeadAddress`.
4. **CPR check**: If phase == PREPARE and the entry has a new-version flag, return `CPR_SHIFT_DETECTED`.
5. If found at address >= `SafeReadOnlyAddress` (**mutable region**, including the fuzzy region between SafeReadOnlyAddress and ReadOnlyAddress): call `IsClosedOrTombstoned` (returns RETRY_LATER if closed, NOTFOUND if tombstoned), then read via `ISessionFunctions.ConcurrentReader`. The fuzzy region is safe for reads because they do not modify data. [Source: InternalRead.cs:L118-L131]
6. If found at address >= `HeadAddress` but < `SafeReadOnlyAddress` (**immutable region**): call `IsClosedOrTombstoned`, then read via `ISessionFunctions.SingleReader`. Optionally copy-to-tail if `ReadCopyFrom.AllImmutable` is configured. [Source: InternalRead.cs:L133-L150]
7. If address < `HeadAddress` but >= `BeginAddress` (**on-disk region**): create `PendingContext`, issue async I/O. Completes via `ContinuePending`.
8. Release shared lock in `finally`.

Note: Read uses `SafeReadOnlyAddress` (not `ReadOnlyAddress`) as the mutable/immutable boundary, and uses `ConcurrentReader` in the mutable region and `SingleReader` in the immutable region.

**Upsert (`InternalUpsert`)**: [Source: InternalUpsert.cs]
1. `FindOrCreateTagAndTryTransientXLock` -- acquire exclusive bucket lock, find or create tag.
2. `TryFindRecordForUpdate` searching only down to `ReadOnlyAddress` (shallow search -- Upsert blindly inserts if the record is below ReadOnlyAddress). [Source: InternalUpsert.cs:L73-L74]
3. **CPR check** via `CheckCPRConsistencyUpsert`.
4. If found at address >= `ReadOnlyAddress` (**mutable region**): if tombstoned, attempt revivification or fall through to new record; otherwise attempt IPU via `ISessionFunctions.ConcurrentWriter`. If ConcurrentWriter fails (e.g., insufficient space), fall through to CreateNewRecord. [Source: InternalUpsert.cs:L105-L144]
5. If found in read-only region or not found: allocate a new record at the tail via `CreateNewRecordUpsert`. The new record is CAS-ed into the hash chain via `CASRecordIntoChain` (which atomically unseals and validates the record). If the old record was in the mutable region, it is sealed/invalidated AFTER the CAS succeeds. [Source: InternalUpsert.cs:L314-L395]
6. On failed CAS (another thread won): invalidate the new record and retry.
7. Release exclusive lock in `finally`.

**Read-Modify-Write (`InternalRMW`)**: [Source: InternalRMW.cs]
1. `FindOrCreateTagAndTryTransientXLock` -- acquire exclusive bucket lock, find or create tag.
2. `TryFindRecordForUpdate` searching down to `HeadAddress` (deep search -- RMW needs to find immutable records for CopyUpdate). [Source: InternalRMW.cs:L77-L78]
3. **CPR check** via `CheckCPRConsistencyRMW`.
4. If found at address >= `ReadOnlyAddress` (**mutable region**): if tombstoned, attempt revivification or create new record with `InitialUpdater`; otherwise attempt IPU via `ISessionFunctions.InPlaceUpdater`. If the value needs to grow beyond allocated space (filler exhausted), fall through to CreateNewRecord. [Source: InternalRMW.cs:L111-L177]
5. **Address fuzzy region** (address >= `SafeReadOnlyAddress` AND < `ReadOnlyAddress`, non-tombstoned): return `RETRY_LATER`. This prevents a lost-update anomaly: during the ReadOnlyAddress transition, a concurrent RMW could read a stale value if it proceeded with CopyUpdate while another thread is still doing IPU on the same record. The code comment says: "Fuzzy Region: Must retry after epoch refresh, due to lost-update anomaly." Note: this address-based fuzzy region between `SafeReadOnlyAddress` and `ReadOnlyAddress` (see [Doc 02 Section 2.3](./02-tsavorite-hybrid-log.md#23-address-pointers-and-regions)) is distinct from the checkpoint fuzzy region `[startLogicalAddress, finalLogicalAddress)` described in [Doc 05 Section 2.5](./05-tsavorite-checkpointing.md#25-hybrid-log-checkpoint-hybridlogcheckpointsmtask). [Source: InternalRMW.cs:L178-L183]
6. If found below SafeReadOnlyAddress but still in memory (**safe read-only region**): create a new record at the tail via `CreateNewRecordRMW`, calling `ISessionFunctions.CopyUpdater` to merge the modification with the existing value.
7. If **not found in memory**: issue async I/O. On completion, `CopyUpdater` merges the modification with the disk-resident value.
8. If **new key** (or tombstoned with no revivification): call `ISessionFunctions.InitialUpdater` to create the initial value.
9. Release exclusive lock in `finally`.

**Delete (`InternalDelete`)**: [Source: InternalDelete.cs]
1. `FindTagAndTryTransientXLock` -- acquire exclusive bucket lock, find tag. (Uses `FindTag`, not `FindOrCreateTag` -- delete does not create new tag entries.)
2. `TryFindRecordForUpdate` searching down to `HeadAddress` (deep search -- to find existing tombstones and avoid creating duplicates). [Source: InternalDelete.cs:L70-L71]
3. **CPR check** via `CheckCPRConsistencyDelete`.
4. If found at address >= `ReadOnlyAddress` (**mutable region**): if already tombstoned, return `NOTFOUND` (already deleted). Otherwise, set tombstone in-place via `ISessionFunctions.ConcurrentDeleter`. If revivification is enabled and elision is possible, add the record to the free pool. [Source: InternalDelete.cs:L104-L140]
5. If found in read-only region: if already tombstoned, return `NOTFOUND`. Otherwise, fall through to CreateNewRecord.
6. If **not found or on-disk**: fall through to `CreateNewRecordDelete`, which allocates a new tombstone record at the tail. Delete does NOT return success for non-existent keys -- it creates a tombstone to ensure the deletion is recorded for recovery and replication. [Source: InternalDelete.cs:L150-L155]
7. Release exclusive lock in `finally`.

**Record Chain Traversal (`FindRecord.cs`)**: The `TraceBackForKeyMatch` method walks the hash chain from the hash index entry. For each record address, it checks: (a) is the address above `minAddress` (in memory)? (b) is the record a "valid traceback record"? (c) does the key match? The validity check uses `IsValidTracebackRecord`: a record is valid for traceback if it is non-Invalid OR if it is Sealed (because sealed records need RETRY handling). Only purely Invalid (non-Sealed) records are skipped. [Source: FindRecord.cs:L121-L123] The traversal skips read-cache entries (detected via the ReadCache bit in the address) by calling through to the read cache's chain.

**Search depth summary**: Upsert searches to `ReadOnlyAddress` (shallow); RMW and Delete search to `HeadAddress` (deep). Read searches to `HeadAddress`. This means Upsert may create a duplicate record in the hash chain when the existing record is in the read-only region, while RMW explicitly finds and copies from immutable records.

## 3. Source File Map

| File | Purpose | Key Lines |
|------|---------|-----------|
| `Index/Common/RecordInfo.cs` | 8-byte record header with all flag bits | L13-L294 |
| `VarLen/SpanByte.cs` | Variable-length byte array with inline or pointer modes | L22-L597 |
| `Allocator/SpanByteAllocatorImpl.cs` | Record layout: GetKey, GetValue, GetRecordSize, filler | L68-L202 |
| `Index/Tsavorite/Implementation/Revivification/RevivificationManager.cs` | Free record pool manager | L9-L85 |
| `Index/Tsavorite/Implementation/Revivification/RevivificationSettings.cs` | Configuration for revivification | Full file |
| `Index/Tsavorite/Implementation/Revivification/RevivificationStats.cs` | Statistics tracking | Full file |
| `Compaction/TsavoriteCompaction.cs` | Compaction (lookup and scan strategies) | L9-L163 |
| `Compaction/LogCompactionFunctions.cs` | Functions adapter for compaction sessions | Full file |
| `Compaction/ICompactionFunctions.cs` | User-provided liveness predicate | Full file |
| `Compaction/CompactionType.cs` | Enum: Scan vs Lookup | Full file |
| `VarLen/SpanByteComparer.cs` | Hash and equality for SpanByte keys | Full file |
| `Index/Tsavorite/Implementation/InternalRead.cs` | Read operation: find record, IPU/async I/O paths | Full file |
| `Index/Tsavorite/Implementation/InternalUpsert.cs` | Upsert operation: IPU, RCU, new-key insertion | Full file |
| `Index/Tsavorite/Implementation/InternalRMW.cs` | RMW operation: in-place update, copy-update, initial update | Full file |
| `Index/Tsavorite/Implementation/InternalDelete.cs` | Delete operation: in-place tombstone, tail tombstone | Full file |
| `Index/Tsavorite/Implementation/FindRecord.cs` | Hash chain traversal: TryFindRecordInMemory, TraceBackForKeyMatch | Full file |
| `Index/Tsavorite/Implementation/OperationStackContext.cs` | Per-operation state tracking for hash index traversal | Full file |
| `Index/Tsavorite/Implementation/BlockAllocate.cs` | Block allocation path for RCU tail records | Full file |

## 4. Performance Notes

- **8-byte RecordInfo**: Fits in a single atomic word, enabling lock-free CAS operations for sealing, validation, and flag updates. No separate lock structure needed.
- **Inline SpanByte**: When serialized, the key and value data are contiguous with the RecordInfo, maximizing cache locality for small records.
- **Filler mechanism**: Avoids RCU for values that grow during RMW, which would otherwise require allocating a new record, copying the key, and CAS-updating the hash index.
- **Revivification**: Reduces log growth for delete-heavy workloads by reusing record slots. The size-binned free list enables O(1) lookup for suitable records.
- **Compact record alignment**: 8-byte alignment (`kRecordAlignment = 8`) balances alignment overhead against space efficiency. For SpanByte records, the minimum overhead is 8 (RecordInfo) + 4 (key length) + 4 (value length) = 16 bytes per record.

## 5. Rust Mapping Notes

- `RecordInfo` maps to a `#[repr(C)] struct RecordInfo(AtomicU64)` with bitfield accessors via shifts and masks. Use `compare_exchange` for `TrySeal` and `TryResetModifiedAtomic`.
- `SpanByte` in serialized form is simply `&[u8]` with a 4-byte length prefix. In Rust, this maps to a slice reference with explicit length read. The Unserialized form is less relevant as Rust handles references natively.
- Record layout in the log can use `#[repr(C, packed)]` or manual offset arithmetic on `&[u8]` page slices.
- Revivification's free record pool maps to a `Vec<crossbeam::queue::SegQueue<(u64, usize)>>` (per-bin queues of (address, size) pairs).
- Compaction requires a temporary store instance; in Rust this means either allocating a separate `TsavoriteKV` or using a simpler temporary hash map.

## 6. Kotlin/JVM Mapping Notes

- **RecordInfo**: Store as a `long` in a `ByteBuffer` or `long[]`. Bitfield extraction is identical. Atomic CAS via `VarHandle` or `Unsafe.compareAndSwapLong`.
- **SpanByte equivalent**: Since JVM doesn't have pointer arithmetic, represent as offset+length into a `ByteBuffer`. The "unserialized" form can be a `byte[]` reference with offset.
- **No value types**: RecordInfo and SpanByte cannot be stack-allocated structs. Use primitive `long` for RecordInfo and manual offset management for SpanByte.
- **Filler mechanism**: Requires reading raw bytes from the ByteBuffer at computed offsets. Use `ByteBuffer.getInt(offset)` for length fields.
- **Revivification**: Free record pool can use `ConcurrentLinkedQueue<Long>[]` (array of queues indexed by size bin).
- **GC concerns**: If using on-heap byte arrays for pages, deleted record slots cannot be truly "freed" -- they are reused via revivification or reclaimed via compaction. Off-heap (`ByteBuffer.allocateDirect`) avoids this.

## 7. Open Questions

1. **Leftover bits**: 7 bits (positions 48-54) are currently unused, reclaimed from former lock bits. Could these be used for additional metadata (e.g., record type tags, compression flags)?
2. **ETag and VectorSet integration**: The `ETag` and `VectorSet` bits in RecordInfo suggest application-level metadata baked into the storage engine. How tightly coupled are these to Garnet's Redis compatibility layer?
3. **Revivification and compaction interaction**: If revivification is active, does compaction skip records that are in the free pool? What happens if a free-pooled record is in the region being compacted?
4. **SpanByte metadata overhead**: The ExtraMetadata (8 bytes) and Namespace (1 byte) fields consume payload space. For small values, this is significant. Is there a way to externalize this metadata?
5. **Cross-record atomicity**: For operations that need to atomically update multiple records (e.g., rename), how does the current record management support multi-record transactions?
