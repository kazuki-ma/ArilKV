# Round 2 Review: Storage Engine Documents (02-06)
## Reviewer: reviewer-red

---

### Document: 02-tsavorite-hybrid-log.md

#### Critical Issues (must fix)

- [C1] **Address diagram mislabels regions.** Section 2.3 shows `TailAddress` at the rightmost position and `BeginAddress` at the leftmost, but the labels between them are misleading. The diagram labels the region between `ReadOnlyAddress` and `TailAddress` as "mutable region", but the text itself says `ReadOnlyAddress` is the boundary between mutable and read-only. The region between `HeadAddress` and `ReadOnlyAddress` is labeled "read-only in memory" which is correct, but the region between `BeginAddress` and `HeadAddress` is labeled "on disk only" then the very next region is labeled "on-disk + in-memory", which is confusing. The actual code has `HeadAddress` as the boundary (below = on disk, above = in memory). The diagram should be redrawn for clarity. Additionally, the doc omits `ClosedUntilAddress` (AllocatorBase.cs:L109) which is distinct from `SafeHeadAddress` and tracks the actual progress of page closing.

- [C2] **`SafeHeadAddress` description is incorrect.** The document says SafeHeadAddress is "Set by `OnPagesClosed` as the highest address of the range being closed." The actual code comment (AllocatorBase.cs:L97-99) says: "This is set by OnPagesClosed as the highest address of the range it is starting to close; thus it leads ClosedUntilAddress." This is the opposite of what the doc implies -- SafeHeadAddress is the *start* (highest) of the range being closed, not the end. The doc also fails to explain the distinction between `SafeHeadAddress` and `ClosedUntilAddress`, which are separate fields with different semantics.

- [C3] **TailPageOffset packing is not documented accurately.** Section 2.5 says TailPageOffset stores "page number in upper 32 bits, offset in lower 32 bits." The actual code at AllocatorBase.cs:L986-L1017 shows `TryAllocate` uses `Interlocked.Add(ref TailPageOffset.PageAndOffset, numSlots)` on a `PageOffset` structure. The document does not explain the `PageOffset` struct layout, nor the critical edge case at line 1009 where `Offset > PageSize` triggers `HandlePageOverflow`. The fact that `PageAndOffset` is a single `long` with page in upper bits and offset in lower bits means an add that overflows the offset portion carries into the page portion -- this is an essential implementation detail for reimplementation that is missing.

#### Major Issues (should fix)

- [M1] **Missing source files from coverage.** The Source File Map omits several important allocator files that exist in the codebase:
  - `AllocatorScan.cs` -- scan iteration logic
  - `AllocatorRecord.cs` -- record-level operations
  - `AsyncIOContext.cs` -- async I/O context for disk reads
  - `ScanIteratorBase.cs`, `SpanByteScanIterator.cs` -- iterator implementations
  - `WorkQueueFIFO.cs`, `WorkQueueLIFO.cs` -- work queue implementations used by the allocator
  - `ErrorList.cs` -- error tracking
  These are relevant for anyone reimplementing the full allocator.

- [M2] **Flush pipeline described too superficially.** Section 2.6 describes the flush in 4 bullet points but omits: (a) The `PageStatusIndicator` and `PendingFlush` mechanisms are mentioned in passing but never explained. (b) The `FlushCallback` mechanism for notifying completion. (c) The critical `ShiftReadOnlyAddress` / `ShiftHeadAddress` methods and their interaction with epoch protection (these are the most complex parts of the allocator). (d) How partial-page flushes work for the last page.

- [M3] **Sector alignment details missing.** The doc mentions sector-aligned pages but does not explain that `AllocatePage` adds `2 * sectorSize` to the allocation (SpanByteAllocatorImpl.cs:L231) and aligns within that buffer. This detail is critical for direct I/O correctness and is needed for reimplementation.

#### Minor Issues (nice to fix)

- [m1] **`kFirstValidAddress = 64` context missing.** The doc states the value is 64 but does not explain *why* 64 bytes are reserved. Is it to avoid zero-page issues? Cache line alignment? The rationale matters for reimplementation decisions.

- [m2] **Overflow page pool capacity hardcoded.** The doc says "capacity 4" matching the code (SpanByteAllocatorImpl.cs:L26), but does not explain why 4 was chosen or whether this should be tuned.

- [m3] **`PersistedBeginAddress` not mentioned.** AllocatorBase.cs:L116 defines `PersistedBeginAddress` as "The lowest valid address on disk - updated when truncating log." This is distinct from `BeginAddress` and relevant for recovery.

#### Strengths

- Excellent explanation of the lock-free tail allocation protocol with clear step-by-step description.
- Good coverage of the mutable fraction concept and its implications.
- The Rust and Kotlin mapping notes are practical and actionable.
- Record layout description with alignment is clear and matches the code.

---

### Document: 03-tsavorite-hash-index.md

#### Critical Issues (must fix)

- [C1] **HashBucketEntry bit layout is wrong.** Section 2.2 describes the layout as:
  ```
  Bit 63:     Tentative flag (1 bit)
  Bits 62-48: Tag (15 bits in position, 14 bits of actual tag value)
  Bit 47:     ReadCache flag (1 bit)
  Bits 46-0:  Address (47 bits)
  ```
  But the actual code (HashBucketEntry.cs:L9) comments state:
  ```
  // Long value layout: [1-bit tentative][15-bit TAG][48-bit address]
  ```
  And Constants.cs shows `kAddressBits = 48` and `kAddressMask = (1L << 48) - 1`. The Address field is 48 bits, not 47. The ReadCache bit at position 47 is *part of* the 48-bit address field (it is bit 47 within the address). The document incorrectly separates the ReadCache bit from the address and states "Bits 46-0: Address (47 bits)". The correct layout is:
  ```
  Bit 63:     Tentative flag (1 bit)
  Bits 62-48: Tag (15 bits positional, 14 bits actual)
  Bits 47-0:  Address (48 bits, with bit 47 serving as ReadCache indicator within the address)
  ```
  This is confirmed by the `Address` property getter: `word & Constants.kAddressMask` which masks 48 bits including bit 47.

- [C2] **Missing the Pending bit.** Constants.cs:L28-L30 defines `kPendingBitShift = 62` and `kPendingBitMask = 1L << 62`. This bit occupies position 62, which is between the Tentative bit (63) and the Tag field. The document makes no mention of this bit at all. While the comment in HashBucketEntry.cs says the layout is `[1-bit tentative][15-bit TAG][48-bit address]`, the Pending bit at position 62 suggests the Tag is actually 13 bits of tag value at positions 48-60, with Pending at 62 and Tentative at 63. However, the Tag property getter uses `Constants.kTagPositionMask` which is `((1L << 14) - 1) << 48`, masking bits 48-61. So the tag spans bits 48-61 (14 bits) and Pending at bit 62 would overlap or conflict. This needs careful investigation and documentation -- the interaction between the Pending bit and the Tag field is ambiguous and must be clarified for reimplementation.

- [C3] **`kTagShift` constant value mismatch.** The document states `kTagShift = 48 (= 62 - 14)` but the code says `kTagShift = 62 - kTagSize = 62 - 14 = 48`. This is correct. However, the document then says the tag occupies "Bits 62-48" but `kTagPositionMask = kTagMask << kTagShift = ((1L << 14) - 1) << 48` which means the tag occupies bits 48-61 (14 bits), NOT bits 62-48. The range "62-48" is 15 bits, but only 14 bits are used. The document acknowledges this ("15 bits in position, 14 bits of actual tag value") but the bit range is misleading. It should say "Bits 61-48: Tag (14 bits)".

#### Major Issues (should fix)

- [M1] **Latch spin counts described incorrectly.** Section 2.4 says "Spin up to `kMaxLockSpins = 10` times" for shared latch. The actual code (HashBucket.cs:L43-L57) shows the spin loop uses `Thread.Yield()` between iterations, and the spin count starts at `kMaxLockSpins` and decrements. When it reaches 0, it returns false. So it spins `kMaxLockSpins` times, not "up to" -- it's exactly 10 loop iterations. The document also says exclusive latch drain spins are "kMaxReaderLockDrainSpins = 100" but the code says `kMaxReaderLockDrainSpins = kMaxLockSpins * 10 = 100`. These happen to match now, but the derived formula should be documented since the value could change.

- [M2] **Overflow bucket allocation mechanism is incomplete.** Section 2.7 says overflow buckets are allocated from `MallocFixedPageSize<HashBucket>` with "pages of 65536 entries" but does not explain: (a) how `MallocFixedPageSize.Allocate()` works (lock-free bump allocator within pages, page-level allocation via `GC.AllocateArray`), (b) how `GetPhysicalAddress` converts a logical page+offset to a native pointer, (c) how `Free` works. These details are needed for reimplementation.

- [M3] **Lock table not documented.** The `OverflowBucketLockTable` (in `Implementation/Locking/OverflowBucketLockTable.cs`) uses overflow bucket entries for key-level locking. This is a significant subsystem that is entirely absent from the document. The `TransientLocking.cs` file shows how transient locks work during operations. For reimplementation, understanding how the hash index supports key-level locking is essential.

- [M4] **`HashEntryInfo` struct not documented.** The `Implementation/HashEntryInfo.cs` file defines the `HashEntryInfo` struct which is the primary interface through which operations interact with the hash table (it carries bucket, slot, tag, and entry information). This is used by every `FindTag` and `FindOrCreateTag` call but is not mentioned in the document.

#### Minor Issues (nice to fix)

- [m1] **Tag collision probability.** The doc says "~1/16384 per entry" but the collision probability depends on bucket occupancy. The correct statement is: given two entries in the same bucket, the probability of a tag match is 1/16384. For n entries, the probability of at least one false match is approximately n/16384.

- [m2] **Resize chunk count.** The doc says "kNumMergeChunks = 256" but the code defines `kNumMergeChunkBits = 8` and `kNumMergeChunks = 1 << kNumMergeChunkBits = 256`. The derivation should be shown.

- [m3] **Source file line ranges seem outdated.** Several line references (e.g., `TsavoriteBase.cs:L133-L172`) may be approximate. For durability, reference method names in addition to line numbers.

#### Strengths

- Very clear explanation of the FindTag and FindOrCreateTag protocols with step-by-step flow.
- Good coverage of the tentative insertion mechanism and why it prevents duplicates.
- Cache-line analysis is well done.
- The stale entry reclamation in `FindTagOrFreeInternal` is correctly documented.

---

### Document: 04-tsavorite-epoch-and-concurrency.md

#### Critical Issues (must fix)

- [C1] **`Release()` does NOT free the slot on every call.** Section 2.6 shows the Release pseudocode clearing both `localCurrentEpoch` and `threadId`, then setting `entry = kInvalidIndex`. But looking at the actual code (LightEpoch.cs:L530-L544), `Release()` does exactly this. However, the document says `Suspend()` calls `Release()` then `SuspendDrain()`. The actual `Suspend()` (LightEpoch.cs:L323-L327) calls `Release()` which always frees the slot. This means that every Suspend/Resume cycle requires re-acquiring an epoch table entry via `ReserveEntryForThread`. The document's pseudocode for `Resume()` calls `Acquire()` which calls `ReserveEntryForThread` -- this is correct but the document does not highlight that this makes Suspend/Resume a heavier operation than it might appear (it involves a CAS on the table entry). This is a design decision that affects reimplementation choices.

#### Major Issues (should fix)

- [M1] **`SuspendDrain` algorithm is wrong in the document.** Section 2.9 says SuspendDrain "briefly `Resume()` and `Release()` to trigger drain processing." The actual code (LightEpoch.cs:L446-L464) shows:
  ```csharp
  while (drainCount > 0) {
      Thread.MemoryBarrier();
      // scan for any active threads
      for (index = 1..kTableSize) {
          if (entry_epoch != 0) return;  // someone else is active, let them drain
      }
      Resume();    // re-acquire epoch protection
      Release();   // release it again, triggering drain in Acquire
  }
  ```
  The document says `Resume()` and `Release()` but the code calls `Resume()` and then `Release()` (not `Suspend()`). `Release()` is different from `Suspend()` because `Suspend()` would recursively call `SuspendDrain()` again. The document needs to be precise about which methods are called, as this is a subtle and correctness-critical distinction.

- [M2] **Drain list CAS protocol not fully explained.** Section 2.8 describes registering an action but omits the important detail that the intermediate value `long.MaxValue - 1` is used as a "claimed but not yet populated" sentinel (LightEpoch.cs:L371). This prevents another thread from using the slot while the action and epoch are being written. The two-step write (CAS to claim, then write fields, then set final epoch) is a classic lock-free pattern that must be reimplemented correctly.

- [M3] **Missing `Dispose` lifecycle.** The `LightEpoch.Dispose()` method (LightEpoch.cs:L228-L249) uses a `kDisposedFlag` bit in `waiterCount` and cancels waiting threads via `CancellationTokenSource`. This cleanup path is important for correct resource management but is not documented.

- [M4] **`IEpochAccessor` interface not documented.** The document lists `IEpochAccessor.cs` in the file map but does not explain the interface contract. For reimplementation, the interface (Resume/Suspend/ProtectAndDrain/TrySuspend) defines the public API that all consumers of epoch protection must use.

#### Minor Issues (nice to fix)

- [m1] **Instance tracking detail.** The doc says `SelectInstance()` uses "CAS on a static `InstanceTracker` buffer" (Section 2.4). The code uses `Interlocked.CompareExchange(ref entry, 1, kInvalidIndex)` where kInvalidIndex = 0, and the value 1 simply means "in use". The document should note that the CAS value is 1, not the instanceId.

- [m2] **kTableSize computation.** The document correctly states `max(128, Environment.ProcessorCount * 2)` but should note this is a `ushort`, which means the maximum table size is 65535. This is relevant if running on machines with >32K processors (theoretically).

- [m3] **Thread.MemoryBarrier placement.** Section 2.9 mentions "insert a memory barrier" but does not explain that this is specifically `Thread.MemoryBarrier()` (a full fence), and why it's needed -- to ensure the thread sees the latest epoch table state after another thread has released.

#### Strengths

- The core epoch protection protocol (Acquire/Release/ProtectAndDrain) is explained clearly with pseudocode.
- The `ComputeNewSafeToReclaimEpoch` algorithm is accurately described and matches the source.
- The two-probe hashing scheme for entry acquisition is well covered.
- Good identification of the ARM/RISC-V memory ordering concern in Open Questions.

---

### Document: 05-tsavorite-checkpointing.md

#### Critical Issues (must fix)

- [C1] **Phase enum values are wrong.** Section 2.1 lists `IN_PROGRESS = 0`, `WAIT_INDEX_CHECKPOINT = 1`, etc. The actual code (StateTransitions.cs:L28-L52) defines Phase as a plain enum, so these values should be correct. However, the document should verify: the actual C# enum assigns values sequentially starting from 0 unless overridden, so `IN_PROGRESS = 0`, `WAIT_INDEX_CHECKPOINT = 1`, `WAIT_FLUSH = 2`, `PERSISTENCE_CALLBACK = 3`, `REST = 4`, `PREPARE = 5`, `PREPARE_GROW = 6`, `IN_PROGRESS_GROW = 7`. This matches the document. **No issue here** -- marking as verified.

- [C2] **Recovery.cs line reference is wrong and Recovery section is too vague for reimplementation.** Section 2.11 references `Recovery.cs:L160-L200` but the actual recovery logic is spread across multiple files: `Recovery.cs`, `IndexRecovery.cs`, `IndexCheckpoint.cs`, `Checkpoint.cs`, and `DeltaLog.cs`. The 7-step description is a high-level summary that omits critical details: (a) How is the hash index rebuilt from the log if no index checkpoint exists? (b) How are delta logs iterated and applied? (c) The `undoNextVersion` logic is mentioned but not explained -- what exactly is undone and how? (d) How does recovery handle partially-written records? An engineer cannot reimplement recovery from this description alone.

#### Major Issues (should fix)

- [M1] **State machine driver loop description is inaccurate.** Section 2.2 shows the loop as:
  ```
  loop:
    GlobalStateMachineStep(currentState)
    ProcessWaitingListAsync()
    if state == REST: break
  ```
  The actual code (StateMachineDriver.cs:L319-L355) shows:
  ```csharp
  do {
      GlobalStateMachineStep(systemState);
      await ProcessWaitingListAsync(token);
  } while (systemState.Phase != Phase.REST);
  ```
  The document omits: (a) the `try/catch` that calls `FastForwardStateMachineToRest` on exception, (b) the `finally` block that sets `stateMachineCompleted` and clears the state machine reference, (c) the error handling path. These are important for reimplementation robustness.

- [M2] **Missing `StreamingSnapshotCheckpointSM` documentation.** The file `StreamingSnapshotCheckpointSM.cs`, `StreamingSnapshotCheckpointSMTask.cs`, and `StreamingSnapshotTsavoriteKV.cs` exist in the codebase but are only mentioned in Open Questions. Streaming snapshots are a significant feature that deserves at least a subsection explaining the purpose and high-level mechanism.

- [M3] **`CheckpointVersionShiftStart` and `CheckpointVersionShiftEnd` not explained.** Section 2.5 references these methods but does not explain what they do. These are critical for understanding how the version boundary is established in the log during checkpointing.

- [M4] **`DeltaLog` structure not documented.** Section 2.8 describes delta entries as `[8-byte logical address][4-byte size][record bytes]` but does not reference the `DeltaLog.cs` file that implements this. The DeltaLog has its own header format, append/read protocol, and page management. This is needed for implementing incremental snapshots.

- [M5] **`RecoveryInfo.cs` is referenced but its path is wrong.** The source file map lists `Index/CheckpointManagement/RecoveryInfo.cs`, but the document should clarify the full metadata fields structure and serialization format for checkpoint metadata. An implementer needs to know the exact binary layout of the metadata file.

#### Minor Issues (nice to fix)

- [m1] **`TsavoriteStateMachineProperties.cs` not mentioned.** This file exists in `Index/Checkpointing/` and contains store-specific properties for checkpoint state machines.

- [m2] **`IStateMachineCallback` not explained.** `StateMachineDriver.UnsafeRegisterCallback` (StateMachineDriver.cs:L181-L185) allows external callbacks on state transitions. This is used by Garnet for cross-store coordination but is not documented.

- [m3] **Version number bit width.** Section 2.1 states version is 56 bits. The code (StateTransitions.cs:L70-L71) confirms: `kVersionBits = kPhaseShiftInWord = 64 - 8 = 56`. This is correct.

#### Strengths

- Good high-level overview of the three checkpoint strategies with clear pros/cons.
- The state machine phase sequence is clearly documented.
- The fuzzy region concept is well explained.
- The HybridLogCheckpointSMTask per-phase table is a useful reference.

---

### Document: 06-tsavorite-record-management.md

#### Critical Issues (must fix)

- [C1] **`SkipOnScan` does NOT skip tombstoned records.** Section 2.10 states: "Tombstone records are skipped during scans (`SkipOnScan`). [Source: RecordInfo.cs:L258]." The actual code at RecordInfo.cs:L258 is:
  ```csharp
  public readonly bool SkipOnScan => IsClosedWord(word);
  ```
  And `IsClosedWord` (RecordInfo.cs:L80):
  ```csharp
  private static bool IsClosedWord(long word) => (word & (kValidBitMask | kSealedBitMask)) != kValidBitMask;
  ```
  This returns true when the record is NOT (Valid AND NOT Sealed). A tombstoned record that is Valid and not Sealed has `SkipOnScan = false` -- it will NOT be skipped. Only Invalid or Sealed records are skipped by `SkipOnScan`. Tombstone filtering must be done by the scan caller, not by `SkipOnScan`. This is a factual error that would cause incorrect behavior in a reimplementation.

- [C2] **SpanByte MetadataSize computation is oversimplified.** Section 2.4 states "MetadataSize: Determined by flags -- 0 if neither flag set, 8 if ExtraMetadata, 1 if Namespace." The actual code (SpanByte.cs:L100):
  ```csharp
  public readonly int MetadataSize => ((length & ExtraMetadataBitMask) >> (30 - 3)) + ((length & NamespaceBitMask) >> 29);
  ```
  This does bit shifting: `ExtraMetadataBitMask` (bit 30) shifted right by 27 gives 8 when set. `NamespaceBitMask` (bit 29) shifted right by 29 gives 1 when set. So MetadataSize can be 0, 1, 8, or 9. The document says "0 if neither flag set, 8 if ExtraMetadata, 1 if Namespace" but does NOT mention the case where both flags are set (MetadataSize = 9). However, the code also has Debug.Assert statements (SpanByte.cs:L149, L166) that say "Don't use both extension for now", meaning both flags should not be set simultaneously. This nuance (that MetadataSize CAN be 9 but SHOULD NOT be) is important for reimplementation and is missing.

- [C3] **RecordInfo bit layout has an error in the leftover bits description.** Section 2.1 says "Bits 54-48: Leftover bits (7 bits) - reclaimed from former lock bits, currently unused." The code (RecordInfo.cs:L14) has the comment:
  ```
  // [VectorSet][Modified][InNewVersion][Filler][Dirty][ETag][Sealed][Valid][Tombstone][LLLLLLL] [RAAAAAAA] ...
  // where L = leftover, R = readcache, A = address
  ```
  The leftover bits are positions 48-54 (7 bits). The document's numbering "Bits 54-48" is correct. But the doc says these are "currently unused" -- this is misleading. While the code does not assign named flags to these bits, the code comment suggests "R = readcache" at bit position within the address. The PreviousAddress field (bits 47-0) is 48 bits and includes a readcache bit similar to the hash index entry. The document does not mention that `PreviousAddress` can encode a read cache address (note: RecordInfo.cs:L289 calls `IsReadCache(PreviousAddress)`).

#### Major Issues (should fix)

- [M1] **Missing `FreeRecordPool` internal structure.** Section 2.8 describes the RevivificationManager but omits the internal structure of `FreeRecordPool`. The `FreeRecord` struct (FreeRecordPool.cs:L14-L43) packs address and size into a single `long` word, with size using the upper bits. The size-segregated bins (`RevivificationBin`) have a specific structure with inline records and an overflow queue. This is essential for reimplementation of the revivification system.

- [M2] **Missing `RecordLengths.cs` and `CheckEmptyWorker.cs`.** The revivification subsystem includes `RecordLengths.cs` (for computing record lengths) and `CheckEmptyWorker.cs` (background worker that checks for empty bins). These files are in `Implementation/Revivification/` but are not mentioned in the source file map.

- [M3] **IPU/RCU mechanism described without code references.** Section 2.7 describes in-place update and read-copy-update but does not reference the actual implementation files: `InternalUpsert.cs`, `InternalRMW.cs`, `InternalDelete.cs`, `InternalRead.cs`, `TryCopyToTail.cs`, `ConditionalCopyToTail.cs`. These files contain the actual decision logic for when to do IPU vs RCU and the step-by-step protocol. Without them, an engineer would not know the full protocol.

- [M4] **Compaction description is too high-level.** Section 2.9 describes two compaction strategies but omits: (a) The `ICompactionFunctions.IsDeleted` callback that determines record liveness beyond just tombstone checking. (b) How `CompactLookup` handles the case where a record has been updated since the scan started. (c) The `LogCompactionFunctions` adapter and how it wraps user functions.

- [M5] **Missing `BlockAllocate.cs` reference.** The `BlockAllocate.cs` file implements the block allocation path used during RCU operations (allocating space at the tail for a new copy of a record). This is a key part of the record management lifecycle.

#### Minor Issues (nice to fix)

- [m1] **RecordInfo `InitializeToSealedAndInvalid` detail.** Section 2.2 says `WriteInfo` "sets the record to Sealed + Invalid" but the code (RecordInfo.cs:L233) shows `InitializeToSealedAndInvalid` sets `word = kSealedBitMask` which clears ALL other bits including PreviousAddress. Then `WriteInfo` separately sets PreviousAddress and InNewVersion. This ordering matters.

- [m2] **Missing `AtomicOwner.cs`.** The `Allocator/AtomicOwner.cs` file implements atomic ownership tracking which is used in record management. It is not mentioned.

- [m3] **SpanByte max payload length.** The doc says "max ~512MB" for bits 28-0, but `~HeaderMask` would give `(1 << 29) - 1 = 536,870,911 bytes ~ 512MB`. More precisely, 29 bits give a max of 512MiB minus 1 byte. The doc should state this precisely.

- [m4] **`ClearBitsForDiskImages` clears only Dirty and Sealed.** Section 2.11 correctly describes this, but does not explain WHY Dirty is cleared (it's because dirty tracking is per-checkpoint-cycle and needs to be reset on recovery). This rationale aids reimplementation.

#### Strengths

- The RecordInfo bit layout is well-documented with computed bit offsets matching the code.
- Good explanation of the sealed/valid lifecycle states.
- The filler mechanism description is thorough and accurate.
- Clear distinction between IPU and RCU strategies.
- Revivification concept is well-motivated.

---

### Cross-Document Issues

- [X1] **No document covers the core operation implementations.** The files `InternalRead.cs`, `InternalUpsert.cs`, `InternalRMW.cs`, `InternalDelete.cs`, `ContinuePending.cs`, and `HandleOperationStatus.cs` implement the core CRUD operations that tie together the hash index (doc 03), hybrid log (doc 02), epoch protection (doc 04), and record management (doc 06). These are the "glue" files that combine all subsystems, and none of the five documents adequately covers them. An engineer reading all five docs would still not know how a Read or Upsert operation flows end-to-end.

- [X2] **Read cache is mentioned in multiple docs but never explained.** Doc 02 (Section 7.2), Doc 03 (Section 2.2 ReadCache bit, Section 7.3), Doc 06 (RecordInfo PreviousAddress with read cache bit) all reference the read cache but defer to "separate documentation." The `ReadCache.cs` and `TryCopyToReadCache.cs` files exist. If read cache is out of scope, all docs should consistently say so and explain the minimal impact on the core design.

- [X3] **Lock table and transient locking not covered.** The `OverflowBucketLockTable.cs` and `TransientLocking.cs` files implement key-level locking using the hash index overflow buckets. This is referenced nowhere in docs 02-06 but is critical for understanding concurrent operation correctness.

- [X4] **`FindRecord.cs` and `OperationStackContext.cs` bridge hash index and log.** `FindRecord.cs` implements the traversal from hash index entry through the log record chain (following PreviousAddress). `OperationStackContext.cs` maintains the state during an operation's traversal. Neither is documented.

- [X5] **DeltaLog recovery path spans docs 05 and 06.** Doc 05 describes incremental snapshot checkpoint, and doc 06 describes record layout. But the DeltaLog entry format (`[8-byte address][4-byte size][record bytes]`) is only in doc 05, while the record structure it contains (RecordInfo + key + value) is in doc 06. An engineer implementing delta log replay needs to cross-reference both docs, and neither provides the complete picture.

---

### Summary

**Totals: 8 critical, 18 major, 14 minor issues across 5 documents.**

| Document | Critical | Major | Minor |
|----------|----------|-------|-------|
| 02-tsavorite-hybrid-log.md | 3 | 3 | 3 |
| 03-tsavorite-hash-index.md | 3 | 4 | 3 |
| 04-tsavorite-epoch-and-concurrency.md | 1 | 4 | 3 |
| 05-tsavorite-checkpointing.md | 1* | 5 | 3 |
| 06-tsavorite-record-management.md | 3 | 5 | 4 |
| Cross-document | - | - | - |

*Note: One initially flagged critical issue (Phase enum values) was verified correct and downgraded.

**Overall Assessment:**

The documents are well-structured and provide a solid conceptual foundation. The Rust/Kotlin mapping notes are a standout feature. However, there are significant factual errors that would lead to incorrect reimplementation (especially the SkipOnScan/tombstone error in doc 06 and the HashBucketEntry bit layout error in doc 03). The biggest structural gap is the lack of end-to-end operation flow documentation -- the five docs describe the components well individually but do not explain how they work together during a Read, Upsert, RMW, or Delete operation. The recovery path in doc 05 is also too superficial for reimplementation. After addressing the critical and major issues, these documents would be a strong foundation for reimplementation work.
