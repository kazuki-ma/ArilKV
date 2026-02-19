# Round 4 Review: Storage Engine Documents (02-06) -- Deep Technical Accuracy
## Reviewer: reviewer-red

This review focuses on algorithm and data structure accuracy: bit layouts, CAS operations, address boundary checks, state transitions, and the new Section 2.12 end-to-end operation flows. All claims verified against source code.

---

### Document: 02-tsavorite-hybrid-log.md

#### Critical Issues

_None found._ The Round 2 critical issues (SafeHeadAddress, ClosedUntilAddress, PageOffset struct) have been correctly addressed.

#### Major Issues

- [M1] **Mutable region boundary inconsistency with Doc 06.** Section 2.3 correctly defines `ReadOnlyAddress` as the boundary between mutable and read-only regions. However, the actual Read operation (InternalRead.cs:L118) uses `SafeReadOnlyAddress` (not `ReadOnlyAddress`) as the boundary between the mutable callback (`ConcurrentReader`) and the immutable callback (`SingleReader`). Doc 06 Section 2.12 inherits this error (see Doc 06 issues below). Doc 02 should note that `SafeReadOnlyAddress` lags behind `ReadOnlyAddress` and that the Read path uses `SafeReadOnlyAddress` while write paths (Upsert, RMW, Delete) use `ReadOnlyAddress`. This is a subtle but functionally important distinction.

- [M2] **`TryAllocate` return semantics incomplete.** Section 2.5 describes the `numSlots` parameter as "record size in 8-byte units" and mentions the page overflow handling, but does not document the three distinct return statuses: `SUCCESS` (allocation succeeded on current page), `RETRY_NOW` (a page overflow occurred, another thread owns the page turn, retry immediately), and `RETRY_LATER` (the next page's buffer slot has not been flushed yet). These statuses are essential for reimplementors to handle correctly. [Source: AllocatorBase.cs:L986-L1017]

#### Minor Issues

- [m1] **Overflow page pool capacity.** Section 2.9 says "capacity 4" for the overflow pool. The actual capacity is configurable via `OverflowPool<T>(int size)`, with 4 being the default value used for SpanByteAllocator. A reimplementor should know this is configurable, not fixed.

---

### Document: 03-tsavorite-hash-index.md

#### Critical Issues

_None found._ The Round 2 critical issues (ReadCache bit in address, Pending bit, tag bit count) have been correctly addressed.

#### Major Issues

- [M1] **FindTag description omits Closed record handling.** Section 2.5 describes `FindTag` as scanning entries 0..6 for a matching non-tentative entry. But the actual lookup path in the Internal* operations does not use `FindTag` directly -- it uses `FindTagAndTryTransientSLock` (for Read) or `FindTagAndTryTransientXLock` (for Upsert/RMW/Delete), which combine tag lookup with bucket-level transient locking in a single operation. The doc should note that all tag operations are paired with transient locking in practice, even though `FindTag` exists as a separate method. This is critical for reimplementation because a naive implementation that separates find-then-lock would have a TOCTOU race.

- [M2] **Hash bucket latch spin limits need clarification.** Section 2.4 documents `kMaxLockSpins = 10` for shared latch and `kMaxReaderLockDrainSpins = 100` for exclusive latch reader drain. However, it does not document what happens when these limits are exceeded: for shared latch, the method returns false and the caller must retry; for exclusive latch, the exclusive bit is released and the method returns false. This failure-and-retry protocol is essential for correctness -- a reimplementor who assumes these operations always succeed would introduce deadlocks or missed updates. [Source: HashBucket.cs:L39-L58, L77-L110]

#### Minor Issues

- [m1] **Overflow bucket CAS failure handling.** Section 2.7 says "the allocation is freed and the search continues from the winner's bucket." This is correct for the hash index itself, but the term "freed" is imprecise -- the overflow bucket is returned to the `MallocFixedPageSize` allocator, which may or may not actually free it (it uses a free list internally). This distinction matters for memory accounting.

---

### Document: 04-tsavorite-epoch-and-concurrency.md

#### Critical Issues

_None found._ The Round 2 critical issue (SuspendDrain using Resume/Release instead of Suspend) has been correctly addressed.

#### Major Issues

- [M1] **BumpCurrentEpoch(Action) description slightly misleading on epoch increment path.** Section 2.8 step 1 says: "Bump the global epoch: `PriorEpoch = Interlocked.Increment(ref CurrentEpoch) - 1`." The actual code (LightEpoch.cs:L363) calls `BumpCurrentEpoch() - 1`, where the no-arg `BumpCurrentEpoch()` (LightEpoch.cs:L342-L353) does `Interlocked.Increment(ref CurrentEpoch)` AND then calls `Drain(nextEpoch)` or `ComputeNewSafeToReclaimEpoch(nextEpoch)`. This means there is an additional drain pass BEFORE the action registration loop begins. The doc's description implies the increment is a standalone atomic operation, but it actually triggers drain processing as a side effect. A reimplementor who implements only the increment (without the side-effect drain) could have delayed action processing.

- [M2] **Drain list `drainCount` increment timing.** Section 2.8 step 3-4 describes the CAS-claim-then-populate protocol but does not mention that `Interlocked.Increment(ref drainCount)` happens AFTER the action is fully registered (epoch overwritten with PriorEpoch). This is important: if `drainCount` were incremented before the epoch field was set to PriorEpoch, another thread's `Drain()` call could try to process the slot while it still has the sentinel value `long.MaxValue - 1`, which would be a bug. The actual code (LightEpoch.cs:L373-L375) correctly increments `drainCount` after the final `drainList[i].epoch = PriorEpoch` store. A reimplementor needs to preserve this ordering. [Source: LightEpoch.cs:L371-L375]

#### Minor Issues

- [m1] **SuspendDrain code comment says "Resume()" but this calls "Acquire()".** Section 2.9 says "Resume(); // re-acquire epoch protection (calls Acquire, not full Suspend/Resume)". This is accurate -- `Resume()` calls `Acquire()` (L335-L336). However, the comment in the doc says `Acquire` which may confuse readers who expect the public API name `Resume`. The doc should clarify that `Resume()` is the public method and `Acquire()` is the private implementation it delegates to.

---

### Document: 05-tsavorite-checkpointing.md

#### Critical Issues

_None found._

#### Major Issues

- [M1] **BumpCurrentEpoch(Action) in Section 2.3 step 5 omits epoch protection requirement.** The doc says step 5 is "`epoch.BumpCurrentEpoch(() => MakeTransitionWorker(nextState))`" but does not mention that `BumpCurrentEpoch()` asserts the calling thread is epoch-protected (`Debug.Assert(ThisInstanceProtected())`; LightEpoch.cs:L344). The `StateMachineDriver.GlobalStateMachineStep` method must be called on an epoch-protected thread. This is not obvious and a reimplementor could call the state machine step from an unprotected context, causing assertion failures or incorrect behavior.

- [M2] **Recovery protocol Section 2.11 is too high-level for reimplementation.** The 7-step recovery protocol omits several important details:
  - Step 3 does not explain that index recovery from a checkpoint file requires reading sector-aligned pages of hash buckets plus overflow buckets from a separate overflow file.
  - Step 5 does not explain the `undoNextVersion` mechanism: recovery scans the fuzzy region backwards and sets `Invalid` on records that have `InNewVersion` set, effectively rolling back to version v. The scan direction and invalidation method matter for correctness.
  - The doc does not mention `RecoveryInfo.objectLogSegmentOffsets` handling for GenericAllocator, which is less relevant for Garnet but is part of the recovery protocol. [Source: Recovery.cs:L160-L200]

#### Minor Issues

- [m1] **Phase enum value documentation could include numeric values.** Section 2.1 lists the Phase values with their enum names and descriptions but does not explicitly state the numeric values (IN_PROGRESS=0, WAIT_INDEX_CHECKPOINT=1, etc.). Since the state machine relies on these values for bit-packing into SystemState, the numeric values matter. They are listed in the table but a reimplementor might not realize they are contiguous integers starting from 0.

---

### Document: 06-tsavorite-record-management.md

#### Critical Issues

- [C1] **Section 2.12 Read flow uses wrong mutable region boundary.** The Read operation description says: "If found in the **mutable region** (>= ReadOnlyAddress): read the value directly via `ISessionFunctions.SingleReader`." TWO errors here:
  1. The boundary is `SafeReadOnlyAddress`, NOT `ReadOnlyAddress`. InternalRead.cs:L118 checks `stackCtx.recSrc.LogicalAddress >= hlogBase.SafeReadOnlyAddress`.
  2. In the mutable region, Read uses `ConcurrentReader`, NOT `SingleReader`. `SingleReader` is used for the immutable (read-only) region. InternalRead.cs:L128 calls `ConcurrentReader`; L143 calls `SingleReader`.

  These are factual errors that would cause a reimplementor to use the wrong callback at the wrong boundary. [Source: InternalRead.cs:L118-L149]

- [C2] **Section 2.12 RMW flow omits the fuzzy region (SafeReadOnlyAddress to ReadOnlyAddress).** The RMW description goes from "mutable region" (IPU) directly to "read-only region" (CopyUpdate). But InternalRMW.cs:L178 has a critical check:
  ```csharp
  if (stackCtx.recSrc.LogicalAddress >= hlogBase.SafeReadOnlyAddress && !stackCtx.recSrc.GetInfo().Tombstone)
  {
      status = OperationStatus.RETRY_LATER;
      goto LatchRelease;
  }
  ```
  Records in the **fuzzy region** (between `SafeReadOnlyAddress` and `ReadOnlyAddress`) that are not tombstoned cause `RETRY_LATER` to avoid a lost-update anomaly. This is a correctness-critical behavior: if a reimplementor omits this check, concurrent RMW operations during the ReadOnlyAddress transition could lose updates. The comment in the code explicitly says "Fuzzy Region: Must retry after epoch refresh, due to lost-update anomaly." [Source: InternalRMW.cs:L178-L183]

#### Major Issues

- [M1] **Section 2.12 omits transient bucket locking for ALL operations.** All four operations use transient bucket-level locking:
  - Read: `FindTagAndTryTransientSLock` (shared lock) + `TransientSUnlock` in finally
  - Upsert: `FindOrCreateTagAndTryTransientXLock` (exclusive lock) + `TransientXUnlock` in finally
  - RMW: `FindOrCreateTagAndTryTransientXLock` (exclusive lock) + `TransientXUnlock` in finally
  - Delete: `FindTagAndTryTransientXLock` (exclusive lock) + `TransientXUnlock` in finally

  The doc describes these operations using the simpler names `FindTag` and `FindOrCreateTag`, which are the underlying tag operations, but completely omits the transient locking step. This is a major gap because: (a) the locking is integral to correctness -- it prevents races between record insertion and record elision/revivification; (b) Read uses a shared lock while write operations use exclusive locks, which is a critical distinction; (c) the lock must be held for the duration of the operation and released in a `finally` block. A reimplementation that omits transient locking would have race conditions under concurrent revivification. [Source: InternalRead.cs:L63, InternalUpsert.cs:L58, InternalRMW.cs:L62, InternalDelete.cs:L55]

- [M2] **Section 2.12 omits CPR consistency checks.** All write operations (Upsert, RMW, Delete) include CPR (Concurrent Prefix Recovery) consistency checks after finding the record:
  - `CheckCPRConsistencyUpsert` (InternalUpsert.cs:L274-L298)
  - `CheckCPRConsistencyRMW` (InternalRMW.cs:L325-L359)
  - `CheckCPRConsistencyDelete` delegates to `CheckCPRConsistencyUpsert` (InternalDelete.cs:L200-L203)

  These checks implement the CPR protocol: a thread in version V encountering a V+1 record returns `CPR_SHIFT_DETECTED`; a thread in V+1 encountering a V record forces RCU (CreateNewRecord) instead of IPU. Read also has a CPR check at InternalRead.cs:L115-L116. The doc does not mention CPR consistency at all, despite it being essential for checkpoint correctness. [Source: InternalUpsert.cs:L87-L103, InternalRMW.cs:L93-L109, InternalRead.cs:L115-L116]

- [M3] **Section 2.12 Read flow description omits ReadCache handling.** The Read operation has a significant ReadCache fast path (InternalRead.cs:L81-L103) that is checked BEFORE the main log traversal. If the hash entry points to the read cache, `FindInReadCache` is called first. Only if the record is not in the read cache does it fall through to `TryFindRecordForRead` on the main log. The doc's description starts with "FindTag in the hash index" and goes straight to hash chain traversal, skipping the read cache entirely.

- [M4] **Section 2.12 Upsert/RMW search depth mismatch.** The doc says Upsert and RMW both search for existing records, but does not document their different search depths:
  - **Upsert** searches down to `ReadOnlyAddress` only (InternalUpsert.cs:L74: `TryFindRecordForUpdate(ref key, ref stackCtx, hlogBase.ReadOnlyAddress, out status)`). It blindly inserts if the record is below ReadOnlyAddress.
  - **RMW** searches down to `HeadAddress` (InternalRMW.cs:L78: `TryFindRecordForUpdate(ref key, ref stackCtx, hlogBase.HeadAddress, out status)`). This allows RMW to find records in the immutable region for CopyUpdate.
  - **Delete** also searches down to `HeadAddress` (InternalDelete.cs:L71).

  This difference is significant for reimplementation: Upsert's shallow search means it may create duplicate records in the hash chain (the old read-only version is still there), while RMW explicitly finds and copies from immutable records. [Source: InternalUpsert.cs:L73-L74, InternalRMW.cs:L77-L78, InternalDelete.cs:L70-L71]

- [M5] **Section 2.12 "Record Chain Traversal" description inaccurate.** The doc says `TryFindRecordInMemory` and `TraceBackForKeyMatch` check "(c) is the record valid (not sealed/invalid)?" But the actual code at FindRecord.cs:L123 defines `IsValidTracebackRecord` as:
  ```csharp
  static bool IsValidTracebackRecord(RecordInfo recordInfo) => !recordInfo.Invalid || recordInfo.IsSealed;
  ```
  This means traceback returns records that are either non-Invalid OR Sealed. Sealed records ARE returned (because they need RETRY handling), and only purely Invalid (not Sealed) records are skipped. The doc's summary is misleading -- it implies sealed records are skipped during traversal, but they are explicitly included.

- [M6] **Section 2.12 Upsert description says "unseal and validate the new record" on successful CAS, but the actual mechanism is different.** The document says: "On successful CAS, unseal and validate the new record." Looking at the actual CreateNewRecordUpsert code (InternalUpsert.cs:L314-L395): after a successful CAS via `CASRecordIntoChain`, the code calls `PostCopyToTail` and `PostSingleWriter`, then seals/invalidates the OLD source record -- it does NOT explicitly "unseal and validate" the new record in this path. The `UnsealAndValidate` call happens inside `CASRecordIntoChain` as part of the atomic insert. The doc's description implies it's a separate step after CAS, which could lead a reimplementor to incorrectly sequence these operations.

- [M7] **Section 2.12 Delete flow says "return success" for not-found keys, but the actual behavior is to create a tombstone record.** The doc says: "If **not found**: return success (delete of non-existent key is a no-op in Tsavorite)." But looking at InternalDelete.cs:L150-L155, when no record is found (or record is on-disk), the code falls through to `CreateNewRecord` which allocates a new tombstone record at the tail. It does NOT simply return success. Only if the record is already tombstoned does it return NOTFOUND early (lines L109-L110, L145-L146).

#### Minor Issues

- [m1] **Section 2.12 Read flow says "If the record is a tombstone, return NOTFOUND" as step 6.** This is misleading about when the check happens. In the actual code, tombstone checking happens via `srcRecordInfo.IsClosedOrTombstoned(ref status)` at lines L125-L126 and L140-L141, BEFORE the reader callbacks. It is not a separate final step; it is inline with the region checks. Additionally, `IsClosedOrTombstoned` checks both Closed (Sealed/Invalid -> RETRY_LATER) and Tombstone (-> NOTFOUND) in a single operation, which the doc does not convey.

- [m2] **Section 2.1 omits `InitialValid` sentinel value.** The RecordInfo section documents the lifecycle starting from `WriteInfo`, but there is a `InitialValid` static field (RecordInfo.cs:L55) used as a dummy/sentinel in the Internal* operations. While minor, a reimplementor seeing `RecordInfo dummyRecordInfo = RecordInfo.InitialValid` in the source would benefit from understanding this pattern.

- [m3] **Section 2.7 RCU step 1 says "The old record is sealed via TrySeal()."** This is not entirely accurate for all paths. In Upsert, the old record is sealed AFTER the new record's CAS succeeds (InternalUpsert.cs:L371 or L381), not before. Only RMW in some paths seals before (via the IsFrozen check in revivification). The doc implies sealing always precedes the new record creation.

---

### Cross-Document Issues

- [X1] **ReadOnlyAddress vs SafeReadOnlyAddress inconsistency.** Doc 02 defines both `ReadOnlyAddress` and `SafeReadOnlyAddress`, correctly noting that Safe versions lag behind. Doc 06 Section 2.7 says IPU applies "when a record is in the mutable region (address >= ReadOnlyAddress)." The actual Upsert/RMW/Delete operations check `>= ReadOnlyAddress` for IPU (correct), but Read checks `>= SafeReadOnlyAddress` for the ConcurrentReader path (different from what Doc 06 says). This inconsistency needs explicit documentation: Read uses SafeReadOnlyAddress, write operations use ReadOnlyAddress, and the reason is that reads in the fuzzy region (between SafeReadOnlyAddress and ReadOnlyAddress) are safe (reads don't modify), while writes need the stricter boundary.

- [X2] **Transient locking not covered in any document.** Doc 03 covers the bucket latch mechanism (shared/exclusive in Entry[7]) but does not explain that this is used for "transient" operation-duration locking during Internal* operations. Doc 06 Section 2.12 uses the simplified `FindTag`/`FindOrCreateTag` names. The transient locking layer (which wraps tag operations with bucket latching) sits between the hash index layer (doc 03) and the record management layer (doc 06) but is not documented anywhere. This gap means a reimplementor would not know that: (a) every operation holds a bucket latch for its duration, (b) reads use shared latches while writes use exclusive latches, (c) the latch is released in a finally block to handle exceptions.

- [X3] **CPR protocol not explained end-to-end.** Doc 05 describes the checkpoint state machine phases and the fuzzy region concept. Doc 06 Section 2.12 describes the CRUD operations. But no document explains how CPR consistency checks in the CRUD operations (CheckCPRConsistencyUpsert, CheckCPRConsistencyRMW) interact with the checkpoint state machine. The protocol is: during PREPARE phase, V threads encountering V+1 records return CPR_SHIFT_DETECTED; during IN_PROGRESS/WAIT_FLUSH phases, V+1 threads encountering V records force RCU. This is essential glue logic between doc 05 and doc 06 that is currently missing.

---

### Summary of Round 2 Fixes Verified

All 8 critical issues from Round 2 have been addressed:
1. Doc 02 C1 (SafeHeadAddress, ClosedUntilAddress) -- FIXED correctly
2. Doc 02 C2 (SafeHeadAddress description) -- FIXED correctly
3. Doc 02 C3 (PageOffset struct) -- FIXED correctly
4. Doc 03 C1 (ReadCache bit in address) -- FIXED correctly
5. Doc 03 C2 (Missing Pending bit) -- FIXED correctly
6. Doc 04 C1 (SuspendDrain Resume/Release) -- FIXED correctly
7. Doc 06 C1 (SkipOnScan tombstone) -- FIXED correctly
8. Doc 06 C2 (SpanByte MetadataSize=9) -- FIXED correctly

### New Issues Summary

| Severity | Doc 02 | Doc 03 | Doc 04 | Doc 05 | Doc 06 | Cross |
|----------|--------|--------|--------|--------|--------|-------|
| Critical | 0 | 0 | 0 | 0 | 2 | 0 |
| Major | 2 | 2 | 2 | 2 | 7 | 3 |
| Minor | 1 | 1 | 1 | 1 | 3 | 0 |
| **Total** | **3** | **3** | **3** | **3** | **12** | **3** |

**Total: 2 critical, 18 major, 7 minor, 3 cross-document = 30 issues**

The most significant cluster of issues is in Doc 06 Section 2.12 (end-to-end operation flows), which has two critical factual errors (wrong mutable boundary and wrong callback for Read, missing fuzzy region for RMW) and seven major omissions (transient locking, CPR checks, ReadCache fast path, search depth differences, traceback validity semantics, CAS/seal ordering, and Delete tombstone-creation behavior). The operation flow descriptions need substantial revision to be accurate enough for reimplementation.
