# Round 9 Final Review: Storage Engine Documents (02-06)
## Reviewer: reviewer-red

**Perspective: Can someone reimplement this subsystem from these documents ALONE?**

---

## 1. Source Reference Sampling (12 samples, ~20%)

| # | Doc | Reference | Content Expected | Result |
|---|-----|-----------|-----------------|--------|
| 1 | 02 | Constants.cs:L75-L78 | kInvalidAddress, kFirstValidAddress | PASS |
| 2 | 02 | AllocatorBase.cs:L791-L812 | GetTailAddress() | PASS |
| 3 | 03 | HashBucket.cs:L15-L30 | Latch encoding constants | PASS |
| 4 | 03 | TsavoriteBase.cs:L175-L216 | FindOrCreateTag | PASS |
| 5 | 04 | LightEpoch.cs:L98, L166-L176 | kTableSize, allocation | PASS |
| 6 | 04 | LightEpoch.cs:L555-L602 | TryAcquireEntry | PASS |
| 7 | 05 | StateMachineDriver.cs:L215-L253 | GlobalStateMachineStep | PASS* |
| 8 | 05 | HybridLogCheckpointSMTask.cs:L31-L98 | Per-phase logic | PASS |
| 9 | 06 | RecordInfo.cs:L14, L23-L48 | RecordInfo bit layout | PASS |
| 10 | 06 | SpanByteAllocatorImpl.cs:L107-L122 | Filler mechanism | PASS |
| 11 | 06 | InternalDelete.cs:L70-L71 | Delete search to HeadAddress | PASS |
| 12 | 06 | FindRecord.cs:L121-L123 | IsValidTracebackRecord | PASS |

**Overall: 12/12 PASS.** All source references point to the correct content at the documented line numbers.

*Sample 7 note: Minor accuracy issue described below (Doc 05 M1).

---

## 2. Design "WHY" Check

The documents have improved significantly in providing rationale. Specific "why" observations:

**Well-documented WHY:**
- Doc 02: Cache-line alignment for buckets is explained in terms of cache misses (Performance Notes). Lock-free allocation via PageOffset packing is explained with the carry-into-page insight. Mutable fraction rationale (90% IPU vs 10% RO buffer) is clear.
- Doc 03: Tag filtering probability (1/16384 false-positive rate with 14 bits) is quantified. Tentative bit purpose (concurrent insertion ABA avoidance) is explained. Transient locking rationale (TOCTOU prevention) is now documented.
- Doc 04: SuspendDrain Resume/Release (not Suspend) reason is explained (recursion avoidance). Design note about Release() always freeing slots is excellent.
- Doc 05: Two-phase Before/After design rationale is clear. Fuzzy region definition with start/final addresses is well-motivated.
- Doc 06: Sealed+Invalid initialization reason (prevent scans seeing partial data, survive recovery). RMW fuzzy region RETRY_LATER reason (lost-update anomaly prevention). Upsert shallow vs RMW deep search rationale.

**Missing WHY (Major):**

- [M-WHY-1] **Doc 02: Why is `kFirstValidAddress = 64` instead of, say, 8 or 16?** The doc states the value but not the reason. Is 64 related to cache-line alignment? Is it to provide space for sentinel addresses 0-2 plus padding? A reimplementor would need to know whether this value is arbitrary or structurally required.

- [M-WHY-2] **Doc 03: Why does the hash table use a 14-bit tag specifically?** The doc explains what the tag does but not why 14 bits was chosen. With 8 entries per bucket and 14-bit tags, the false positive rate is already very low. Was 14 chosen because bits 63 (tentative) and 62 (pending) consume 2 of the "upper 16" bits, leaving exactly 14? This structural constraint should be documented.

---

## 3. Critical Issues

_None found._ All critical issues from Rounds 2 and 4 have been addressed. The two critical issues from Round 4 (Read flow using wrong boundary/callback, RMW missing fuzzy region) are now correctly documented in Doc 06 Section 2.12.

---

## 4. Major Issues

- [M1] **Doc 05 Section 2.3: `GlobalStateMachineStep` epoch protection claim is backwards.** The doc says: "The `GlobalStateMachineStep` method must be called on an epoch-protected thread." But the actual code at StateMachineDriver.cs:L243 asserts `Debug.Assert(!epoch.ThisInstanceProtected())` -- the method asserts the thread is NOT epoch-protected. The method itself calls `epoch.Resume()` at L246 to acquire protection before `BumpCurrentEpoch()`. The claim that `BumpCurrentEpoch` requires protection (LightEpoch.cs:L344) is correct, but the framing about the calling context of `GlobalStateMachineStep` is inverted. A reimplementor who pre-acquires epoch protection before calling the state machine step would hit the assertion. **Fix: Change to "The `GlobalStateMachineStep` method acquires epoch protection internally via `epoch.Resume()` before calling `BumpCurrentEpoch`. It must be called on a thread that is NOT already epoch-protected."**

- [M2] **Doc 02 Section 2.6: Flush pipeline remains too vague for reimplementation.** The 4-step flush description is adequate for understanding the concept but insufficient for writing working code. Missing details that block reimplementation:
  - How `PageStatusIndicator` tracks whether a page is dirty/in-flight/clean (the state transitions).
  - The `PendingFlush` queue mechanism: how many flushes can be in-flight per page, how the completion callback advances `FlushedUntilAddress`.
  - The `FlushCallback` / `AsyncFlushPageCallback` completion path that chains flush completion to head-address advancement.
  - How partial-page flushes work when the tail is mid-page during a checkpoint.

  This is the single largest "magic step" remaining in the documents. An engineer would need to read the source code to implement flushing -- the document alone is insufficient.

- [M3] **Doc 03 Section 2.8: Index resizing lacks procedural detail.** The section describes the concept (two table versions, 256 chunks, split based on new hash bit) but does not provide enough detail to reimplement. Missing:
  - How `SplitBuckets` is called per-operation during `IN_PROGRESS_GROW` (it is invoked at the top of every Internal* operation when `phase == IN_PROGRESS_GROW`).
  - What splitting a bucket actually means: for each entry, check if the new bit in the hash is 0 or 1; entries with bit=1 are moved to the new table's extended bucket.
  - How the overflow chain is handled during splitting (entries must be re-distributed across old and new overflow chains).
  - How the state machine coordinates the transition from `state[0]` to `state[1]` (or vice versa).

---

## 5. Minor Issues

- [m1] **Doc 04 Performance Notes: "No locks on fast path" claim is slightly misleading.** The note says "Resume and Suspend are a single store each (after the initial reservation)." But as the doc itself explains in Section 2.6, Release() zeroes both `localCurrentEpoch` and `threadId`, sets `entry = kInvalidIndex`, and potentially releases the waiterSemaphore. That is 3-4 operations, not "a single store." The initial reservation (CAS on threadId) is also non-trivial. The performance note should say "Resume is a CAS + store; Suspend is three stores plus a conditional semaphore release."

- [m2] **Doc 06 Section 2.12 Delete step 6: "Delete does NOT return success for non-existent keys" needs nuance.** The doc correctly states that Delete creates a tombstone for on-disk/not-found cases. However, the `CreateNewRecordDelete` path returns `OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord)` on success (InternalDelete.cs:L273), which the session layer maps to `Status.Found = false` at the API level. So from the user's perspective, deleting a non-existent key returns "not found" (which is arguably "success with no effect"). The doc could clarify the distinction between the internal status code and the external API behavior.

---

## 6. Cross-Reference Check

The integration editor has added useful cross-references. Verified:

| Cross-reference | Location | Target | Correct? |
|----------------|----------|--------|----------|
| Doc 02 -> Doc 06 Section 2.12 | Section 2.3 | Operation flow usage of boundaries | YES |
| Doc 02 -> Doc 06 Section 2.5 | Section 2.8 | Full record layout | YES |
| Doc 03 -> Doc 06 Section 2.12 | Section 2.5 | Transient lock integration | YES |
| Doc 04 -> Doc 05 Section 2.3 | Section 1 Overview | Checkpoint state machine uses epoch | YES |
| Doc 05 -> Doc 04 Section 2.8 | Section 2.3 | Epoch deferred action mechanism | YES |
| Doc 05 -> Doc 06 Section 2.12 | Section 2.4 | CPR consistency checks | YES |
| Doc 06 -> Doc 03 Section 2.4 | Section 2.12 | Latch bit encoding | YES |
| Doc 06 -> Doc 03 Section 2.5 | Section 2.12 | Find-and-lock protocol | YES |
| Doc 06 -> Doc 05 Section 2.4 | Section 2.12 | Version change phases | YES |
| Doc 06 -> Doc 05 Section 2.5 | Section 2.12 | Fuzzy region definition | YES |
| Doc 06 -> Doc 02 Section 2.3 | Section 2.12 | Address region boundaries | YES |

All cross-references verified as correct and pointing to the appropriate sections.

---

## 7. Completeness for Reimplementation Assessment

**Can an engineer write working code from each section?**

| Section | Verdict | Notes |
|---------|---------|-------|
| Doc 02: Logical addressing | YES | Complete bit layout and computation |
| Doc 02: Circular buffer | YES | Clear structure with allocation code |
| Doc 02: Address pointers | YES | All pointers documented with roles |
| Doc 02: Tail allocation | YES | PageOffset trick well-explained |
| Doc 02: **Page flushing** | **NO** | Major gap (M2): insufficient detail for flush pipeline |
| Doc 02: Page eviction | MOSTLY | Conceptually clear, but depends on flush pipeline |
| Doc 03: Hash computation | YES | Tag extraction, bucket index, all constants |
| Doc 03: Bucket/entry layout | YES | Full bit layout for entries and latches |
| Doc 03: FindTag/FindOrCreateTag | YES | Algorithm steps with CAS protocol |
| Doc 03: Transient locking | YES | Now documented with rationale |
| Doc 03: **Index resizing** | **NO** | Major gap (M3): concept only, no procedural detail |
| Doc 04: Epoch table | YES | Full layout, acquire/release, drain |
| Doc 04: Deferred actions | YES | CAS protocol well-documented |
| Doc 04: SuspendDrain | YES | Subtle recursion avoidance explained |
| Doc 05: SystemState | YES | Bit layout verified against source |
| Doc 05: State machine framework | YES | Three-layer architecture clear |
| Doc 05: Checkpoint strategies | MOSTLY | Fold-over and snapshot clear; incremental less so |
| Doc 05: **Recovery** | **PARTIALLY** | Better than before but still high-level |
| Doc 06: RecordInfo | YES | Complete bit layout with lifecycle |
| Doc 06: SpanByte | YES | Header format, two forms, MetadataSize |
| Doc 06: Record layout | YES | Exact offsets and alignment rules |
| Doc 06: Revivification | MOSTLY | Conceptually clear; free pool bin structure underspecified |
| Doc 06: Operation flows | YES | Now includes transient locking, CPR, fuzzy regions, search depths |

**Blocking gaps for reimplementation: 2 (flush pipeline, index resizing)**
**Partially sufficient: 3 (page eviction, recovery, revivification free pool)**
**Sufficient: 18 sections**

---

## 8. Verdict

The documents are in strong shape after 3 revision cycles. The Round 4 critical issues (wrong Read boundary/callback, missing RMW fuzzy region) have been fully addressed. Section 2.12 in Doc 06 is now comprehensive and accurate, covering transient locking, CPR checks, fuzzy regions, search depths, and correct callback names.

**Remaining blocking issues for reimplementation:**
1. Page flush pipeline (Doc 02) -- the biggest remaining gap
2. Index resizing procedure (Doc 03)
3. One factual error in Doc 05 (epoch protection assertion is inverted)

These are all fixable with targeted additions. The core architecture, data structures, algorithms, and concurrency protocols are well-documented and source-verified.

**Recommendation: Address M1 (Doc 05 factual error) and M2/M3 (flush/resize gaps) in the final polish round. The documents are otherwise ready for use as reimplementation guides.**
