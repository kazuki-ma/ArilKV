# Final Feasibility Assessment: Rust x Storage Engine (Round 7)
## Assessor: assess-rust-sys

## Changes Since Round 5

This assessment incorporates significant revisions to the documentation, particularly:

1. **Doc 02 Section 2.3/2.5**: Clarified SafeReadOnlyAddress vs ReadOnlyAddress boundaries for read vs write paths; documented TryAllocate's three return statuses (SUCCESS, RETRY_NOW, RETRY_LATER).
2. **Doc 03 Section 2.4/2.5**: Explicit spin counts and failure semantics for latch protocol; new transient locking section documenting the combined find-and-lock operations that prevent TOCTOU races.
3. **Doc 04 Section 2.8**: `drainCount` ordering invariant clarified -- must be incremented AFTER epoch field is set.
4. **Doc 05 Section 2.3/2.11**: `BumpCurrentEpoch` epoch-protection assertion; expanded recovery protocol with backwards fuzzy region scan and `SetInvalid` semantics.
5. **Doc 06 Section 2.7/2.12**: RCU protocol corrected (old record sealed AFTER new record CAS, not before); operation flows completely rewritten with transient bucket locking, CPR consistency checks, fuzzy region RETRY_LATER for RMW, search depth differences (Upsert shallow vs RMW/Delete deep), and read cache fast path.

---

### Coverage Rating: A-

Upgraded from B+. The revised documentation now covers the critical operation-level semantics that were previously missing. The operation flows in doc 06 Section 2.12 are the most important addition -- they specify exactly how the hash index, log regions, epoch system, transient locking, and CPR version checks interact during each CRUD operation. This was the single largest gap in the Round 5 assessment. The remaining gaps (I/O device abstraction, memory ordering for non-x86) persist but are less blocking given the overall documentation quality.

---

### Updated Dimension Scores

| Dimension | Round 5 | Round 7 | Delta | Notes |
|-----------|---------|---------|-------|-------|
| Memory Management Mapping | 9 | 9 | 0 | No change. Page lifecycle and flush pipeline are well-documented. The SafeReadOnlyAddress clarification helps Rust implementors understand which boundary to check for mutable vs immutable access. |
| Concurrency Model Mapping | 8 | 9 | +1 | The transient bucket locking protocol is now fully documented, including the critical insight that find + lock must be atomic (not sequential). The latch spin counts and failure semantics (return false, caller retries entire operation) are explicitly specified. The `drainCount` ordering invariant in the epoch drain list is now clear. These details are essential for a correct Rust implementation. |
| Generic Monomorphization | 10 | 10 | 0 | No change. Rust's native monomorphization remains a perfect fit. |
| Lock-free Data Structures | 9 | 9 | 0 | No substantive change. The CAS protocols were already well-documented; the latch failure semantics add precision. |
| Unsafe Code Requirements | 7 | 8 | +1 | The RCU protocol correction (seal old record AFTER CAS, not before) reduces the complexity of the unsafe code surface. With the correct ordering, there is no window where both old and new records are simultaneously accessible in conflicting states. The transient bucket locking also simplifies the unsafe reasoning -- while holding the bucket lock, no concurrent modification to that key's record chain can occur. This enables a cleaner safe abstraction boundary. |
| Cache-line Alignment | 10 | 10 | 0 | No change. |
| I/O and Storage | 6 | 6 | 0 | No change. The I/O device abstraction remains undocumented. Recovery protocol is improved but the device layer is still a gap. |
| Performance Parity | 9 | 9 | 0 | No change. |

**New Average: 8.75/10** (up from 8.5)

---

### Evaluation of the Feasibility Document (16-feasibility-rust-vs-kotlin.md)

The feasibility document accurately represents the Rust storage engine challenges and advantages. Specific evaluation:

**Accurately represented:**

1. **D1 (Memory Layout, Rust 5/5)**: Correct. `#[repr(C, packed)]`, `#[repr(C, align(64))]`, and `zerocopy` are the right tools. The docs now confirm that record layout, RecordInfo bitfields, and SpanByte all map naturally to Rust's memory model.

2. **D2 (Concurrency, Rust 5/5)**: Correct. The revised docs now fully specify the transient locking protocol, which maps to `AtomicU64` CAS operations with Acquire/Release ordering. The epoch system maps to `crossbeam-utils::CachePadded` + `thread_local!`. The feasibility doc correctly identifies `parking_lot::Mutex` for spin-then-park locks.

3. **D3 (Monomorphization, Rust 5/5)**: Correct. The `ISessionFunctions` callback trait with monomorphized dispatch is the primary hot-path optimization, and Rust handles this identically to C#.

4. **D5 (Storage Engine Core, Rust 5/5)**: Correct. The revised operation flows in doc 06 Section 2.12 confirm that every CRUD operation follows a pattern (acquire transient lock, traverse chain, check CPR, operate, release lock) that maps cleanly to Rust's scoped-guard pattern.

**Areas where the feasibility doc could be more precise:**

1. **Transient locking complexity not fully reflected**: The feasibility doc mentions CAS-based entry manipulation but does not highlight the combined find-and-lock operations (`FindTagAndTryTransientSLock`, `FindOrCreateTagAndTryTransientXLock`). In Rust, these would need to be implemented as a single atomic operation to prevent TOCTOU races. This is achievable (the latch is encoded in the overflow entry's upper bits, and the tag search is a separate non-atomic scan) but the coupling between latch acquisition and tag lookup adds complexity. The feasibility doc should note this.

2. **CPR consistency checks not mentioned**: The feasibility doc does not discuss CPR (Concurrent Prefix Recovery) version checks, which are now documented in detail. In Rust, every CRUD operation must check the system state phase and version, and handle `CPR_SHIFT_DETECTED` by returning a retry. This adds a branch to every hot-path operation. It is not a difficulty for Rust per se (it is identical logic in any language), but it is a correctness concern that the feasibility doc should acknowledge.

3. **Fuzzy region handling for RMW**: The doc 06 revision documents a subtle lost-update anomaly in the fuzzy region between SafeReadOnlyAddress and ReadOnlyAddress, where RMW must return `RETRY_LATER`. This is a correctness requirement that any reimplementation must handle. The feasibility doc does not mention it. In Rust, the check is a simple address comparison, but getting the boundary wrong would cause silent data corruption.

4. **RCU ordering correction**: The feasibility doc states that RCU involves "sealing the old record" then creating a new one. The revised docs (doc 06 Section 2.7) correct this: the old record is sealed/invalidated AFTER the new record is CAS-ed into the chain, not before. This ordering matters because it means the old record remains accessible (valid, not sealed) until the new record is installed. In Rust, this is a sequencing concern in the unsafe code -- the old record must not be invalidated until after the CAS succeeds.

5. **Search depth asymmetry**: Upsert searches only to ReadOnlyAddress (shallow search), while RMW and Delete search to HeadAddress (deep search). This means Upsert can create duplicate records in the chain when an older version exists in the read-only region. The feasibility doc does not mention this asymmetry, but it has implications for hash chain length management in a Rust implementation.

6. **Unsafe surface area question (Open Question 1)**: The feasibility doc asks "What percentage of the Rust codebase would require unsafe?" Based on the revised documentation, I can now give a more precise estimate. The storage engine core (docs 02-06) constitutes the unsafe-heavy portion. Within that:
   - Hybrid log allocator: ~60% unsafe (page allocation, pointer arithmetic, atomic tail allocation)
   - Hash index: ~70% unsafe (raw pointer bucket access, CAS on packed entries, latch protocol)
   - Epoch system: ~50% unsafe (cache-aligned table access, thread-local storage, fence operations)
   - Record management: ~40% unsafe (record access via page pointers, RecordInfo CAS, SpanByte offset arithmetic)
   - Checkpointing: ~20% unsafe (mostly safe state machine logic, with unsafe only for I/O buffer management)

   Overall, approximately 45-50% of the storage engine code would contain `unsafe` blocks. However, the revised docs make clear that the unsafe surface can be organized into a small number of encapsulated abstractions: `Page`, `RecordRef`, `BucketRef`, `EpochGuard`. The operation-level code (InternalRead, InternalUpsert, etc.) can be written in safe Rust using these abstractions.

---

### Remaining Gaps

Gaps from Round 5 that are now RESOLVED:

1. ~~**Operation flow details**~~: Fully resolved by doc 06 Section 2.12. The CRUD operation flows now specify transient locking, CPR checks, region-based dispatch, and retry semantics.
2. ~~**RCU protocol ordering**~~: Corrected in doc 06 Section 2.7.
3. ~~**Recovery fuzzy region handling**~~: Expanded in doc 05 Section 2.11 with backwards scan and SetInvalid semantics.
4. ~~**TryAllocate return codes**~~: Documented in doc 02 Section 2.5.

Gaps that PERSIST:

1. **I/O Device Abstraction (High)**: The `IDevice` interface, segment management, file naming, and async I/O completion model remain undocumented. This is the single largest remaining gap for a complete reimplementation.

2. **Memory Ordering Specification (Medium)**: The docs still acknowledge x86 TSO dependence without specifying per-operation ordering requirements. The transient locking protocol adds new ordering concerns: the latch acquisition must be visible before the tag scan to prevent reading stale entries. On x86 this is automatic; on ARM it requires explicit barriers.

3. **ReadCache Subsystem (Medium)**: Doc 06 Section 2.12 now mentions the read cache fast path in InternalRead, but the ReadCache allocator, eviction policy, and interaction with the main log remain undocumented.

4. **Session Lifecycle (Low-Medium)**: `ClientSession`, `ISessionFunctions`, and the relationship between sessions and epoch protection are referenced but not documented in a dedicated section.

5. **Error Handling (Low-Medium)**: What happens on I/O failure during flush? On CAS failure in `CASRecordIntoChain`? The docs describe the happy path well but error/retry paths are not exhaustively specified.

---

### Risk Analysis (Updated)

**Reduced risks since Round 5:**

1. **Concurrency correctness** (reduced from Medium to Low-Medium): The transient bucket locking protocol and CPR checks are now well-specified. A Rust implementation can follow the documented protocol directly. The latch protocol's failure semantics (return false, retry entire operation) map to Rust's `Result` type naturally.

2. **Epoch-lifetime bridging** (reduced from High to Medium): The operation flows in doc 06 Section 2.12 make clear that epoch protection is acquired at the beginning of each session operation and released at the end. The transient lock is held for the operation duration. This scoped pattern maps well to Rust's RAII guards: an `EpochGuard` acquired via `epoch.protect()` that drops at scope exit, and a `BucketLock<'a>` that holds a reference to the bucket and releases the latch on drop.

**Persistent risks:**

1. **I/O layer design (Medium-High)**: Without the device abstraction documentation, the Rust implementation must either reverse-engineer the C# I/O layer or design its own. The former risks compatibility issues; the latter risks correctness issues (especially around segment management and recovery).

2. **Unsafe code volume (Medium)**: The ~45-50% unsafe estimate is manageable but requires disciplined encapsulation. The key risk is that bugs in the unsafe core (e.g., incorrect offset arithmetic in record access, missing barriers in latch protocol) would be undetectable by the compiler. Mitigation: `loom` for concurrency testing, `miri` for UB detection, `cargo-fuzz` for input fuzzing.

3. **Async/epoch interaction (Medium)**: Both C# and Rust struggle with epoch protection in async contexts. The docs note that epoch must be suspended before `await` and resumed after. In Rust with `tokio`, this means the I/O path (page flush, async read from disk) must suspend epoch protection before yielding. This is a correctness invariant that cannot be enforced at compile time.

---

### Recommended Rust Crate Updates

No changes from Round 5 recommendations. The `loom` crate is now more strongly recommended given the additional concurrency complexity from transient locking and CPR checks:

| Subsystem | Crate | Purpose |
|-----------|-------|---------|
| **Concurrency Testing** | `loom` | Model-check the transient locking protocol, CAS retry loops, and epoch drain ordering under all possible thread interleavings |
| **UB Detection** | `miri` | Run under Miri to detect undefined behavior in unsafe record access, pointer arithmetic, and packed struct manipulation |

All other crate recommendations from Round 5 remain valid.

---

### Final Verdict

The documentation has reached a level of quality where a Rust reimplementation of the Tsavorite storage engine is feasible without reading C# source code, with one exception: the I/O device layer. The revised operation flows (doc 06 Section 2.12) are the most critical improvement -- they connect the individual subsystem descriptions into coherent end-to-end behavior specifications.

The feasibility document (16-feasibility-rust-vs-kotlin.md) accurately identifies Rust as the superior language for the storage engine core. Its scores (D1: 5/5, D2: 5/5, D3: 5/5, D5: 5/5) are well-justified by the documentation. The document should be updated to reflect the transient locking protocol, CPR consistency checks, and RCU ordering correction, but these additions would reinforce rather than change the overall recommendation.

**Final scores: Average 8.75/10, Coverage A-.**
