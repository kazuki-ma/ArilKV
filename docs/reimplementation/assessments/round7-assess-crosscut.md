# Round 7: Final Cross-Cutting Feasibility Assessment
## Assessor: assess-crosscut
## Date: Round 7 (post-revision)

---

## 1. Feasibility Document (Doc 16) Review

### 1.1 Overall Assessment: Strong

The feasibility document is well-structured with a clear 10-dimension scoring framework. The analysis correctly identifies the critical dimensions (D1, D3, D5) where Garnet's architecture most strongly favors Rust. The hybrid approach recommendation in Section 4.4 is thoughtful and practical.

### 1.2 Accuracy Issues

**Issue F1: D4 (Async I/O) gives Kotlin/JVM 4/5 -- this is slightly generous.**
The document correctly notes Netty's maturity but underweights the challenge of integrating Netty with a custom off-heap storage engine. Garnet's design assumes the I/O layer passes raw byte pointers directly to the parser and storage layer (zero-copy non-TLS path, doc 07). On the JVM, Netty's `ByteBuf` must be converted to `ByteBuffer` or offset-based access patterns, adding a translation layer. Additionally, the TLS state machine (doc 07 Section 2.5) with its three states (Rest/Active/Waiting) maps more naturally to Rust's async/await than to Netty's pipeline model. A score of 3.5/5 would be more accurate.

**Issue F2: D5 (Storage Engine) Rust score of 5/5 is slightly optimistic.**
While Rust's language features are a strong fit, the feasibility document doesn't adequately acknowledge the sheer complexity of the storage engine. Doc 06 Section 2.12 (rewritten since Round 5) reveals that every operation involves transient bucket locking, CPR consistency checks, read-cache fast paths, fuzzy region retry logic for RMW, and record chain traversal with validity filtering. Porting this requires not just language features but deep understanding of the invariants. A 4.5/5 acknowledging implementation complexity (vs language fit) would be more honest.

**Issue F3: The hybrid approach (Section 4.4) underestimates FFI cost.**
The document acknowledges FFI overhead but dismisses it with "could be mitigated by batching." For Garnet's architecture, EVERY key-value operation crosses the storage boundary -- there is no natural batching point except at the TCP receive level (which is already how Garnet works). The FFI boundary would need to be at the session level (batch of parsed commands -> Rust storage engine -> batch of responses), which requires serializing/deserializing `ArgSlice` arrays and `SpanByteAndMemory` outputs across the boundary. This is a substantial engineering challenge that should be explicitly called out.

**Issue F4: Missing dimension -- Testing and Correctness Verification.**
The 10-dimension framework covers development and performance but omits testing infrastructure. Garnet has complex correctness invariants (epoch safety, CPR consistency, fuzzy region filtering, transaction isolation). A reimplementation needs extensive testing. Rust's type system helps prevent some bugs but cannot encode epoch invariants. The JVM has better fuzzing and property-testing ecosystems (jqwik, QuickCheck ports).

**Issue F5: Section 6 Open Question 1 -- unsafe surface area estimate is too vague.**
The document asks "what percentage of the Rust codebase would require unsafe?" but doesn't attempt an estimate. Based on the 15 documents: the hybrid log (doc 02), hash index (doc 03), epoch framework (doc 04), record management (doc 06), RESP parser (doc 08), and network I/O (doc 07) all require extensive unsafe code. The session functions (doc 10) and data structures (doc 11) can be mostly safe. Estimate: 40-50% of the Rust codebase would be in `unsafe` blocks or within modules that wrap unsafe operations behind safe interfaces.

### 1.3 Completeness Issues

**Missing from feasibility doc:**
1. **Memory overhead comparison**: No estimate of memory efficiency per language. Rust can match C#'s memory layout. JVM adds ~16 bytes per object header + padding. For a system storing millions of small records, this compounds to significant overhead.
2. **Warm-up characteristics**: Rust has no warm-up (compiled ahead of time). JVM has JIT warm-up that affects p99 latency for the first minutes of operation. C# has similar JIT characteristics. This is relevant for the comparison.
3. **Debugging and profiling story**: Rust's debugging tools (gdb, lldb, perf) are capable but less ergonomic than JVM's (async-profiler, JFR, jconsole). This affects ongoing operational cost.
4. **Community/hiring considerations**: Not a technical dimension but practically important for organizations choosing a reimplementation language.

---

## 2. Terminology Inconsistency Resolution Status

Re-evaluating the 9 inconsistencies I identified in Round 5:

| # | Inconsistency | Status | Details |
|---|--------------|--------|---------|
| 1 | "Phantom" VectorContext in Doc 01 vs full instantiation in Docs 09, 15 | **PARTIALLY FIXED** | Doc 01 now says "TVectorContext is a phantom type parameter used for compile-time constraint checking only -- the constructor does not accept a vector context, and vector operations route through StorageSession/VectorManager directly" (line 151). Doc 09 line 97 echoes this. Doc 15 still shows all three `BasicContext` instantiations in the type alias. The docs are now **internally consistent**: the type parameter exists in the generic signature, but the runtime struct holds only two contexts. The term "phantom" is technically accurate for the runtime layout. **Resolved.** |
| 2 | ReadOnlyAddress vs SafeReadOnlyAddress usage in Doc 06 | **FIXED** | Doc 02 line 59 now explicitly states "The Read path uses SafeReadOnlyAddress (not ReadOnlyAddress) as its mutable/immutable boundary." Doc 06 Section 2.12 now distinguishes per-operation boundaries: Read uses SafeReadOnlyAddress, Upsert/RMW/Delete use ReadOnlyAddress. The fuzzy region between SafeReadOnlyAddress and ReadOnlyAddress is explicitly documented with its RETRY_LATER behavior for RMW. **Resolved.** |
| 3 | Unqualified "epoch" in Doc 01 | **PARTIALLY FIXED** | Doc 01 line 155 now says "The cluster session acquires/releases its own epoch independently of the storage-layer epoch used by Tsavorite operations." Open Question #2 (line 199) also distinguishes "cluster epoch" from "Tsavorite storage epoch." However, the main text in Section 2.1 (line 19) still says just "epoch framework (LightEpoch)" without qualification. Minor remaining issue -- acceptable since the later sections disambiguate. **Mostly resolved.** |
| 4 | ArgSlice vs SpanByte unserialized form in Doc 08 | **NOT FIXED** | Doc 08 still describes ArgSlice as "pointer+length pairs" (line 64) without connecting to SpanByte's unserialized form. Doc 09 line 98 now notes `key.SpanByte` "constructs a new SpanByte struct on the stack from the ArgSlice's ptr and length" but the connection is in Doc 09, not Doc 08 where the reader first encounters ArgSlice. **Unfixed, minor.** |
| 5 | "Mutable region" consistency | **CONSISTENT** | Already consistent in Round 5, confirmed still consistent. |
| 6 | Unqualified "session" | **NOT FIXED** | The docs still use "session" ambiguously. Doc 01 uses "session" for both RespServerSession (line 127) and Tsavorite session contexts. Doc 10 uses "session" for StorageSession. No explicit "RESP session" vs "storage session" qualification has been added. **Unfixed, minor** -- context usually makes meaning clear. |
| 7 | Dual-store vs triple-store | **FIXED** | Doc 01 line 48 shows VectorStore in the component diagram. Doc 10 line 9 mentions "vector store" alongside main and object stores. Doc 15 shows all three BasicContext instantiations. The triple-store architecture is now consistently acknowledged. **Resolved.** |
| 8 | LockableContext not detailed in Doc 10 | **NOT FIXED** | Doc 10 mentions LockableContext in Section 1 ("holding BasicContext and LockableContext pairs for both stores") but does not explain how session function callbacks differ when invoked through LockableContext vs BasicContext. This matters for transactions: under LockableContext, the callbacks execute with bucket locks already held, changing the concurrency semantics. **Unfixed, moderate.** |
| 9 | "Fuzzy region" dual meaning (checkpoint vs AOF) | **IMPROVED** | Doc 05 now uses "fuzzy region" for the log range `[startLogicalAddress, finalLogicalAddress)`. Doc 14 uses "fuzzy region" for the AOF window between CheckpointStartCommit and CheckpointEndCommit markers. Doc 06 introduces a THIRD use: the "fuzzy region" between SafeReadOnlyAddress and ReadOnlyAddress in the hybrid log. Doc 14 line 129 now explicitly says "The fuzzy region is the window between CheckpointStartCommit and CheckpointEndCommit markers" which helps localize the meaning. However, the triple use of "fuzzy region" for three related-but-distinct concepts could still confuse a reimplementor. **Improved but still a minor risk.** |

**Summary: 4 fully resolved, 2 mostly resolved, 3 unfixed (all minor to moderate).**

---

## 3. Remaining Cross-Document Gaps Affecting Reimplementation

### 3.1 Gaps Fixed Since Round 5

1. **Doc 06 Section 2.12 (Operation Flows)**: Now thoroughly documents transient bucket locking, CPR consistency checks, read-cache fast paths, fuzzy region RETRY_LATER for RMW, and per-operation search depth. This was the most significant gap and is now well-covered.
2. **Doc 13 Gossip Algorithm**: The failure-budget rewrite is a critical correction. The old description (contact exactly N nodes) was fundamentally wrong about the algorithm's behavior (contact all fresh nodes, stopping only after N failures).
3. **Doc 12 GarnetWatchApi**: Now correctly documented as `IGarnetReadApi` only (read-only interceptor), not full `IGarnetApi`.
4. **Doc 02 SafeReadOnlyAddress**: Now includes the Read-vs-Write boundary distinction.

### 3.2 Gaps Still Present

**Gap G1 (High Impact): DatabaseManager / Multi-Database Architecture**
Still undocumented. Doc 01 mentions `DatabaseManager` in the component diagram. Doc 14 mentions per-database AOF and `SwitchActiveDatabaseContext`. But no document explains:
- How multiple databases are created and managed
- How SELECT switches the active database context
- How per-database isolation is achieved in the storage layer
- How cross-database operations (FLUSHALL, SWAPDB) work

This gap affects reimplementation because the database architecture touches the storage session, AOF, and cluster layers simultaneously.

**Gap G2 (High Impact): Pub/Sub Broker**
Still completely undocumented. Doc 01 line 143 now adds that ServerSessionBase has "abstract methods `Publish` and `PatternPublish` for pub/sub", which is new and useful context. But the broker itself -- message delivery, subscription management, channel matching, memory model -- is absent. SUBSCRIBE/PUBLISH are core Redis features.

**Gap G3 (Medium Impact): ACL / Authentication**
Still undocumented. Required for production deployment.

**Gap G4 (Medium Impact): Read Cache**
Doc 06 Section 2.12 (Read operation) now mentions the read-cache fast path: "If the hash entry points to the read cache (`stackCtx.hei.IsReadCache`), call `FindInReadCache` first." This is new since Round 5 and documents the interaction point. However, the read cache's overall lifecycle (enablement, promotion policy, eviction, interaction with main log page eviction) remains undocumented.

**Gap G5 (Medium Impact): Key Expiration / Background Sweep**
No document describes proactive key expiration. The docs describe lazy expiration (check on access) in docs 10 and 11. Hash and SortedSet have per-element lazy cleanup with HCOLLECT/ZCOLLECT internal operations (doc 11). But how and when these internal collection operations are triggered (background timer? cron? manual?) is not documented.

**Gap G6 (Low Impact): Blocking Commands**
Doc 11 lists BLPOP/BRPOP/BLMOVE as ListObject operations but the blocking coordination mechanism (session-level wait, timeout, wake-on-push) is not documented.

**Gap G7 (Low Impact): Configuration / Startup / Shutdown**
Still undocumented. Doc 01 provides the component hierarchy but not the initialization sequence, configuration validation, or graceful shutdown procedure.

**Gap G8 (Low Impact): VectorSet / DiskANN**
Docs 10, 15 reference VectorManager and VectorSessionFunctions. Doc 06 mentions VectorSet bit in RecordInfo. But no dedicated document covers the DiskANN indexing algorithm, vector store creation, or search semantics. However, VectorSet is a newer feature and may be lower priority for an initial reimplementation.

---

## 4. Final Scores

### Rust Full-Stack Score: 7.5/10 (Confidence: Medium-High)

**Unchanged from Round 5.** The document revisions (especially doc 06 Section 2.12) have revealed more complexity in the storage engine operations (transient locking, CPR checks, read-cache integration, fuzzy region retry) but this complexity exists regardless of the implementation language. Rust remains well-suited for the core storage engine:

- **Strengths**: Native monomorphization, explicit memory layout, strong atomics, zero-cost abstractions, no GC pauses. The epoch framework, hybrid log, and hash index map directly to Rust primitives.
- **Primary risk**: The unsafe surface area (~40-50% of storage engine code) is large. Bugs in unsafe Rust are as dangerous as bugs in C. The CPR consistency checks and fuzzy region logic documented in doc 06 are intricate invariants that `unsafe` code must maintain.
- **Secondary risk**: Async/epoch interaction. Epoch protection is thread-bound (doc 04). Rust's async runtime can migrate tasks between threads. This requires careful scoping of epoch guards, similar to the pattern Garnet uses for C# async (suspend epoch before await, resume after).
- **Score rationale**: 7.5 reflects "feasible with significant effort." Rust is the best language choice for matching Garnet's performance characteristics, but the storage engine complexity is substantial regardless of language.

### Kotlin/JVM Full-Stack Score: 5.5/10 (Confidence: Medium)

**Unchanged from Round 5.** The document revisions reinforce the JVM's challenges:

- **Strengths**: Rapid development for application-layer features (data structures, transactions, cluster, AOF). Mature ecosystem. Object store (doc 11) maps naturally.
- **Primary risk**: The storage engine (docs 02-06) requires extensive off-heap programming via `Unsafe`/`ByteBuffer`/`VarHandle`. Doc 06 Section 2.12's rewritten operation flows show that every single operation involves: (1) bucket latch acquisition on a cache-line-aligned bucket, (2) atomic CAS on 64-bit packed entries, (3) record traversal with bitfield checks on RecordInfo, (4) CPR version comparisons, (5) in-place mutation or RCU on variable-length SpanByte records. All of this must be implemented with off-heap primitives on the JVM.
- **Performance ceiling**: No monomorphization means every `ISessionFunctions` callback goes through virtual dispatch. For a system that processes millions of operations per second, the 25-100ns per-operation overhead from virtual dispatch alone (doc 15) limits throughput to 30-50% of C#/Rust.
- **Score rationale**: 5.5 reflects "feasible but with fundamental performance limitations." A JVM reimplementation can achieve functional correctness but cannot match Garnet's performance targets.

### Feasibility Doc (Doc 16) Score: 8/10

Strong analysis with correct overall conclusions. The recommendation of Rust as primary language is well-justified. Deductions for: slightly generous JVM networking score (F1), missing testing dimension (F4), underestimated FFI cost for hybrid approach (F3), and no memory overhead or warm-up analysis (1.3).

---

## 5. Summary of Changes Since Round 5

| Area | Round 5 Finding | Round 7 Status |
|------|----------------|----------------|
| Doc 06 operation flows | "referenced but not fully documented" | **Completely rewritten** with transient locking, CPR, read-cache, fuzzy region |
| Doc 13 gossip algorithm | Described as sample-based "contact N nodes" | **Corrected** to failure-budget "contact all fresh, stop after N failures" |
| Doc 12 GarnetWatchApi | Incorrectly implied full IGarnetApi | **Corrected** to IGarnetReadApi (read-only) |
| VectorStore phantom context | Inconsistent "phantom" vs "real" | **Clarified** consistently across docs 01, 09, 15 |
| SafeReadOnlyAddress boundaries | Not distinguished per-operation | **Clarified** in docs 02 and 06 with per-operation boundary table |
| Terminology (9 issues) | 9 inconsistencies identified | 4 fully resolved, 2 mostly resolved, 3 unfixed (minor) |
| Missing subsystems | 10 gaps identified | 1 partially addressed (read cache fast path in doc 06), 9 still missing |
| Feasibility doc (Doc 16) | Did not exist | **New**, scored 8/10 |

---

## 6. Final Recommendations

### For Doc 16 (Feasibility):
1. Adjust D4 (Async I/O) Kotlin score from 4/5 to 3.5/5
2. Adjust D5 (Storage Engine) Rust score from 5/5 to 4.5/5 to acknowledge implementation complexity
3. Add a testing/correctness dimension (D11)
4. Add memory overhead and warm-up analysis to Section 4
5. Quantify the unsafe surface area estimate in Open Question 1 (~40-50%)
6. Expand FFI cost analysis in Section 4.4

### For the documentation set:
1. **Priority 1**: Write a DatabaseManager / Multi-Database document (Gap G1). This affects storage sessions, AOF, and cluster.
2. **Priority 2**: Write a Pub/Sub Broker document (Gap G2). Core Redis feature.
3. **Priority 3**: Document key expiration background sweep mechanism (Gap G5).
4. **Low priority**: Resolve remaining 3 terminology inconsistencies (#4 ArgSlice/SpanByte, #6 session qualification, #8 LockableContext in doc 10).

### For reimplementation planning:
1. The storage engine (docs 02-06) should be prototyped first regardless of language choice. It is the highest-risk, highest-complexity component.
2. Budget 30% more time than estimated for the storage engine -- doc 06's revised Section 2.12 reveals correctness invariants (CPR checks, fuzzy region retry, read-cache interaction, transient locking) that are easy to get wrong.
3. Invest heavily in testing: property-based tests for epoch safety, fuzz testing for RESP parsing, stress tests for concurrent record operations.
