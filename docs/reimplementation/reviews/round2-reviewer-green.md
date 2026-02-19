# Round 2 Review: Application Layer Documents (10-14)
## Reviewer: reviewer-green

---

### Document: 10-storage-session-functions.md

#### Critical Issues (must fix)

- [C1] **Missing source files from file map.** The actual `libs/server/Storage/Functions/MainStore/` directory contains `VarLenInputMethods.cs`, `CallbackMethods.cs`, and `VectorSessionFunctions.cs` in addition to the files listed. `VarLenInputMethods.cs` implements `GetLength` callbacks that use `etagOffsetForVarlen` -- an essential piece of the ETag allocation story that is described in Section 2.7 but never connected to the file that implements it. `CallbackMethods.cs` handles `CheckpointCompletionCallback` and other lifecycle callbacks. `VectorSessionFunctions.cs` implements the separate vector store session functions. These are needed for reimplementation.

- [C2] **Incomplete ObjectSessionFunctions coverage.** Section 2 devotes subsections 2.2-2.8 to `MainSessionFunctions` but only a brief subsection 2.9 to `ObjectSessionFunctions`. However, the object store function files include `ReadMethods.cs`, `UpsertMethods.cs`, `DeleteMethods.cs`, `PrivateMethods.cs`, `VarLenInputMethods.cs`, and `CallbackMethods.cs` in addition to `ObjectSessionFunctions.cs` and `RMWMethods.cs`. The object store write/read/delete paths have their own unique patterns (e.g., expiry checking against `Expiration` property, size tracking via `objectStoreSizeTracker`, `CopyUpdate` serialization state machine interaction). These are not described at all. A reimplementor would not know how the object store read path checks expiry, or how the object store delete path interacts with the `CacheSizeTracker`.

#### Major Issues (should fix)

- [M1] **Section 2.3 claims "RecordInfo.ETag flag is cleared on expired records"** but the actual code in `ReadMethods.cs` should be verified against the exact mechanism. The document references `ReadMethods.cs:L16-L113` and `L116-L213` but does not specify how `RecordInfo` ETag flags are managed. The RecordInfo struct lives in Tsavorite, not in Garnet, so a reimplementor would need to understand this cross-layer interaction. The document should clarify whether the ETag clearing happens via RecordInfo mutation or via the session function returning false.

- [M2] **Section 2.5 RMW Phase descriptions reference specific line numbers that may drift.** The document references `RMWMethods.cs:L25-L76`, `L79-L321`, `L339-L937`, `L1075-L1499`. While these are helpful snapshots, the document does not explain the `RMWAction` enum (`ExpireAndResume`, `ExpireAndStop`) or how Tsavorite uses the return values to drive the state machine. An engineer reimplementing this needs to understand the `IPUResult` and `RMWAction` contracts between the storage engine and the session functions. This should be explicit rather than implied.

- [M3] **AOF serialization format details are thin.** Section 2.8 says AOF writes call `WriteLogRMW` / `WriteLogUpsert` / `WriteLogDelete` but does not describe the actual wire format of these entries (what bytes are serialized in what order). This information is essential for reimplementation and for compatibility with Document 14 (AOF & Durability). Document 14 covers the header and high-level payload types but the actual serialization of `RawStringInput` and `ObjectInput` is not described in either document.

#### Minor Issues (nice to fix)

- [m1] **Section 2.1 says `VectorManager vectorManager`** but does not explain what the VectorManager does or how it interacts with session functions. A brief note would help orient readers.

- [m2] **Section 2.2 describes `NeedAofLog = 0x1` stored in `UserData`** but does not mention that `UserData` is a single-byte field on `RMWInfo`/`UpsertInfo`, not an arbitrary user data store. This matters for understanding the protocol.

- [m3] **Section 2.6 on Delete path is very short.** The VectorSet cancellation behavior is mentioned but the conditions under which a normal (non-VectorSet) delete proceeds are not clearly stated. The watch version increment on delete should be explicitly described.

#### Strengths

- Excellent depth on the RMW path with all 6 phases clearly delineated.
- The ETag state machine (Section 2.7) is well-described with the reset discipline warning.
- Rust and Kotlin mapping notes are practical and specific.
- The deferred AOF logging pattern is clearly explained.

---

### Document: 11-data-structures-objects.md

#### Critical Issues (must fix)

- [C1] **GarnetObjectType enum values are wrong for PExpire.** Section 2.3 claims `PExpire=0xfb` but the actual code in `GarnetObjectType.cs` shows `PExpire = 0xf8` (line 44). The document also lists `Expire=0xff, Persist=0xfd, TTL=0xfe, PExpire=0xfb` but the actual values are: `DelIfExpIm=0xf7, PExpire=0xf8, ExpireTime=0xf9, PExpireTime=0xfa, All=0xfb, PTtl=0xfc, Persist=0xfd, Ttl=0xfe, Expire=0xff`. The document omits `DelIfExpIm` (0xf7), `ExpireTime` (0xf9), `PExpireTime` (0xfa), `All` (0xfb which is NOT PExpire), and `PTtl` (0xfc). This is factually incorrect and would cause a reimplementation to use wrong dispatch codes.

- [C2] **SetObject has no per-element expiration.** Section 2.6 does not mention this explicitly, which is correct -- but Section 2.7 for SortedSetObject states per-element expiration exists. The problem is the document does not mention that SetObject does NOT have per-element expiration, while HashObject and SortedSetObject do. This asymmetry is important for a reimplementor to understand. More critically, the SetObject also has `SUNIONSTORE`, `SDIFFSTORE`, and `SINTERSTORE` operations (confirmed in `SetObject.cs:L31-L36`) that are not listed in Section 2.6's operations list.

- [C3] **ListObject operations list is incomplete.** Section 2.5 lists `LPUSH, RPUSH, LPOP, RPOP, LLEN, LTRIM, LRANGE, LINDEX, LINSERT, LREM, LSET, LPOS` but the actual `ListOperation` enum (from `ListObject.cs`) also includes `LPUSHX`, `RPUSHX`, `RPOPLPUSH`, `LMOVE`, `BRPOP`, and `BLPOP`. These are significant operations -- especially the blocking operations `BRPOP`/`BLPOP` and the atomic `RPOPLPUSH`/`LMOVE` which have non-trivial implementation requirements.

#### Major Issues (should fix)

- [M1] **Section 2.2 serialization state machine description has inaccuracies.** The document says "If state is `REST`, transition to `SERIALIZING`, serialize directly to the writer via `DoSerialize()`, return to `REST`." This is correct. But it then says "If state is `SERIALIZED`, a cached `byte[] serialized` array exists from a prior `CopyUpdate` -- write it directly." Looking at the actual code (`GarnetObjectBase.cs:L58-L73`), when state is `SERIALIZED`, the code checks if `serialized` is not null and writes it, otherwise writes `GarnetObjectType.Null`. The document does not mention the null case. Additionally, the description of CopyUpdate for the `isInNewVersion == false` case says "null out the old value, since the clone is the only live copy" but the actual code (`GarnetObjectBase.cs:L87-L101`) transitions to SERIALIZED state first (not nulling the serialized data) and sets `oldValue = null`. The document should clarify that transitioning to SERIALIZED state for the `!isInNewVersion` case is about preventing further mutations to the old object, not about caching serialized data.

- [M2] **Missing HCOLLECT/ZCOLLECT operations.** The `HashOperation` enum includes `HCOLLECT` and `SortedSetOperation` includes `ZCOLLECT`. These are internal garbage collection operations for expired elements that are dispatched through `Operate()`. They are not mentioned in the document. A reimplementor needs to know about this mechanism for triggering lazy cleanup from outside the object.

- [M3] **SortedSetObject operations list is incomplete.** Section 2.7 lists many operations but misses `ZREMRANGEBYLEX`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZDIFF`, `ZEXPIRE`, `ZTTL`, `ZPERSIST`, `ZCOLLECT` from the `SortedSetOperation` enum. Several of these (ZEXPIRE, ZTTL, ZPERSIST) are essential for the per-element expiration feature that the document does describe.

- [M4] **Object store source file map is missing files.** The actual `ObjectStore/` directory contains `CallbackMethods.cs`, `PrivateMethods.cs`, `ReadMethods.cs`, `UpsertMethods.cs`, `DeleteMethods.cs`, and `VarLenInputMethods.cs` in addition to the two files listed. These implement the full ISessionFunctions callback chain for the object store.

#### Minor Issues (nice to fix)

- [m1] **HashObject serialization format description uses "OR'd" terminology.** Section 2.4 says the key length is `OR'd with ExpirationBitMask`. More precisely, the ExpirationBitMask is `1 << 31` (the sign bit of the int), confirmed in `HashObject.cs:L67`. The document should state this explicitly so a reimplementor knows to use bit 31.

- [m2] **Section 2.9 says "Creates a new object via `GarnetObject.Create(type)` factory method"** but no such static factory method exists in the codebase. The actual code in `ObjectSessionFunctions` creates objects by calling the appropriate constructor (e.g., `new SortedSetObject()`, `new ListObject()`) or uses the `CustomCommandManager` for custom types. This is a minor factual error.

- [m3] **SortedSetComparer details are not provided.** Section 2.7 mentions `SortedSetComparer` for score-then-lexicographic ordering but does not describe the actual comparison logic, which is important for byte-level tie-breaking behavior. The tie-breaking uses `ByteArrayComparer` for element comparison.

#### Strengths

- The dual-store architecture overview is clear and well-motivated.
- GarnetObjectBase serialization state machine is thoroughly described.
- Size tracking formulas are precise and include alignment calculations.
- Per-field expiration mechanism is well-explained for HashObject.

---

### Document: 12-transactions-and-scripting.md

#### Critical Issues (must fix)

- [C1] **Unlock ordering claim is incorrect.** Section 2.4 states "UnlockAllKeys: Unlocks in reverse order (object store first, then main store)". However, examining `TxnKeyEntry.cs:L177-L189`, `UnlockAllKeys()` unlocks main store keys first (`keys.AsSpan()[..mainKeyCount]`), then object store keys (`keys.AsSpan().Slice(mainKeyCount, keyCount - mainKeyCount)`). This is the SAME order as locking, not reverse order. The actual code at lines 180-183 shows: first `lockableContext.Unlock` for main store, then `objectStoreLockableContext.Unlock` for object store. This is a factual error that could mislead a reimplementor about the locking protocol.

- [C2] **Section 2.4 claims "Main store keys sort before object store keys"** in the sort comparator. This needs verification. The `TxnKeyComparison` class (referenced in source file map as `TxnKeyComparison.cs`) contains the actual comparison logic. The document does not show the comparison delegate, so the claim about main-before-object ordering is unverified and potentially wrong. Since locking locks main store first and then object store, the sort must indeed group by store type, but the document should provide the actual comparison logic to enable reimplementation.

#### Major Issues (should fix)

- [M1] **Missing LockKeys detail during command queuing.** Section 2.3 says "Calls `txnManager.LockKeys(commandInfo)` which inspects the command's key specification to determine which keys to lock and with what lock type." But looking at the `TransactionManager`, the key addition happens through `AddKey` on `TxnKeyEntries`. The document does not describe how the command's key specification is parsed (which arguments are keys, which are values) or how the lock type (Shared vs Exclusive) is determined from the command info. This is a non-trivial piece of logic needed for reimplementation.

- [M2] **Section 2.6 EXEC description lists step ordering but key details are imprecise.** Step 4d says "Calls `keyEntries.LockAllKeys()` to acquire sorted locks" but does not mention that sorting happens INSIDE `LockAllKeys()` (confirmed in `TxnKeyEntry.cs:L120`), not as a separate step. This is important because the comment in the code explains that sort must happen with a stable hash table (not in GROW phase), which is why it's inside the transaction flow.

- [M3] **Section 2.9 Stored Procedures is missing the `GarnetWatchApi` description.** The document mentions three API wrappers but does not describe `GarnetWatchApi` -- the one used in Prepare phase that intercepts key accesses to build the lock set and watch list. This is a critical piece: the Prepare phase API wrapper must convert every key access into a `Watch` + `AddKey` call on the `TransactionManager`. Without understanding this, a reimplementor cannot implement the Prepare phase correctly.

- [M4] **Transaction buffer replay mechanism is underspecified.** Section 2.6 step 1 mentions resetting `endReadHead` to `txnStartHead` but does not explain how the RESP processor knows to replay commands. The actual mechanism involves setting the read head back so the main processing loop re-parses the commands. The document should clarify this buffer-rewind-and-replay pattern, including how the RESP processor state (e.g., `state == Running`) affects command dispatch.

#### Minor Issues (nice to fix)

- [m1] **Section 2.4 says TxnKeyEntry is "10-byte layout"** but the `StructLayout` attribute specifies `Size = 10`. The actual memory footprint may differ due to alignment if not pinned. The document correctly mentions pinning in Section 4 but should note that the explicit `Size = 10` attribute is what enforces the layout.

- [m2] **Section 2.4 says the array uses "doubling strategy" for growth** but the actual code (`TxnKeyEntry.cs:L96-L101`) creates a new array with `keys.Length * 2` and uses `Array.Copy` -- it does NOT use `GC.AllocateArray` with `pinned: true` for the grown array. Only the initial allocation is pinned. This means after growth, the array is no longer pinned. This is a potential issue worth noting.

- [m3] **`TxnKeyComparison.cs` is listed in the source file map but its contents are not described.** The comparison delegate is essential for understanding the lock ordering protocol.

#### Strengths

- The transaction state machine (Section 2.1) is clearly described with state transitions.
- The WatchVersionMap mechanism (Section 2.5) is thoroughly explained including false-positive analysis.
- The Commit/Reset flow (Section 2.7) correctly describes the cleanup sequence.
- Performance notes about pinned arrays and buffer replay are insightful.

---

### Document: 13-cluster-and-replication.md

#### Critical Issues (must fix)

- [C1] **Gossip implementation is in `ClusterManager` (partial class), NOT in a standalone `Gossip.cs` class.** The document repeatedly references `[Source: libs/cluster/Server/Gossip/Gossip.cs:L16-L100]` but the actual gossip code lives in `libs/cluster/Server/Gossip/Gossip.cs` which is a file within the `ClusterManager` partial class (line 16: `internal sealed partial class ClusterManager`). The document's Section 2.4 says "The gossip system is implemented in the `ClusterManager` class (partial class spanning multiple files)" which is correct, but the source references suggest there is a separate `Gossip` class. This inconsistency should be resolved.

- [C2] **Gossip sample-based selection algorithm is described incorrectly.** Section 2.4 says the sample method "picks `maxRandomNodesToPoll` (3) random nodes, selects the one with the oldest last-gossip timestamp." Looking at the actual code in `Gossip.cs:L396-L443`, `GossipSampleSend` first calculates a fraction of total nodes based on `GossipSamplePercent`, then for each slot in that fraction, it picks `maxRandomNodesToPoll` random nodes and selects the one with the earliest `GossipSend` timestamp. This is NOT a simple "pick 3, choose oldest" -- it's a loop that selects `count` nodes total using power-of-2 choices per selection. The document mischaracterizes this as a single selection.

- [C3] **RecoveryStatus enum is incomplete.** Section 2.5 lists `NoRecovery`, `ReadRole`, `CheckpointRecoveredAtReplica`, and "Various intermediate states." The actual `RecoveryStatus` enum (`RecoveryStatus.cs`) has: `NoRecovery`, `InitializeRecover`, `ClusterReplicate`, `ClusterFailover`, `ReplicaOfNoOne`, `CheckpointRecoveredAtReplica`, `ReadRole`. The "intermediate states" are concrete named states that correspond to specific recovery scenarios. A reimplementor needs to know all of them: `InitializeRecover` (startup recovery), `ClusterReplicate` (becoming a replica), `ClusterFailover` (failover recovery), `ReplicaOfNoOne` (promoting to standalone).

#### Major Issues (should fix)

- [M1] **Section 2.5 says ReplicationManager contains `ReplicationSyncManager replicationSyncManager`** but looking at `ReplicationManager.cs:L26`, it's `replicationSyncManager` -- confirmed. However, the document does not describe what `ReplicationSyncManager` actually does. It coordinates the multi-step sync process, including sending/receiving checkpoint files and managing the AOF streaming lifecycle. Without this, the sync protocol description in Section 2.6 is incomplete.

- [M2] **Section 2.5 says "Primary: `ReplicationOffset` returns `appendOnlyFile.TailAddress`"** but the actual code (`ReplicationManager.cs:L52-L65`) shows the primary returns `storeWrapper.appendOnlyFile.TailAddress` only if `EnableAOF` is true AND `TailAddress > kFirstValidAofAddress`. Otherwise it returns `kFirstValidAofAddress`. The `kFirstValidAofAddress` constant is not defined in the visible portion but is clearly used as a minimum offset. This fallback is important for understanding replication offset semantics.

- [M3] **Failover description (Section 2.7) is thin.** It says "A replica promotes itself to primary by updating its local config and broadcasting the change" but does not describe the `TakeOverFromPrimary` method (`ClusterConfig.cs:L1252-L1269`) which assigns all of the primary's slots to the local node and sets role to PRIMARY. The actual failover also involves `ReplicaFailoverSession` (in `libs/cluster/Server/Failover/`) which is not mentioned at all. The failover process has voting, epoch bumping, and slot reassignment phases that are important for correctness.

- [M4] **Section 2.8 `IClusterProvider` interface description is too brief.** The listed methods (`IsClusterEnabled`, `GetClusterConfig()`, `ValidateSlot(...)`, `GetReplicationManager()`) are a small subset of the actual interface. The provider also handles authentication, session management, cluster command processing, and migration coordination. The document should at least list the key additional methods relevant to reimplementation.

- [M5] **Missing source files.** The `libs/cluster/Server/Failover/` directory is not mentioned. `libs/cluster/Server/ClusterManagerWorkerState.cs` (handles config epoch management, FORGET, RESET) is not in the file map. `libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs`, `GarnetServerNode.cs` are not listed but are essential for the gossip transport layer.

#### Minor Issues (nice to fix)

- [m1] **Section 2.1 says `Copy()` does "a shallow copy of both arrays"** using `Array.Copy`. This is correct for the slotMap (value type elements) and workers (struct elements). But Worker is a struct with string fields (reference types), so the "shallow" copy shares the string references. This is fine for the immutable pattern but should be noted.

- [m2] **Section 4 says "the 16384-element array is ~49KB"** but `HashSlot` uses `LayoutKind.Explicit` without specifying `Size`. With fields at offset 0 (ushort, 2 bytes) and offset 2 (byte, 1 byte), the minimum size is 3 bytes, but without `Size = 3`, the runtime may pad to 4 bytes. If 4 bytes, the array is ~64KB, not ~49KB. The document should verify this.

- [m3] **`TryMerge` in `Gossip.cs` uses a copy-then-merge-then-CAS pattern** that is described in Section 2.4 as "CAS-based config update: Read current config, Call `currentConfig.Merge(incomingConfig)`, CAS the new config." But the actual code (`Gossip.cs:L123-L132`) does `current.Copy()` first, then `currentCopy.Merge(senderConfig, workerBanList, logger).HandleConfigEpochCollision(senderConfig, logger)`, then CAS. The Copy step is important and should be explicit in the document.

#### Strengths

- Clear description of the copy-on-write pattern for ClusterConfig.
- HashSlot struct layout is precisely described with field offsets.
- The three pillars framing (ClusterConfig, Gossip, ReplicationManager) provides good structure.
- Slot verification semantics (MOVED, ASK, ASKING) are correctly described.

---

### Document: 14-aof-and-durability.md

#### Critical Issues (must fix)

- [C1] **Source file `AofProcessor.ReplayOps.cs` does NOT exist.** Section 3 (Source File Map) lists `libs/server/AOF/AofProcessor.ReplayOps.cs` as "Individual store operation replay methods" but this file does not exist. The replay operation methods (`StoreUpsert`, `StoreRMW`, `StoreDelete`, `ObjectStoreRMW`, `ObjectStoreUpsert`, `ObjectStoreDelete`) are all defined within `AofProcessor.cs` itself. This is a factual error that would send a reimplementor looking for a file that doesn't exist.

- [C2] **Section 2.1 AofHeader field overlay description is inaccurate for `databaseId`.** The document says `databaseId` at offset 3 overlays `procedureId`. This is correct per the code (`AofHeader.cs:L55-L56`). However, the document does not adequately explain that these overlays mean `unsafeTruncateLog` and `padding` share the same byte (offset 1), and `databaseId` and `procedureId` share the same byte (offset 3). For the FLUSH entries, the `procedureId` field contains the database ID. This aliasing is subtle and crucial for correctly parsing AOF entries. The document should make explicit that for `FlushDb` entries, `header.databaseId` IS `header.procedureId` (they occupy the same byte).

#### Major Issues (should fix)

- [M1] **Section 2.4 AofProcessor description says construction "Creates a dedicated `RespServerSession`"** but looking at the actual constructor (`AofProcessor.cs:L60-L81`), it creates a `RespServerSession` through an `obtainServerSession` lambda and also creates an `AofReplayCoordinator`. The constructor also takes a `recordToAof` parameter that controls whether the replay session records to AOF (for replication chain scenarios). This is important because a replica that receives AOF entries may need to re-record them for its own replicas. The document mentions this parenthetically but does not explain the mechanism.

- [M2] **Section 2.4 Recovery flow description does not match actual code.** The document says "Scans the AOF from `BeginAddress` to `untilAddress`" but the actual `Recover` method (`AofProcessor.cs:L102-L188`) first switches the active database context, then scans the specific database's AOF. The document does not describe the multi-database recovery flow: the `Recover` method takes a `GarnetDatabase db` parameter and recovers that specific database's AOF. This per-database recovery model is not described.

- [M3] **Section 2.6 Transaction Replay Coordination says "Two modes: Recovery (not as replica): Replays operations directly without locking. Replica replay: Uses `TransactionManager` with full key locking."** This is a high-level claim but the actual implementation in `AofReplayCoordinator` is more complex. The `TransactionGroup.cs` file in the ReplayCoordinator directory handles the buffering and replay logic. The document should reference this file and explain how session IDs are used to group interleaved transactions.

- [M4] **Section 2.9 AOF Payload Format is incomplete.** For `StoreUpsert`, the document says `[key SpanByte][value SpanByte]` but examining the actual `StoreUpsert` replay code (`AofProcessor.cs:L359-L379`), after key and value SpanBytes, there is also a serialized `RawStringInput` that is deserialized via `storeInput.DeserializeFrom(curr)`. This means the actual format is `[key SpanByte][value SpanByte][RawStringInput]`, not just `[key SpanByte][value SpanByte]`. This is a significant omission for format compatibility.

#### Minor Issues (nice to fix)

- [m1] **Section 2.2 says `AofStoreType` has `ReplicationType = 0x3`** which matches the code. But the document does not explain which `AofEntryType` values map to `ReplicationType`. Looking at the `ToAofStoreType` method in `AofProcessor.cs:L551-L563`, there is NO entry type that maps to `ReplicationType`. This enum value appears unused in the dispatch logic, which is worth noting.

- [m2] **Section 2.5 Fuzzy Region Handling says "Operations during the fuzzy region are buffered (not immediately replayed)."** The actual logic is more nuanced: the `SkipRecord` method (`AofProcessor.cs:L509-L564`) only buffers records with versions NEWER than the current store version (via `IsNewVersionRecord`). Records with old versions are simply skipped. Only new-version records are buffered for post-checkpoint replay. The document should distinguish between "skipped" and "buffered."

- [m3] **The `AofReplayContext` and `TransactionGroup` files in `libs/server/AOF/ReplayCoordinator/` are not listed in the source file map.** Only `AofReplayCoordinator.cs` is listed. These additional files contain the replay context state and transaction group management.

- [m4] **Section 2.7 Checkpoint Integration mentions `StateMachineDriver` coordinates periodic checkpoints** but does not reference the source file for this component or explain how it relates to the AOF safe address tracking. This cross-reference to Document 10 and the Tsavorite engine documents would help.

#### Strengths

- The AofHeader struct layout is precisely described with byte offsets.
- AofEntryType enum values are accurately listed and categorized.
- The fuzzy region concept is well-explained at a high level.
- The dual-use design (recovery + replication) is clearly motivated.
- The zero-copy header reading pattern is well-described in performance notes.

---

### Cross-Document Issues

- [X1] **AOF serialization format is split across documents 10 and 14 but neither is complete.** Document 10 describes WHEN AOF entries are written (deferred via NeedAofLog flag). Document 14 describes the header format and high-level payload types. But the actual byte-level serialization of `RawStringInput`, `ObjectInput`, and `CustomProcedureInput` payloads is not described in either document. A reimplementor needs this for format compatibility.

- [X2] **Object store session functions span documents 10 and 11 without clear boundary.** Document 10 briefly covers `ObjectSessionFunctions` in Section 2.9 and points to Document 11 for object type details. Document 11 covers the object types but not the session function callbacks in detail. The full object store callback chain (InitialUpdater -> InPlaceUpdater -> CopyUpdater for each operation) is not described in either document.

- [X3] **Stored procedure execution spans documents 12 and 14 without consistent detail.** Document 12 describes the stored procedure execution model with three phases (Prepare/Main/Finalize) and the three API wrappers. Document 14 describes AOF logging of stored procedures. But neither document fully describes how `CustomProcedureInput` is serialized/deserialized, or how the procedure registry maps IDs to implementations during replay.

- [X4] **Replication and AOF overlap between documents 13 and 14 needs cross-references.** Document 13 briefly describes AOF-based replication. Document 14 describes fuzzy regions and checkpoint markers. The streaming checkpoint markers (`MainStoreStreamingCheckpointStartCommit`, etc.) are described in Document 14's enum listing but their usage during diskless replication (Document 13) is not cross-referenced.

- [X5] **The `GarnetObjectType` enum inaccuracy in Document 11 (C1) would propagate to any reimplementor also reading Documents 10 and 12.** The object store RMW path (Document 10, Section 2.9) dispatches through these type values. If the enum values are wrong, the entire object store dispatch would be broken.

---

### Summary

| Document | Critical | Major | Minor |
|----------|----------|-------|-------|
| 10 - Storage Session Functions | 2 | 3 | 3 |
| 11 - Data Structures & Objects | 3 | 4 | 3 |
| 12 - Transactions & Scripting | 2 | 4 | 3 |
| 13 - Cluster & Replication | 3 | 5 | 3 |
| 14 - AOF & Durability | 2 | 4 | 4 |
| Cross-Document | - | - | 5 |
| **Total** | **12** | **20** | **21** |

**Overall Assessment:** The documents demonstrate strong architectural understanding and provide good structural overviews. The writing quality is high, with clear explanations of design patterns (copy-on-write, deferred AOF, serialization state machine). However, there are significant factual errors (GarnetObjectType enum values, unlock ordering, non-existent source file) that would directly mislead reimplementors. The most systemic weakness is incomplete source file coverage -- each document's file map misses 2-6 files, and the detailed description of code paths often stops at the main store, leaving object store paths under-documented. The cross-document boundaries for AOF format, stored procedures, and object store session functions need explicit cross-references to avoid gaps. After fixing the critical issues, these documents would be solid foundations for reimplementation.
