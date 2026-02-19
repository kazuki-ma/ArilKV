# Garnet Reimplementation Documentation -- Master Index

This is the master index for the Garnet reimplementation documentation suite. These 17 documents provide a comprehensive technical analysis of Microsoft Garnet's architecture, internals, and feasibility assessment for reimplementation in Rust and Kotlin/JVM.

**Total word count**: ~40,763 words across 16 technical documents plus this index.

---

## 1. Document Index

| # | Title | Description | Key Subsystems | Est. Reading Time |
|---|-------|-------------|----------------|-------------------|
| [00](./00-index.md) | Master Index | Navigation, glossary, dependency graph, coverage matrix, and reading guides | All | 10 min |
| [01](./01-architecture-overview.md) | Architecture Overview | Top-level system architecture, component diagram, data flow from TCP accept to response send, key interfaces | GarnetServer, StoreWrapper, RespServerSession, GarnetApi | 13 min |
| [02](./02-tsavorite-hybrid-log.md) | Tsavorite Hybrid Log Allocator | Circular buffer page management, logical address space, tail allocation, page flush and eviction | AllocatorBase, SpanByteAllocator, PageUnit, OverflowPool | 10 min |
| [03](./03-tsavorite-hash-index.md) | Tsavorite Hash Index | Lock-free cache-line-aligned hash table, tag-based lookup, overflow buckets, online resizing | HashBucket, HashBucketEntry, TsavoriteBase, MallocFixedPageSize | 10 min |
| [04](./04-tsavorite-epoch-and-concurrency.md) | Tsavorite Epoch Protection & Concurrency | LightEpoch framework, epoch table, acquire/release lifecycle, deferred action drain list | LightEpoch, Metadata, Entry, EpochActionPair | 10 min |
| [05](./05-tsavorite-checkpointing.md) | Tsavorite Checkpointing & Recovery | State machine framework, fold-over/snapshot/incremental checkpoints, recovery protocol | StateMachineDriver, SystemState, HybridLogCheckpointSMTask, Recovery | 10 min |
| [06](./06-tsavorite-record-management.md) | Tsavorite Record Management | RecordInfo bit layout, SpanByte format, IPU vs RCU, revivification, compaction, end-to-end CRUD flows | RecordInfo, SpanByte, RevivificationManager, InternalRead/Upsert/RMW/Delete | 18 min |
| [07](./07-network-layer.md) | Network Layer | SAEA-based async I/O, connection lifecycle, buffer pools, send throttle, TLS integration | GarnetServerTcp, NetworkHandler, GarnetTcpNetworkSender, LimitedFixedBufferPool | 12 min |
| [08](./08-resp-parser.md) | RESP Parser | Zero-allocation pointer-based parsing, SessionParseState, ArgSlice, command dispatch table | RespReadUtils, SessionParseState, RespCommand, RespWriteUtils | 12 min |
| [09](./09-request-lifecycle.md) | Request Lifecycle | Complete GET/SET/INCR traces from TCP receive to response send, pipelining, scatter-gather | RespServerSession, BasicCommands, StorageSession, MainSessionFunctions | 14 min |
| [10](./10-storage-session-functions.md) | Storage Session Functions | ISessionFunctions callback implementation, RMW phases, ETag state machine, AOF logging strategy | MainSessionFunctions, ObjectSessionFunctions, FunctionsState, EtagState | 13 min |
| [11](./11-data-structures-objects.md) | Data Structures & Object Store | IGarnetObject interface, Hash/List/Set/SortedSet objects, serialization state machine, per-field expiry | HashObject, ListObject, SetObject, SortedSetObject, GarnetObjectBase | 11 min |
| [12](./12-transactions-and-scripting.md) | Transactions & Scripting | MULTI/EXEC, sorted lock acquisition, WatchVersionMap, stored procedures (RUNTXP) | TransactionManager, TxnKeyEntries, WatchVersionMap, LockableContext | 12 min |
| [13](./13-cluster-and-replication.md) | Cluster & Replication | ClusterConfig copy-on-write, gossip protocol, replication sync, failover | ClusterConfig, Gossip, ReplicationManager, FailoverManager | 12 min |
| [14](./14-aof-and-durability.md) | AOF & Durability | Append-only file format, AofHeader, replay engine, fuzzy region handling, transaction replay | AofHeader, AofProcessor, AofReplayCoordinator, TsavoriteLog | 14 min |
| [15](./15-performance-engineering.md) | Performance Engineering | Catalog of all optimization techniques: monomorphization, cache-line alignment, zero-allocation, lock-free | All performance-critical subsystems | 22 min |
| [16](./16-feasibility-rust-vs-kotlin.md) | Feasibility: Rust vs Kotlin/JVM | 10-dimension scoring comparison, key findings, hybrid approach, recommendation | Cross-cutting assessment | 11 min |

---

## 2. Recommended Reading Order

### Sequential Path (Cover-to-Cover)

For a complete understanding, read in document order (01 through 16). The documents are structured in layers:

1. **Architecture** (01): System overview and component relationships
2. **Storage Engine** (02-06): Bottom-up through hybrid log, hash index, epoch, checkpointing, record management
3. **Server/Network** (07-09): Network I/O, RESP parsing, request lifecycle
4. **Application Layer** (10-14): Session functions, data structures, transactions, cluster, AOF
5. **Cross-Cutting** (15-16): Performance techniques catalog and feasibility assessment

### Quick Start Path (Architecture-First)

For engineers wanting to understand the architecture quickly before diving into details:

| Step | Document | Why |
|------|----------|-----|
| 1 | [01 -- Architecture Overview](./01-architecture-overview.md) | Component diagram, data flow, key abstractions |
| 2 | [15 -- Performance Engineering](./15-performance-engineering.md) | All optimization techniques in one place; establishes the "why" behind design decisions |
| 3 | [09 -- Request Lifecycle](./09-request-lifecycle.md) | End-to-end GET/SET/INCR traces connecting all layers |
| 4 | [02 -- Tsavorite Hybrid Log](./02-tsavorite-hybrid-log.md) | Core storage data structure |
| 5 | [06 -- Tsavorite Record Management](./06-tsavorite-record-management.md) | Record format and CRUD operation flows that tie docs 02-05 together |
| 6 | [16 -- Feasibility Assessment](./16-feasibility-rust-vs-kotlin.md) | Language comparison and recommendation |

After the quick start, fill in the remaining documents as needed: 03 (hash index), 04 (epoch), 05 (checkpointing), 07 (network), 08 (parser), 10 (session functions), 11 (data structures), 12 (transactions), 13 (cluster), 14 (AOF).

---

## 3. Document Dependency Graph

The following ASCII diagram shows which documents reference or depend on other documents. An arrow `A --> B` means "A references B" or "understanding A requires familiarity with B."

```
                        +-------+
                        |  01   |  Architecture Overview
                        +---+---+
                            |
            +---------------+---------------+
            |               |               |
            v               v               v
        +-------+       +-------+       +-------+
        |  02   |       |  07   |       |  15   |
        | Hybrid|       |Network|       | Perf  |
        |  Log  |       | Layer |       | Eng   |
        +---+---+       +---+---+       +---+---+
            |               |               |
     +------+------+        v          references
     |      |      |    +-------+      02-10,15
     v      v      v    |  08   |
 +-----+ +-----+ +-----+  RESP |
 | 03  | | 04  | | 06  | Parser|
 |Hash | |Epoch| |Rec  +---+---+
 |Index| |Prot.| |Mgmt |   |
 +--+--+ +--+--+ +--+--+   v
    |        |       |  +-------+
    |        |       +->|  09   |
    |        |       |  |Request|
    +--------+-------+  |Lifecy.|
    |        |       |  +---+---+
    |        v       |      |
    |    +-------+   |      v
    +--->|  05   |   |  +-------+
         |Chkpt &|   +->|  10   |
         |Recov. |      |Session|
         +-------+      |Funcs. |
                        +---+---+
                            |
              +-------------+-------------+
              |             |             |
              v             v             v
          +-------+     +-------+     +-------+
          |  11   |     |  12   |     |  14   |
          | Data  |     | Trans.|     |  AOF  |
          |Structs|     |Script.|     |Durabi.|
          +-------+     +---+---+     +---+---+
                            |             |
                            v             v
                        +-------+     +-------+
                        |  13   |     |  13   |
                        |Cluster|<----|Cluster|
                        | Repl. |     | Repl. |
                        +-------+     +-------+

                        +-------+
                        |  16   |  Feasibility Assessment
                        +-------+  (references all 01-15)
```

**Key dependency chains:**
- **Storage engine**: 02 --> 03, 04, 06 --> 05 (understanding the hybrid log is prerequisite for hash index, epoch, and record management)
- **Request processing**: 07 --> 08 --> 09 --> 10 (network to parser to lifecycle to session functions)
- **Durability**: 10 --> 14 --> 13 (session functions write AOF, AOF streams to cluster replicas)
- **Transactions**: 03 + 04 + 06 --> 12 (hash index locking, epoch coordination, record ops feed into transaction system)
- **Cross-cutting**: 15 references 02-10 (performance techniques span all layers); 16 references all 01-15

---

## 4. Glossary

### Storage Engine Terms

| Term | Definition | Primary Doc |
|------|-----------|-------------|
| **Tsavorite** | Garnet's embedded key-value storage engine (fork of Microsoft FASTER). Implements hybrid log, hash index, epoch protection, and checkpointing. | 02-06 |
| **Hybrid Log** | Tsavorite's central data structure: a circular buffer of memory pages that seamlessly tiers between RAM and SSD. Recent records are mutable in memory; older records are flushed to disk and become immutable. | 02 |
| **Epoch** | A monotonically increasing counter used for safe memory reclamation and global synchronization. Threads "protect" themselves at a given epoch; deferred actions execute when all threads advance past a target epoch. | 04 |
| **LightEpoch** | Tsavorite's epoch-based reclamation framework. Maintains a cache-line-aligned table of per-thread epoch entries, supporting acquire/release/drain operations. | 04 |
| **SpanByte** | Variable-length byte sequence with a 4-byte inline length header (29 bits length + 3 flag bits). Exists in serialized (inline data) and unserialized (pointer to external memory) forms. | 06 |
| **RecordInfo** | 8-byte (64-bit) record header packed into a single `long`. Contains PreviousAddress (48 bits), Tombstone, Valid, Sealed, ETag, Dirty, Filler, InNewVersion, Modified, and VectorSet flags. | 06 |
| **ArgSlice** | 12-byte struct (8-byte pointer + 4-byte length) referencing argument data in the receive buffer. Zero-copy -- no string allocation on the hot path. | 08 |
| **HashBucket** | 64-byte (one cache line) hash table bucket containing 7 data entries + 1 overflow/latch entry. Each entry is 8 bytes with tag, address, tentative, and pending fields. | 03 |

### Address Space Terms

| Term | Definition | Primary Doc |
|------|-----------|-------------|
| **TailAddress** | Next allocation point in the hybrid log. Derived from the lock-free `TailPageOffset`. | 02 |
| **ReadOnlyAddress** | Boundary between mutable and read-only in-memory regions. Records above this can be updated in-place (IPU). | 02 |
| **SafeReadOnlyAddress** | Lags behind ReadOnlyAddress. Updated after all threads acknowledge the shift. Used by Read operations as the mutable/immutable boundary. | 02 |
| **HeadAddress** | Lowest in-memory address. Records below this are only on disk. | 02 |
| **SafeHeadAddress** | Leads ClosedUntilAddress. Set by `OnPagesClosed` as it begins closing a range of pages. | 02 |
| **BeginAddress** | Lowest valid address in the log. Records below are truncated. | 02 |

### Operation Terms

| Term | Definition | Primary Doc |
|------|-----------|-------------|
| **RMW (Read-Modify-Write)** | Tsavorite's atomic update operation. Attempts in-place update first (IPU), falls back to copy-update (RCU). Proceeds through NeedInitialUpdate, InitialUpdater, InPlaceUpdater, NeedCopyUpdate, CopyUpdater phases. | 06, 10 |
| **IPU (In-Place Update)** | Updating a record directly in the mutable region of the hybrid log without allocating a new record. | 06 |
| **RCU (Read-Copy-Update)** | Creating a new record at the log tail with the updated value, then CAS-updating the hash index to point to it. Used when the existing record is in the read-only or on-disk region. | 06 |
| **CAS (Compare-and-Swap)** | Atomic operation that updates a memory location only if it contains an expected value. Used extensively for lock-free hash index updates, RecordInfo flag manipulation, and epoch table entry acquisition. | 03, 04, 06 |
| **CPR (Concurrent Prefix Recovery)** | Consistency checks during checkpointing. A thread in version V encountering a V+1 record retries; a V+1 thread encountering a V record forces RCU. | 05, 06 |

### Network and Protocol Terms

| Term | Definition | Primary Doc |
|------|-----------|-------------|
| **SAEA (SocketAsyncEventArgs)** | .NET's high-performance async socket I/O pattern. Reuses event argument objects to avoid per-operation allocation. | 07 |
| **RESP (Redis Serialization Protocol)** | The text-based protocol used by Redis clients. Commands are arrays of bulk strings (`*N\r\n$L\r\ndata\r\n`). | 08 |

### Concurrency and Performance Terms

| Term | Definition | Primary Doc |
|------|-----------|-------------|
| **Monomorphization** | Compiler technique where generic code is specialized for each concrete type instantiation, producing distinct native code. Eliminates virtual dispatch. C# JIT and Rust both do this; JVM does not due to type erasure. | 01, 15, 16 |
| **Type erasure** | JVM generic implementation where type parameters are replaced by `Object` at runtime, preventing monomorphization and requiring boxing for primitive types. | 01, 16 |
| **Zero-copy** | Processing data without copying it between buffers. Garnet achieves this by parsing RESP directly from the receive buffer (ArgSlice pointers) and writing responses directly to the send buffer. | 07, 08 |
| **Fuzzy region (address)** | The address range between `SafeReadOnlyAddress` and `ReadOnlyAddress` in the hybrid log. RMW operations return RETRY_LATER in this region to prevent lost-update anomalies. | 02, 06 |
| **Fuzzy region (checkpoint)** | The log range `[startLogicalAddress, finalLogicalAddress)` during checkpointing where records from both version v and v+1 coexist. Recovery must handle this range specially. | 05, 14 |
| **Transient locking** | Bucket-level latch acquired for the duration of a single operation (Read, Upsert, RMW, Delete). Uses shared (read) or exclusive (write) locks encoded in the overflow entry of the hash bucket. | 03, 06 |
| **Phantom type parameter** | A generic type parameter used for compile-time constraint checking but not stored at runtime. `GarnetApi<TContext, TObjectContext, TVectorContext>` has `TVectorContext` as a phantom parameter -- no corresponding field exists. | 01 |

### Checkpoint and Durability Terms

| Term | Definition | Primary Doc |
|------|-----------|-------------|
| **Fold-over** | Simplest checkpoint strategy. Shifts ReadOnlyAddress to the tail, flushing all in-memory data. Subsequent updates require RCU until the mutable region refills. | 05 |
| **Snapshot** | Checkpoint strategy that writes in-memory pages to a separate file without advancing ReadOnlyAddress. In-place updates continue uninterrupted. | 05 |
| **Revivification** | Reuse of deleted (tombstoned) record slots to reduce log growth. A size-segregated free record pool tracks available slots. | 06 |
| **AOF (Append-Only File)** | Write-ahead log recording every mutation as a structured binary entry. Used for crash recovery (replay from last checkpoint) and replication (streaming to replicas). | 14 |
| **Gossip protocol** | Cluster protocol for propagating configuration changes across nodes. Supports broadcast and sample-based modes with a failure-budget sampling algorithm. | 13 |
| **Slot migration** | Process of moving hash slot ownership from one cluster node to another, with MIGRATING/IMPORTING intermediate states. | 13 |

---

## 5. Source Directory Coverage Matrix

| Source Directory | Primary Doc | Secondary Docs | Coverage |
|-----------------|-------------|----------------|----------|
| `libs/storage/Tsavorite/cs/src/core/Allocator/` | 02 | 06 | High |
| `libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/` | 03 | 06, 15 | High |
| `libs/storage/Tsavorite/cs/src/core/Epochs/` | 04 | 05, 15 | High |
| `libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/` | 05 | 04 | High |
| `libs/storage/Tsavorite/cs/src/core/Index/Recovery/` | 05 | 14 | Medium |
| `libs/storage/Tsavorite/cs/src/core/VarLen/` | 06 | 02, 15 | High |
| `libs/storage/Tsavorite/cs/src/core/Index/Common/` | 06 | 03, 15 | High |
| `libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/` | 06 | 09, 10 | High |
| `libs/storage/Tsavorite/cs/src/core/Compaction/` | 06 | -- | Medium |
| `libs/common/Networking/` | 07 | 09, 15 | High |
| `libs/common/Memory/` | 07 | 15 | High |
| `libs/common/` (RespReadUtils, RespWriteUtils) | 08 | 09, 15 | High |
| `libs/server/Servers/` | 07 | 01 | High |
| `libs/server/Sessions/` | 01 | 09 | Medium |
| `libs/server/Resp/` | 08, 09 | 15 | High |
| `libs/server/Resp/Parser/` | 08 | 15 | High |
| `libs/server/Storage/Session/` | 09 | 10 | High |
| `libs/server/Storage/Functions/MainStore/` | 10 | 09, 15 | High |
| `libs/server/Storage/Functions/ObjectStore/` | 10 | 11 | High |
| `libs/server/Storage/Functions/` (FunctionsState, EtagState) | 10 | -- | High |
| `libs/server/Objects/` | 11 | 10 | High |
| `libs/server/Transaction/` | 12 | 10, 14 | High |
| `libs/cluster/Server/` | 13 | -- | High |
| `libs/cluster/Server/Gossip/` | 13 | -- | High |
| `libs/cluster/Server/Replication/` | 13 | 14 | High |
| `libs/cluster/Server/Migration/` | 13 | -- | Medium |
| `libs/cluster/Server/Failover/` | 13 | -- | Medium |
| `libs/server/AOF/` | 14 | 10, 13 | High |
| `libs/server/API/` | 01 | 09 | Medium |
| `libs/server/StoreWrapper.cs` | 01 | -- | Medium |
| `libs/host/` | 01 | -- | Low |
| `main/GarnetServer/` | 01 | -- | Low |

### Known Gaps

The following subsystems are mentioned but not deeply covered in dedicated documents:

- **Pub/Sub**: Mentioned in the architecture overview (01) as part of StoreWrapper but not covered in a dedicated document. The pub/sub broker, channel subscriptions, and pattern matching are undocumented.
- **ACL (Access Control Lists)**: Mentioned in StoreWrapper (01) but authentication and authorization internals are not covered.
- **Read Cache**: Mentioned in docs 02, 03, and 06 as an optional allocator mode. The read cache allocation strategy, eviction policy, and integration with the hash index are not fully documented.
- **DatabaseManager**: Referenced in docs 01 and 14. Multi-database support (SELECT command), per-database store instances, and cross-database operations are not detailed.
- **VectorSet / DiskANN**: Mentioned in docs 01, 06, 10, and 15. The DiskANN index, vector similarity search, and the third Tsavorite store instance are referenced but not covered in a dedicated document.
- **Config / Startup**: Server configuration parsing, option validation, and startup orchestration in `libs/host/` are mentioned in doc 01 but not detailed.
- **Custom Commands**: The extensibility framework for custom commands and stored procedures is mentioned in docs 10-12 but the registration, dispatch, and lifecycle are not fully documented.
- **Client Library**: `libs/client/` is listed in the project map (01) but not covered.

---

## 6. Feasibility Summary

From [Document 16 -- Feasibility Assessment: Rust vs Kotlin/JVM](./16-feasibility-rust-vs-kotlin.md):

| Dimension | Rust | Kotlin/JVM | Delta |
|-----------|------|------------|-------|
| D1: Memory Layout & Zero-Copy | 5 | 2 | +3 Rust |
| D2: Concurrency Primitives | 5 | 3 | +2 Rust |
| D3: Generics & Monomorphization | 5 | 2 | +3 Rust |
| D4: Async I/O & Networking | 4 | 4 | 0 |
| D5: Storage Engine Core | 5 | 2 | +3 Rust |
| D6: Data Structures | 4 | 4 | 0 |
| D7: Transaction System | 4 | 3 | +1 Rust |
| D8: AOF & Durability | 4 | 3 | +1 Rust |
| D9: Cluster & Replication | 4 | 4 | 0 |
| D10: Ecosystem & Operations | 3 | 5 | +2 Kotlin |
| **Weighted Total** | **43** | **32** | **+11 Rust** |

**Recommendation**: Rust is the recommended primary language for a full reimplementation. The three most architecturally demanding subsystems (hybrid log, hash index, session function callbacks) all require low-level memory control that Rust provides natively.

**Hybrid approach**: For organizations with strong JVM expertise, a Kotlin application layer with a Rust storage engine is a viable alternative that balances development velocity with performance requirements.

---

## 7. Document History

These documents were developed through 10 rounds of iterative development:

| Rounds | Phase | Description |
|--------|-------|-------------|
| 1-3 | Initial drafts + structural review + first revision | Three writer agents produced the initial 16 documents in parallel (storage engine, server/network, application layer). Three reviewer agents performed structural review. Writers revised based on feedback. |
| 4-6 | Technical accuracy review + feasibility pre-assessment + second revision | Reviewer agents performed line-by-line technical accuracy review. Five assessor agents pre-assessed Rust and Kotlin feasibility across storage, application, and cross-cutting dimensions. Writers incorporated all feedback. |
| 7-8 | Full feasibility assessment + integration editing | Dedicated assessor agent produced the comprehensive feasibility document (16). Integration editor ensured consistent terminology, cross-references, and section structure across all 16 documents. |
| 9-10 | Final adversarial review + polish | Adversarial reviewers challenged technical claims, verified source references, and stress-tested the mapping notes. Final polish pass for consistency, this index document, and known-gap documentation. |
