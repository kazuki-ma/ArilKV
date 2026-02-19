# Cross-Cutting Feasibility Assessment
## Assessor: assess-crosscut

---

## Document Coherence Rating: B+

The 15 documents form a remarkably coherent body of work. The terminology is largely consistent, the cross-references are accurate, and the layered structure (storage engine -> networking -> application) maps clearly to Garnet's architecture. The grade is not an A because of several terminology inconsistencies and a few interface gaps documented below.

---

## Terminology Inconsistencies

| Term | Doc X usage | Doc Y usage | Recommended |
|------|------------|------------|-------------|
| "BasicContext" type parameters | Doc 01 says `GarnetApi` has "three type parameters but only stores two context instances at runtime (main and object); the `TVectorContext` is a phantom type parameter" | Docs 09, 10, 15 show `BasicGarnetApi` with three full `BasicContext` instantiations (main, object, vector), clearly not phantom | Clarify in Doc 01 that the vector context exists as a real type parameter and field; remove "phantom" claim |
| "ReadOnlyAddress" vs "SafeReadOnlyAddress" | Doc 02 clearly distinguishes these as separate pointers with a lag | Doc 06 Section 2.7 references "read-only region" without specifying which address boundary | Consistently use "ReadOnlyAddress" when referring to the boundary and "read-only region" for the zone between HeadAddress and ReadOnlyAddress |
| "Epoch" (singular) | Doc 01 refers to "epoch-based concurrency" generically | Docs 04, 13 distinguish between Tsavorite storage epoch and cluster epoch as separate LightEpoch instances | Always qualify: "storage epoch" or "cluster epoch" |
| "SpanByte" serialized vs unserialized | Doc 06 describes both forms with clear flag semantics | Doc 08 refers to ArgSlice as "pointer+length pairs" without connecting to SpanByte's unserialized form | Doc 08 should note ArgSlice is conceptually similar to SpanByte's unserialized form |
| "Mutable region" | Doc 02 defines as addresses >= ReadOnlyAddress | Doc 06 uses "mutable region" in Section 2.7 consistently | Consistent -- no action needed |
| "Session" | Doc 01, 07 use "session" for network-level RespServerSession | Doc 04, 10 use "session" for Tsavorite ClientSession/BasicContext | Qualify as "RESP session" vs "Tsavorite session" or "storage session" |
| "Store" (dual vs triple) | Doc 01, 10, 11 describe main + object stores | Docs 09, 10, 15 show three stores (main + object + vector) | Acknowledge the triple-store architecture consistently; Doc 01 partially does this but the component diagram only shows "MainStore", "ObjectStore", and "VectorStore" without full treatment |
| "LockableContext" | Doc 12 explains its role in transactions | Doc 10 mentions it exists but doesn't detail its interaction with session functions | Doc 10 should briefly explain how LockableContext differs from BasicContext for session function callbacks |
| "Fuzzy region" | Doc 05 defines it as `[startLogicalAddress, finalLogicalAddress)` in checkpoint context | Doc 14 uses "fuzzy region" for AOF replay between CheckpointStartCommit and CheckpointEndCommit markers | These are related but distinct concepts; clarify that the AOF fuzzy region markers correspond to checkpoint fuzzy region boundaries |

---

## Missing Interfaces

The following component interactions are referenced but not fully documented in any document:

### 1. DatabaseManager / Multi-Database Architecture
Doc 01 mentions `StoreWrapper` contains `DatabaseManager -> MainStore, ObjectStore, VectorStore`. Doc 14 mentions per-database AOF and `SwitchActiveDatabaseContext`. However, **no document explains the DatabaseManager itself**: how databases are created, how the multi-database model works, how SELECT switches between databases, or how per-database isolation is achieved. This is a significant gap for reimplementation.

### 2. Pub/Sub Broker
Doc 01 lists "Pub/Sub broker" as a component of StoreWrapper. No document covers the publish/subscribe system. For a Redis-compatible reimplementation, this is a required feature. The broker's interaction with sessions, its message delivery guarantees, and its memory model are undocumented.

### 3. ACL (Access Control Lists)
Doc 01 mentions "ACL" in StoreWrapper. No document covers the ACL system, authentication model, or per-command permission checking.

### 4. Custom Command Registration / Extensibility
Docs 10, 11 reference `CustomCommandManager` and custom object factories. No document explains the custom command registration API, the module loading mechanism, or how custom commands integrate with the RESP dispatch table.

### 5. VectorManager / DiskANN Integration
Docs 09, 10 reference VectorManager and DiskANN vector indexes. No dedicated document covers the vector store subsystem, its indexing algorithm, or its interaction with the Tsavorite storage layer. This is architecturally significant as it adds a third store type.

### 6. AsyncProcessor <-> Session Coordination
Doc 09 mentions the AsyncProcessor for async GET operations and describes the SpinLock serialization with the main session thread. However, the full coordination protocol (SingleWaiterAutoResetEvent, push response format, error handling) is only partially documented.

### 7. Read Cache
Doc 02 mentions the allocator can be instantiated as a read cache. Doc 03 mentions ReadCache bit in hash bucket entries. No document covers the read cache subsystem: when it's enabled, how entries are promoted, or how it interacts with the main log.

### 8. CacheSizeTracker / Eviction Policy
Docs 10, 11 reference `CacheSizeTracker` and `objectStoreSizeTracker` for memory-based eviction. No document explains the eviction policy, triggers, or how it interacts with the Tsavorite hybrid log's page eviction.

### 9. IDevice Abstraction
Doc 01 mentions `IDevice` (LocalDevice, AzureStorage). No document covers the device abstraction layer, which is critical for disk I/O and cloud storage integration.

### 10. Configuration / Startup Lifecycle
Docs reference `GarnetServer.cs` in `libs/host/` but no document covers the full configuration parsing, option validation, store initialization order, or graceful shutdown procedure.

---

## Architecture Completeness

### Component Dependency Graph

From the 15 documents, the following dependency graph can be constructed:

```
GarnetServer (entry point)
  -> GarnetServer (host layer: config, init)
    -> StoreWrapper
      -> DatabaseManager
        -> TsavoriteKV (main store per DB)
        -> TsavoriteKV (object store per DB)
        -> TsavoriteKV (vector store per DB) [NEW in recent versions]
        -> AppendOnlyFile (per DB)
      -> PubSubBroker [UNDOCUMENTED]
      -> ACL [UNDOCUMENTED]
      -> CustomCommandManager [PARTIALLY DOCUMENTED]
      -> WatchVersionMap
    -> GarnetServerTcp
      -> ServerTcpNetworkHandler
        -> NetworkHandler (buffer management)
          -> LimitedFixedBufferPool
          -> GarnetTcpNetworkSender (send + throttle)
        -> RespServerSession
          -> SessionParseState (RESP parsing)
          -> StorageSession
            -> BasicContext / LockableContext (main store)
            -> BasicContext / LockableContext (object store)
            -> BasicContext / LockableContext (vector store)
            -> MainSessionFunctions / ObjectSessionFunctions / VectorSessionFunctions
              -> FunctionsState (AOF, watch, ETag, etc.)
          -> TransactionManager
            -> TxnKeyEntries (lock management)
            -> WatchedKeysContainer
          -> AsyncProcessor [PARTIALLY DOCUMENTED]
    -> ClusterProvider (optional)
      -> ClusterConfig (copy-on-write)
      -> GossipManager
      -> ReplicationManager
        -> AofProcessor (replay engine)
        -> CheckpointStore
        -> FailoverManager
```

### Missing Subsystems / Code Paths

1. **Pub/Sub**: Completely missing. SUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE.
2. **ACL / AUTH**: Completely missing.
3. **Multiple databases (SELECT)**: Referenced but not explained.
4. **Read Cache**: Referenced but not explained.
5. **Object eviction policy**: Referenced but not explained.
6. **VectorSet / DiskANN**: Only partially referenced from session functions; no dedicated document.
7. **Blocking commands (BLPOP, BRPOP, BLMOVE, WAIT)**: Doc 11 lists them as ListObject operations but the blocking wait mechanism (session-level coordination, timeout handling) is not documented.
8. **OBJECT ENCODING / OBJECT FREQ / OBJECT HELP**: Object introspection commands.
9. **Key expiration / TTL background sweep**: Doc 10 describes expiry checking in session function callbacks. No document describes how expired keys are proactively cleaned up (background scan vs lazy expiry only).
10. **Latency metrics / slow log**: Doc 09 mentions `LatencyMetrics?.Start(NET_RS_LAT)` but the metrics subsystem is undocumented.

### Startup/Shutdown Lifecycle
**Partially documented**. Doc 01 describes the component hierarchy and data flow but does not cover:
- Configuration parsing and validation
- Store initialization order
- Warmup / recovery sequence
- Graceful shutdown (connection draining, checkpoint-on-exit, AOF flush)
- Signal handling

---

## Rust Full-Stack Score: 7.5/10

### Analysis

**Strengths for Rust reimplementation:**
- Garnet's core design principles (zero-allocation, monomorphization, unsafe pointer arithmetic, epoch-based concurrency) are natural fits for Rust
- Rust provides true monomorphization natively, eliminating a major complexity of the C# approach
- The lock-free data structures (hash index, epoch table, hybrid log allocation) map directly to Rust's atomic primitives
- Ownership semantics align well with Garnet's careful lifetime management of buffers and records
- `io_uring` on Linux could exceed the .NET SAEA model's performance
- `crossbeam-epoch` provides a proven epoch-based reclamation implementation

**Challenges for Rust reimplementation:**
1. **Hybrid log lifetime management** (Hardest subsystem): The hybrid log's record references span mutable/read-only/disk regions with different lifetime semantics. Rust's borrow checker will make it difficult to express "this reference is valid while the epoch is held and the record hasn't been evicted." Extensive use of `unsafe` will be required, negating some of Rust's safety benefits. Estimated: 6-8 engineer-months for the hybrid log + hash index alone.

2. **Session function callbacks**: The `ISessionFunctions` pattern requires callbacks that borrow from both the log record (via raw pointer) and the session state simultaneously. This creates complex borrowing patterns that will likely require `unsafe` or clever abstraction.

3. **Object store with GC**: Garnet's object store relies on .NET's GC for heap-allocated objects (HashObject, ListObject, etc.). In Rust, these would be `Arc<Mutex<...>>` or similar, adding overhead for reference counting and locking that .NET's GC handles transparently.

4. **Serialization state machine** (GarnetObjectBase): The CAS-based REST/SERIALIZING/SERIALIZED transitions for checkpoint consistency are straightforward in C# but require careful `unsafe` work in Rust due to shared mutable state.

5. **TLS integration**: The async SslStream state machine (doc 07) is complex. `rustls` is a good alternative but the integration with buffer management differs significantly.

6. **Ecosystem gaps**: No existing Rust crate provides Tsavorite-equivalent functionality. The hybrid log, hash index, checkpointing, and compaction must be built from scratch.

### Hardest Subsystem to Port
**Tsavorite Storage Engine (docs 02-06)**, specifically the hybrid log allocator with its page eviction, epoch-protected access, and record revivification. The interplay between mutable/read-only/on-disk regions, the lock-free tail allocation, and the page flush pipeline create a tightly coupled system where Rust's ownership model will fight the design at every turn. The hash index (doc 03) is comparatively straightforward.

### Effort Estimate
28-36 engineer-months (see table below)

---

## Kotlin Full-Stack Score: 5.5/10

### Analysis

**Strengths for Kotlin/JVM reimplementation:**
- Mature ecosystem: Netty for networking, Kotlin coroutines for async, rich standard library
- Garbage collection handles object store lifecycle naturally
- Rapid development: less boilerplate than Rust, easier to iterate
- The object store (doc 11) maps almost perfectly to JVM's object model
- Transaction management (doc 12) is straightforward on JVM
- Cluster/replication (doc 13) can leverage existing JVM networking libraries

**JVM Limitations that Compound Across Subsystems:**

1. **No monomorphization (compounds everywhere)**: This is the single biggest limitation. It affects:
   - Storage engine: Every `ISessionFunctions` callback is a virtual dispatch (~5-10ns per call, 5-10 calls per operation = 25-100ns overhead per operation)
   - Hash index: Generic allocator callbacks cannot be inlined
   - RESP parsing: Command dispatch through interface calls
   - **Cumulative impact**: Estimated 200-400ns additional overhead per operation vs C#/Rust, which at millions of ops/sec translates to significant throughput loss

2. **No value types (compounds with storage and parsing)**:
   - `RecordInfo` (8 bytes) becomes a boxed Long or raw byte manipulation
   - `SpanByte` cannot exist as a value type; must use offset+length into ByteBuffer
   - `ArgSlice` becomes an object allocation or manual offset tracking
   - `HashBucketEntry` becomes a long in an array
   - **Cumulative impact**: Every record access, every parse, every hash lookup involves indirection or manual offset math instead of direct struct access

3. **No cache-line alignment control (compounds with concurrency)**:
   - Epoch table entries: Cannot guarantee 64-byte alignment without `Unsafe.allocateMemory`
   - Hash buckets: Cannot guarantee cache-line alignment
   - **Impact**: False sharing degrades concurrent performance, especially under high thread counts

4. **GC pressure from hot-path allocations**:
   - Without value types, temporary objects (ArgSlice, SpanByteAndMemory, RawStringInput) must be heap-allocated or simulated via ThreadLocal pools
   - GC pauses can spike p99 latency even with ZGC/Shenandoah
   - **Impact**: Latency jitter that Garnet's design specifically avoids

5. **No unsafe pointer arithmetic (compounds with RESP parsing and storage)**:
   - Every `*(ulong*)ptr` pattern in RESP parsing must become `ByteBuffer.getLong(offset)`
   - The SIMD-like uppercase trick requires careful ByteBuffer manipulation
   - Direct record access in the hybrid log requires either `sun.misc.Unsafe` or Project Panama

### Realistic Performance Ceiling
Based on the compounding limitations, a well-optimized JVM implementation would likely achieve:
- **Throughput**: 30-50% of C#/Rust Garnet for simple GET/SET operations (due to monomorphization loss + allocation overhead)
- **Latency p50**: Comparable (within 2x) due to JIT warmup eventually optimizing hot paths
- **Latency p99**: 3-5x worse due to GC pauses and lack of deterministic memory layout
- **Memory efficiency**: 1.5-2x higher memory usage due to object headers, padding, and lack of value types

This puts a JVM reimplementation at roughly comparable to or somewhat better than Redis (which is single-threaded), but significantly below Garnet's performance targets.

---

## Missing Cross-Cutting Concerns

### 1. Configuration / Startup
No document covers:
- The full set of configuration options and their defaults
- Configuration validation and error reporting
- Store initialization order and dependencies
- First-run vs recovery-from-checkpoint startup paths
- Environment variable and command-line argument parsing

### 2. Monitoring / Metrics
Only one mention of metrics (`LatencyMetrics?.Start(NET_RS_LAT)` in doc 09). Missing:
- Garnet's metrics/stats subsystem (INFO command output)
- Slow log mechanism
- Per-command latency tracking
- Connection count, memory usage, ops/sec reporting
- Integration with external monitoring (Prometheus, OpenTelemetry)

### 3. Error Handling Patterns
The documents describe specific error cases (WRONGTYPE, buffer overflow, lock failure) but do not describe:
- The overall error handling philosophy (exceptions vs error codes vs result types)
- How fatal errors propagate (GarnetException at LogLevel.Critical in doc 08)
- Connection-level vs session-level vs server-level error recovery
- How malformed RESP input is handled beyond individual parse failures

### 4. Testing Strategy
No document mentions:
- Unit testing approach for individual subsystems
- Integration testing for cross-subsystem interactions
- Compatibility testing against Redis test suites (redis-benchmark, redis-cli)
- Performance regression testing methodology
- Chaos/fault injection testing for cluster mode

### 5. Migration / Compatibility
No document covers:
- Redis command compatibility matrix (which commands are supported, which are not)
- Wire protocol compatibility guarantees (RESP2/RESP3 differences)
- Data migration from Redis to Garnet (RDB import, etc.)
- Data migration from Garnet to a reimplementation (checkpoint format portability)
- Client library compatibility expectations

### 6. Logging and Diagnostics
- No coverage of logging levels, log output format, or structured logging
- No coverage of diagnostic dump capabilities (MEMORY DOCTOR, DEBUG commands)

### 7. Security Beyond ACL
- TLS certificate management and rotation
- Network isolation and bind address configuration
- Protected mode behavior

---

## Effort Estimates

| Subsystem | Rust (eng-months) | Kotlin (eng-months) | Confidence |
|-----------|-------------------|---------------------|------------|
| Hybrid Log + Allocator (02) | 5-6 | 4-5 | High |
| Hash Index (03) | 2-3 | 2-3 | High |
| Epoch Framework (04) | 1-2 | 1-2 | High |
| Checkpointing (05) | 3-4 | 2-3 | Medium |
| Record Management (06) | 2-3 | 2-3 | Medium |
| Network Layer (07) | 2-3 | 1-2 (Netty) | High |
| RESP Parser (08) | 1-2 | 1-2 | High |
| Request Lifecycle (09) | 1-2 | 1-2 | Medium |
| Session Functions (10) | 3-4 | 2-3 | Medium |
| Data Structures (11) | 2-3 | 1-2 | High |
| Transactions (12) | 2-3 | 2-3 | Medium |
| Cluster + Replication (13) | 4-5 | 3-4 | Low |
| AOF + Durability (14) | 2-3 | 2-3 | Medium |
| Performance Tuning (15) | 2-3 | 3-4 | Low |
| **Missing subsystems*** | 3-5 | 2-4 | Low |
| **Integration + Testing** | 4-6 | 4-6 | Low |
| **TOTAL** | **38-54** | **31-47** | Low-Medium |

*Missing subsystems: Pub/Sub, ACL, Read Cache, VectorSet/DiskANN, Blocking Commands, Multi-DB, Metrics, Key Expiry.

**Note**: These estimates assume a team of experienced systems programmers. The Rust estimate is higher due to the need to build Tsavorite-equivalent storage engine from scratch with safe abstractions over extensive unsafe code. The Kotlin estimate is lower for raw development time but the resulting system will have significantly lower performance. These estimates do NOT include the time to achieve Garnet-equivalent performance -- performance tuning alone could double the estimates.

### Summary Table

| Language | Engineer-Months (dev) | Engineer-Months (with perf tuning) | Confidence |
|----------|----------------------|-------------------------------------|------------|
| Rust | 38-54 | 50-70 | Low-Medium |
| Kotlin | 31-47 | 45-65 | Low-Medium |

---

## Recommended Reading Order

For an engineer starting a reimplementation, the documents should be read in this order:

### Phase 1: Understand the Architecture (read first)
1. **Doc 01 - Architecture Overview**: Start here to understand the full system, component relationships, and design philosophy
2. **Doc 15 - Performance Engineering**: Read this immediately after to understand WHY the architecture is designed the way it is -- every design decision is performance-motivated
3. **Doc 09 - Request Lifecycle**: Trace a complete request through the system to see how all components interact

### Phase 2: Storage Engine Deep Dive (implement first)
4. **Doc 04 - Epoch & Concurrency**: The foundational concurrency primitive used everywhere
5. **Doc 02 - Hybrid Log Allocator**: The core data structure -- everything builds on this
6. **Doc 03 - Hash Index**: The lookup structure that sits on top of the hybrid log
7. **Doc 06 - Record Management**: How records are laid out, updated, and recycled
8. **Doc 05 - Checkpointing**: How the storage engine achieves durability

### Phase 3: Network and Protocol (implement second)
9. **Doc 07 - Network Layer**: TCP I/O, buffer management, connection lifecycle
10. **Doc 08 - RESP Parser**: Protocol parsing, command dispatch, response writing

### Phase 4: Application Logic (implement third)
11. **Doc 10 - Storage Session Functions**: The callback interface between Redis commands and Tsavorite
12. **Doc 11 - Data Structures & Objects**: Hash, List, Set, SortedSet implementations
13. **Doc 14 - AOF & Durability**: Write-ahead logging and crash recovery

### Phase 5: Distributed Features (implement last)
14. **Doc 12 - Transactions & Scripting**: MULTI/EXEC, stored procedures
15. **Doc 13 - Cluster & Replication**: Clustering, gossip, replication, failover

---

## Final Summary

The documentation set is strong and provides sufficient detail for a reimplementation of the core system. The primary gaps are in cross-cutting concerns (configuration, monitoring, testing) and several subsystems that are referenced but not documented (Pub/Sub, ACL, Read Cache, VectorSet, Blocking Commands, Multi-DB). The storage engine documentation (docs 02-06) is the strongest section, with deep technical detail including bit layouts, algorithms, and source references. The application layer documentation (docs 10-14) is also strong but has more open questions around edge cases.

A Rust reimplementation is architecturally well-suited but will require extensive unsafe code for the storage engine. A Kotlin/JVM reimplementation is faster to develop but will face fundamental performance limitations due to type erasure, lack of value types, and GC pressure. Neither language is a perfect fit -- Rust trades development speed for performance parity, while Kotlin/JVM trades performance for development speed.

The single most impactful improvement to the documentation set would be adding a **Doc 00 - Configuration & Startup** that covers the full initialization lifecycle, configuration options, and shutdown procedure, plus dedicated documents for Pub/Sub and the multi-database architecture.
