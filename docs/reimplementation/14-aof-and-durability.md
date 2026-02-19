# 14 -- AOF & Durability

## 1. Overview

Garnet's durability model is built on an Append-Only File (AOF) that records every mutation as a structured binary entry. The AOF is implemented using Tsavorite's `TsavoriteLog` -- a high-performance, concurrent, append-only log with page-based allocation and asynchronous commit. Combined with periodic checkpoints of the Tsavorite stores, the AOF provides point-in-time recovery: after a crash, the system restores the latest checkpoint and replays the AOF from that checkpoint's address to the log tail.

The AOF also serves as the replication transport (see [Doc 13 Section 2.6](./13-cluster-and-replication.md#26-replication-sync-protocol) for the replication sync protocol): primary nodes stream their AOF tail to replicas, which replay entries through the same `AofProcessor` used for crash recovery. This dual-use design means the serialization format, the replay logic, and the version-tracking machinery are shared between recovery and replication paths.

[Source: libs/server/AOF/AofHeader.cs:L8-L62]
[Source: libs/server/AOF/AofEntryType.cs:L6-L92]
[Source: libs/server/AOF/AofProcessor.cs:L24-L81]

## 2. Detailed Design

### 2.1 AofHeader: The 16-Byte Entry Header

Every AOF entry begins with a fixed 16-byte header using explicit struct layout:

```
Offset 0:  byte  aofHeaderVersion  -- Format version (currently 2)
Offset 1:  byte  padding           -- Alignment / future use
Offset 2:  AofEntryType opType     -- Operation type (1 byte)
Offset 3:  byte  procedureId       -- Stored procedure ID (for StoredProcedure entries)
Offset 4:  long  storeVersion      -- Transaction/store version (8 bytes)
Offset 12: int   sessionID         -- Session that generated this entry (4 bytes)
```

Two fields overlay others via `FieldOffset`, sharing the same byte:
- **`unsafeTruncateLog`** at offset 1 (overlays `padding`): Used with FLUSH commands to indicate whether to truncate the log. Since both fields occupy the same byte, writing `unsafeTruncateLog` overwrites `padding` and vice versa.
- **`databaseId`** at offset 3 (overlays `procedureId`): Used with FLUSH commands to specify the target database. For `FlushDb` entries, `header.databaseId` and `header.procedureId` are the **same byte** -- reading either field returns the same value. For `StoredProcedure` entries, this byte contains the procedure ID; for `FlushDb` entries, it contains the database ID.

The `AofHeaderVersion` constant (currently 2) is set in the constructor and checked during replay to handle format evolution. Version 2 introduced fuzzy region handling for checkpoints.

The header design prioritizes compactness and zero-copy access: the header can be read directly from a byte pointer via `*(AofHeader*)ptr` without deserialization, leveraging the fixed layout.

[Source: libs/server/AOF/AofHeader.cs:L8-L62]

### 2.2 AofEntryType: Operation Classification

The `AofEntryType` enum (`byte`) categorizes entries by store and operation:

**Main store operations** (0x00-0x02):
- `StoreUpsert = 0x00`: Direct key-value write.
- `StoreRMW = 0x01`: Read-modify-write (INCR, APPEND, SET with conditions, etc.).
- `StoreDelete = 0x02`: Key deletion.

**Object store operations** (0x10-0x12):
- `ObjectStoreUpsert = 0x10`
- `ObjectStoreRMW = 0x11`
- `ObjectStoreDelete = 0x12`

**Transaction markers** (0x20-0x22):
- `TxnStart = 0x20`: Begin of an atomic transaction group.
- `TxnCommit = 0x21`: End of transaction; all entries since TxnStart should be applied atomically.
- `TxnAbort = 0x22`: Transaction aborted; discard entries since TxnStart.

**Checkpoint markers** (0x30-0x43):
- `CheckpointStartCommit = 0x30`: Marks the beginning of a checkpoint's fuzzy region.
- `CheckpointEndCommit = 0x32`: Marks the end of the fuzzy region.
- `MainStoreStreamingCheckpointStartCommit = 0x40` / `EndCommit = 0x42`: For diskless sync.
- `ObjectStoreStreamingCheckpointStartCommit = 0x41` / `EndCommit = 0x43`: For diskless sync.

**Other**:
- `StoredProcedure = 0x50`: Entire stored procedure replay.
- `FlushAll = 0x60`: Flush all databases.
- `FlushDb = 0x61`: Flush a specific database.

The `AofStoreType` internal enum classifies entries by store type for routing: MainStoreType, ObjectStoreType, TxnType, ReplicationType, CheckpointType, FlushDbType. Notable mappings in `ToAofStoreType` (`AofProcessor.cs:L551-L563`): `StoredProcedure` maps to `TxnType` (not a separate category), meaning stored procedures are version-filtered using the same logic as transactions (against `objectStore.CurrentVersion`). `ReplicationType` is defined in the enum but no `AofEntryType` value maps to it -- it appears unused. `MainStoreStreamingCheckpointStartCommit` is NOT in the `ToAofStoreType` mapping (only `CheckpointStartCommit` and `ObjectStoreStreamingCheckpointStartCommit` are); this is safe because streaming checkpoint markers are handled in `ProcessAofRecordInternal` before reaching `SkipRecord`.

[Source: libs/server/AOF/AofEntryType.cs:L6-L92]

### 2.3 AOF Write Path

AOF entries are written by the session function callbacks (see document 10). The write path follows this pattern:

1. **During mutation**: The session function sets `NeedAofLog` flag in `UserData` of the operation info struct.
2. **Post-operation callback**: `PostRMWOperation`, `PostUpsertOperation`, or `PostDeleteOperation` checks the flag and calls the appropriate write method.
3. **Write methods** (`WriteLogRMW`, `WriteLogUpsert`, `WriteLogDelete`): These serialize the `AofHeader` plus the operation-specific payload (key, value, input) and call `appendOnlyFile.Enqueue(...)`.
4. **Enqueue**: Tsavorite's `TsavoriteLog.Enqueue` appends the entry to the log's current page. The entry is visible to readers immediately after enqueue returns. Durability (flush to disk) happens asynchronously via the log's commit thread.

**Transaction wrapping**: For MULTI/EXEC transactions:
- `TxnStart` is written when `TransactionManager.Run()` succeeds (locks acquired, watches validated).
- Each command's operation writes its own AOF entry during replay.
- `TxnCommit` is written by `TransactionManager.Commit()`.
- All entries between TxnStart and TxnCommit share the same `storeVersion`.

**Stored procedure logging**: When `StoredProcMode` is true, individual operation logging is suppressed. Instead, `TransactionManager.Log()` writes a single `StoredProcedure` entry containing the procedure ID and the serialized `CustomProcedureInput`. During replay, the entire procedure is re-executed.

**Conditional logging**: The `PerformWrites` flag on `TransactionManager` tracks whether any write operations occurred. Read-only transactions (all commands are reads) skip both `TxnStart` and `TxnCommit` entries to avoid unnecessary AOF overhead.

[Source: libs/server/Transaction/TransactionManager.cs:L273-L295]
[Source: libs/server/Transaction/TransactionManager.cs:L406-L411]

### 2.4 AofProcessor: Replay Engine

The `AofProcessor` class is the central replay engine for both crash recovery and replication:

**Construction** (`AofProcessor.cs:L60-L81`): Takes `StoreWrapper`, optional `IClusterProvider`, a `recordToAof` bool, and a logger. Creates a new `StoreWrapper` clone via `new StoreWrapper(storeWrapper, recordToAof)` -- the `recordToAof` parameter controls whether the replay session re-records entries to AOF. This is critical for replication chains: a replica that receives AOF entries from its primary may need to re-record them to its own AOF so that its sub-replicas can consume them. The constructor then creates a `RespServerSession` via an `obtainServerSession` lambda (enabling lazy creation of additional sessions), switches to the default database context, and initializes an `AofReplayCoordinator`.

**Recovery flow** (`Recover` method, `AofProcessor.cs:L102-L188`): Takes a `GarnetDatabase db` parameter for per-database recovery:
1. Switches the active database context to the target database via `SwitchActiveDatabaseContext(db)`, which updates `basicContext`, `objectStoreBasicContext`, and `activeVectorManager` to match the database.
2. Scans that database's AOF from `BeginAddress` to `untilAddress` (defaults to `TailAddress`).
3. For each entry, calls `ProcessAofRecordInternal`.
4. Reports throughput metrics: records/sec, GiB/sec.

This per-database model means each `GarnetDatabase` has its own `AppendOnlyFile`, and recovery iterates over each database independently.

**ProcessAofRecordInternal**: The main dispatch method:
1. Reads the `AofHeader` directly from the byte pointer.
2. Waits for any in-flight vector operations to complete (for consistency).
3. Delegates to `AofReplayCoordinator.AddOrReplayTransactionOperation` for transaction grouping. If the entry is part of a transaction (between TxnStart and TxnCommit), it is buffered until the commit.
4. Handles checkpoint markers:
   - `CheckpointStartCommit`: Enters fuzzy region. If already in a fuzzy region (missed EndCommit), clears the buffer and restarts.
   - `CheckpointEndCommit`: Exits fuzzy region. If replaying as a replica and the store version is ahead, takes a checkpoint. Then processes buffered fuzzy region operations.
5. Handles FLUSH operations by calling `storeWrapper.FlushAllDatabases` or `FlushDatabase`.
6. Delegates normal operations to `ReplayOp`.

**ReplayOp**: Routes by `AofEntryType`:
- `StoreUpsert/RMW/Delete`: Calls the appropriate Tsavorite context method.
- `ObjectStoreUpsert/RMW/Delete`: Same for the object store.
- `StoredProcedure`: Calls `aofReplayCoordinator.ReplayStoredProc`.
- `TxnCommit`: Calls `ProcessFuzzyRegionTransactionGroup`.

[Source: libs/server/AOF/AofProcessor.cs:L60-L94]
[Source: libs/server/AOF/AofProcessor.cs:L197-L334]

### 2.5 Fuzzy Region Handling

The "fuzzy region" in the AOF context refers to the window between `CheckpointStartCommit` and `CheckpointEndCommit` markers in the AOF stream. This corresponds to the checkpoint fuzzy region defined in the hybrid log as `[startLogicalAddress, finalLogicalAddress)` (see [Doc 05 Section 2.5](./05-tsavorite-checkpointing.md#25-hybrid-log-checkpoint-hybridlogcheckpointsmtask)), where records from both version v and v+1 coexist. The AOF markers delineate the same window from the replay perspective. During this window, a checkpoint is being taken, and operations may be included in either the checkpoint or the AOF replay -- but not both. The fuzzy region handling ensures:

1. When `CheckpointStartCommit` is seen, `inFuzzyRegion` is set to true.
2. The `SkipRecord` method (`AofProcessor.cs:L509-L564`) handles entries differently based on context. Version comparison uses the appropriate store: `MainStoreType` entries compare against `store.CurrentVersion`, while `ObjectStoreType` AND `TxnType` entries (including `StoredProcedure`) both compare against `objectStore.CurrentVersion`. This asymmetry means transaction markers and stored procedures are version-filtered using the object store's version, not the main store's.
   - **During recovery (not as replica)**: Entries with `storeVersion < store.CurrentVersion` (for the appropriate store) are **skipped** (already captured in the checkpoint). All other entries are replayed normally.
   - **As replica in fuzzy region**: Entries with `storeVersion > store.CurrentVersion` (new-version records) are **buffered** for post-checkpoint replay. Old-version entries are replayed immediately. This distinction between "skipped" and "buffered" is important: skipped entries are discarded, while buffered entries are deferred.
3. When `CheckpointEndCommit` is seen:
   a. If replaying as a replica and the checkpoint version is ahead, a local checkpoint is taken.
   b. Buffered fuzzy region operations are processed with version-aware filtering.
   c. The buffer is cleared.

This mechanism prevents double-application: an operation that was captured in the checkpoint is not also replayed from the AOF, and vice versa.

[Source: libs/server/AOF/AofProcessor.cs:L215-L251]

### 2.6 Transaction Replay Coordination

The `AofReplayCoordinator` (nested class inside `AofProcessor`) groups transaction entries using three supporting types:

- **`AofReplayContext`** (`AofReplayContext.cs`): Holds per-replay mutable state including `fuzzyRegionOps` (List of buffered fuzzy region entries), `activeTxns` (Dictionary<int, TransactionGroup> mapping session IDs to in-progress transaction groups), `txnGroupBuffer` (Queue of transaction groups deferred during fuzzy regions), `inFuzzyRegion` flag, and pre-allocated input structs (`storeInput`, `objectStoreInput`, `customProcInput`) with a shared `SessionParseState`.

- **`TransactionGroup`** (`TransactionGroup.cs`): Simple wrapper holding `List<byte[]> operations` -- the buffered AOF entries belonging to a single transaction.

- **`AddOrReplayTransactionOperation`**: When a `TxnStart` entry is seen, creates a new `TransactionGroup` in `activeTxns` keyed by session ID. Subsequent entries with the same session ID are added to the group's operations list. When `TxnCommit` is seen:
  - If in a fuzzy region, the commit marker is added to `fuzzyRegionOps` and the group is enqueued in `txnGroupBuffer` for deferred replay.
  - Otherwise, the group is replayed immediately via `ProcessTransactionGroup`.
  - `TxnAbort` clears and removes the group. Nested `TxnStart` throws.

- **`ProcessTransactionGroup`** (`AofReplayCoordinator.cs:L187-L261`): Two modes:
  - **Recovery** (not as replica): Replays operations directly without locking via `ProcessTransactionGroupOperations`, since recovery is single-threaded and no concurrent reads exist.
  - **Replica replay**: Extracts keys from each operation via `SaveTransactionGroupKeysToLock` (iterates entries, reads key SpanByte, calls `txnManager.SaveKeyEntryToLock` with `LockType.Exclusive`), then runs `txnManager.Run(internal_txn: true)`, replays operations through the lockable context, and commits.

- **`ReplayStoredProc`**: Deserializes `CustomProcedureInput` from the entry payload and calls `respServerSession.RunTransactionProc` with the procedure ID from the header and `isRecovering: true`.

Transaction groups are keyed by `sessionID` from the AOF header, allowing interleaved transactions from different sessions to be correctly grouped.

[Source: libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs]
[Source: libs/server/AOF/ReplayCoordinator/AofReplayContext.cs]
[Source: libs/server/AOF/ReplayCoordinator/TransactionGroup.cs]

### 2.7 Checkpoint Integration

Garnet's durability model combines checkpoints with AOF:

1. **Periodic checkpoints**: The `StateMachineDriver` coordinates periodic checkpoints of both the main store and object store.
2. **Safe AOF address**: Each checkpoint records the AOF address at which it was taken. After recovery from a checkpoint, only AOF entries after this address need replay.
3. **AOF truncation**: After a successful checkpoint, the AOF can be truncated up to the checkpoint's safe address, reclaiming disk space.

The `storeVersion` field in `AofHeader` links each AOF entry to a specific Tsavorite version, enabling the fuzzy region logic to determine which entries are already captured in a checkpoint.

### 2.8 Durability Guarantees

Garnet's durability model provides these guarantees:

- **Committed writes**: Once `TsavoriteLog.Enqueue` returns, the entry is in the in-memory log. Durability to disk depends on the commit policy (synchronous or asynchronous). With async commit, there is a window of potential data loss.
- **Transaction atomicity**: Entries between `TxnStart` and `TxnCommit` are either all replayed or none (if `TxnAbort` is seen or EOF occurs before `TxnCommit`).
- **Checkpoint + AOF**: Full durability requires both a checkpoint and the AOF from the checkpoint's safe address. Loss of either component prevents full recovery.
- **Replication**: AOF entries are streamed to replicas. The replication offset tracks progress. A replica's durability is bounded by its replication lag.

### 2.9 AOF Payload Format

After the 16-byte header, the payload varies by entry type:

**StoreUpsert**: `[key SpanByte][value SpanByte][RawStringInput]` -- raw key and value with SpanByte headers (4-byte length prefix), followed by a serialized `RawStringInput`. During replay, `storeInput.DeserializeFrom(curr)` reconstructs the input after reading key and value (`AofProcessor.cs:L365-L373`).
**StoreRMW**: `[key SpanByte][input RawStringInput]` -- key plus the RMW input that encodes the command and arguments.
**StoreDelete**: `[key SpanByte]` -- key only.
**ObjectStoreRMW**: `[key byte[]][input ObjectInput]` -- key plus object-store input.
**ObjectStoreUpsert**: `[key byte[]][serialized IGarnetObject]` -- key plus fully serialized object.
**ObjectStoreDelete**: `[key byte[]]` -- key only.
**StoredProcedure**: `[CustomProcedureInput]` -- procedure ID is in the header; the payload is the serialized input.
**TxnStart/TxnCommit/TxnAbort**: Header only, no payload.
**FlushAll/FlushDb**: Header only (database ID in `databaseId` field, truncate flag in `unsafeTruncateLog`).

## 3. Source File Map

| File | Purpose |
|------|---------|
| `libs/server/AOF/AofHeader.cs` | 16-byte header struct with explicit layout and field overlays |
| `libs/server/AOF/AofEntryType.cs` | `AofEntryType` enum (byte) and `AofStoreType` internal enum |
| `libs/server/AOF/AofProcessor.cs` | Replay engine: construction, recovery, ProcessAofRecordInternal, ReplayOp, StoreUpsert/RMW/Delete, ObjectStore replay methods, SkipRecord |
| `libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs` | Transaction grouping, fuzzy region buffering, stored proc replay |
| `libs/server/AOF/ReplayCoordinator/AofReplayContext.cs` | Per-replay mutable state: fuzzyRegionOps, activeTxns, txnGroupBuffer, input structs |
| `libs/server/AOF/ReplayCoordinator/TransactionGroup.cs` | Transaction operation buffer (List of byte arrays) |
| `libs/server/Storage/Functions/MainStore/PrivateMethods.cs` | WriteLogUpsert, WriteLogRMW, WriteLogDelete |
| `libs/server/Transaction/TransactionManager.cs` | TxnStart/TxnCommit AOF writing |

## 4. Performance Notes

- **Zero-copy header read**: `*(AofHeader*)ptr` reads the header directly from the log page without deserialization. This is critical for replay throughput.
- **TsavoriteLog append performance**: `TsavoriteLog.Enqueue` is designed for high-throughput concurrent appends. Multiple sessions can append simultaneously without contention beyond the page allocation lock.
- **Async commit**: The default commit policy is asynchronous, batching multiple entries per disk flush. This provides high throughput at the cost of a durability window.
- **Transaction grouping**: Buffering transaction entries until TxnCommit avoids partial replays and enables batch application.
- **Version-based filtering**: The `SkipRecord` method avoids replaying entries that are already captured in a checkpoint, reducing recovery time.
- **Conditional AOF writes**: Read-only transactions skip AOF entirely. The `PerformWrites` flag avoids TxnStart/TxnCommit entries for read-only transactions.
- **Stored procedure efficiency**: A single StoredProcedure AOF entry replaces potentially many individual entries, reducing AOF size for complex operations.

## 5. Rust Mapping Notes

- **AofHeader**: `#[repr(C, packed)]` struct with explicit field layout. Use `bytemuck` or `zerocopy` for safe zero-copy casts from byte slices.
- **TsavoriteLog**: Replace with a custom append-only log or use an existing Rust crate like `rio` (io_uring-based) or a custom page-based log. Key requirements: concurrent append, page-based allocation, async flush, scan from address.
- **AofProcessor**: Implement as a struct holding mutable store references. The dispatch in `ProcessAofRecordInternal` maps to a `match` on `AofEntryType`.
- **Fuzzy region buffer**: Use a `Vec<Vec<u8>>` for buffered entries, flushed on CheckpointEndCommit.
- **Transaction grouping**: Use `HashMap<i32, Vec<(AofHeader, Vec<u8>)>>` keyed by session ID.
- **Zero-copy parsing**: Use `&[u8]` slices with offset tracking. The `AofHeader` can be read via `std::ptr::read_unaligned` or `bytemuck::from_bytes`.
- **Async commit**: Use `tokio::fs::File` with `write_all` and explicit `sync_data()` calls. Consider `io_uring` for batched writes.

## 6. Kotlin/JVM Mapping Notes

- **AofHeader**: Cannot replicate the exact layout. Use a class with `ByteBuffer` serialization/deserialization helpers. For hot-path parsing, use `ByteBuffer.getShort()` / `getInt()` / `getLong()` at fixed offsets.
- **TsavoriteLog equivalent**: Consider a custom `MappedByteBuffer`-based log or use a library like Chronicle Queue for high-performance append-only logging.
- **Zero-copy reads**: `ByteBuffer` provides positional reads without copying, approximating the `*(AofHeader*)ptr` pattern.
- **Transaction grouping**: `HashMap<Int, MutableList<ByteArray>>` keyed by session ID.
- **Async commit**: Use `FileChannel.force(false)` for data-only sync. Consider `AsynchronousFileChannel` for non-blocking writes.
- **Fuzzy region**: Same buffering approach using `ArrayList<ByteArray>`.
- **Stored procedure replay**: Requires a procedure registry mapping IDs to implementations, identical to the C# pattern.

## 7. Open Questions

1. **AOF format portability**: The current format uses .NET-specific `SpanByte` serialization. Should a reimplementation define a portable binary format, potentially compatible with Redis AOF?
2. **Sync commit option**: The current model is primarily async. Should the reimplementation support a configurable `fsync` policy (every write, every N ms, or every N entries)?
3. **AOF compaction**: Unlike Redis, Garnet does not appear to have AOF rewriting/compaction. The checkpoint+truncation model replaces this. Is this sufficient for all workloads?
4. **Concurrent replay**: Recovery is currently single-threaded per database. Could parallel replay (partitioned by key hash) improve recovery time?
5. **Stored procedure determinism**: Non-deterministic procedures (e.g., using timestamps) may replay differently. Should the AOF capture the procedure's output instead of its input?
6. **Cross-database AOF**: Each database has its own AOF. Cross-database operations (FLUSHALL) write to one AOF but affect all databases. How should this be coordinated in a reimplementation?
7. **Header versioning**: The `aofHeaderVersion` field enables format evolution, but the current code only checks `version > 1` for fuzzy region support. Should there be a more structured migration path?
8. **TxnType version comparison uses objectStore.CurrentVersion**: In `SkipRecord`, both `ObjectStoreType` and `TxnType` entries (including `StoredProcedure`) compare against `objectStore.CurrentVersion`, NOT `mainStore.CurrentVersion`. The rationale for this asymmetry is not documented in the source code. One plausible explanation is that transactions and stored procedures may modify both stores, and the object store version serves as the "conservative" version boundary (if the two stores can have different versions during certain checkpoint phases). **WARNING to reimplementors**: Do not change this to `mainStore.CurrentVersion` without fully understanding the version progression of both stores during CPR checkpointing. Incorrect version filtering causes either double-application (data corruption) or skipped entries (data loss).
