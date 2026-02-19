# 10 -- Storage Session Functions

## 1. Overview

Garnet's storage layer communicates with the Tsavorite storage engine through a callback-based interface called `ISessionFunctions`. Rather than embedding command logic inside the storage engine, Garnet implements all Redis-specific semantics -- expiry checking, RMW arithmetic, ETag management, watch-version tracking, and AOF logging -- inside callback structs that Tsavorite invokes at precisely defined points during record operations. This design keeps the storage engine generic while giving Garnet full control over data interpretation.

There are two parallel implementations: `MainSessionFunctions` handles the main (string/raw) store where both keys and values are `SpanByte`, and `ObjectSessionFunctions` handles the object store where keys are `byte[]` and values are `IGarnetObject`. Both share a common `FunctionsState` instance that carries per-session mutable state including AOF references, the watch version map, memory pools, ETag tracking state, and custom command registries.

The `StorageSession` class ties these together, holding `BasicContext` and `LockableContext` pairs for both stores (and a vector store), providing the bridge between the RESP command processor and the Tsavorite engine.

[Source: libs/server/Storage/Functions/MainStore/MainSessionFunctions.cs:L11-L31]
[Source: libs/server/Storage/Functions/FunctionsState.cs:L13-L52]
[Source: libs/server/Storage/Session/StorageSession.cs]

## 2. Detailed Design

### 2.1 FunctionsState: Per-Session Shared State

Every session function instance holds a reference to a `FunctionsState` object that is created once per session. This class is `sealed` (preventing virtual dispatch overhead) and carries:

- **`TsavoriteLog appendOnlyFile`**: The AOF log for write-ahead logging. Null when AOF is disabled.
- **`WatchVersionMap watchVersionMap`**: A shared power-of-2 `long[]` array for optimistic watch-based transactions. Every mutation increments the version at `map[keyHash & sizeMask]` using `Interlocked.Increment`.
- **`MemoryPool<byte> memoryPool`**: Used for output buffer allocation when responses exceed the inline `SpanByte` capacity.
- **`CacheSizeTracker objectStoreSizeTracker`**: Tracks heap memory consumed by object store values for eviction decisions.
- **`GarnetObjectSerializer garnetObjectSerializer`**: Type-dispatched serializer/deserializer for `IGarnetObject` instances.
- **`EtagState etagState`**: Mutable per-operation state tracking ETag offsets and values within the current record being processed.
- **`byte respProtocolVersion`**: RESP2 vs RESP3, affecting nil response format.
- **`bool StoredProcMode`**: When true, suppresses per-operation AOF logging (the stored procedure logs as a single unit).
- **`VectorManager vectorManager`**: Manages DiskANN vector set indexes, including index creation, element addition/removal, and search. It coordinates with session functions for VADD/VREM operations and handles async vector operations that run on background threads.

Custom commands are resolved through `GetCustomCommandFunctions(int id)` which delegates to the `CustomCommandManager` registry.

[Source: libs/server/Storage/Functions/FunctionsState.cs:L14-L52]

### 2.2 MainSessionFunctions: The ISessionFunctions Implementation

`MainSessionFunctions` is declared as a `readonly unsafe partial struct` implementing `ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>`. The `struct` declaration is critical: it avoids heap allocation and enables the JIT to devirtualize and inline the callback methods when Tsavorite calls them through generic type parameters rather than interface dispatch.

A single `const byte NeedAofLog = 0x1` flag is stored in `RMWInfo.UserData` / `UpsertInfo.UserData` to defer AOF writes until after the record operation completes (in `PostRMWOperation` / `PostUpsertOperation`), ensuring the AOF entry is only written when the operation actually succeeds.

[Source: libs/server/Storage/Functions/MainStore/MainSessionFunctions.cs:L11-L23]

### 2.3 Read Path: SingleReader and ConcurrentReader

Both `SingleReader` and `ConcurrentReader` are invoked by Tsavorite during the Read operation flow (see [Doc 06 Section 2.12](./06-tsavorite-record-management.md#212-end-to-end-operation-flows) for the complete Read operation trace showing where these callbacks are invoked based on record address region). `SingleReader` is called when a record is in the read-only region or on disk (accessed by a single thread), and `ConcurrentReader` when a record is in the mutable region (possibly accessed concurrently). They follow nearly identical logic:

1. **Expiry check**: If the value has 8 bytes of metadata (`MetadataSize == 8`) and the metadata indicates expiry, the session function calls `recordInfo.ClearHasETag()` to mutate the RecordInfo (a Tsavorite struct that lives alongside the record in the log), then returns `false` to signal a miss. Returning false tells Tsavorite to treat this as a cache miss. The ETag clearing happens via direct RecordInfo mutation, not via the return value.
2. **VectorSet type mismatch detection**: If the record is marked `VectorSet` in `RecordInfo` but the command is not legal on vector sets (or vice versa), cancel the operation.
3. **ETag-aware reads**: If the record has an ETag (`RecordInfo.ETag`), `EtagState.SetValsForRecordWithEtag` is called to set `etagSkippedStart` (8 bytes) and read the etag value from the payload prefix.
4. **Response formatting**: For plain GET, `CopyRespTo` formats the value as a RESP bulk string, skipping the ETag prefix. For GETWITHETAG/GETIFNOTMATCH, `CopyRespWithEtagData` returns both the etag and value. For commands with specific read formatting needs (GETBIT, BITCOUNT, TTL, GETRANGE, etc.), `CopyRespToWithInput` dispatches based on the command type.

The duplication between SingleReader and ConcurrentReader exists because Tsavorite calls them in different concurrency contexts; the logic is intentionally kept identical.

[Source: libs/server/Storage/Functions/MainStore/ReadMethods.cs:L16-L113]
[Source: libs/server/Storage/Functions/MainStore/ReadMethods.cs:L116-L213]

### 2.4 Write Path: SingleWriter and ConcurrentWriter

Upsert operations (SET without conditions) go through the writer callbacks:

- **`SingleWriter`**: Called for new records. Clears any existing ETag flag on the record, then delegates to `SpanByteFunctions.DoSafeCopy` which handles length validation and byte copying. The expiration metadata is set from `input.arg1`.
- **`ConcurrentWriter`**: Called for in-place updates to existing records in the mutable region. Same logic as `SingleWriter` plus it increments the watch version map (only if the record was not already marked Modified, preventing double-increment during RCU).
- **`PostSingleWriter` / `PostUpsertOperation`**: After successful writes, increments the watch version and sets the `NeedAofLog` flag. `PostUpsertOperation` then writes the AOF entry via `WriteLogUpsert`.

[Source: libs/server/Storage/Functions/MainStore/UpsertMethods.cs:L14-L57]

### 2.5 RMW Path: The Core Mutation Engine

Read-Modify-Write is the most complex path, implementing all conditional and arithmetic mutations. It proceeds through multiple phases:

**Phase 1 -- NeedInitialUpdate** (line 25): Determines whether to create a new record if the key does not exist. Returns `false` for commands that require an existing key: `SETKEEPTTLXX`, `PERSIST`, `EXPIRE`, `GETDEL`, `DELIFEXPIM`, `GETEX`, `DELIFGREATER`. For `SETIFGREATER`/`SETIFMATCH`, sets `etagOffsetForVarlen = EtagSize` (8) to reserve space for the ETag in the new record.

**Phase 2 -- InitialUpdater** (line 79): Creates the initial value for a new record. Command-specific logic includes:
- `PFADD`: Initializes a HyperLogLog sparse representation.
- `SET`/`SETEXNX`: Copies input value, optionally prepends an 8-byte ETag (initialized to `NoETag + 1 = 1`).
- `SETIFMATCH`/`SETIFGREATER`: Copies value with ETag from client.
- `INCR`/`INCRBY`/`DECR`/`DECRBY`: Creates a numeric string representation.
- `SETBIT`/`BITFIELD`: Allocates bitmap of required size.
- `SETRANGE`: Zero-fills gap before offset, copies value.
- `VADD`: Creates a DiskANN vector index.

**Phase 3 -- InPlaceUpdater** (line 339): Attempts to modify the record in place without allocation. Internally uses an `IPUResult` enum (`Failed`=0, `Succeeded`=1, `NotUpdated`=2) to drive the return value to Tsavorite. Returning `false` (Failed) tells Tsavorite to proceed to NeedCopyUpdate/CopyUpdater. Returning `true` with a modification completes the operation. The `RMWAction` field on `RMWInfo` provides additional control: `ExpireAndResume` tells Tsavorite to delete the expired record and retry the RMW from the beginning (triggers NeedInitialUpdate again), while `ExpireAndStop` deletes the record and returns without retrying (used by GETDEL, DELIFEXPIM). `CancelOperation` aborts the RMW entirely. The flow:
1. Check expiry -- if expired, set `rmwInfo.Action = RMWAction.ExpireAndResume` (or `ExpireAndStop` for DELIFEXPIM) and return Failed to trigger record deletion and retry.
2. If the record has an ETag, initialize `EtagState` from the payload.
3. Command-specific logic: For SET variants, check if the new value fits in the existing record space (`value.Length`). For INCR/DECR, `TryInPlaceUpdateNumber` attempts to update the numeric string in place (possible only if the digit count does not increase). For SETIFMATCH/SETIFGREATER, compare etags and conditionally update.
4. If `shouldUpdateEtag` is true at the end, increment the ETag in the payload.

**Phase 4 -- NeedCopyUpdate** (line 941): Called when InPlaceUpdater returns Failed. For SETEXNX, checks if the old value is expired (allows resume) or still valid (NX violated, returns false). For SETIFMATCH/SETIFGREATER, re-checks etag comparison and writes failure response if mismatch.

**Phase 5 -- CopyUpdater** (line 1075): Allocates a new record and copies the mutation result. Similar to InPlaceUpdater but operates on `oldValue` -> `newValue` pair. After copying, increments ETag if needed.

**Phase 6 -- PostCopyUpdater / PostRMWOperation**: Increments watch version and writes AOF log.

[Source: libs/server/Storage/Functions/MainStore/RMWMethods.cs:L25-L76]
[Source: libs/server/Storage/Functions/MainStore/RMWMethods.cs:L79-L321]
[Source: libs/server/Storage/Functions/MainStore/RMWMethods.cs:L339-L937]
[Source: libs/server/Storage/Functions/MainStore/RMWMethods.cs:L1075-L1499]

### 2.6 Delete Path

`SingleDeleter` and `ConcurrentDeleter` handle record deletion. Special handling exists for VectorSet records: if the value is non-zero length (index still active), the delete is cancelled via `CancelOperation` to prevent deletion while the DiskANN index is in use. Otherwise, the ETag is cleared and the watch version is incremented.

[Source: libs/server/Storage/Functions/MainStore/DeleteMethods.cs]

### 2.7 VarLenInputMethods and CallbackMethods

Two additional files in the MainStore functions directory complete the callback implementation:

**`VarLenInputMethods.cs`** implements the `GetLength` family of callbacks that Tsavorite calls to determine how much space to allocate for new records during RMW and Upsert operations. These methods use `etagState.etagOffsetForVarlen` to add 8 extra bytes when the operation will create a record with an ETag. This is the mechanism that connects the ETag allocation story in Section 2.7 to actual record sizing.

**`CallbackMethods.cs`** implements lifecycle callbacks including `CheckpointCompletionCallback` and `ReadCompletionCallback`. These are largely no-op in the current implementation but are required by the `ISessionFunctions` contract.

**`VectorSessionFunctions.cs`** implements a separate `ISessionFunctions` for the vector store, handling DiskANN-specific read/write/RMW operations on a third Tsavorite store instance.

[Source: libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs]
[Source: libs/server/Storage/Functions/MainStore/CallbackMethods.cs]
[Source: libs/server/Storage/Functions/MainStore/VectorSessionFunctions.cs]

### 2.8 ObjectSessionFunctions: Full Callback Chain

`ObjectSessionFunctions` implements `ISessionFunctions<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>` with its own complete set of callback files mirroring the main store structure:

- **ReadMethods.cs**: `SingleReader` and `ConcurrentReader` check expiry against `value.Expiration` (a `long` property, unlike the main store's inline metadata). If expired, returns false. Otherwise delegates to `value.Operate()` for command-specific read formatting. No ETag handling exists in the object store.
- **UpsertMethods.cs**: `SingleWriter` and `ConcurrentWriter` handle direct object assignment. After write, `objectStoreSizeTracker` is updated with the new object's `Size` property.
- **DeleteMethods.cs**: `SingleDeleter` and `ConcurrentDeleter` update the `objectStoreSizeTracker` by subtracting the deleted object's size, and increment the watch version.
- **PrivateMethods.cs**: Contains AOF log write helpers (`WriteLogRMW`, `WriteLogUpsert`, `WriteLogDelete`) for the object store, serializing the `ObjectInput` and key into the AOF.
- **VarLenInputMethods.cs**: Implements `GetLength` callbacks for the object store (simpler than main store since object values are heap references with fixed pointer size in the log).
- **CallbackMethods.cs**: Lifecycle callbacks, mostly no-ops.

The key difference from `MainSessionFunctions` is that object store in-place updates almost always succeed: since Tsavorite stores only a pointer to the heap object, the "value" in the log never changes size. The actual mutation happens on the heap object through `value.Operate()`. CopyUpdate is triggered only by version transitions (checkpointing), where `GarnetObjectBase.CopyUpdate()` handles the serialization state machine (see Document 11).

[Source: libs/server/Storage/Functions/ObjectStore/ObjectSessionFunctions.cs]
[Source: libs/server/Storage/Functions/ObjectStore/RMWMethods.cs]
[Source: libs/server/Storage/Functions/ObjectStore/ReadMethods.cs]
[Source: libs/server/Storage/Functions/ObjectStore/UpsertMethods.cs]
[Source: libs/server/Storage/Functions/ObjectStore/DeleteMethods.cs]
[Source: libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs]
[Source: libs/server/Storage/Functions/ObjectStore/VarLenInputMethods.cs]
[Source: libs/server/Storage/Functions/ObjectStore/CallbackMethods.cs]

### 2.9 ETag State Machine (Main Store Only)

The `EtagState` struct tracks per-operation ETag context:
- `etagOffsetForVarlen`: Set to 8 when allocating records that need ETag space (used by `GetLength` callbacks).
- `etagSkippedStart`: Set to 8 for records with ETags, indicating the payload starts after the ETag.
- `etagAccountedLength`: The effective value length minus the ETag bytes.
- `etag`: The current ETag value read from the record.

`SetValsForRecordWithEtag` initializes all four fields from a record's payload. `ResetState` clears them back to defaults. Every RMW code path must ensure `ResetState` is called before returning, especially on early-exit paths, to avoid stale ETag state leaking into the next operation.

[Source: libs/server/Storage/Functions/EtagState.cs]

### 2.10 AOF Logging Strategy

AOF writes are deferred using the `UserData` flag pattern. During the main operation (InPlaceUpdater, CopyUpdater, SingleWriter), a `NeedAofLog` bit is set on the operation info struct. The actual write happens in the `Post*` callback (`PostRMWOperation`, `PostUpsertOperation`) by calling `WriteLogRMW` / `WriteLogUpsert` / `WriteLogDelete`. These methods serialize the key, input, and version into the AOF's `TsavoriteLog`. This deferred approach ensures no AOF entry is written for failed operations.

When `StoredProcMode` is true, individual operation AOF logging is suppressed; the entire stored procedure is logged as a single `StoredProcedure` entry.

[Source: libs/server/Storage/Functions/MainStore/RMWMethods.cs:L1492-L1499]
[Source: libs/server/Storage/Functions/MainStore/UpsertMethods.cs:L49-L56]

## 3. Source File Map

| File | Purpose |
|------|---------|
| `libs/server/Storage/Functions/MainStore/MainSessionFunctions.cs` | Struct declaration, `ISessionFunctions` interface binding |
| `libs/server/Storage/Functions/MainStore/RMWMethods.cs` | Full RMW callback chain (~1500 lines) |
| `libs/server/Storage/Functions/MainStore/ReadMethods.cs` | SingleReader, ConcurrentReader |
| `libs/server/Storage/Functions/MainStore/UpsertMethods.cs` | SingleWriter, ConcurrentWriter |
| `libs/server/Storage/Functions/MainStore/DeleteMethods.cs` | SingleDeleter, ConcurrentDeleter |
| `libs/server/Storage/Functions/MainStore/PrivateMethods.cs` | Helper methods: CopyTo, CopyRespTo, number operations, AOF log writers |
| `libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs` | GetLength callbacks for record sizing, uses etagOffsetForVarlen |
| `libs/server/Storage/Functions/MainStore/CallbackMethods.cs` | Lifecycle callbacks (CheckpointCompletionCallback, etc.) |
| `libs/server/Storage/Functions/MainStore/VectorSessionFunctions.cs` | Vector store ISessionFunctions for DiskANN |
| `libs/server/Storage/Functions/FunctionsState.cs` | Per-session shared state |
| `libs/server/Storage/Functions/EtagState.cs` | ETag offset/value tracking |
| `libs/server/Storage/Functions/ObjectStore/ObjectSessionFunctions.cs` | Object store callback struct |
| `libs/server/Storage/Functions/ObjectStore/RMWMethods.cs` | Object store RMW callbacks |
| `libs/server/Storage/Functions/ObjectStore/ReadMethods.cs` | Object store SingleReader, ConcurrentReader with expiry via Expiration property |
| `libs/server/Storage/Functions/ObjectStore/UpsertMethods.cs` | Object store SingleWriter, ConcurrentWriter with size tracking |
| `libs/server/Storage/Functions/ObjectStore/DeleteMethods.cs` | Object store deletion with CacheSizeTracker update |
| `libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs` | Object store AOF log write helpers |
| `libs/server/Storage/Functions/ObjectStore/VarLenInputMethods.cs` | Object store GetLength callbacks |
| `libs/server/Storage/Functions/ObjectStore/CallbackMethods.cs` | Object store lifecycle callbacks |
| `libs/server/Storage/Session/StorageSession.cs` | Session holder with BasicContext/LockableContext pairs |

## 4. Performance Notes

- **Struct callbacks**: `MainSessionFunctions` is a `readonly struct`, enabling the JIT to inline callback methods when Tsavorite's generic methods are specialized. This eliminates virtual dispatch on the hot path.
- **Deferred AOF writes**: The `NeedAofLog` flag in `UserData` avoids conditional branching inside the hot mutation path. The actual serialization happens after the record lock is released.
- **In-place update priority**: The InPlaceUpdater path is tried first because it avoids allocation and copy. CopyUpdater is the fallback when the value does not fit or when the record is in the read-only region.
- **ETag state as struct**: `EtagState` is a value type on `FunctionsState`, avoiding allocation. It is reset after every operation to prevent stale state.
- **Watch version map**: Uses `Interlocked.Increment` on a power-of-2 `long[]` array, providing O(1) version tracking with minimal contention when the array is large enough relative to the number of watched keys.

## 5. Rust Mapping Notes

- **ISessionFunctions trait**: Map to a Rust trait with associated types for Key, Value, Input, Output, Context. The callback methods become trait methods.
- **readonly struct -> non-allocating implementation**: In Rust, implement the trait directly on a struct that holds `&FunctionsState`. No heap allocation needed.
- **SpanByte operations**: Replace with `&[u8]` / `&mut [u8]` slices. The in-place update pattern maps naturally to mutable slice operations.
- **EtagState**: Can be a stack-local struct passed by `&mut` reference. The reset discipline translates well to Rust's ownership model -- consider using a guard pattern that resets on drop.
- **Interlocked operations**: Use `std::sync::atomic::AtomicI64` with `Ordering::SeqCst` for the watch version map.
- **MemoryPool**: Replace with a custom slab allocator or use `Vec<u8>` recycling. Consider `bumpalo` for arena-style allocation within a session.
- **IPUResult enum**: Direct mapping to a Rust enum with `#[repr(u8)]`.
- **Partial struct / multiple files**: Rust uses `impl` blocks in separate files with `mod` declarations. The pattern maps cleanly.

## 6. Kotlin/JVM Mapping Notes

- **ISessionFunctions interface**: Map to a Kotlin interface with generic type parameters. The JIT will optimize virtual dispatch for common implementations.
- **Value type semantics**: JVM does not have value types (pre-Valhalla). `EtagState` would need to be an object with explicit pooling or a ThreadLocal to avoid allocation pressure.
- **SpanByte**: Map to `ByteBuffer` (direct) for off-heap or `ByteArray` for on-heap. The in-place update pattern requires careful buffer management.
- **Interlocked operations**: Use `java.util.concurrent.atomic.AtomicLongArray` for the watch version map.
- **Unsafe pointer arithmetic**: Replace with `ByteBuffer.position()` / `ByteBuffer.limit()` management or `sun.misc.Unsafe` for performance-critical paths.
- **Struct inlining**: Without value types, consider making `MainSessionFunctions` a final class and relying on JIT devirtualization of final method calls.

## 7. Open Questions

1. **ETag reset discipline**: The current code has many early-return paths that must each call `EtagState.ResetState`. This is fragile. Should the reimplementation use RAII/try-finally to guarantee cleanup?
2. **SingleReader / ConcurrentReader duplication**: The two methods are nearly identical. Could a single generic implementation be shared, parameterized on the concurrency context?
3. **VectorSet special cases**: The VectorSet handling (VADD/VREM) is interleaved with standard string operations in RMWMethods. Should vector operations be factored into a separate callback implementation?
4. **Custom command dispatch**: The `cmd > LastValidCommand` pattern for custom commands requires a stable command ID space. How should this be managed in a reimplementation?
5. **AOF serialization format**: The current format serializes raw `SpanByte` pointers. A reimplementation needs a portable binary format -- should it match Redis AOF format for compatibility?
