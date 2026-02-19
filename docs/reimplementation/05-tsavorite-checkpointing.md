# Tsavorite Checkpointing & Recovery

## 1. Overview

Tsavorite's checkpointing system enables **consistent point-in-time snapshots** of the entire key-value store (hash index + hybrid log) that can be used for crash recovery. The system supports multiple checkpoint strategies -- fold-over, snapshot, and incremental snapshot -- each with different tradeoffs between speed, space, and recovery time.

Checkpointing is orchestrated by a **state machine framework** that coordinates global phase transitions across all threads using epoch-based synchronization. The state machine ensures that a consistent "cut" of the log is captured even while concurrent operations continue.

## 2. Detailed Design

### 2.1 SystemState: Phase + Version

The global system state is a packed 64-bit value containing a phase (8 bits) and a version number (56 bits): [Source: StateTransitions.cs:L57-L111]

```
Bits 63-56: Phase (enum)
Bits 55-0:  Version (monotonically increasing)
```

Phases defined in the `Phase` enum: [Source: StateTransitions.cs:L27-L52]

| Phase | Value | Description |
|-------|-------|-------------|
| `IN_PROGRESS` | 0 | Entering new version (v+1) |
| `WAIT_INDEX_CHECKPOINT` | 1 | Waiting for index checkpoint I/O |
| `WAIT_FLUSH` | 2 | Waiting for hybrid log flush |
| `PERSISTENCE_CALLBACK` | 3 | Writing metadata and issuing callbacks |
| `REST` | 4 | No state machine active (steady state) |
| `PREPARE` | 5 | Preparing for checkpoint, still in version v |
| `PREPARE_GROW` | 6 | Preparing for index resize |
| `IN_PROGRESS_GROW` | 7 | Index resize in progress |

### 2.2 State Machine Architecture

The state machine framework consists of three layers:

**IStateMachineTask**: Individual tasks that implement per-phase logic via `GlobalBeforeEnteringState` and `GlobalAfterEnteringState`. [Source: IStateMachineTask.cs]

**StateMachineBase**: Composes multiple tasks and defines phase transitions via `NextState`. Delegates `GlobalBeforeEnteringState`/`GlobalAfterEnteringState` to all tasks in order. [Source: StateMachineBase.cs:L10-L40]

**StateMachineDriver**: The execution engine. Runs the state machine loop asynchronously: [Source: StateMachineDriver.cs:L319-L355]

```
loop:
  GlobalStateMachineStep(currentState)     // Advance to next phase
  ProcessWaitingListAsync()                // Wait for I/O completion
  if state == REST: break
```

### 2.3 State Machine Step Execution

`GlobalStateMachineStep` is the critical coordination method: [Source: StateMachineDriver.cs:L215-L253]

1. Call `stateMachine.NextState(systemState)` to determine the next phase.
2. Call `stateMachine.GlobalBeforeEnteringState(nextState)` -- this is where most checkpoint logic runs.
3. **Write the new phase** to `systemState.Word` (atomic store, visible to all threads).
4. Create new semaphores for `waitForTransitionIn` and `waitForTransitionOut`.
5. **Bump the epoch** with a deferred action: `epoch.BumpCurrentEpoch(() => MakeTransitionWorker(nextState))`. Critically, `GlobalStateMachineStep` asserts the thread is NOT epoch-protected on entry (`Debug.Assert(!epoch.ThisInstanceProtected())`; StateMachineDriver.cs:L243) and then acquires protection internally via `epoch.Resume()`. The `BumpCurrentEpoch` call (which requires epoch protection, LightEpoch.cs:L344) happens inside this protected region. A reimplementor who pre-protects the thread before calling `GlobalStateMachineStep` would hit the assertion. [Source: StateMachineDriver.cs:L243-L246]
6. The `MakeTransitionWorker` (which fires when all threads have seen the new epoch) calls `GlobalAfterEnteringState`.

This two-phase design (Before + After) allows actions that must happen before any thread sees the new state (Before) and actions that require all threads to have acknowledged the transition (After).

### 2.4 Version Change State Machine

`VersionChangeSM` is the base state machine for version bumps: [Source: VersionChangeSM.cs:L9-L40]

```
REST -> PREPARE (still in version v)
PREPARE -> IN_PROGRESS (version becomes v+1)
IN_PROGRESS -> REST
```

During `PREPARE -> IN_PROGRESS`, the version number increments. All threads eventually see the new version through `ProtectAndDrain`, creating a "fuzzy boundary" in the log where records before the boundary are version v and records after are version v+1. The CPR consistency checks in each operation (see [Doc 06 Section 2.12](./06-tsavorite-record-management.md#212-end-to-end-operation-flows)) enforce correct behavior during this transition.

### 2.5 Hybrid Log Checkpoint (HybridLogCheckpointSMTask)

This is the base class for log checkpoint tasks. Per-phase logic: [Source: HybridLogCheckpointSMTask.cs:L31-L98]

| Phase | GlobalBeforeEnteringState | GlobalAfterEnteringState |
|-------|--------------------------|-------------------------|
| `PREPARE` | Record `startLogicalAddress` (tail at checkpoint start), `beginAddress`, version | - |
| `IN_PROGRESS` | Call `CheckpointVersionShiftStart` (mark records with InNewVersion bit) | `TrackLastVersion` (wait for active transactions in old version to drain) |
| `WAIT_FLUSH` | Call `CheckpointVersionShiftEnd`, record `finalLogicalAddress` (tail at end of fuzzy region), `headAddress`, `nextVersion` | - |
| `PERSISTENCE_CALLBACK` | Write checkpoint metadata to persistent storage | - |
| `REST` | Cleanup checkpoint state, signal completion | - |

The **fuzzy region** is the log range `[startLogicalAddress, finalLogicalAddress)` that contains records from both version v and v+1. Recovery must handle this region specially.

### 2.6 Fold-Over Checkpoint (FoldOverSMTask)

The simplest checkpoint strategy. During `WAIT_FLUSH`: [Source: FoldOverSMTask.cs:L23-L57]

1. Call `ShiftReadOnlyToTail` to move the ReadOnlyAddress to the current tail, forcing all in-memory data to be flushed.
2. Wait for the flush to complete via a semaphore.
3. The `finalLogicalAddress` is set to the flushed tail.

**Pros**: Simple, fast checkpoint initiation.
**Cons**: Advances ReadOnlyAddress, so subsequent updates require RCU (copy-on-write) until the mutable region refills. Recovery must replay the fuzzy region.

### 2.7 Snapshot Checkpoint (SnapshotCheckpointSMTask)

Creates a separate copy of the in-memory log without advancing ReadOnlyAddress: [Source: SnapshotCheckpointSMTask.cs:L23-L93]

During `PREPARE`: Sets `useSnapshotFile = 1` in checkpoint info.

During `WAIT_FLUSH`:
1. Record `snapshotFinalLogicalAddress` and `snapshotStartFlushedLogicalAddress`.
2. Create a snapshot device and object log device.
3. Call `AsyncFlushPagesToDevice` to write the in-memory pages (from `snapshotStartFlushedLogicalAddress` to `finalLogicalAddress`) to the snapshot file.

During `PERSISTENCE_CALLBACK`: Record `flushedLogicalAddress` and transfer checkpoint to `_lastSnapshotCheckpoint`.

**Pros**: Does not advance ReadOnlyAddress, so in-place updates continue uninterrupted. Better steady-state performance.
**Cons**: Requires writing a full copy of in-memory pages. More I/O and temporary storage.

### 2.8 Incremental Snapshot Checkpoint (IncrementalSnapshotCheckpointSMTask)

Builds on a previous full snapshot by writing only **delta records** (dirty records since the last snapshot): [Source: IncrementalSnapshotCheckpointSMTask.cs:L23-L77]

During `PREPARE`: Reuses the last snapshot checkpoint state.

During `WAIT_FLUSH`:
1. Creates a `DeltaLog` backed by a delta file device.
2. Calls `AsyncFlushDeltaToDevice`, which scans in-memory pages and writes only dirty records (identified by the `Dirty` bit in `RecordInfo`) to the delta log.
3. Each delta entry is: `[8-byte logical address][4-byte size][record bytes]`.

During `PERSISTENCE_CALLBACK`: Records `deltaTailAddress` and writes incremental metadata.

**Recovery**: Apply the base snapshot, then replay all delta logs in order.

### 2.9 Index Checkpoint (IndexCheckpointSMTask)

Persists the hash index to disk: [Source: IndexCheckpointSMTask.cs:L12-L65]

| Phase | Action |
|-------|--------|
| `PREPARE` | Initialize index checkpoint, record start logical address, call `TakeIndexFuzzyCheckpoint` (starts async write of hash buckets) |
| `WAIT_INDEX_CHECKPOINT` | Add a waiter for the index checkpoint I/O to complete |
| `WAIT_FLUSH` | Record `num_buckets` (overflow allocator high-water mark) and `finalLogicalAddress` |
| `PERSISTENCE_CALLBACK` | Write index metadata |
| `REST` | Cleanup |

### 2.10 Full Checkpoint (FullCheckpointSM)

Combines index and hybrid log checkpoints: [Source: FullCheckpointSM.cs:L9-L38]

Phase sequence: `REST -> PREPARE -> IN_PROGRESS -> WAIT_INDEX_CHECKPOINT -> WAIT_FLUSH -> PERSISTENCE_CALLBACK -> REST`

The `WAIT_INDEX_CHECKPOINT` phase is inserted between `IN_PROGRESS` and `WAIT_FLUSH` to allow the index checkpoint to complete before the log flush.

### 2.11 Recovery Protocol

Recovery reads checkpoint metadata, then: [Source: Recovery.cs:L160-L200]

1. Load the hybrid log checkpoint info (token, version, addresses).
2. Load the index checkpoint info (if available).
3. Reconstruct the hash index from the index checkpoint. This involves reading sector-aligned pages of hash buckets from the index checkpoint file plus overflow buckets from a separate overflow file. If no index checkpoint exists, the index must be rebuilt by scanning the log.
4. Read log pages from disk (either main log for fold-over, or snapshot file + delta logs for snapshot checkpoints).
5. For the fuzzy region, undo records marked `InNewVersion` if `undoNextVersion` is set (recovery to version v, not v+1). The `undoNextVersion` mechanism scans the fuzzy region **backwards** and sets `Invalid` on records that have `InNewVersion` set, effectively rolling back to version v. The scan direction and the use of `SetInvalid()` (not `SetTombstone`) matter for correctness -- invalid records are skipped by all operations. [Source: Recovery.cs:L160-L200]
6. Apply delta logs if using incremental snapshots. Each delta entry is `[8-byte logical address][4-byte size][record bytes]` (see Section 2.8). Delta entries are applied in order to overwrite the base snapshot's pages.
7. Set allocator addresses to their recovered values.

### 2.12 Checkpoint Metadata

Key fields in `HybridLogRecoveryInfo`:
- `version`, `nextVersion`: Version before and after the checkpoint.
- `beginAddress`: Lowest valid address.
- `startLogicalAddress`: Tail at PREPARE phase (start of fuzzy region).
- `finalLogicalAddress`: Tail at WAIT_FLUSH phase (end of fuzzy region).
- `flushedLogicalAddress`: How far the main log is flushed on disk.
- `headAddress`: Head address at checkpoint time.
- `useSnapshotFile`: Whether a snapshot file was used.
- `objectLogSegmentOffsets`: Segment offsets for the object log (if applicable).

## 3. Source File Map

| File | Purpose | Key Lines |
|------|---------|-----------|
| `Index/Checkpointing/StateMachineDriver.cs` | State machine execution engine | L16-L382 |
| `Index/Checkpointing/StateTransitions.cs` | Phase enum, SystemState struct, ResizeInfo | L8-L187 |
| `Index/Checkpointing/StateMachineBase.cs` | Base class composing IStateMachineTask list | L10-L40 |
| `Index/Checkpointing/IStateMachine.cs` | Interface for state machines | Full file |
| `Index/Checkpointing/IStateMachineTask.cs` | Interface for checkpoint tasks | Full file |
| `Index/Checkpointing/VersionChangeSM.cs` | Base version change state machine | L9-L40 |
| `Index/Checkpointing/HybridLogCheckpointSM.cs` | Hybrid log checkpoint state machine | L9-L40 |
| `Index/Checkpointing/HybridLogCheckpointSMTask.cs` | Base task for hybrid log checkpoints | L14-L99 |
| `Index/Checkpointing/FoldOverSMTask.cs` | Fold-over checkpoint implementation | L13-L57 |
| `Index/Checkpointing/SnapshotCheckpointSMTask.cs` | Snapshot checkpoint implementation | L13-L93 |
| `Index/Checkpointing/IncrementalSnapshotCheckpointSMTask.cs` | Incremental snapshot implementation | L13-L77 |
| `Index/Checkpointing/IndexCheckpointSMTask.cs` | Index checkpoint implementation | L12-L65 |
| `Index/Checkpointing/FullCheckpointSM.cs` | Full (index + log) checkpoint | L9-L38 |
| `Index/Recovery/Recovery.cs` | Recovery logic | L160-L200 |
| `Index/CheckpointManagement/RecoveryInfo.cs` | Checkpoint metadata structures | Full file |

## 4. Performance Notes

- **Async state machine**: The state machine runs on a background Task, not blocking caller threads. Phase transitions use epoch bumps (which are amortized across threads) rather than explicit barriers.
- **Fuzzy checkpointing**: No need to quiesce the system. Operations continue during checkpointing; the fuzzy region is handled during recovery.
- **Incremental snapshots**: Only dirty records are written, dramatically reducing I/O for workloads with high locality. The `Dirty` bit in RecordInfo is set on every in-place update.
- **Throttled flush**: `ThrottleCheckpointFlushDelayMs` allows checkpoint I/O to be spread over time, reducing impact on foreground latency.
- **Transaction draining**: `TrackLastVersion` uses a semaphore to wait for active transactions in the old version to complete, without blocking new transactions in the new version.

## 5. Rust Mapping Notes

- The state machine can be implemented as an `async fn` that loops through phases, using `tokio::sync::Semaphore` for waiting.
- `SystemState` is an `AtomicU64` with bitfield extraction.
- The epoch-based `BumpCurrentEpoch(action)` pattern maps to a closure registered with the epoch system.
- Checkpoint I/O uses `tokio::fs` or `io_uring` for async writes.
- Delta log writing needs careful handling of concurrent page access; consider `RwLock` per page or epoch-protected scanning.
- The `IStateMachineTask` trait can be a Rust trait with `fn global_before_entering_state(&self, next: SystemState, driver: &StateMachineDriver)`.

## 6. Kotlin/JVM Mapping Notes

- The state machine loop maps to a Kotlin coroutine with `suspend` functions for async waits.
- `SystemState` can be an `AtomicLong` with bitwise operations for phase/version.
- Checkpoint I/O uses `AsynchronousFileChannel` or coroutine-wrapped NIO.
- The `IStateMachineTask` interface maps directly to a Kotlin interface.
- Delta log writing requires epoch protection or explicit synchronization to scan pages safely.
- Transaction tracking uses `AtomicLong` counters and `CompletableDeferred` for drain notification.

## 7. Open Questions

1. **Concurrent checkpoint initiation**: `StateMachineDriver.Register` returns false if a state machine is already running. How should callers handle checkpoint request queuing?
2. **Error handling**: If a state machine step throws, `FastForwardStateMachineToRest` resets the system. But partially-written checkpoint files may be left on disk. Is cleanup the caller's responsibility?
3. **Streaming snapshots**: `StreamingSnapshotCheckpointSM` exists but its use case (external replication?) needs documentation.
4. **Index checkpoint consistency**: The index is checkpointed during `PREPARE` (before version bump), but records continue to be inserted. How does recovery reconcile index entries that point to post-checkpoint records?
5. **Multi-store coordination**: Garnet uses two stores (main + object). How are their checkpoints coordinated to ensure a consistent cross-store snapshot?
