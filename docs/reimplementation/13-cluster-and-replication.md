# 13 -- Cluster & Replication

## 1. Overview

Garnet implements a Redis-compatible cluster mode following the Redis Cluster specification: 16,384 hash slots distributed across nodes, gossip-based failure detection, and AOF-based primary-replica replication. The cluster subsystem is architecturally separated from the core server in the `libs/cluster/` directory and communicates with the server through the `IClusterProvider` interface, allowing Garnet to run in both standalone and cluster modes without code changes to the core.

The three pillars of the cluster system are:
1. **ClusterConfig**: An immutable copy-on-write configuration object that maps hash slots to worker nodes and tracks node roles, epochs, and replication relationships.
2. **Gossip**: A configurable protocol (broadcast or sample-based) that propagates configuration changes across the cluster.
3. **ReplicationManager**: Manages the primary-replica lifecycle including checkpoint synchronization, AOF streaming, and failover.

[Source: libs/cluster/Server/ClusterConfig.cs:L22-L92]
[Source: libs/cluster/Server/Gossip/Gossip.cs:L16-L30]
[Source: libs/cluster/Server/Replication/ReplicationManager.cs:L20-L65]

## 2. Detailed Design

### 2.1 ClusterConfig: Immutable Copy-on-Write Configuration

`ClusterConfig` is an immutable data structure containing:

- **`HashSlot[] slotMap`**: A 16,384-element array mapping each hash slot to a worker. Each `HashSlot` is a 3-byte struct at explicit layout:
  - Offset 0: `ushort _workerId` (2 bytes) -- index into the workers array
  - Offset 2: `SlotState _state` (1 byte) -- OFFLINE, STABLE, MIGRATING, IMPORTING, FAIL, NODE, INVALID

  The `workerId` property has special behavior: for `MIGRATING` state, it returns `1` (LOCAL_WORKER_ID) instead of the actual target worker ID, because a migrating slot is still locally owned until migration completes.

- **`Worker[] workers`**: Array of node descriptors. Index 0 is reserved (UNASSIGNED), index 1 is always the local node (LOCAL_WORKER_ID). Each `Worker` contains: `Nodeid`, `Address`, `Port`, `ConfigEpoch`, `Role` (PRIMARY/REPLICA/UNASSIGNED), `ReplicaOfNodeId`, `ReplicationOffset`, `hostname`.

**Copy-on-write pattern**: All mutations return a new `ClusterConfig` instance. The `Copy()` method does a shallow copy of both arrays. Mutations like `SetSlotState`, `AddWorker`, `SetLocalWorkerRole` operate on the copy. The new config is then atomically published via CAS on a volatile reference. This ensures readers always see a consistent snapshot without locks.

**Constants**:
- `RESERVED_WORKER_ID = 0`
- `LOCAL_WORKER_ID = 1`
- `MIN_HASH_SLOT_VALUE = 0`
- `MAX_HASH_SLOT_VALUE = 16384`

[Source: libs/cluster/Server/ClusterConfig.cs:L22-L107]
[Source: libs/cluster/Server/HashSlot.cs:L48-L67]

### 2.2 HashSlot and SlotState

The `HashSlot` struct uses `LayoutKind.Explicit` for compact 3-byte representation:

```
[FieldOffset(0)] ushort _workerId
[FieldOffset(2)] SlotState _state
```

`SlotState` enum values:
- **OFFLINE** (0): Slot not assigned to any node.
- **STABLE** (1): Slot assigned and serving requests.
- **MIGRATING** (2): Slot being moved to another node. Reads still served locally; writes may be redirected.
- **IMPORTING** (3): Node is receiving this slot from another node.
- **FAIL** (4): Slot's owner is in FAIL state.
- **NODE** (5): Not a real state; used with CLUSTER SETSLOT NODE command.
- **INVALID** (6): Invalid state marker.

The `IsLocal(ushort slot)` method checks if `workerId == LOCAL_WORKER_ID`. For MIGRATING slots, the property getter returns LOCAL_WORKER_ID, so `IsLocal` returns true -- this is correct because a migrating slot is still owned locally until the migration is finalized.

[Source: libs/cluster/Server/HashSlot.cs:L11-L67]

### 2.3 Slot Verification

On every command in cluster mode, the server verifies that the accessed key's hash slot is owned by the local node. The hash slot is computed using CRC16 of the key (or the hash tag within `{...}` brackets). If the slot is not local:
- **STABLE on another node**: Return `-MOVED slot host:port`.
- **MIGRATING**: Return `-ASK slot host:port` (telling the client to retry on the target).
- **IMPORTING**: Accept the command only if preceded by ASKING.

This verification happens in the RESP command processing loop before any storage operation.

### 2.4 Gossip Protocol

The gossip system is implemented in the `ClusterManager` class (partial class spanning multiple files). Key parameters:

- **`gossipDelay`**: Interval between gossip rounds (configurable).
- **`clusterTimeout`**: How long to wait before marking a node as potentially failed.
- **`GossipSamplePercent`**: Determines whether to use broadcast or sample-based gossip.
- **`maxRandomNodesToPoll = 3`**: For sample-based gossip, the number of random nodes to consider.

**Gossip modes**:

1. **Broadcast** (`BroadcastGossipSend`): Sends gossip to all connected nodes. Simple but O(n) per round.
2. **Sample-based** (`GossipSampleSend`): Calculates `count = max(min(1, nodeCount), ceil(nodeCount * GossipSamplePercent / 100))`. Critically, `count` is a **failure budget**, NOT a success target. The algorithm (`Gossip.cs:L395-L444`):

   ```
   startTime = now
   while count > 0:
       pick maxRandomNodesToPoll (3) random nodes via GetRandomConnection
       select the one with earliest GossipSend timestamp (< startTime)
       if no candidate found: break  // all nodes are fresh
       if TryGossip succeeds:
           gossip_success_count++
           continue          // SUCCESS: does NOT decrement count
       else (timeout/exception):
           remove failed connection
           count--            // FAILURE: decrements count
   ```

   The `continue` on success (line 427) skips the `count--` at line 441. This means the loop gossips to **as many nodes as possible** until either: (a) no node has a stale-enough timestamp (`currNode == null`), or (b) `count` failures accumulate. The effective behavior is: contact all nodes whose `GossipSend < startTime`, stopping only after `count` failures. This is fundamentally different from contacting exactly `count` nodes -- in a healthy cluster, the loop contacts potentially ALL connected nodes per round. The failure budget prevents infinite retries when nodes are unreachable. Distinctness is not enforced, though successful gossips update the node's timestamp, making re-selection unlikely within the same round.

[Source: libs/cluster/Server/Gossip/Gossip.cs:L395-L444]

**Main gossip loop** (`GossipMain`): Periodic loop with configurable delay. Each iteration:
1. Choose broadcast or sample based on `GossipSamplePercent`.
2. Send gossip messages containing the local config.
3. Receive and merge incoming configs.

**Config merging** (`TryMerge`): CAS-based config update:
1. Read current config.
2. Call `currentConfig.Merge(incomingConfig)` which creates a new config respecting config epochs (higher epoch wins for slot ownership claims).
3. CAS the new config into place.
4. Flush to disk via `FlushConfig`.

**Config epoch collision handling**: When two nodes claim the same config epoch, `HandleConfigEpochCollision` resolves by node-ID comparison (lexicographic). The node with the "lesser" ID bumps its epoch. This is deterministic, ensuring all nodes converge to the same resolution.

**Ban list**: The `workerBanList` (ConcurrentDictionary) temporarily blocks gossip from specific nodes. Used during FORGET operations to prevent re-introduction of forgotten nodes.

**Merge suspension**: The `activeMergeLock` (SingleWriterMultiReaderLock) allows FORGET operations to block config merges while executing, preventing race conditions where a forgotten node is re-added by a concurrent gossip merge.

[Source: libs/cluster/Server/Gossip/Gossip.cs:L16-L100]

### 2.5 Replication Architecture

The `ReplicationManager` manages the full replication lifecycle:

**Components**:
- **`AofProcessor aofProcessor`**: Replays AOF entries received from the primary.
- **`CheckpointStore checkpointStore`**: Manages checkpoint files for initial sync.
- **`ReplicationSyncManager replicationSyncManager`**: Coordinates the sync process.
- **`SingleWriterMultiReaderLock recoverLock` / `recoveryStateChangeLock`**: Protect recovery state transitions.

**Replication offset tracking**:
- **Primary**: `ReplicationOffset` returns `appendOnlyFile.TailAddress` if AOF is enabled and `TailAddress > kFirstValidAofAddress`, otherwise returns `kFirstValidAofAddress` as a minimum floor. This fallback handles the case where AOF has not yet received any entries.
- **Replica**: Maintains a separate `replicationOffset` field that is updated as AOF entries are received and applied.
- **`ReplicationOffset2`**: Tracks the last valid offset for a previous primary (after failover), enabling partial resynchronization.

**Recovery states** (`RecoveryStatus` enum, defined in `RecoveryStatus.cs`):
- `NoRecovery` (0): Normal operation, no recovery in progress.
- `InitializeRecover` (1): Recovery during server initialization (startup with existing data).
- `ClusterReplicate` (2): Node is in the process of becoming a replica of another node.
- `ClusterFailover` (3): Node is recovering during a failover operation.
- `ReplicaOfNoOne` (4): Node is promoting from replica to standalone primary.
- `CheckpointRecoveredAtReplica` (5): Checkpoint from primary has been recovered at this replica; ready for AOF streaming.
- `ReadRole` (6): Reading role information; prevents role changes during commits or checkpoints.

Each state corresponds to a specific recovery scenario that restricts certain operations.

[Source: libs/cluster/Server/Replication/RecoveryStatus.cs:L9-L39]

**`IsRecovering`**: Returns true for any state other than `NoRecovery` or `ReadRole`. When recovering, certain operations are restricted.
**`CannotStreamAOF`**: Returns true when recovering and not yet at `CheckpointRecoveredAtReplica` state.

[Source: libs/cluster/Server/Replication/ReplicationManager.cs:L20-L120]

### 2.6 Replication Sync Protocol

The initial sync between a primary and a new replica follows this sequence:

1. **Replica sends CLUSTER REPLICATE**: Declares intent to replicate from the primary.
2. **Primary initiates checkpoint**: Takes a checkpoint of both stores.
3. **Checkpoint transfer**: Primary sends checkpoint files to replica. Two modes:
   - **Disk-based**: Checkpoint files are written to disk and transferred via the network.
   - **Diskless sync** (`ReplicaDisklessSync`): Checkpoint data is streamed directly without intermediate disk writes, using streaming checkpoint markers (`MainStoreStreamingCheckpointStartCommit`/`EndCommit`).
4. **Replica recovers checkpoint**: Loads the checkpoint into its stores.
5. **AOF streaming begins**: Primary starts streaming AOF entries from the point after the checkpoint (see [Doc 14 Section 2.3](./14-aof-and-durability.md#23-aof-write-path) for the AOF entry format and [Doc 14 Section 2.4](./14-aof-and-durability.md#24-aofprocessor-replay-engine) for the replay engine).
6. **Continuous replication**: Primary streams AOF entries as they are appended. Replica applies them through the `AofProcessor`. The streaming checkpoint markers (`MainStoreStreamingCheckpointStartCommit`/`EndCommit`) are described in [Doc 14 Section 2.2](./14-aof-and-durability.md#22-aofentrytype-operation-classification).

**Safe AOF address tracking**: The `GetRecoveredSafeAofAddress()` and `GetCurrentSafeAofAddress()` methods return the minimum safe address across both stores, ensuring that AOF truncation does not remove entries needed by either store.

### 2.7 Failover

Failover is managed by the `FailoverManager` and dedicated session classes in `libs/cluster/Server/Failover/`:

- **`ReplicaFailoverSession`**: Manages the replica-side failover. When a primary fails, a replica initiates failover by bumping its config epoch, calling `TakeOverFromPrimary` on the `ClusterConfig` (which reassigns all of the primary's slots to the local node and sets role to PRIMARY), and broadcasting the updated config via gossip.
- **`PrimaryFailoverSession`**: Manages graceful failover initiated by the primary (e.g., `CLUSTER FAILOVER`), coordinating with the target replica.
- **`FailoverStatus`**: Tracks the failover progress (e.g., waiting for offset sync, epoch bump, slot reassignment).

The failover sequence:
1. Replicas detect primary failure via gossip timeout.
2. A replica initiates failover: bumps config epoch, takes over slots via `TakeOverFromPrimary`.
3. `ReplicationOffset2` is set to the last known offset from the old primary.
4. Updated config is broadcast to the cluster.
5. Other replicas can perform partial resync if their offset is within the new primary's history.

[Source: libs/cluster/Server/Failover/ReplicaFailoverSession.cs]
[Source: libs/cluster/Server/Failover/PrimaryFailoverSession.cs]
[Source: libs/cluster/Server/Failover/FailoverManager.cs]

### 2.8 ClusterProvider Interface

The `IClusterProvider` interface decouples the cluster subsystem from the core server:
- `IsClusterEnabled`: Whether clustering is active.
- `GetClusterConfig()`: Returns current configuration.
- `ValidateSlot(...)`: Hash slot verification for commands.
- `GetReplicationManager()`: Access to replication state.

When clustering is disabled, a null or stub provider is used, and all cluster-related code paths are skipped.

## 3. Source File Map

| File | Purpose |
|------|---------|
| `libs/cluster/Server/ClusterConfig.cs` | Immutable config with slotMap and workers arrays |
| `libs/cluster/Server/HashSlot.cs` | 3-byte HashSlot struct and SlotState enum |
| `libs/cluster/Server/Gossip/Gossip.cs` | Gossip protocol: main loop, broadcast/sample, merge |
| `libs/cluster/Server/ClusterManager.cs` | Cluster state management, config publishing |
| `libs/cluster/Server/Replication/ReplicationManager.cs` | Replication lifecycle and offset tracking |
| `libs/cluster/Server/Replication/ReplicationSyncManager.cs` | Sync coordination |
| `libs/cluster/Server/Replication/CheckpointStore.cs` | Checkpoint file management |
| `libs/cluster/Server/Migration/` | Slot migration implementation |
| `libs/cluster/Server/Failover/FailoverManager.cs` | Failover lifecycle management |
| `libs/cluster/Server/Failover/ReplicaFailoverSession.cs` | Replica-initiated failover (epoch bump, slot takeover) |
| `libs/cluster/Server/Failover/PrimaryFailoverSession.cs` | Primary-initiated graceful failover |
| `libs/cluster/Server/Failover/FailoverStatus.cs` | Failover progress tracking enum |
| `libs/cluster/Server/Replication/RecoveryStatus.cs` | Recovery status enum (7 states) |
| `libs/cluster/Server/ClusterManagerWorkerState.cs` | Config epoch management, FORGET, RESET |
| `libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs` | Gossip transport: connection pool management |
| `libs/cluster/Server/Gossip/GarnetServerNode.cs` | Per-node gossip state (timestamps, connection) |
| `libs/cluster/Server/Worker.cs` | Worker (node) descriptor struct |
| `libs/cluster/IClusterProvider.cs` | Interface decoupling cluster from server |

## 4. Performance Notes

- **Copy-on-write config**: No locks needed for readers. Config mutations are O(16384) for the slot array copy, but this is rare (only on config changes) and the 16384-element array is ~49KB, easily fitting in cache.
- **HashSlot compactness**: 3 bytes per slot means the entire slot map is ~49KB. This fits in L1 cache on most CPUs, making slot lookups fast.
- **Gossip sample strategy**: The failure-budget design means sample-based gossip is nearly O(n) in healthy clusters (contacting all stale nodes), similar to broadcast. The `count` failure budget prevents runaway retries when nodes are down. Power-of-2 random choices for candidate selection bias toward the stalest nodes, ensuring rapid convergence.
- **AOF-based replication**: Streaming raw AOF entries avoids re-serialization. The primary simply forwards its log tail to replicas.
- **Volatile config reference**: The `CurrentConfig` reference is volatile, providing lock-free reads with acquire semantics. Writers use CAS for atomic updates.

## 5. Rust Mapping Notes

- **ClusterConfig immutability**: Use `Arc<ClusterConfig>` for shared ownership. Mutations clone, modify, then `AtomicPtr::compare_and_swap` the Arc.
- **HashSlot struct**: `#[repr(C, packed)]` with `u16` + `u8` for 3-byte layout. Consider alignment padding if performance testing shows unaligned access issues.
- **SlotState enum**: `#[repr(u8)]` enum.
- **Gossip protocol**: Async gossip loop using `tokio::select!` for timer + message reception.
- **Copy-on-write config swap**: Use `arc_swap::ArcSwap<ClusterConfig>` for lock-free CAS on the config reference. This is more ergonomic than raw `AtomicPtr`.
- **Replication**: Use Tokio TCP streams for checkpoint transfer and AOF streaming. The AOF streaming can use a `broadcast` channel or a custom ring buffer.
- **Worker array**: Use a `Vec<Worker>` inside the config. Since configs are immutable after creation, no synchronization needed for reads.
- **Ban list**: `DashMap<String, i64>` for concurrent access.

## 6. Kotlin/JVM Mapping Notes

- **ClusterConfig immutability**: Use an immutable data class with `AtomicReference<ClusterConfig>` for CAS-based updates. Kotlin's `copy()` on data classes is convenient for the copy-on-write pattern.
- **HashSlot**: Cannot achieve 3-byte layout on JVM. Use a `ShortArray` for worker IDs and a `ByteArray` for states as parallel arrays to minimize object overhead.
- **Gossip protocol**: Use Kotlin coroutines with `delay()` for the gossip timer. `Channel<GossipMessage>` for message passing.
- **Replication**: Use Netty or Ktor for network I/O. AOF streaming maps to Netty's `ChunkedWriteHandler`.
- **Copy-on-write**: `AtomicReference<ClusterConfig>` with `compareAndSet`.
- **Ban list**: `ConcurrentHashMap<String, Long>`.
- **Slot map**: Consider a `ShortArray(16384)` + `ByteArray(16384)` for compact representation, avoiding object allocation per slot.

## 7. Open Questions

1. **Gossip protocol evolution**: The current gossip implementation follows Redis Cluster's protocol. Should a reimplementation use a more modern protocol (e.g., SWIM) for faster failure detection?
2. **Partial resync**: How robust is the ReplicationOffset2-based partial resync? Under what conditions does it degrade to full resync?
3. **Diskless sync complexity**: Streaming checkpoints add significant complexity (separate markers for main/object store). Is the performance benefit over disk-based sync sufficient to justify the complexity?
4. **Config epoch resolution**: The node-ID-based epoch collision resolution is deterministic but could repeatedly penalize the same node. Should a randomized backoff be used instead?
5. **Multi-database cluster**: The current architecture ties replication to a per-database AOF. How should cross-database cluster operations (like FLUSHALL) be coordinated?
6. **Slot migration atomicity**: During migration, there is a window where the slot is MIGRATING on the source and IMPORTING on the target. How are in-flight transactions handled?
7. **Slot migration protocol details**: The slot migration protocol (CLUSTER SETSLOT MIGRATING/IMPORTING, key-by-key MIGRATE, CLUSTER SETSLOT NODE finalization) is implemented in `libs/cluster/Server/Migration/` but not documented in this document. A reimplementation needs the full protocol: how keys are enumerated for migration, how MIGRATE serializes and transfers individual keys, how the source handles requests for already-migrated keys (ASK redirection), and how SETSLOT NODE finalizes ownership on both source and target.
