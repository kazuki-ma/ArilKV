# 12 -- Transactions & Scripting

## 1. Overview

Garnet implements Redis-compatible transactions (MULTI/EXEC) and stored procedures (RUNTXP) through a unified `TransactionManager` that orchestrates key locking, optimistic watch validation, and AOF logging. The transaction system uses Tsavorite's `LockableContext` for pessimistic key locking during execution, combined with a `WatchVersionMap` for optimistic concurrency control during the queuing phase.

The transaction lifecycle has three distinct phases: **queuing** (MULTI to EXEC, where commands are validated and keys are collected), **execution** (EXEC, where keys are locked and commands are replayed), and **completion** (commit to AOF and unlock). Stored procedures collapse these phases into a programmatic Prepare/Main/Finalize pattern.

The system supports both the main store and object store within a single transaction, using separate lockable contexts for each store and a unified sorted key array for deadlock-free lock acquisition.

[Source: libs/server/Transaction/TransactionManager.cs:L41-L95]
[Source: libs/server/Transaction/TxnRespCommands.cs:L14-L91]

## 2. Detailed Design

### 2.1 Transaction State Machine

Transactions progress through four states defined by the `TxnState` enum:

```
None -> Started -> Running -> (reset to None)
None -> Started -> Aborted -> (reset to None)
```

- **`None`** (0): No active transaction. Commands execute normally.
- **`Started`** (1): MULTI has been issued. Commands are being queued (skipped). Keys are collected for locking.
- **`Running`** (2): EXEC has acquired locks and is replaying queued commands.
- **`Aborted`** (3): A command during the queuing phase failed validation (wrong args, unsupported command in txn). EXEC will return an error.

The state is stored as a public field `TransactionManager.state` and checked by `IsSkippingOperations()` which returns true for `Started` or `Aborted` states. The RESP command processor uses this to route commands to `NetworkSKIP` during the queuing phase.

[Source: libs/server/Transaction/TxnState.cs:L9-L27]
[Source: libs/server/Transaction/TransactionManager.cs:L262-L266]

### 2.2 MULTI: Beginning a Transaction

`NetworkMULTI()` initializes the transaction:
1. Checks for nested MULTI (returns error if `state != None`).
2. Records `txnStartHead = readHead` -- the buffer position where the first queued command starts. This is used later to replay commands from the buffer.
3. Sets `state = TxnState.Started`.
4. Resets `operationCntTxn = 0` (count of queued commands).
5. Saves `recvBufferPtr` for cluster slot verification.

[Source: libs/server/Transaction/TxnRespCommands.cs:L19-L37]

### 2.3 Command Queuing: NetworkSKIP

When `state` is `Started` or `Aborted`, each command goes through `NetworkSKIP` instead of normal execution:

1. **Command validation**: Retrieves `RespCommandInfo` for the command and checks `AllowedInTxn`. Unknown commands or commands not allowed in transactions cause an abort.
2. **Arity validation**: Checks argument count against the command's arity specification. Positive arity means exact count; negative arity means minimum count.
3. **WATCH rejection**: WATCH/WATCHMS/WATCHOS inside MULTI are rejected with an error (but do not abort the transaction).
4. **Multi-database rejection**: SELECT (to a different DB) and SWAPDB are currently rejected inside transactions with an abort, because cross-database transactions are not yet supported.
5. **Key locking**: Calls `txnManager.LockKeys(commandInfo)` which inspects the command's key specification to determine which keys to lock and with what lock type (shared for read-only, exclusive for write). Keys are added to `TxnKeyEntries`.
6. **Queue response**: Writes `+QUEUED\r\n` to the client.
7. **Counter**: Increments `operationCntTxn`.

[Source: libs/server/Transaction/TxnRespCommands.cs:L97-L186]

### 2.4 TxnKeyEntry and TxnKeyEntries: Lock Management

The `TxnKeyEntry` struct is a compact 10-byte layout with explicit field offsets:

```
Offset 0: long keyHash    (8 bytes) -- Tsavorite hash of the key
Offset 8: bool isObject    (1 byte) -- whether this is an object store key
Offset 9: LockType lockType (1 byte) -- None/Shared/Exclusive
```

It implements `ILockableKey`, the interface Tsavorite uses for its locking protocol.

`TxnKeyEntries` manages the array of keys to lock:
- **AddKey**: Hashes the key using the appropriate context (main or object store), grows the array if needed (doubling strategy), and appends the entry. Tracks `mainKeyCount` separately from total `keyCount`.
- **LockAllKeys**: The critical locking method. The lock ordering is based on hash bucket identity, which ties into the hash index latch protocol (see [Doc 03 Section 2.4](./03-tsavorite-hash-index.md#24-latch-encoding-in-overflow-entry) for the bucket-level latch mechanism):
  1. Sorts the key array using `MemoryExtensions.Sort` with the `TxnKeyComparison.Compare` delegate. The comparison logic (`TxnKeyEntryComparison.cs:L31-L41`) first sorts by `isObject` (`false` < `true`, so main store keys come before object store keys), then within each store type, delegates to the respective lockable context's `CompareKeyHashes` which sorts by lock code (hash bucket) and then by lock type. This two-level sort is critical: it groups keys by store type (enabling separate `Lock` calls per store) while maintaining Tsavorite's required lock ordering within each store.
  2. Locks main store keys via `lockableContext.Lock<TxnKeyEntry>(keys[..mainKeyCount])`.
  3. Locks object store keys via `objectStoreLockableContext.Lock<TxnKeyEntry>(keys[mainKeyCount..keyCount])`.

  Note: Sorting happens INSIDE `LockAllKeys()`, not as a separate step before it. The code comment explains this is intentional: the hash table must be stable (not in GROW phase) during both sorting and locking, so both operations must occur within the same transaction flow.

  Sorting ensures deadlock freedom: all transactions acquire locks in the same global order.

[Source: libs/server/Transaction/TxnKeyEntryComparison.cs:L15-L42]

- **TryLockAllKeys**: Same as above but with a timeout. Returns false if locks cannot be acquired within the specified `TimeSpan`. Used by stored procedures with `FailFastOnKeyLockFailure`.

- **UnlockAllKeys**: Unlocks main store keys first (`lockableContext.Unlock` on `keys[..mainKeyCount]`), then object store keys (`objectStoreLockableContext.Unlock` on `keys[mainKeyCount..keyCount]`). Note: unlike lock acquisition, unlock ordering has no correctness implications for deadlock freedom -- Tsavorite's `Unlock` simply releases whatever is held regardless of order. Only the **lock acquisition order** (sorted, main store first) is critical for deadlock prevention. Resets all counts and flags to zero after unlocking.

[Source: libs/server/Transaction/TxnKeyEntry.cs:L20-L48]
[Source: libs/server/Transaction/TxnKeyEntry.cs:L50-L207]
[Source: libs/server/Transaction/TxnKeyEntry.cs:L177-L189]

### 2.5 WatchVersionMap: Optimistic Concurrency

The `WatchVersionMap` is a fixed-size power-of-2 `long[]` array shared across all sessions:

- **`ReadVersion(long keyHash)`**: `Interlocked.Read(ref map[keyHash & sizeMask])` -- reads the current version atomically.
- **`IncrementVersion(long keyHash)`**: `Interlocked.Increment(ref map[keyHash & sizeMask])` -- called by every mutation (in session function callbacks).

The watch protocol:
1. **WATCH key**: The session reads the current version from the map and stores it in a `WatchedKeysContainer`.
2. **Between WATCH and EXEC**: Other sessions may mutate the watched keys, incrementing their versions.
3. **EXEC**: Before acquiring locks, `ValidateWatchVersion()` re-reads each watched key's version and compares against the saved version. If any version changed, the transaction returns nil (EXEC fails).

This is a hash-based scheme, so false positives (spurious failures) can occur when different keys hash to the same slot. The array size (configurable) controls the false-positive rate. With a 1024-entry map, keys that collide in the lower 10 bits of their hash will interfere with each other.

[Source: libs/server/Transaction/WatchVersionMap.cs:L14-L42]

### 2.6 EXEC: Transaction Execution

`NetworkEXEC()` handles three cases based on the current state:

**Case 1: Running** (re-entry during replay): Calls `txnManager.Commit()` to finalize.

**Case 2: Aborted**: Returns `EXECABORT` error and resets.

**Case 3: Started** (normal path):
1. Saves the current `endReadHead` and resets it to `txnStartHead` (the position of the first queued command).
2. Calls `GetKeysForValidation` which saves watched keys to the cluster key validation list.
3. Performs cluster slot verification (`NetworkKeyArraySlotVerify`) to ensure all keys belong to the local node.
4. Calls `txnManager.Run()` which:
   a. Saves watched keys to the lock list.
   b. Acquires a transaction version from `StateMachineDriver.AcquireTransactionVersion()`.
   c. Calls `BeginLockable` on the appropriate contexts (main and/or object store).
   d. Calls `keyEntries.LockAllKeys()` to acquire sorted locks.
   e. Calls `watchContainer.ValidateWatchVersion()` for optimistic validation.
   f. Calls `StateMachineDriver.VerifyTransactionVersion()` to ensure version consistency.
   g. Calls `LocksAcquired` to inform contexts of the version.
   h. Writes `TxnStart` to AOF if there are write operations.
   i. Sets `state = TxnState.Running`.
5. If `Run()` succeeds, writes the array length header (`*N\r\n`). The buffer replay mechanism works by rewinding `endReadHead` to `txnStartHead` (step 1), which causes the main RESP processing loop to re-parse commands starting from the first queued command. Because `state` is now `TxnState.Running`, the command processor executes commands normally (not through NetworkSKIP), but with the lockable contexts active. When the processing loop reaches the EXEC command again, `NetworkEXEC` sees `state == Running` and calls `Commit()`.
6. If `Run()` fails (watch validation failed), restores `endReadHead` and returns nil array.

[Source: libs/server/Transaction/TxnRespCommands.cs:L39-L91]
[Source: libs/server/Transaction/TransactionManager.cs:L365-L415]

### 2.7 Commit and Reset

**Commit** (`TransactionManager.Commit`):
1. If write operations occurred and AOF is enabled, writes a `TxnCommit` entry with the transaction version.
2. Resets the watch container (clears all watches).
3. Calls `Reset(true)` (running=true).

**Reset** (`TransactionManager.Reset`):
1. If running: Unlocks all keys, ends lockable contexts, calls `StateMachineDriver.EndTransaction(txnVersion)`.
2. Clears all state: version, start head, operation count, state back to `None`, store type to 0, `StoredProcMode` to false, `PerformWrites` to false.

[Source: libs/server/Transaction/TransactionManager.cs:L286-L295]
[Source: libs/server/Transaction/TransactionManager.cs:L155-L189]

### 2.8 WATCH / UNWATCH

`CommonWATCH(StoreType type)` iterates through the provided keys and calls `txnManager.Watch(key, type)` for each. The `Watch` method:
1. Updates the transaction store type (Main, Object, or All).
2. Adds the key to the `WatchedKeysContainer`.
3. Calls `basicContext.ResetModified(key)` to clear the modified flag, enabling Tsavorite-level change detection.

Three variants exist: `WATCH` (both stores), `WATCH MS` (main store only), `WATCH OS` (object store only).

`UNWATCH` resets the watch container if no transaction is in progress (`state == None`). If a transaction is started, UNWATCH is a no-op (watches are cleared on EXEC/DISCARD).

[Source: libs/server/Transaction/TxnRespCommands.cs:L208-L266]
[Source: libs/server/Transaction/TransactionManager.cs:L297-L310]

### 2.9 Stored Procedures (RUNTXP)

`RunTransactionProc` implements the stored procedure execution model:

1. **Prepare**: Calls `proc.Prepare(garnetTxPrepareApi, ref procInput)` using a `GarnetWatchApi<BasicGarnetApi>` wrapper. This wrapper implements **`IGarnetReadApi` only** (not the full `IGarnetApi`) -- it is a read-only interceptor (`GarnetWatchApi.cs:L14`). For each read operation (GET, LLEN, ZCARD, HGET, etc.), it calls `garnetApi.WATCH(key, storeType)` to register the key in the watch list before delegating to the underlying API. Write operations are NOT available through this wrapper. The key registration for the lock set (`AddKey`) is handled separately by the stored procedure's Prepare method calling `txnPrepareApi.AddKey(key, lockType)` on the `TransactionManager` directly. This design ensures the Prepare phase only performs reads to discover data-dependent key sets; all writes happen in the Main phase under locks. If Prepare returns false, the transaction is aborted.
2. **Run**: Calls `TransactionManager.Run()` with optional `fail_fast_on_lock` and `lock_timeout` parameters from the procedure's configuration.
3. **Main**: Calls `proc.Main(garnetTxMainApi, ref procInput, ref output)` using a `LockableGarnetApi` that routes operations through the lockable contexts. All data access during Main is under locks.
4. **Log**: Logs the entire stored procedure as a single `StoredProcedure` AOF entry with the procedure ID and input.
5. **Commit**: Standard commit flow.
6. **Finalize**: Calls `proc.Finalize(garnetTxFinalizeApi, ref procInput, ref output)` using a basic (unlocked) API. Finalize runs after locks are released and can perform follow-up operations. Skipped during AOF recovery.

The three API wrappers provide different interface levels: `GarnetWatchApi` implements `IGarnetReadApi` (read-only, for Prepare), `LockableGarnetApi` implements full `IGarnetApi` (for Main, routed through lockable contexts), and `BasicGarnetApi` implements full `IGarnetApi` (for Finalize, routed through basic contexts after locks are released).

[Source: libs/server/Transaction/TransactionManager.cs:L191-L260]

### 2.10 DISCARD

`NetworkDISCARD()` resets the transaction without executing, calling `txnManager.Reset(false)` (not running, so no locks to release). Returns error if no transaction is active.

[Source: libs/server/Transaction/TxnRespCommands.cs:L191-L201]

## 3. Source File Map

| File | Purpose |
|------|---------|
| `libs/server/Transaction/TransactionManager.cs` | Core transaction lifecycle: Run, Commit, Reset, Watch, stored proc execution |
| `libs/server/Transaction/TxnRespCommands.cs` | MULTI, EXEC, DISCARD, WATCH, UNWATCH, RUNTXP command handlers |
| `libs/server/Transaction/TxnState.cs` | Transaction state enum |
| `libs/server/Transaction/TxnKeyEntry.cs` | Key entry struct (10 bytes) and key entries collection with sort/lock/unlock |
| `libs/server/Transaction/WatchVersionMap.cs` | Shared version map for optimistic concurrency |
| `libs/server/Transaction/TxnKeyEntryComparison.cs` | Custom key comparison: sorts by isObject then by Tsavorite lock code |
| `libs/server/Transaction/WatchedKeysContainer.cs` | Container for watched key versions |
| `libs/server/Transaction/TransactionManager.ClusterSlotVerify.cs` | Cluster slot verification for transactions |

## 4. Performance Notes

- **Sorted lock acquisition**: Sorting keys before locking is essential for deadlock freedom. The sort uses `MemoryExtensions.Sort` on a `Span<TxnKeyEntry>`, which is cache-friendly for the small 10-byte entries.
- **Pinned key array**: `TxnKeyEntries` is initially allocated with `GC.AllocateArray<TxnKeyEntry>(initialCount, pinned: true)`, avoiding GC relocation during lock operations. Note: if the array grows (via `AddKey` when `keyCount >= keys.Length`), the new array uses `new TxnKeyEntry[keys.Length * 2]` which is NOT pinned. This means pinning is only guaranteed for the initial allocation size.
- **Watch version map sizing**: Larger maps reduce false-positive watch failures. The power-of-2 constraint enables fast modular indexing via bitwise AND.
- **Buffer replay**: Commands are replayed from the original receive buffer by resetting `endReadHead` to `txnStartHead`. No command re-parsing or copying is needed.
- **Transaction version**: The `StateMachineDriver` provides monotonic version numbers that integrate with Tsavorite's version tracking for checkpoint consistency.
- **TryLock with timeout**: Stored procedures can use `FailFastOnKeyLockFailure` with a `TimeSpan` timeout to avoid indefinite blocking, important for latency-sensitive workloads.

## 5. Rust Mapping Notes

- **TxnKeyEntry**: Map to `#[repr(C)]` struct with explicit field layout. The `ILockableKey` trait becomes a Rust trait.
- **LockableContext**: Tsavorite's lockable context pattern needs a Rust equivalent. Consider using `parking_lot::RwLock` per hash bucket or a sorted-lock acquisition protocol over a lock table.
- **WatchVersionMap**: `Vec<AtomicI64>` with `fetch_add(1, Ordering::SeqCst)` for increment and `load(Ordering::SeqCst)` for read.
- **Transaction state machine**: Use a Rust enum with `AtomicU8` for the state field, or an enum with match-based transitions.
- **Buffer replay**: In Rust, store a byte slice reference `&[u8]` for the queued commands and replay by re-parsing from the saved offset. Lifetimes must be managed carefully.
- **Stored procedures**: Use a trait with `prepare`, `main`, `finalize` methods. The three API wrappers become different implementations of a common data-access trait.
- **Sorted lock protocol**: Use `sort_unstable_by` on a `Vec<TxnKeyEntry>` for the lock ordering.

## 6. Kotlin/JVM Mapping Notes

- **TxnKeyEntry**: Map to a data class with `Long keyHash`, `Boolean isObject`, `LockType lockType`. The 10-byte layout cannot be replicated on the JVM but field-level optimization is not critical.
- **WatchVersionMap**: Use `AtomicLongArray` which provides the same semantics as the .NET `Interlocked` operations on an array.
- **LockableContext**: Implement using `ReentrantReadWriteLock` per hash bucket or a lock-striping library like Guava's `Striped<Lock>`.
- **Buffer replay**: Store the byte array and offset. Replay by creating a new `ByteBuffer` view at the saved position.
- **Stored procedures**: Define a `CustomTransactionProcedure` interface with `prepare`, `main`, `finalize` methods and three IGarnetApi implementations for the different phases.
- **Transaction isolation**: The JVM's memory model guarantees happen-before for `volatile` reads/writes, sufficient for the state machine transitions.

## 7. Open Questions

1. **Watch granularity**: The hash-based watch version map can cause false-positive failures. Should the reimplementation use per-key watches (more memory, no false positives) or keep the hash-based approach (compact, some false positives)?
2. **Cross-database transactions**: Currently blocked with an error. What is the design for supporting transactions spanning multiple databases?
3. **Lock timeout defaults**: The default `LockAllKeys` blocks indefinitely. Should there be a configurable global timeout to prevent deadlocks from bugs?
4. **Stored procedure AOF replay**: The entire stored procedure is re-executed during AOF replay. If the procedure has non-deterministic behavior (e.g., reads system time), the replay may diverge. How should this be addressed?
5. **Transaction buffer size**: Commands are replayed from the receive buffer. If the buffer is very large (many queued commands), this could cause memory pressure. Should there be a limit on transaction size?
