# Tsavorite Epoch Protection & Concurrency

## 1. Overview

`LightEpoch` is Tsavorite's **epoch-based memory reclamation and synchronization framework**. It provides a lightweight mechanism for threads to indicate they are in a "protected" region where certain global invariants hold (e.g., pages they reference will not be evicted). The epoch system enables:

1. **Safe memory reclamation**: Pages and overflow buckets can be freed only when no thread could still reference them.
2. **Deferred actions**: Actions (like page eviction or checkpoint transitions) are registered to fire when all threads have observed a new epoch.
3. **State machine coordination**: Global state transitions (checkpoints, resizes) use epoch bumps to ensure all threads have acknowledged the transition (see [Doc 05 Section 2.3](./05-tsavorite-checkpointing.md#23-state-machine-step-execution) for how the checkpoint state machine uses `BumpCurrentEpoch`).

The design is inspired by epoch-based reclamation (EBR) from the academic literature, but adapted for Tsavorite's specific needs: it uses a **cache-line-aligned epoch table** with thread-local entries, two fast probe offsets per thread, and a drain list for deferred actions.

## 2. Detailed Design

### 2.1 Core Concepts

- **Global Epoch** (`CurrentEpoch`): A monotonically increasing `long` counter. Starts at 1. Incremented via `BumpCurrentEpoch()`. [Source: LightEpoch.cs:L142]
- **SafeToReclaimEpoch**: The highest epoch E such that no thread is currently protected at any epoch <= E. Any deferred action registered for epoch <= SafeToReclaimEpoch is safe to execute. [Source: LightEpoch.cs:L147]
- **Protection**: A thread "protects" itself by copying `CurrentEpoch` into its epoch table entry. While protected, it guarantees that resources associated with that epoch or later will not be reclaimed. [Source: LightEpoch.cs:L295]

### 2.2 Epoch Table Entry (64 bytes)

Each entry is cache-line-sized to avoid false sharing: [Source: LightEpoch.cs:L719-L735]

```
Offset  Field                  Type    Description
0x00    localCurrentEpoch      long    The epoch at which this thread is protected (0 = unprotected)
0x08    threadId               int     Managed thread ID (0 = entry is free)
0x0C    [padding]              -       Padding to 64 bytes
```

```csharp
[StructLayout(LayoutKind.Explicit, Size = 64)]  // kCacheLineBytes = 64
struct Entry {
    [FieldOffset(0)]  public long localCurrentEpoch;
    [FieldOffset(8)]  public int threadId;
}
```

### 2.3 Epoch Table Layout

The table has `kTableSize = max(128, Environment.ProcessorCount * 2)` entries, allocated as a pinned array with cache-line alignment: [Source: LightEpoch.cs:L98, L166-L176]

```csharp
tableRaw = GC.AllocateArray<Entry>(kTableSize + 2, pinned: true);
tableAligned = (Entry*)((p + 63) & ~63);  // align to 64-byte boundary
```

Entry 0 is reserved (never used). Valid indices are 1..kTableSize.

### 2.4 Instance Tracking

Multiple `LightEpoch` instances can coexist (up to `MaxInstances = 16`). Each instance gets a unique `instanceId` via `SelectInstance()` using CAS on a static `InstanceTracker` buffer. [Source: LightEpoch.cs:L22-L45, L187-L197]

Thread-local metadata uses `[ThreadStatic]` fields in the `Metadata` class:
- `threadId`: Managed thread ID (initialized once per thread). [Source: LightEpoch.cs:L56]
- `startOffset1`, `startOffset2`: Two hash-based offsets for fast probing into the epoch table. [Source: LightEpoch.cs:L62-L68]
- `Entries`: An `InstanceIndexBuffer` (16 ints) that maps each LightEpoch instance's `instanceId` to this thread's epoch table index. [Source: LightEpoch.cs:L82]

### 2.5 Acquiring an Epoch Table Entry (ReserveEntryForThread)

When a thread first calls `Resume()` for a LightEpoch instance: [Source: LightEpoch.cs:L661-L671]

1. If `Metadata.threadId == 0`, initialize it from `Environment.CurrentManagedThreadId` and compute two hash probes using Murmur3: `startOffset1 = 1 + (Murmur3(threadId) % kTableSize)` and `startOffset2 = 1 + ((Murmur3(threadId) >> 16) % kTableSize)`.
2. Call `TryAcquireEntry`, which probes `startOffset1`, then swaps offsets and probes `startOffset2`, then linearly scans twice around the table. [Source: LightEpoch.cs:L555-L602]
3. Each probe attempts `Interlocked.CompareExchange(&entry.threadId, myThreadId, 0)`.
4. If the table is full, fall back to `ReserveEntryWait` which increments a `waiterCount` and blocks on a `SemaphoreSlim` until a slot is released. [Source: LightEpoch.cs:L624-L654]

### 2.6 Protect / Suspend Lifecycle

The critical fast path for epoch protection: [Source: LightEpoch.cs:L500-L544]

**Acquire (Resume):**
```csharp
void Acquire() {
    ref var entry = ref Metadata.Entries.GetRef(instanceId);
    ReserveEntryForThread(ref entry);
    (tableAligned + entry)->localCurrentEpoch = CurrentEpoch;  // PROTECT
    if (drainCount > 0) Drain(localCurrentEpoch);              // Process pending actions
}
```

**Release (Suspend):**
```csharp
void Release() {
    ref var entry = ref Metadata.Entries.GetRef(instanceId);
    (tableAligned + entry)->localCurrentEpoch = 0;  // UNPROTECT
    (tableAligned + entry)->threadId = 0;            // Free slot
    entry = kInvalidIndex;
    if (waiterCount > 0) waiterSemaphore.Release();  // Wake waiting thread
}
```

**Design note**: `Release()` always frees the epoch table entry (`threadId = 0`, `entry = kInvalidIndex`). This means every `Suspend()`/`Resume()` cycle requires a full re-acquisition via `ReserveEntryForThread` (including a CAS on the epoch table). This is heavier than it might appear -- it is not a simple flag flip. Reimplementations should consider whether to preserve the entry across suspend/resume for frequently-used sessions, trading table capacity for reduced overhead.

**ProtectAndDrain (refresh):**
```csharp
void ProtectAndDrain() {
    (tableAligned + entry)->localCurrentEpoch = CurrentEpoch;  // Refresh to latest epoch
    if (drainCount > 0) Drain(localCurrentEpoch);
    if (waiterCount > 0) SuspendResume();  // Give waiters a chance
}
```
[Source: LightEpoch.cs:L286-L307]

### 2.7 Computing SafeToReclaimEpoch

`ComputeNewSafeToReclaimEpoch` scans all epoch table entries and finds the minimum non-zero `localCurrentEpoch`. The safe-to-reclaim epoch is one less than this minimum: [Source: LightEpoch.cs:L421-L440]

```csharp
long ComputeNewSafeToReclaimEpoch(long currentEpoch) {
    long oldestOngoingCall = currentEpoch;
    for (int index = 1; index <= kTableSize; ++index) {
        long entry_epoch = (tableAligned + index)->localCurrentEpoch;
        if (entry_epoch != 0 && entry_epoch < oldestOngoingCall)
            oldestOngoingCall = entry_epoch;
    }
    SafeToReclaimEpoch = oldestOngoingCall - 1;
    return SafeToReclaimEpoch;
}
```

This scan is O(kTableSize) but the table is small (128-256 entries) and cache-line-aligned, so it is fast in practice.

### 2.8 Deferred Action System (Drain List)

The drain list is a fixed-size array of `EpochActionPair` (epoch + Action delegate), with `kDrainListSize = 16` slots: [Source: LightEpoch.cs:L103, L137]

**Registering an action** (`BumpCurrentEpoch(Action onDrain)`): [Source: LightEpoch.cs:L361-L408]

1. Bump the global epoch via the no-arg `BumpCurrentEpoch()`, which does `Interlocked.Increment(ref CurrentEpoch)` AND then triggers a drain pass (`Drain(nextEpoch)` or `ComputeNewSafeToReclaimEpoch`). This side-effect drain is important: it processes pending actions before the registration loop begins. `PriorEpoch = result - 1`. [Source: LightEpoch.cs:L342-L353, L363]
2. Scan the drain list for a free slot (`epoch == long.MaxValue`) or a reclaimable slot (`epoch <= SafeToReclaimEpoch`).
3. CAS the slot's epoch to the sentinel value `long.MaxValue - 1` ("claimed but not yet populated"). This prevents other threads from using the slot while the action is being written.
4. Write the action delegate, then overwrite the epoch with `PriorEpoch`. Only after this final write does `Interlocked.Increment(ref drainCount)` make the slot visible for draining. The ordering is critical: `drainCount` must be incremented AFTER the epoch field is set to `PriorEpoch`, otherwise another thread's `Drain()` could see the slot with the sentinel value `long.MaxValue - 1`. If a reclaimable slot was claimed, its old action is extracted and executed inline before the new action is stored. [Source: LightEpoch.cs:L371-L375]
5. If the drain list is full (no free or reclaimable slot after a full scan), call `ProtectAndDrain()` to process pending actions, yield the thread, then retry from the beginning.
6. After successfully registering the action, call `ProtectAndDrain()` once more, which may immediately execute the just-registered action if enough threads have advanced.

**Draining actions** (`Drain(long nextEpoch)`): [Source: LightEpoch.cs:L471-L496]

1. Call `ComputeNewSafeToReclaimEpoch(nextEpoch)`.
2. For each drain list entry where `trigger_epoch <= SafeToReclaimEpoch`, CAS-claim the slot, extract and execute the action, then mark the slot as free.

### 2.9 SuspendDrain: The Last Thread Out

When a thread suspends and sets its `localCurrentEpoch` to 0, it may be the last active thread. `SuspendDrain` handles this: [Source: LightEpoch.cs:L446-L464]

```csharp
while (drainCount > 0) {
    Thread.MemoryBarrier();   // full fence: ensure we see latest epoch table state
    for (index = 1..kTableSize) {
        if (entry_epoch != 0) return;  // another thread is active, let it drain
    }
    // No active threads -- we must drain ourselves
    Resume();    // re-acquire epoch protection (calls Acquire, not full Suspend/Resume)
    Release();   // release it again; Acquire triggers drain processing
}
```

Key subtlety: this calls `Resume()` then `Release()`, NOT `Suspend()`. Using `Suspend()` would recursively call `SuspendDrain()` again. `Release()` directly frees the epoch table entry without the recursive drain check. The `Thread.MemoryBarrier()` is a full fence (not just a compiler barrier) to ensure visibility of epoch table updates from other threads on weakly-ordered architectures.

### 2.10 Thread Safety Guarantees

The epoch system provides the following guarantees:
- While a thread holds epoch protection (between `Resume`/`Suspend`), any resource that was valid at the time of `Resume` remains valid.
- A deferred action registered at epoch E will not execute until all threads that were protected at epoch E have subsequently called `ProtectAndDrain` or `Suspend`.
- Page eviction, overflow bucket reclamation, and checkpoint transitions all use this mechanism for safe coordination.

## 3. Source File Map

| File | Purpose | Key Lines |
|------|---------|-----------|
| `Epochs/LightEpoch.cs` | Complete epoch implementation: table, acquire/release, drain | L15-L747 |
| `Epochs/IEpochAccessor.cs` | Interface for epoch suspend/resume | L9-L22 |
| `Index/Tsavorite/TsavoriteThread.cs` | Per-session epoch integration | Full file |
| `Allocator/AllocatorBase.cs` | Uses epoch for page eviction protection | L22, L240-L311 |
| `Index/Checkpointing/StateMachineDriver.cs` | Uses epoch bumps for state machine transitions | L246-L252 |

## 4. Performance Notes

- **Cache-line-per-entry**: Each epoch table entry is 64 bytes, eliminating false sharing between threads.
- **Two-probe hashing**: Most threads acquire their entry on the first probe, avoiding table scans.
- **No locks on fast path**: `Resume` and `Suspend` are a single store each (after the initial reservation). `ProtectAndDrain` is a single store plus a drain check.
- **Drain amortization**: Drain processing is done by the thread that observes `drainCount > 0`, distributing work across threads rather than requiring a dedicated thread.
- **Semaphore-based backoff**: When the epoch table is full (very rare), threads wait on a semaphore instead of spinning, avoiding CPU waste. [Source: LightEpoch.cs:L624-L654]
- **Small table size**: `max(128, 2 * ProcessorCount)` entries keeps the `ComputeNewSafeToReclaimEpoch` scan within a few cache lines.

## 5. Rust Mapping Notes

- The epoch table maps to a `Vec<CachePadded<Entry>>` using `crossbeam-utils::CachePadded` for 64-byte alignment.
- `[ThreadStatic]` maps to `thread_local!` or `#[thread_local]` (nightly).
- `Interlocked.CompareExchange` maps to `AtomicI64::compare_exchange` with `Ordering::AcqRel`.
- The drain list can use `AtomicI64` for the epoch field and an `UnsafeCell<Option<Box<dyn FnOnce()>>>` for the action (protected by the CAS protocol).
- Consider `crossbeam-epoch` as an alternative, though Tsavorite's epoch system is simpler and more specialized.
- The `waiterSemaphore` maps to `std::sync::Condvar` or `tokio::sync::Semaphore`.

## 6. Kotlin/JVM Mapping Notes

- **No `[ThreadStatic]`**: Use `ThreadLocal<T>` for per-thread metadata. This adds indirection but is functionally equivalent.
- **Epoch table**: Allocate as a `LongArray` (for `localCurrentEpoch`) and `IntArray` (for `threadId`), each with explicit padding to avoid false sharing. Alternatively, use a single `long[]` with stride 8 (8 longs = 64 bytes per entry).
- **Atomic operations**: `VarHandle` for atomic access to array elements, or `AtomicLongArray`.
- **Drain actions**: Use `AtomicReferenceArray<Runnable>` alongside `AtomicLongArray` for epochs.
- **Memory barriers**: `VarHandle.fullFence()` for the barrier in `SuspendDrain`.
- **Performance concern**: JVM `ThreadLocal` is slower than C# `[ThreadStatic]` due to hash table lookup. For extreme performance, consider using thread IDs to index directly into an array.

## 7. Open Questions

1. **Epoch table sizing**: The table size `max(128, 2 * ProcessorCount)` may be too small for applications with many short-lived threads (e.g., async runtimes with thread pool churn). The semaphore-based backoff handles overflow, but performance impact is unclear.
2. **Drain list capacity**: With `kDrainListSize = 16`, a burst of concurrent epoch-bump operations could temporarily fill the list, causing threads to spin on `ProtectAndDrain`. Is 16 sufficient for all workloads?
3. **Memory ordering**: The code relies on x86 TSO for some orderings (stores are not reordered with other stores). A port to ARM or RISC-V would need additional barriers.
4. **Integration with async/await**: Epoch protection is thread-bound. C# async methods can resume on different threads, which breaks epoch invariants. Tsavorite addresses this by suspending epochs before awaits and resuming after, but the pattern is error-prone.
