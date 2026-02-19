# 11 -- Data Structures & Object Store

## 1. Overview

Garnet's dual-store architecture separates simple string values (main store, `SpanByte` key-value pairs managed inline by Tsavorite) from complex heap-allocated data structures (object store, `byte[]` keys to `IGarnetObject` values). The object store handles Redis Hash, List, Set, SortedSet, and extensible custom types. Each object type is a self-contained C# class that implements the `IGarnetObject` interface, carrying its own in-memory data structure, serialization logic, size tracking, and per-element expiration where applicable.

This design means the storage engine (Tsavorite) never needs to understand the internal structure of objects -- it only sees opaque `IGarnetObject` references. All type-specific logic lives in the object classes themselves, which Tsavorite invokes through the `Operate()` method during RMW callbacks.

[Source: libs/server/Objects/Types/IGarnetObject.cs]
[Source: libs/server/Objects/Types/GarnetObjectBase.cs:L18-L37]

## 2. Detailed Design

### 2.1 The IGarnetObject Interface

The `IGarnetObject` interface defines the contract between Tsavorite and Garnet's object types:

- **`byte Type`**: Returns the `GarnetObjectType` enum value (SortedSet=1, List=2, Hash=3, Set=4, or custom types starting at higher values).
- **`long Expiration`**: Get/set the key-level expiration timestamp in ticks. Zero means no expiration.
- **`long Size`**: Tracked heap memory consumption for eviction decisions. Updated by each mutation.
- **`Operate(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion, out long sizeChange)`**: The core dispatch method. The object interprets the `ObjectInput.header` to determine the sub-command (LPUSH, SADD, ZADD, etc.) and executes it against its internal data structure, writing RESP-formatted results to `output`.
- **`Serialize(BinaryWriter writer)`**: Serializes the object for checkpointing or replication.
- **`CopyUpdate(ref IGarnetObject oldValue, ref IGarnetObject newValue, bool isInNewVersion)`**: Handles the Tsavorite copy-on-write protocol during log compaction or version transitions.
- **`Scan(...)`**: Cursor-based iteration over the object's elements.

[Source: libs/server/Objects/Types/IGarnetObject.cs]

### 2.2 GarnetObjectBase: The Abstract Foundation

All built-in object types extend `GarnetObjectBase`, which provides:

**Serialization state machine**: A CAS-based state machine managed by an `int serializationState` field with `Interlocked.CompareExchange` transitions. The `SerializationPhase` enum defines four values: `REST`, `SERIALIZING`, `SERIALIZED`, `DONE` (`SerializationPhase.cs:L6-L12`). In practice, the active transitions are REST -> SERIALIZING -> SERIALIZED (or back to REST). `DONE` exists in the enum but is not currently used in the codebase. This solves a critical concurrency problem: during checkpointing, Tsavorite may need to serialize an object while application threads continue to mutate it.

The `Serialize` method works as follows:
1. If state is `REST`, transition to `SERIALIZING`, serialize directly to the writer via `DoSerialize()`, return to `REST`.
2. If state is `SERIALIZED`, a cached `byte[] serialized` array exists from a prior `CopyUpdate` -- write it directly.
3. If another thread is currently serializing, spin-yield until the state settles.

The `CopyUpdate` method implements Tsavorite's copy-on-write contract:
- If `isInNewVersion` is false (not during checkpoint): Transitions the old object's state to `SERIALIZED` (preventing further mutations) and sets `oldValue = null`. The transition to SERIALIZED is about marking the old object as logically dead, not about caching serialized data. The cloned new value becomes the sole live copy.
- If `isInNewVersion` is true (checkpoint in progress): Serializes the current state to a `byte[] serialized` cache via `MemoryStream` + `BinaryWriter`, then transitions to `SERIALIZED`. Future `Serialize` calls on this object will use the cached byte array instead of re-serializing. This ensures the checkpoint captures a consistent snapshot while the application continues mutating the clone.

In both cases, if `serialized` is null when `Serialize` is called in the `SERIALIZED` state, a `GarnetObjectType.Null` byte is written to the stream, handling the case where the object was nulled out before serialization.

[Source: libs/server/Objects/Types/GarnetObjectBase.cs:L20-L123]
[Source: libs/server/Objects/Types/GarnetObjectBase.cs:L146-L150]

### 2.3 GarnetObjectType Enum

The type enum uses a `byte` representation:

```
Null=0, SortedSet=1, List=2, Hash=3, Set=4
```

Special "command" types occupy the high byte range (0xf7-0xff) and are used in `ObjectInput.header` to dispatch operations without a separate command field:

```
DelIfExpIm=0xf7, PExpire=0xf8, ExpireTime=0xf9, PExpireTime=0xfa,
All=0xfb, PTtl=0xfc, Persist=0xfd, Ttl=0xfe, Expire=0xff
```

Note: `All` (0xfb) indicates a custom object command, not a data type. `DelIfExpIm` (0xf7) is a conditional deletion for in-memory expired keys. The extension class defines `LastObjectType = Set` and `FirstSpecialObjectType = DelIfExpIm`, establishing the boundary between data types and command types.

This overloading of the type byte for both type identification and command dispatch is a compact encoding choice that avoids an extra field in the hot-path `ObjectInput` struct.

[Source: libs/server/Objects/Types/GarnetObjectType.cs:L9-L87]

### 2.4 HashObject

**Backing structure**: `Dictionary<byte[], byte[]>` with `ByteArrayComparer` for key equality.

**Per-field expiration**: Hash supports per-field TTLs through two auxiliary structures:
- `Dictionary<byte[], long> expirationTimes`: Maps field names to expiration timestamps.
- `PriorityQueue<byte[], long> expirationQueue`: Min-heap ordered by expiration time for efficient lazy cleanup.

When a field is accessed, the code checks `expirationTimes` for that field. Expired fields are lazily removed. The `PriorityQueue` allows efficient bulk expiration during scans.

**Size tracking**: Each field's contribution is calculated as:
```
Utility.RoundUp(key.Length, IntPtr.Size) + Utility.RoundUp(value.Length, IntPtr.Size) + 2 * ByteArrayOverhead + DictionaryEntryOverhead
```
The `IntPtr.Size` rounding accounts for .NET memory allocator alignment. `ByteArrayOverhead` includes the array header (16 bytes on x64). `DictionaryEntryOverhead` accounts for the hash bucket entry.

**Serialization format** (BinaryWriter): For each field:
1. `int keyLength` (OR'd with `ExpirationBitMask` if the field has an expiration)
2. `byte[] key`
3. `int valueLength`
4. `byte[] value`
5. `long expiration` (only if `ExpirationBitMask` was set)

The `ExpirationBitMask` is `1 << 31` (bit 31, the sign bit of the int). This encoding avoids an extra byte per field when no expiration exists.

**Operations**: The `HashOperation` enum defines: HCOLLECT, HEXPIRE, HTTL, HPERSIST, HGET, HMGET, HSET, HMSET, HSETNX, HLEN, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HINCRBY, HINCRBYFLOAT, HRANDFIELD, HSCAN, HSTRLEN. Notably, `HCOLLECT` is an internal garbage collection operation that triggers bulk cleanup of expired fields -- it is dispatched through `Operate()` from outside the object (e.g., by a background task). `HEXPIRE`, `HTTL`, and `HPERSIST` manage per-field expiration.

[Source: libs/server/Objects/Hash/HashObject.cs:L27-L67]

### 2.5 ListObject

**Backing structure**: `LinkedList<byte[]>`. The doubly-linked list provides O(1) head/tail operations (LPUSH/RPUSH/LPOP/RPOP) and O(n) indexed access (LINDEX/LINSERT).

**Operations**: The `ListOperation` enum defines: LPOP, LPUSH, LPUSHX, RPOP, RPUSH, RPUSHX, LLEN, LTRIM, LRANGE, LINDEX, LINSERT, LREM, RPOPLPUSH, LMOVE, LSET, BRPOP, BLPOP, LPOS. Each is dispatched through the `Operate()` method based on `ObjectInput.header.type`. Notable additions beyond basic operations: `LPUSHX`/`RPUSHX` (push only if key exists), `RPOPLPUSH`/`LMOVE` (atomic pop-and-push between lists), and `BRPOP`/`BLPOP` (blocking pop operations that require special session-level coordination for the blocking wait).

[Source: libs/server/Objects/List/ListObject.cs:L18-L38]

**Size tracking**: Each element contributes `Utility.RoundUp(element.Length, IntPtr.Size) + ByteArrayOverhead + LinkedListEntryOverhead`.

**Serialization**: Count followed by (length, data) pairs for each element.

[Source: libs/server/Objects/List/ListObject.cs]

### 2.6 SetObject

**Backing structure**: `HashSet<byte[]>` with `ByteArrayComparer`.

**Operations**: The `SetOperation` enum defines: SADD, SREM, SPOP, SMEMBERS, SCARD, SSCAN, SMOVE, SRANDMEMBER, SISMEMBER, SMISMEMBER, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE, SINTER, SINTERSTORE. The `*STORE` variants write results to a destination key, requiring cross-key coordination.

**No per-element expiration**: Unlike HashObject and SortedSetObject, SetObject does NOT support per-element expiration. This asymmetry is by design: Redis does not define per-member TTLs for sets.

**Size tracking**: Each member contributes `Utility.RoundUp(member.Length, IntPtr.Size) + ByteArrayOverhead + HashSetEntryOverhead`.

[Source: libs/server/Objects/Set/SetObject.cs:L18-L36]

### 2.7 SortedSetObject

**Backing structure**: A dual data structure:
- `SortedSet<(double Score, byte[] Element)>` with a custom `SortedSetComparer`: Maintains score-ordered iteration (ZRANGEBYSCORE, ZRANK).
- `Dictionary<byte[], double>` with `ByteArrayComparer`: Provides O(1) score lookup by element name (ZSCORE, ZADD update).

The `SortedSetComparer` orders first by score (double comparison), then lexicographically by element bytes for tie-breaking. This dual structure trades memory for the ability to support both score-range and element-lookup queries efficiently.

**Per-element expiration**: Similar to HashObject, uses `Dictionary<byte[], long> expirationTimes` + `PriorityQueue<byte[], long> expirationQueue`.

**GEO commands**: Implemented on top of the sorted set by encoding latitude/longitude as geohash scores. This follows the Redis convention where GEO is backed by ZSET.

**Operations**: The `SortedSetOperation` enum defines: ZADD, ZCARD, ZPOPMAX, ZSCORE, ZREM, ZCOUNT, ZINCRBY, ZRANK, ZRANGE, GEOADD, GEOHASH, GEODIST, GEOPOS, GEOSEARCH, ZREVRANK, ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZLEXCOUNT, ZPOPMIN, ZRANDMEMBER, ZDIFF, ZSCAN, ZMSCORE, ZEXPIRE, ZTTL, ZPERSIST, ZCOLLECT. The `ZEXPIRE`/`ZTTL`/`ZPERSIST` operations manage per-element expiration. `ZCOLLECT` is an internal garbage collection operation (analogous to `HCOLLECT` for hashes) that triggers bulk cleanup of expired elements.

[Source: libs/server/Objects/SortedSet/SortedSetObject.cs]

### 2.8 GarnetObjectSerializer

The `GarnetObjectSerializer` is responsible for type-dispatched deserialization. During checkpoint recovery or replication, it reads a type byte from the stream and constructs the appropriate object:

```csharp
type switch {
    SortedSet => new SortedSetObject(reader),
    List      => new ListObject(reader),
    Hash      => new HashObject(reader),
    Set       => new SetObject(reader),
    _         => customObjectFactory.Create(type, reader)  // extensibility
}
```

Custom object types register factories through the `CustomCommandManager`, allowing third-party data structures to be seamlessly checkpointed and replicated.

[Source: libs/server/Objects/Types/GarnetObjectSerializer.cs]

### 2.9 Object Store Session Functions

`ObjectSessionFunctions` implements `ISessionFunctions<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>` and follows a similar pattern to `MainSessionFunctions`:

- **InitialUpdater**: Creates a new object by calling the appropriate constructor (e.g., `new SortedSetObject()`, `new ListObject()`, `new HashObject()`, `new SetObject()`) based on the type byte, or uses `CustomCommandManager` for custom object types. Then calls `value.Operate()` to apply the initial mutation.
- **InPlaceUpdater**: Checks expiry, handles Expire/Persist/TTL pseudo-commands, then delegates to `value.Operate()`. Unlike the main store, in-place update almost always succeeds because the object is a heap reference -- only the pointer is stored in the log, so no space-check is needed.
- **CopyUpdater**: Calls `oldValue.CopyUpdate()` to handle the serialization state machine, then calls `value.Operate()` on the new clone.
- **Size tracking**: After each mutation, the `sizeChange` output from `Operate()` is applied to the `objectStoreSizeTracker` to maintain accurate memory accounting.

[Source: libs/server/Storage/Functions/ObjectStore/ObjectSessionFunctions.cs]
[Source: libs/server/Storage/Functions/ObjectStore/RMWMethods.cs]

## 3. Source File Map

| File | Purpose |
|------|---------|
| `libs/server/Objects/Types/IGarnetObject.cs` | Interface definition |
| `libs/server/Objects/Types/GarnetObjectBase.cs` | Abstract base with serialization state machine |
| `libs/server/Objects/Types/GarnetObjectType.cs` | Type enum (byte-encoded) |
| `libs/server/Objects/Types/GarnetObjectSerializer.cs` | Type-dispatched deserialization |
| `libs/server/Objects/Hash/HashObject.cs` | Dictionary-backed hash with per-field expiry |
| `libs/server/Objects/List/ListObject.cs` | LinkedList-backed list |
| `libs/server/Objects/Set/SetObject.cs` | HashSet-backed set |
| `libs/server/Objects/SortedSet/SortedSetObject.cs` | Dual SortedSet+Dictionary sorted set |
| `libs/server/Storage/Functions/ObjectStore/ObjectSessionFunctions.cs` | Object store ISessionFunctions |
| `libs/server/Storage/Functions/ObjectStore/RMWMethods.cs` | Object store RMW callbacks |
| `libs/server/Storage/Functions/ObjectStore/ReadMethods.cs` | Object store read callbacks with Expiration-based expiry |
| `libs/server/Storage/Functions/ObjectStore/UpsertMethods.cs` | Object store write callbacks with size tracking |
| `libs/server/Storage/Functions/ObjectStore/DeleteMethods.cs` | Object store delete with CacheSizeTracker update |
| `libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs` | Object store AOF log helpers |
| `libs/server/Storage/Functions/ObjectStore/VarLenInputMethods.cs` | Object store record sizing |
| `libs/server/Storage/Functions/ObjectStore/CallbackMethods.cs` | Object store lifecycle callbacks |

## 4. Performance Notes

- **Heap indirection**: Object store values are pointers to heap objects. This means Tsavorite only stores a reference in the log, making in-place updates trivially successful (no space check needed), but incurring GC pressure for the heap objects themselves.
- **Serialization state machine**: The CAS-based REST/SERIALIZING/SERIALIZED transitions avoid locking during checkpoints. The spin-yield on contention is acceptable because serialization contention is rare (only during checkpoint transitions).
- **Per-field expiration lazy cleanup**: Expired fields are not proactively removed. They are cleaned up on access or during scans. This avoids background timer overhead but means memory is not reclaimed until the field is touched.
- **SortedSet dual structure**: The duplicate storage (SortedSet + Dictionary) doubles memory for sorted sets but provides O(log n) range queries and O(1) element lookups simultaneously. This matches Redis's skiplist+dict design.
- **Size tracking granularity**: Size updates happen per-operation through the `sizeChange` output parameter, allowing the `CacheSizeTracker` to maintain running totals without scanning.

## 5. Rust Mapping Notes

- **IGarnetObject trait**: Define a trait with `fn operate(&mut self, input: &ObjectInput, output: &mut ObjectOutput) -> (bool, i64)` plus serialization methods.
- **GarnetObjectBase**: Use an enum with variants for each type rather than class hierarchy. Rust enums with data are more natural than trait objects for a closed set of types.
- **Serialization state machine**: Replace `Interlocked.CompareExchange` with `AtomicU32` and `compare_exchange`. The spin-yield maps to `std::hint::spin_loop()`.
- **Dictionary/HashSet backing**: Use `HashMap<Vec<u8>, Vec<u8>>` for Hash, `HashSet<Vec<u8>>` for Set. Consider `indexmap` for ordered iteration.
- **SortedSet dual structure**: Use `BTreeSet<(OrderedFloat<f64>, Vec<u8>)>` + `HashMap<Vec<u8>, f64>`. The `OrderedFloat` wrapper handles NaN ordering.
- **Per-field expiration**: Use `HashMap<Vec<u8>, i64>` + `BinaryHeap` (reversed for min-heap).
- **CopyUpdate / Clone**: Implement `Clone` for each object type. The serialization-on-checkpoint pattern translates to serializing to a `Vec<u8>` under an `AtomicU32` state guard.
- **LinkedList**: Rust's `std::collections::LinkedList` is a direct equivalent. For better cache behavior, consider `VecDeque` (which Redis itself uses internally for small lists via listpack/quicklist).

## 6. Kotlin/JVM Mapping Notes

- **IGarnetObject interface**: Direct mapping to a Kotlin interface. The type byte can use a sealed class hierarchy for type-safe dispatch.
- **Serialization state machine**: Use `AtomicInteger` with `compareAndSet`. The `Thread.yield()` maps to `Thread.yield()`.
- **Backing structures**: `HashMap<ByteArray, ByteArray>` with a custom `equals`/`hashCode` wrapper (ByteArray does not have structural equality in Kotlin). Consider `ByteBuffer` keys wrapped in a value class.
- **SortedSet**: Use `TreeSet<Pair<Double, ByteArray>>` with a custom comparator + `HashMap`. Java's `TreeSet` is a Red-Black tree, functionally equivalent to .NET's `SortedSet`.
- **Size tracking**: JVM object overhead varies by implementation. Use `Instrumentation.getObjectSize()` for accurate measurement or estimate based on known object layouts.
- **Per-field expiration**: `HashMap` + `PriorityQueue` maps directly; Java's `PriorityQueue` is a min-heap by default.
- **GC pressure**: The heap-allocated object model is natural on the JVM. Consider off-heap allocation for large sorted sets using `DirectByteBuffer` or a library like Chronicle Map.

## 7. Open Questions

1. **Object store necessity**: Could a reimplementation unify the two stores by using a tagged union value type, avoiding the complexity of dual-store transactions?
2. **Per-field expiration scalability**: The lazy cleanup model means a hash with millions of expired fields will not reclaim memory until accessed. Should there be a background sweep?
3. **Serialization format versioning**: The current binary format has no explicit version tag per object. How should schema evolution be handled across Garnet versions?
4. **SortedSet memory**: The dual SortedSet+Dictionary structure doubles memory. Redis uses a ziplist encoding for small sorted sets. Should a compact encoding be implemented for small cardinalities?
5. **Custom object extensibility**: The factory-based custom object registration requires serializer support. How should a reimplementation handle plugin object types without modifying core code?
