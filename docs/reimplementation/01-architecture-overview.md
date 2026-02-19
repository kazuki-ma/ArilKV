# Architecture Overview

## 1. Overview

Garnet is a high-performance, Redis-compatible cache store developed by Microsoft Research. It achieves roughly 100x throughput improvement over Redis through a combination of epoch-based concurrency (instead of Redis's single-threaded model), zero-copy RESP parsing with unsafe pointer arithmetic, generic monomorphization to eliminate virtual dispatch, and the Tsavorite hybrid log storage engine that seamlessly spans DRAM and SSD.

The system is written in approximately 310K lines of C#, structured as a layered architecture where each layer is designed for minimal allocation and maximal throughput. Garnet accepts standard Redis clients over TCP using the RESP protocol and supports the vast majority of Redis commands, clustering, Lua scripting, pub/sub, and custom extensibility.

## 2. Detailed Design

### 2.1 Design Philosophy

Garnet's architecture is governed by four core principles:

1. **Zero-allocation hot path**: The request processing path from TCP receive to storage lookup to response send avoids heap allocation. Buffers are pooled and pinned; parsing operates on raw `byte*` pointers; response writing happens directly into pre-allocated send buffers.

2. **Generic monomorphization**: C# generics are used throughout the storage and session layers not for polymorphism but for compile-time specialization. `TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>` is instantiated with concrete struct types, eliminating virtual dispatch and enabling the JIT to inline deep call chains. [Source: libs/server/StoreWrapper.cs:L31-L120]

3. **Epoch-based concurrency**: Rather than locking or single-threading, Garnet uses a lightweight epoch framework (`LightEpoch`) that allows concurrent threads to operate on shared data structures with minimal coordination. Threads "acquire" an epoch before accessing data, and deferred actions (e.g., memory reclamation) execute only when all threads have moved past a safe epoch. [Source: libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:L15-L17]

4. **Layered separation**: Network I/O, protocol parsing, command dispatch, and storage operations are cleanly separated through interfaces (`IMessageConsumer`, `INetworkSender`, `ISessionFunctions`), enabling each layer to be optimized independently.

### 2.2 Component Dependency Diagram

```
+------------------------------------------------------------------+
|                     GarnetServer (Entry Point)                    |
|                       main/GarnetServer/                          |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
|                    GarnetServer (Host Layer)                      |
|                        libs/host/                                 |
|  - Configuration parsing, logger setup, store initialization     |
|  - Creates StoreWrapper, GarnetServerTcp, GarnetProvider         |
+------------------------------------------------------------------+
         |                    |                         |
         v                    v                         v
+----------------+  +------------------+  +------------------------+
| GarnetServerTcp|  |   StoreWrapper   |  |    ClusterProvider     |
| libs/server/   |  | libs/server/     |  | libs/cluster/          |
| Servers/       |  | StoreWrapper.cs  |  |                        |
|                |  |                  |  | - Replication           |
| - TCP accept   |  | - DatabaseManager|  | - Slot migration       |
| - Connection   |  |   -> MainStore   |  | - Gossip protocol      |
|   management   |  |   -> ObjectStore |  +------------------------+
+----------------+  |   -> VectorStore |
         |          | - AOF log        |
         v          | - Pub/Sub broker |
+----------------+  | - ACL            |
| NetworkHandler |  +------------------+
| (extends           |
|  NetworkSender     v
|  Base)       +------------------+
| libs/common/ | TsavoriteKV      |
| Networking/  | libs/storage/    |
|              | Tsavorite/       |
| - SAEA recv  |                  |
| - Buffer mgmt| - Hash index     |
| - TLS support| - Hybrid log     |
+----------------+ - Checkpointing  |
         |          | - Epoch protect  |
         v          +------------------+
+------------------+         |
| ServerSessionBase|         v
| libs/server/     |  +------------------+
| Sessions/        |  | IDevice          |
| (abstract, holds |  | (Storage devices)|
|  networkSender)  |  | - LocalDevice    |
+------------------+  | - AzureStorage   |
         |            +------------------+
         v
+------------------+
| RespServerSession|
| libs/server/     |
| Resp/            |
|                  |
| - RESP parse     |
| - Cmd dispatch   |
| - Response gen   |
| - AsyncProcessor |
+------------------+
         |
         v
+----------------+
| StorageSession |
| libs/server/   |
| Storage/       |
| Session/       |
|                |
| - GET/SET/RMW  |
| - Object ops   |
| - Vector ops   |
| - Transaction  |
+----------------+
```

### 2.3 Project Structure Map

| Directory | Purpose | Key Entry Points |
|-----------|---------|------------------|
| `main/GarnetServer/` | Entry point binary | `Program.cs` - `Main()` creates `GarnetServer`, calls `Start()` |
| `libs/host/` | Server host orchestration | `GarnetServer.cs` - configures and wires all components |
| `libs/server/Servers/` | TCP server and connection management | `GarnetServerTcp.cs` - accept loop, handler creation |
| `libs/server/Sessions/` | Session base class | `ServerSessionBase.cs` - abstract base implementing `IMessageConsumer` |
| `libs/server/Resp/` | RESP protocol parsing and command dispatch | `RespServerSession.cs` - main session loop |
| `libs/server/Resp/Parser/` | Command parsing and argument handling | `SessionParseState.cs`, `RespCommand.cs` |
| `libs/server/Resp/Vector/` | Vector store command handling | `VectorManager.cs` - vector similarity search subsystem |
| `libs/server/Storage/Session/` | Storage operation layer | `StorageSession.cs` - bridges commands to Tsavorite |
| `libs/server/Storage/Functions/` | Tsavorite callback functions | `MainSessionFunctions.cs` - read/write/RMW callbacks |
| `libs/common/Networking/` | Network transport abstractions | `NetworkHandler.cs`, `GarnetTcpNetworkSender.cs` |
| `libs/common/Memory/` | Buffer pool management | `LimitedFixedBufferPool.cs` - pooled pinned buffers |
| `libs/common/` | Shared utilities | `RespReadUtils.cs`, `RespWriteUtils.cs` |
| `libs/storage/Tsavorite/` | Hybrid log KV store engine | Core storage engine with epoch protection |
| `libs/cluster/` | Redis cluster protocol support | Gossip, slot migration, replication |
| `libs/client/` | .NET client library | Client-side networking and RESP |

### 2.4 Data Flow: Client Connection to Response

The end-to-end flow for a single request is:

1. **TCP Accept** (`GarnetServerTcp.AcceptEventArg_Completed`): The listening socket accepts a connection. A `ServerTcpNetworkHandler` is created, wrapping a `GarnetTcpNetworkSender` and a pooled receive buffer from `LimitedFixedBufferPool`. The handler registers an async receive callback.

2. **Receive** (`TcpNetworkHandlerBase.HandleReceiveWithoutTLS`): When bytes arrive on the socket via `SocketAsyncEventArgs`, the handler updates `networkBytesRead` and calls `OnNetworkReceiveWithoutTLS`, which sets transport pointers equal to network pointers (zero-copy for non-TLS) and calls `Process()`.

3. **Session Creation** (`NetworkHandler.Process`): On the first receive, `TryCreateMessageConsumer` is called, which creates a `RespServerSession`. This session extends `ServerSessionBase` (which implements `IMessageConsumer`) and holds Tsavorite session contexts (`BasicContext` and `LockableContext` for main and object stores), the parse state, and output buffer pointers. Vector operations are handled through `StorageSession`/`VectorManager` rather than a stored vector context. [Source: libs/server/Sessions/ServerSessionBase.cs:L11]

4. **Message Consumption** (`RespServerSession.TryConsumeMessages`): The session receives a raw `byte*` buffer and length. It acquires the epoch, gets a response buffer via `EnterAndGetResponseObject`, and enters the `ProcessMessages` loop.

5. **Command Parsing** (`ProcessMessages` loop): Iterates while bytes remain, calling `ParseCommand` to identify the RESP command. Arguments are parsed into `SessionParseState`, a pinned array of `ArgSlice` (pointer + length) that avoids any string allocation.

6. **Command Dispatch** (`ProcessBasicCommands`): A `switch` expression dispatches to the appropriate handler method (e.g., `NetworkGET`, `NetworkSET`). The handler accesses key/value arguments through `parseState.GetArgSliceByRef(i)`.

7. **Storage Operation** (`StorageSession.GET`): The handler calls the `GarnetApi` method. `GarnetApi<TContext, TObjectContext, TVectorContext>` has three type parameters but only stores two context instances at runtime (main and object); the `TVectorContext` is a phantom type parameter -- there is no `vectorContext` field in the struct. Vector operations route through `StorageSession` and `VectorManager` directly. [Source: libs/server/API/GarnetApi.cs:L24-L38] The API delegates to `StorageSession`, which calls `basicContext.Read()` on the Tsavorite store. Tsavorite looks up the hash index, finds the record in the hybrid log, and invokes `MainSessionFunctions.SingleReader` to format the response. If the key is a VectorSet type, `SingleReader` returns a WRONGTYPE error. [Source: libs/server/Storage/Functions/MainStore/ReadMethods.cs:L29-L45]

8. **Response Writing**: The `SingleReader` callback writes the RESP-formatted response directly into the output `SpanByteAndMemory`, which points at the send buffer. The session advances `dcurr` (current write position) accordingly.

9. **Send** (`RespServerSession.Send`): After processing all commands in the batch (pipelining support), the accumulated response bytes are sent via `networkSender.SendResponse()`, which calls `Socket.SendAsync` with the SAEA buffer.

### 2.5 Key Abstractions and Interfaces

- **`IMessageConsumer`**: Single method `TryConsumeMessages(byte* reqBuffer, int bytesRead)` returning `int` (bytes consumed). Implemented by `ServerSessionBase` (abstract base class that holds shared state like `bytesRead` and `networkSender`, plus abstract methods `Publish` and `PatternPublish` for pub/sub), with `RespServerSession` as the concrete implementation. [Source: libs/common/Networking/IMessageConsumer.cs:L11-L20, libs/server/Sessions/ServerSessionBase.cs:L11-L55]

- **`INetworkSender`**: Provides `EnterAndGetResponseObject`, `SendResponse`, `GetResponseObjectHead/Tail`. The key abstraction for zero-copy response writing. Implemented by `GarnetTcpNetworkSender` for non-TLS, and by `NetworkHandler` itself for TLS.

- **`IServerHook`**: Interface between the network layer and session creation. `GarnetServerTcp` implements `TryCreateMessageConsumer` and `DisposeMessageConsumer`.

- **`ISessionFunctions<K,V,I,O,C>`**: Tsavorite's callback interface for read/write/RMW operations. `MainSessionFunctions` implements this as a `readonly struct` for maximum inlining.

- **`IGarnetApi`**: The API layer that `RespServerSession` uses for all storage operations. `GarnetApi<TContext, TObjectContext, TVectorContext>` takes three type parameters but only stores two context instances: main store (`TContext`) and object store (`TObjectContext`). The `TVectorContext` is a phantom type parameter used for compile-time constraint checking only -- the constructor does not accept a vector context, and vector operations route through `StorageSession`/`VectorManager` directly. It abstracts over `BasicContext` vs `LockableContext` (for transactions). [Source: libs/server/API/GarnetApi.cs:L24-L38]

### 2.6 Epoch-Based Concurrency Model

Garnet uses `LightEpoch` instances for epoch-based concurrency protection. Each epoch instance maintains a cache-line-aligned table where each thread registers its current epoch value via `[ThreadStatic]` metadata stored in a nested `Metadata` class [Source: libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:L50-L83]. Operations that need protection (e.g., reading from the hash index) acquire the current epoch; deferred cleanup actions (e.g., freeing old log pages) only execute when all threads have advanced past the target epoch. This provides the safety guarantees of reader-writer locks with near-zero overhead on the read path -- just a single atomic write to the epoch table entry. The cluster session acquires/releases its own epoch independently of the storage-layer epoch used by Tsavorite operations.

## 3. Source File Map

| File | Purpose | Key Lines |
|------|---------|-----------|
| `main/GarnetServer/Program.cs` | Entry point | L13-L31 |
| `libs/host/GarnetServer.cs` | Server orchestration, store creation | L32-L120 |
| `libs/server/StoreWrapper.cs` | Wraps DatabaseManager, AOF, Pub/Sub, ACL | L31-L120 |
| `libs/server/Sessions/ServerSessionBase.cs` | Abstract base class implementing IMessageConsumer | L11-L55 |
| `libs/server/Servers/GarnetServerTcp.cs` | TCP accept loop | L65-L256 |
| `libs/server/Servers/ServerTcpNetworkHandler.cs` | Server-side TCP handler | L10-L16 |
| `libs/common/Networking/NetworkHandler.cs` | Base network handler with buffer mgmt | L19-L714 |
| `libs/common/Networking/TcpNetworkHandlerBase.cs` | TCP-specific receive loop | L20-L259 |
| `libs/server/Resp/RespServerSession.cs` | RESP session and command dispatch | L45-L1551 |
| `libs/server/Storage/Session/StorageSession.cs` | Bridge to Tsavorite operations | L21-L100 |
| `libs/common/Memory/LimitedFixedBufferPool.cs` | Tiered buffer pool | L19-L243 |

## 4. Performance Notes

- **No per-request allocation**: The entire hot path operates on pinned buffers and pointer arithmetic. `SessionParseState` uses `GC.AllocateArray<T>(pinned: true)` so `ArgSlice` pointers remain stable.
- **Pipelining**: Multiple commands in a single TCP receive are processed in a tight loop without returning to the event loop, amortizing syscall overhead.
- **Generic specialization**: The type aliases at the top of `RespServerSession.cs` (lines 23-40) show how deeply nested generics produce a single monomorphized code path from session to storage.
- **Event-driven I/O model**: Connections share the .NET thread pool via SAEA callbacks. Each connection's `ProcessMessages` loop runs synchronously on the I/O completion callback thread for the duration of a receive, but there is no dedicated thread per connection. This avoids per-connection thread overhead while still avoiding context switches during request processing.

## 5. Rust Mapping Notes

- **Epoch protection** [Difficulty: Medium]: Use `crossbeam-epoch` for epoch-based reclamation. The core epoch acquire/release pattern maps directly. The challenge is ensuring epoch protection composes correctly with Rust's async runtime -- `crossbeam-epoch::Guard` is `!Send`, preventing it from being held across `.await` points. This requires structuring the code so that storage operations complete synchronously within an epoch guard, matching Garnet's design where `ProcessMessages` holds the epoch for the entire synchronous batch.

- **Monomorphization** [Difficulty: Low]: Rust generics provide true compile-time monomorphization, identical to C#'s JIT monomorphization. The deeply nested generic chain (`TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>` -> `MainSessionFunctions`) maps directly to Rust trait generics. Use `#[inline]` on trait implementations for the `ISessionFunctions` equivalent to ensure the compiler inlines callback methods. Performance parity is expected.

- **Async I/O** [Difficulty: Medium]: Use `tokio` with `io_uring` backend (`tokio-uring`) or `monoio` for thread-per-core designs. `io_uring` natively supports buffer pools via `IORING_OP_PROVIDE_BUFFERS`, which maps to Garnet's `LimitedFixedBufferPool`. Key architectural decision: Garnet's SAEA model runs synchronous processing on I/O completion threads. In Rust, this maps to either (a) thread-per-core with synchronous processing in the event loop, or (b) `tokio` tasks pinned to specific threads. The former is simpler but limits cross-core work stealing.

- **Buffer pools** [Difficulty: Medium]: Use a custom slab allocator with `mmap`-backed pinned memory, organized as size-class queues mirroring `LimitedFixedBufferPool`'s tiered design (4 levels, power-of-2 sizes). `Vec<u8>` is unsuitable because Rust's allocator may relocate on reallocation. `bytes::BytesMut` with a custom pool is an alternative for the send path but lacks stable pointer access for the receive path.

- **SpanByte equivalent** [Difficulty: Low]: Use a `(*const u8, usize)` raw pointer tuple or a custom `SliceMut<'a>` type for zero-copy buffer references. Rust's `&[u8]` slices provide the same semantics with borrow-checker-enforced lifetime safety, which is strictly better than C#'s unsafe pointer approach. The 4-byte length prefix in `SpanByte` can be represented as `#[repr(C)] struct SpanByte { len: i32, data: [u8; 0] }` for layout compatibility with the log format.

- **Layered separation** [Difficulty: Low]: Garnet's layer interfaces (`IMessageConsumer`, `INetworkSender`, `IServerHook`, `ISessionFunctions`) map directly to Rust traits. The trait-based design with generic type parameters preserves the same zero-dispatch-overhead properties via monomorphization.

## 6. Kotlin/JVM Mapping Notes

- **No value types** [Difficulty: High, Risk: Performance]: JVM lacks C# structs. `SpanByte`, `ArgSlice`, `RecordInfo` must become either (a) classes with heap allocation overhead on every access, or (b) off-heap layouts accessed via `sun.misc.Unsafe` / `ByteBuffer` / Project Panama `MemorySegment` (JDK 22+). Option (a) creates GC pressure on the hot path; option (b) requires abandoning JVM safety guarantees. This is the most pervasive challenge -- these types appear in every layer of the architecture.

- **Type erasure** [Difficulty: High, Risk: Performance]: JVM generics are erased, so monomorphization is impossible. The `TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>` chain becomes `TsavoriteKV<Object, Object, Object, Object>` at runtime, with virtual dispatch on every callback invocation. Workarounds: (a) code generation via KSP/annotation processors to produce specialized classes, (b) manual specialization for the SpanByte+SpanByte hot path, (c) accepting 20-40% overhead from virtual dispatch and boxing. This is the single largest structural disadvantage for JVM reimplementation.

- **Epoch protection** [Difficulty: Medium]: Implement manually with `VarHandle` for atomic operations and padded `AtomicLong` arrays for per-thread epoch entries. `ThreadLocal<T>` replaces `[ThreadStatic]` but is slower due to hash-table lookup vs. C#'s direct thread-local storage slot. Use `@Contended` annotation (JDK 9+) for false-sharing prevention on epoch table entries.

- **Pinned memory** [Difficulty: Medium]: Use `ByteBuffer.allocateDirect()` for off-heap memory that won't be moved by GC. Direct buffers have ~20ns allocation overhead vs. near-zero for C#'s `GC.AllocateArray(pinned: true)`. Pool aggressively to amortize. Netty's `PooledByteBufAllocator` provides a production-ready tiered pool.

- **Unsafe pointers** [Difficulty: High, Risk: Safety]: Replace with `sun.misc.Unsafe` address arithmetic or Project Panama `MemorySegment`. All pointer arithmetic in `RespReadUtils`, `NetworkHandler`, and Tsavorite becomes explicit `Unsafe.getInt(base + offset)` calls. This is verbose, error-prone, and cannot be checked by the compiler. Panama's `MemorySegment` provides bounds checking but is newer and less battle-tested in high-performance contexts.

## 7. Open Questions

1. How does the server handle buffer growth under sustained large-request loads? The doubling/shrinking strategy in `NetworkHandler` (lines 502-528) needs stress-testing analysis.
2. The cluster epoch (acquired via `clusterSession?.AcquireCurrentEpoch()`) and the Tsavorite storage epoch are separate instances. The cluster epoch protects cluster metadata operations, while the storage epoch protects hash index and log operations within Tsavorite.
3. How does the `LimitedFixedBufferPool` interact with GC pressure? Since buffers are `GC.AllocateArray(pinned: true)`, they are never relocated -- what is the fragmentation impact?
4. The exact threading model for TLS connections differs from non-TLS (async vs sync). What is the performance delta?
