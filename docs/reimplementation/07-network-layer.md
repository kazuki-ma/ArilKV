# Network Layer

## 1. Overview

Garnet's network layer is built on the .NET `SocketAsyncEventArgs` (SAEA) pattern, a high-performance asynchronous I/O model that avoids per-operation allocation by reusing event argument objects. The layer manages TCP connection accept, per-connection receive/send buffer lifecycle, TLS integration, and backpressure throttling. It is designed so that the common non-TLS path processes received data synchronously on the I/O completion thread, avoiding any context switches or task scheduling between receiving bytes and sending a response.

The network layer is primarily contained in `libs/common/Networking/` with server-specific wiring in `libs/server/Servers/`.

## 2. Detailed Design

### 2.1 Connection Accept Loop

The accept loop is initiated in `GarnetServerTcp.Start()` [Source: libs/server/Servers/GarnetServerTcp.cs:L111-L122]:

```
listenSocket.Bind(endpoint)
listenSocket.Listen(512)        // backlog of 512
listenSocket.AcceptAsync(acceptEventArg)
```

The accept uses a single reusable `SocketAsyncEventArgs` (`acceptEventArg`). When a connection is accepted, `AcceptEventArg_Completed` fires:

```
AcceptEventArg_Completed:
  loop:
    HandleNewConnection(e)    // Create handler for accepted socket
    e.AcceptSocket = null     // Reset for reuse
    if listenSocket.AcceptAsync(e) returns false:
      continue loop           // Synchronous completion, handle immediately
    else:
      break                   // Async completion, callback will fire again
```

This tight loop handles burst accepts without returning to the thread pool. [Source: libs/server/Servers/GarnetServerTcp.cs:L124-L136]

### 2.2 Connection Setup

`HandleNewConnection` [Source: libs/server/Servers/GarnetServerTcp.cs:L138-L203] performs:

1. **TCP NoDelay**: Sets `NoDelay = true` on accepted sockets (except Unix domain sockets) to disable Nagle's algorithm for low-latency responses.

2. **Connection limit check**: Atomically increments `activeHandlerCount` via `Interlocked.Increment`. If the count exceeds `networkConnectionLimit`, the socket is disposed immediately.

3. **Handler creation**: Creates a `ServerTcpNetworkHandler`, which is a `TcpNetworkHandler<GarnetServerTcp>`, which is a `TcpNetworkHandlerBase<GarnetServerTcp, GarnetTcpNetworkSender>`. The constructor chain:
   - Creates a `GarnetTcpNetworkSender` wrapping the socket
   - Allocates a receive buffer from `LimitedFixedBufferPool`
   - Registers in `activeHandlers` dictionary

4. **Start receive**: Calls `handler.Start()`, which creates a new `SocketAsyncEventArgs` for receiving, sets its buffer to the allocated receive buffer, and calls `socket.ReceiveAsync()`.

### 2.3 Class Hierarchy

```
NetworkSenderBase  (abstract, in libs/common/Networking/)
  |-- Implements: INetworkSender
  |-- Provides: base sender functionality (used when TLS mode makes NetworkHandler act as sender)
  |
  +-- NetworkHandler<TServerHook, TNetworkSender>  (abstract)
        |-- Holds: networkReceiveBuffer, transportReceiveBuffer, TNetworkSender
        |-- Logic: Process(), TryProcessRequest(), buffer shift/double/shrink
        |-- Note: Extends NetworkSenderBase so it can act as INetworkSender for TLS mode
        |          (GetNetworkSender() returns `this` for TLS, or the underlying TNetworkSender for non-TLS)
        |
        +-- TcpNetworkHandlerBase<TServerHook, TNetworkSender>  (abstract)
              |-- Holds: Socket, SocketAsyncEventArgs for receive
              |-- Logic: Start(), HandleReceiveWithoutTLS(), HandleReceiveWithTLSAsync()
              |
              +-- TcpNetworkHandler<TServerHook>
                    |-- Specializes TNetworkSender = GarnetTcpNetworkSender
                    |-- Constructor creates GarnetTcpNetworkSender from socket
                    |
                    +-- ServerTcpNetworkHandler  (sealed, in libs/server/)
                          |-- Final concrete class for server connections
```

[Source: libs/common/Networking/NetworkHandler.cs:L19, libs/common/Networking/NetworkSenderBase.cs]

### 2.4 Receive Path (Non-TLS)

The non-TLS receive path is synchronous and tight [Source: libs/common/Networking/TcpNetworkHandlerBase.cs:L196-L216]:

```
HandleReceiveWithoutTLS(sender, e):
  loop:
    if e.BytesTransferred == 0 or error or serverDisposed:
      Dispose(e)
      break
    OnNetworkReceiveWithoutTLS(e.BytesTransferred)
    e.SetBuffer(networkReceiveBuffer, networkBytesRead, remaining)
    if socket.ReceiveAsync(e):
      break        // Will call back asynchronously
    // else: synchronous completion, loop again
```

`OnNetworkReceiveWithoutTLS` [Source: libs/common/Networking/NetworkHandler.cs:L266-L278]:
1. Updates `networkBytesRead += bytesTransferred`
2. Sets `transportReceiveBuffer = networkReceiveBuffer` (zero-copy: no separate transport buffer)
3. Calls `Process()` which invokes `session.TryConsumeMessages()`
4. Calls `EndTransformNetworkToTransport()` to sync `networkBytesRead` back
5. Calls `UpdateNetworkBuffers()` to shift/double/shrink as needed

Key insight: In non-TLS mode, the receive buffer IS the transport buffer. There is no copy between network and application layers. The `byte*` pointer from the receive buffer is passed directly to the RESP parser (see [Doc 09 Section 2.1](./09-request-lifecycle.md#21-complete-get-trace) Steps 2-4 for the complete receive-to-parse data flow).

### 2.5 Receive Path (TLS)

With TLS, a separate transport buffer is needed because `SslStream` decrypts into a different buffer. The flow uses a state machine with three states [Source: libs/common/Networking/NetworkHandler.cs:L95-L96]:

- **Rest**: No active reader task; new data triggers synchronous `Read()`
- **Active**: A reader task is processing; callers wait on `expectingData` semaphore
- **Waiting**: The reader issued `sslStream.ReadAsync()` and is waiting for network data; signaled via `receivedData` semaphore

**Why three states instead of two?** A naive two-state design (idle vs. reading) would either (a) always use async `ReadAsync`, paying the async state machine overhead even when data is already buffered, or (b) risk race conditions when new network data arrives while a reader is mid-processing. The three-state design solves both problems: **Rest** allows the first read attempt to be synchronous (no task allocation); **Active** prevents multiple concurrent readers from entering the processing path; **Waiting** distinguishes "reader is blocked on I/O" from "reader is processing data," allowing the SAEA completion callback to signal the blocked reader via `receivedData` semaphore without interfering with active processing. The key transition is Active->Waiting, which occurs only when `SslStream.ReadAsync` does NOT complete synchronously -- this is the only point where a true `Task` is allocated.

This design minimizes async state machine overhead by attempting synchronous reads first (`result.IsCompletedSuccessfully`) and only falling back to async when the SslStream read truly blocks. When a truly async read occurs, the `SslReaderAsync` fallback path at `NetworkHandler.cs:L392` handles task-based continuation: it awaits the pending `ReadAsync` result and then re-enters the processing loop. This async fallback is architecturally significant for TLS performance, as it determines how the handler transitions between synchronous and asynchronous execution modes. [Source: libs/common/Networking/NetworkHandler.cs:L287-L313, L392]

### 2.6 Buffer Management

**Receive buffer lifecycle**:
- **Allocation**: `networkPool.Get(initialReceiveBufferSize)` returns a `PoolEntry` with a pinned `byte[]` and `byte*` pointer [Source: libs/common/Networking/TcpNetworkHandlerBase.cs:L253-L258]
- **Shift**: After processing, consumed bytes are shifted to the start via `Buffer.MemoryCopy` [Source: libs/common/Networking/NetworkHandler.cs:L530-L540]
- **Double**: If buffer is full after processing, a 2x buffer is allocated via `networkPool.Get(networkReceiveBuffer.Length * 2)`, data is copied via `Array.Copy`, and the old `PoolEntry` is disposed (which returns it to the pool) [Source: libs/common/Networking/NetworkHandler.cs:L502-L510]
- **Shrink**: If buffer was doubled beyond `maxReceiveBufferSize` but current data fits in max, shrink back down [Source: libs/common/Networking/NetworkHandler.cs:L513-L528]

### 2.7 LimitedFixedBufferPool Design

The buffer pool is organized as an array of `numLevels` (default 4) concurrent queues, where level `i` holds buffers of size `minAllocationSize * 2^i`. [Source: libs/common/Memory/LimitedFixedBufferPool.cs:L19-L55]

```
Level 0: buffers of size minAllocationSize       (e.g., 128 KB)
Level 1: buffers of size minAllocationSize * 2    (e.g., 256 KB)
Level 2: buffers of size minAllocationSize * 4    (e.g., 512 KB)
Level 3: buffers of size minAllocationSize * 8    (e.g., 1024 KB)
```

Each level uses a `ConcurrentQueue<PoolEntry>` with a maximum of `maxEntriesPerLevel` (default 16) items. The `Get` method:
1. Atomically increments `totalReferences` (prevents Dispose race)
2. Computes level from requested size: `Position(size)` checks power-of-2 and computes `log2(size/min)`
3. Tries to dequeue from the appropriate level
4. If empty, allocates new pinned array: `GC.AllocateArray<byte>(size, pinned: true)` with `Unsafe.AsPointer` for the `byte*` [Source: libs/common/Memory/PoolEntry.cs:L31-L37]

The `Return` method clears the buffer (`Array.Clear`) before re-enqueueing, preventing information leakage between connections. If the level is full, the buffer is simply not returned (becomes GC garbage). [Source: libs/common/Memory/LimitedFixedBufferPool.cs:L85-L104]

### 2.8 Send Path and Backpressure

The send side uses `GarnetTcpNetworkSender` [Source: libs/common/Networking/GarnetTcpNetworkSender.cs:L17-L338]:

**Response buffer management**:
- A `LightConcurrentStack<GarnetSaeaBuffer>` (capacity `2 * ThrottleMax`) caches reusable send buffers
- Each `GarnetSaeaBuffer` contains a `SocketAsyncEventArgs` + a `PoolEntry` buffer from the pool
- `EnterAndGetResponseObject`: Acquires the `SpinLock` (L123), pops a buffer from the stack (or creates new), returns `byte*` head and tail pointers. The lock is NOT released here -- it remains held.
- `ExitAndReturnResponseObject`: Returns the buffer to the stack AND releases the SpinLock (L150). The lock is held for the entire duration between Enter and Exit, spanning the full `ProcessMessages` call. This serializes response writing between the main thread and the `AsyncProcessor`. [Source: libs/common/Networking/GarnetTcpNetworkSender.cs:L120-L151]

**Throttle mechanism** [Source: libs/common/Networking/GarnetTcpNetworkSender.cs:L262-L281]:

The `throttleCount` tracks in-flight `SendAsync` operations. Each send increments it; the SAEA completion callback decrements it. When `throttleCount >= ThrottleMax` (default 8), the caller blocks on a `SemaphoreSlim`. The completion callback releases the semaphore when the count drops. This prevents unbounded send buffer accumulation when the network or client is slow.

**Send flow**:
```
SendResponse(offset, size):
  _r = responseObject
  responseObject = null
  Send(socket, _r, offset, size):
    Interlocked.Increment(throttleCount)
    if throttleCount > ThrottleMax: throttle.Wait()
    _r.socketEventAsyncArgs.SetBuffer(offset, size)
    _r.socketEventAsyncArgs.UserToken = _r
    socket.SendAsync(_r.socketEventAsyncArgs)

SeaaBuffer_Completed:
  ReturnBuffer(userToken)
  if Interlocked.Decrement(throttleCount) >= ThrottleMax:
    throttle.Release()
```

### 2.9 TLS Integration

TLS is layered transparently via `SslStream` wrapping a custom `NetworkHandlerStream`. In TLS mode:
- Separate transport send/receive buffers are allocated
- `NetworkHandler` implements `INetworkSender` itself, routing `SendResponse` through `sslStream.Write()` + `Flush()`
- Receive data flows: socket -> networkReceiveBuffer -> SslStream.ReadAsync -> transportReceiveBuffer -> Process()
- The `GetNetworkSender()` method returns `this` (the NetworkHandler) for TLS, or the underlying `GarnetTcpNetworkSender` for non-TLS [Source: libs/common/Networking/NetworkHandler.cs:L354]

## 3. Source File Map

| File | Purpose | Key Lines |
|------|---------|-----------|
| `libs/server/Servers/GarnetServerTcp.cs` | TCP accept loop, connection management | L65-L256 |
| `libs/server/Servers/ServerTcpNetworkHandler.cs` | Concrete server TCP handler | L10-L16 |
| `libs/common/Networking/NetworkHandler.cs` | Base handler with buffer management and TLS | L19-L714 |
| `libs/common/Networking/TcpNetworkHandlerBase.cs` | TCP socket receive loop | L20-L259 |
| `libs/common/Networking/TcpNetworkHandler.cs` | Specializes with GarnetTcpNetworkSender | L14-L24 |
| `libs/common/Networking/GarnetTcpNetworkSender.cs` | Send path with throttle | L17-L338 |
| `libs/common/Networking/GarnetSaeaBuffer.cs` | SAEA + pinned buffer pair | L12-L47 |
| `libs/common/Networking/LightConcurrentStack.cs` | SpinLock-based stack for buffer reuse | L10-L72 |
| `libs/common/Memory/LimitedFixedBufferPool.cs` | Tiered buffer pool | L19-L243 |
| `libs/common/Memory/PoolEntry.cs` | Pinned byte array with pointer | L13-L56 |
| `libs/common/Memory/PoolLevel.cs` | ConcurrentQueue wrapper per level | L8-L31 |
| `libs/common/Networking/BufferSizeUtils.cs` | Buffer size calculation | L9-L45 |
| `libs/common/Networking/NetworkSenderBase.cs` | Abstract base class for INetworkSender | - |
| `libs/common/Networking/NetworkHandlerStream.cs` | Custom Stream for TLS SslStream | - |
| `libs/common/Networking/INetworkHandler.cs` | Handler interface | - |
| `libs/common/Networking/WireFormat.cs` | Protocol detection (RESP vs other) | - |
| `libs/server/Servers/GarnetServerBase.cs` | Base class with handler tracking | L18-L100 |

## 4. Performance Notes

- **Zero-copy non-TLS path**: Receive buffer pointer is used directly as the transport buffer. No memcpy between network receive and application processing.
- **SAEA reuse**: The accept loop reuses a single `SocketAsyncEventArgs`. Per-connection receive uses one SAEA for the lifetime of the connection. Send buffers are pooled in `LightConcurrentStack`.
- **SpinLock for send serialization**: The `GarnetTcpNetworkSender` uses `System.Threading.SpinLock` (not `lock`/`Monitor`) for the response object lifecycle. This avoids kernel transitions for the very brief critical section.
- **Pinned arrays**: All network buffers use `GC.AllocateArray<byte>(size, pinned: true)`, which allocates on the pinned object heap (POH). This avoids GC relocation and provides stable `byte*` pointers without `GCHandle`.
- **Buffer clear on return**: `Array.Clear` on returned buffers prevents stale data exposure but adds overhead. This is a security-performance trade-off.
- **Synchronous completion fast path**: Both accept and receive loops check for synchronous completion (`AcceptAsync`/`ReceiveAsync` returning `false`) and loop without returning to the thread pool. This is critical for throughput under high load.

## 5. Rust Mapping Notes

- **SAEA equivalent** [Difficulty: Medium]: Use `io_uring` via `tokio-uring` or `monoio` for zero-copy async I/O. `io_uring` natively supports buffer pools via `IORING_OP_PROVIDE_BUFFERS`, directly mapping to `LimitedFixedBufferPool`. Key architectural decision: Garnet's SAEA model reuses a single `SocketAsyncEventArgs` per connection for the connection lifetime. With `io_uring`, each submission queue entry (SQE) is similarly reusable. The synchronous completion fast path (where `ReceiveAsync` returns `false` and the handler loops without returning to the thread pool) maps to `io_uring`'s completion queue polling mode. Performance parity or improvement is expected due to `io_uring`'s reduced syscall overhead.

- **Buffer pool** [Difficulty: Medium]: Implement with `mmap`-backed memory and a custom slab allocator using `crossbeam::ArrayQueue` per size class. `Vec<u8>` won't work because Rust's global allocator may relocate on reallocation and does not guarantee stable addresses. The 4-level tiered design (128KB to 1024KB) maps to 4 `ArrayQueue<Box<[u8]>>` instances, where each `Box<[u8]>` is allocated once via `mmap` and reused. Buffer clear on return (`Array.Clear` in C#) maps to `ptr::write_bytes(buf.as_mut_ptr(), 0, buf.len())` -- same security trade-off applies.

- **Send throttle** [Difficulty: Low]: Use `tokio::sync::Semaphore` with permits equal to `ThrottleMax` (default 8). Direct mapping -- the semaphore-based backpressure pattern is identical. The `Interlocked.Increment`/`Decrement` on `throttleCount` maps to `Semaphore::acquire()`/`add_permits()`.

- **TLS** [Difficulty: Medium]: Use `rustls` with a custom `AsyncRead`/`AsyncWrite` wrapper. `rustls` avoids the intermediate buffer copy that OpenSSL requires, potentially matching or beating C#'s `SslStream` performance. The three-state TLS state machine (Rest/Active/Waiting) in `NetworkHandler` can be simplified with `rustls`'s `ServerConnection` which handles buffering internally. The `SslReaderAsync` fallback path maps to `rustls`'s `read_tls()` + `process_new_packets()` pattern.

- **SpinLock** [Difficulty: Low]: Use `parking_lot::Mutex` (which spins briefly before parking) or `std::sync::Mutex`. `parking_lot` is preferred for the `GarnetTcpNetworkSender` serialization lock because the critical section spans the entire `ProcessMessages` duration -- `parking_lot` will park the waiting thread (AsyncProcessor) rather than spinning, which is appropriate for this longer hold duration.

- **LightConcurrentStack** [Difficulty: Low]: A `crossbeam::ArrayQueue` with bounded capacity `2 * ThrottleMax`. Direct mapping.

## 6. Kotlin/JVM Mapping Notes

- **NIO equivalent** [Difficulty: Low]: Use Netty's `EpollEventLoop` for epoll-based I/O (preferred over `java.nio.channels.AsynchronousSocketChannel` for performance). Netty provides a complete equivalent of Garnet's network layer: accept loop (`ServerBootstrap`), per-connection handler pipeline (`ChannelHandler`), buffer lifecycle management (`ByteBuf`). The synchronous completion fast path in Garnet's receive loop does not have a direct Netty equivalent -- Netty always returns to the event loop between reads. This adds one event loop iteration (~1-2us) per batch vs. Garnet's tight loop.

- **Buffer pool** [Difficulty: Low]: Use Netty's `PooledByteBufAllocator` which provides a similar tiered pooling strategy with thread-local caches and size classes. Netty's pool is more sophisticated than `LimitedFixedBufferPool` (thread-local caches, sub-page allocation, buddy allocation for large sizes). Performance should be comparable or better for the allocation/deallocation path.

- **Direct buffers** [Difficulty: Medium, Risk: Performance]: Use `ByteBuffer.allocateDirect()` for off-heap memory. Netty's `UnpooledUnsafeDirectByteBuf` provides raw address access via `memoryAddress()`. Key limitation: JVM cannot pass a `byte*` pointer from the receive buffer directly to the RESP parser. Instead, the parser must use index-based access (`ByteBuf.getByte(offset)`), which adds a bounds check per byte. For the hot parsing path, this overhead accumulates -- expect 10-20% parsing throughput reduction vs. C#'s direct pointer access.

- **Backpressure** [Difficulty: Low]: Netty has built-in write watermark support (`ChannelOption.WRITE_BUFFER_WATER_MARK`) which maps to the `ThrottleMax`-based backpressure. The high/low watermark pattern is more flexible than Garnet's fixed counter-based throttle.

- **TLS** [Difficulty: Low]: Use Netty's `SslHandler` with BoringSSL/OpenSSL backend via `netty-tcnative` for best performance. Avoids JDK's slower JSSE implementation. Netty's TLS pipeline integration is simpler than Garnet's manual three-state machine -- the `SslHandler` is inserted into the channel pipeline and transparently handles encrypt/decrypt.

- **Zero-copy limitation** [Difficulty: High, Risk: Performance]: JVM cannot directly pass buffer pointers to application code without JNI or `sun.misc.Unsafe`. Garnet's zero-copy non-TLS path (where the receive buffer pointer IS the transport buffer pointer, and `ArgSlice` points directly into it) has no JVM equivalent. The best approximation is Netty's `CompositeByteBuf` for zero-copy aggregation, combined with `Unsafe.getAddress()` for raw pointer access where needed. Project Panama's `MemorySegment` (JDK 22+) is the modern alternative but adds bounds-checking overhead.

## 7. Open Questions

1. What is the optimal `ThrottleMax` value? The default of 8 was presumably tuned empirically. How does it interact with the NIC's TX ring buffer size?
2. The `LightConcurrentStack` uses `SpinLock` which can cause issues under high contention. With `2 * ThrottleMax = 16` capacity and high send rates, does this become a bottleneck?
3. Buffer shrinking only occurs if `networkBytesRead <= maxReceiveBufferSize` after processing. What prevents memory bloat from a single large request that expands the buffer, followed by small requests that keep it alive?
4. How does the `networkPool` interact with NUMA topology? There is no per-NUMA-node pooling visible in the code.
