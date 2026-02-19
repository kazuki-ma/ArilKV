# Request Lifecycle

## 1. Overview

This document traces the complete lifecycle of GET, SET, and INCR commands through Garnet, from TCP receive to response send. Each step references specific source files and line numbers. The trace follows the non-TLS, non-cluster, non-transaction path -- the hot path that accounts for the vast majority of operations.

## 2. Detailed Design

### 2.1 Complete GET Trace

**Step 1: TCP Receive** (see [Doc 07 Section 2.4](./07-network-layer.md#24-receive-path-non-tls) for the full receive path design)
- File: `libs/common/Networking/TcpNetworkHandlerBase.cs:L196-L210`
- `HandleReceiveWithoutTLS` is called by the SAEA completion callback
- `e.BytesTransferred` contains the number of new bytes
- Calls `OnNetworkReceiveWithoutTLS(e.BytesTransferred)`

**Step 2: Network-to-Transport Bridge**
- File: `libs/common/Networking/NetworkHandler.cs:L266-L278`
- `OnNetworkReceiveWithoutTLS`:
  ```
  networkBytesRead += bytesTransferred
  transportReceiveBuffer = networkReceiveBuffer    // Zero-copy alias
  transportReceiveBufferPtr = networkReceiveBufferPtr
  transportBytesRead = networkBytesRead
  Process()
  ```

**Step 3: Session Creation or Reuse**
- File: `libs/common/Networking/NetworkHandler.cs:L342-L349`
- `Process()` checks if `session != null`. On first call, `serverHook.TryCreateMessageConsumer()` is invoked
- File: `libs/server/Servers/GarnetServerTcp.cs:L212-L233`
- `TryCreateMessageConsumer` creates a `RespServerSession` (which extends `ServerSessionBase` -> `IMessageConsumer`) via the session provider [Source: libs/server/Sessions/ServerSessionBase.cs:L11]

**Step 4: Message Consumption Entry**
- File: `libs/common/Networking/NetworkHandler.cs:L489-L500`
- `TryProcessRequest()`:
  ```
  transportReadHead += session.TryConsumeMessages(
      transportReceiveBufferPtr + transportReadHead,
      transportBytesRead - transportReadHead)
  ```

**Step 5: Session Processing Setup**
- File: `libs/server/Resp/RespServerSession.cs:L453-L551`
- `TryConsumeMessages(byte* reqBuffer, int bytesReceived)`:
  ```
  bytesRead = bytesReceived
  readHead = 0
  LatencyMetrics?.Start(NET_RS_LAT)
  clusterSession?.AcquireCurrentEpoch()     // Epoch protection
  recvBufferPtr = reqBuffer
  networkSender.EnterAndGetResponseObject(out dcurr, out dend)  // Get send buffer
  ProcessMessages()
  ```
  - `dcurr` = current write position in response buffer
  - `dend` = end of response buffer

**Step 6: Command Parsing** (see [Doc 08 Section 2.4](./08-resp-parser.md#24-command-parsing-flow) for parsing details and [Doc 08 Section 2.3](./08-resp-parser.md#23-sessionparsestate) for `SessionParseState` internals)
- File: `libs/server/Resp/RespServerSession.cs:L572-L667`
- `ProcessMessages()` loop:
  ```
  while bytesRead - readHead >= 4:
    cmd = ParseCommand(writeErrorOnFailure: true, out commandReceived)
  ```
- For `*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n`:
  - Reads `*2` -> 2 elements (1 command + 1 argument)
  - Reads `$3` + `GET` -> identifies as `RespCommand.GET`
  - Calls `parseState.Initialize(1)` -> sets up for 1 argument
  - Calls `parseState.Read(0, ref ptr, end)` -> ArgSlice[0].ptr points to "mykey" in receive buffer, length=5

**Step 7: Command Dispatch**
- File: `libs/server/Resp/RespServerSession.cs:L728-L738`
- `ProcessBasicCommands(RespCommand.GET, ref basicGarnetApi)`:
  ```
  RespCommand.GET => NetworkGET(ref storageApi)
  ```

**Step 8: NetworkGET Handler**
- File: `libs/server/Resp/BasicCommands.cs:L23-L56`
- `NetworkGET<TGarnetApi>(ref TGarnetApi storageApi)`:
  ```
  // Two alternate paths checked first:
  if (EnableScatterGatherGet) return NetworkGET_SG(ref storageApi);  // Scatter-gather path
  if (useAsync) return NetworkGETAsync(ref storageApi);              // Async path

  // Standard synchronous path:
  RawStringInput input = new(RespCommand.GET, arg1: -1);
  ref var key = ref parseState.GetArgSliceByRef(0);        // Zero-copy key reference
  var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)); // Output targets send buffer
  var status = storageApi.GET(key, ref input, ref o);
  ```
  - **Scatter-gather path** (`NetworkGET_SG`): When `EnableScatterGatherGet` is configured, parses ahead to find multiple pending GET operations and issues them to Tsavorite before waiting for completions. This improves I/O parallelism when records are on disk. [Source: libs/server/Resp/BasicCommands.cs:L26-L27]
  - **Async path** (`NetworkGETAsync`): When `useAsync` is enabled for the session, pending GETs are dispatched to the `AsyncProcessor` background task (`AsyncGetProcessor`), which handles I/O completions and sends async push responses independently from the main session thread. On the first async operation, a background `Task.Run` creates the `AsyncGetProcessor`. [Source: libs/server/Resp/AsyncProcessor.cs:L48-L72]
  - **Standard path**: `SpanByteAndMemory(dcurr, ...)` is initialized as a SpanByte pointing directly at the response buffer. If the result fits, Tsavorite writes directly there.

**Step 9: GarnetApi.GET**
- The `BasicGarnetApi` type alias resolves to `GarnetApi<BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, ...>, BasicContext<byte[], IGarnetObject, ...>, BasicContext<SpanByte, SpanByte, VectorInput, ...>>` with three type parameters. However, at runtime `GarnetApi` only stores two context instances (`context` for main store and `objectContext`); the vector context type parameter (`TVectorContext`) is phantom -- there is no `vectorContext` field. Vector operations route through `StorageSession`/`VectorManager` directly. [Source: libs/server/API/GarnetApi.cs:L24-L38]
- The API method converts the `ArgSlice` key to `SpanByte` (via `key.SpanByte` which constructs a new `SpanByte` struct on the stack from the ArgSlice's ptr and length -- zero-copy in that no data is copied, but a 12-byte struct is created) and delegates to `StorageSession`

**Step 10: StorageSession.GET**
- File: `libs/server/Storage/Session/MainStore/MainStoreOps.cs:L21-L48`
  ```
  var status = context.Read(ref key, ref input, ref output, ctx);
  if (status.IsPending):
    CompletePendingForSession(ref status, ref output, ref context)
  if (status.Found): return GarnetStatus.OK
  else if (status.IsCanceled): return GarnetStatus.WRONGTYPE    // Key is wrong type (e.g., VectorSet)
  else: return GarnetStatus.NOTFOUND
  ```
  - `context` is `BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>`
  - `context.Read()` enters Tsavorite's hash index lookup
  - The `IsCanceled` status is set by `SingleReader` when it detects a type mismatch (e.g., the value is a VectorSet but the command is a regular GET), mapping to `GarnetStatus.WRONGTYPE` [Source: libs/server/Storage/Session/MainStore/MainStoreOps.cs:L39-L42]

**Step 11: Tsavorite Read Operation**
- Tsavorite performs:
  1. Computes hash of key bytes
  2. Looks up hash bucket (cache-line-aligned, lock-free)
  3. Follows the chain of `HashBucketEntry` values to find the matching key
  4. If record is in memory: calls `MainSessionFunctions.SingleReader`
  5. If record is on disk: returns `IsPending`, triggers async I/O

**Step 12: SingleReader Callback**
- File: `libs/server/Storage/Functions/MainStore/ReadMethods.cs:L16-L113`
- `SingleReader(ref SpanByte key, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)`:
  1. Checks expiry: `if (value.MetadataSize == 8 && CheckExpiry(ref value))` -> returns false if expired
  2. **VectorSet bidirectional type mismatch check** (lines 29-45): Two checks run on every read when `cmd != NONE`:
     - If `RecordInfo.VectorSet` AND command is NOT legal on VectorSet (L33-L37): cancels (prevents string ops on vector data)
     - If NOT `RecordInfo.VectorSet` AND command IS legal on VectorSet (L39-L43): cancels (prevents vector ops on string data)
     Both directions cancel via `readInfo.Action = ReadAction.CancelOperation`, which causes Tsavorite to return `status.IsCanceled` (mapping to `GarnetStatus.WRONGTYPE` upstream). [Source: libs/server/Storage/Functions/MainStore/ReadMethods.cs:L29-L45]
  3. For regular GET (`input.arg1 < 0` check resets cmd to `NONE`): calls `CopyRespTo(ref value, ref dst, ...)`
  4. `CopyRespTo` formats the value as RESP bulk string (`$<len>\r\n<data>\r\n`) and writes directly into `dst`
  - If `dst` is the SpanByte pointing at `dcurr`: data is written directly into the send buffer

**Step 13: Response Handling**
- File: `libs/server/Resp/BasicCommands.cs:L38-L53`
  ```
  switch (status):
    case GarnetStatus.WRONGTYPE:
      WriteError(RESP_ERR_WRONG_TYPE)        // Writes "-WRONGTYPE ..."
    case GarnetStatus.OK:
      if (!o.IsSpanByte)
        SendAndReset(o.Memory, o.Length)    // Result overflowed to heap
      else
        dcurr += o.Length                    // Result written inline to send buffer
    case GarnetStatus.NOTFOUND:
      WriteNull()                            // Writes "$-1\r\n"
  ```
  [Source: libs/server/Resp/BasicCommands.cs:L40-L41]

**Step 14: Batch Completion and Send**
- File: `libs/server/Resp/RespServerSession.cs:L659-L667`
  ```
  // After processing all commands in the batch:
  if dcurr > networkSender.GetResponseObjectHead():
    Send(networkSender.GetResponseObjectHead())
  ```

**Step 15: Network Send**
- File: `libs/server/Resp/RespServerSession.cs:L1347-L1369`
- `Send(byte* d)`:
  ```
  if (dcurr - d > 0):
    if (waitForAofBlocking):
      var task = storeWrapper.WaitForCommitAsync()     // Delegates through DatabaseManager to AOF
      if (!task.IsCompleted): task.AsTask().GetAwaiter().GetResult()  // Block if not already done
    sendBytes = (int)(dcurr - d)
    networkSender.SendResponse(offset, sendBytes)
  ```
  - When `waitForAofBlocking` is true (AOF synchronous commit mode), the response is delayed until the AOF write is flushed to disk. The call goes through `storeWrapper.WaitForCommitAsync()` (which checks `serverOptions.EnableAOF` and coordinates multi-database commit via `DatabaseManager`), NOT directly to `appendOnlyFile`. This ensures durability at the cost of write latency. [Source: libs/server/Resp/RespServerSession.cs:L1357-L1368]
- File: `libs/common/Networking/GarnetTcpNetworkSender.cs:L210-L228`
- `SendResponse(offset, size)`:
  ```
  _r = responseObject
  responseObject = null
  Send(socket, _r, offset, size)
  ```
- File: `libs/common/Networking/GarnetTcpNetworkSender.cs:L308-L329`
- `Send()`:
  ```
  Interlocked.Increment(throttleCount)
  _r.socketEventAsyncArgs.SetBuffer(offset, size)
  _r.socketEventAsyncArgs.UserToken = _r
  socket.SendAsync(_r.socketEventAsyncArgs)
  ```

**Step 16: Cleanup**
- File: `libs/server/Resp/RespServerSession.cs:L522-L528`
  ```
  finally:
    networkSender.ExitAndReturnResponseObject()
    clusterSession?.ReleaseCurrentEpoch()
    scratchBufferBuilder.Reset()
    scratchBufferAllocator.Reset()
  ```
- Returns `readHead` as bytes consumed to the network layer

### 2.2 Complete SET Trace

SET follows the same path through Steps 1-7 but diverges at the command handler:

**Step 7b: Dispatch**
```
RespCommand.SET => NetworkSET(ref storageApi)
```

**Step 8b: NetworkSET Handler**
The SET handler parses optional arguments (EX, PX, NX, XX, KEEPTTL, GET) from `parseState`, constructs a `RawStringInput`, and calls `storageApi.SET()`.

**Step 9b-10b: StorageSession -> Tsavorite**
SET maps to either `Upsert` (simple set) or `RMW` (conditional set with NX/XX/GET):
- **Upsert path**: `context.Upsert(ref key, ref input, ref value, ref output)` -- Tsavorite allocates a new record in the hybrid log, copies key and value into it, and updates the hash index
- **RMW path**: For SET with NX/XX/GET, `context.RMW(ref key, ref input, ref output)` is used. Tsavorite calls `MainSessionFunctions.NeedInitialUpdate`, `InitialUpdater`, or `InPlaceUpdater` depending on whether the key exists

**Step 11b: MainSessionFunctions.InitialUpdater**
- File: `libs/server/Storage/Functions/MainStore/RMWMethods.cs:L79-L119`
- For a new key:
  ```
  value.ShrinkSerializedLength(newInputValue.Length + metadataSize)
  value.ExtraMetadata = expiryTicks    // Set TTL
  newInputValue.CopyTo(value.AsSpan())
  ```
- Response: writes `+OK\r\n` to output

### 2.3 Complete INCR Trace

INCR uses the RMW (Read-Modify-Write) path, which is Tsavorite's atomic update mechanism:

**Step 7c: Dispatch**
```
RespCommand.INCR => NetworkIncrement(RespCommand.INCR, ref storageApi)
```

**Step 8c: NetworkIncrement Handler**
Constructs `RawStringInput(RespCommand.INCR)` and calls `storageApi.Increment()` which routes to `context.RMW()`.

**Step 9c: Tsavorite RMW**
Tsavorite's RMW for INCR:
1. **Hash lookup**: Find the record
2. **If key exists in mutable region**: Call `InPlaceUpdater` -- parse existing value as integer, increment, write back in-place, write response
3. **If key exists in immutable/disk region**: Call `CopyUpdater` -- allocate new record, copy with incremented value
4. **If key does not exist**: Call `NeedInitialUpdate` -> true, then `InitialUpdater` -- create new record with value "1"

The key optimization is that `InPlaceUpdater` modifies the value in the hybrid log without any copy or allocation when the record is in the mutable region. The integer parsing and formatting happen directly on the log record's bytes.

**Value growth handling**: When an increment causes the result to have more digits than the current value (e.g., "99" + 1 = "100"), `InPlaceUpdateNumber` detects that `ndigits > value.LengthWithoutMetadata` and returns `false` [Source: libs/server/Storage/Functions/MainStore/PrivateMethods.cs:L432-L433]. This causes `InPlaceUpdater` to return `IPUResult.Failed`, which signals Tsavorite to fall back to `CopyUpdater`. `CopyUpdater` allocates a new record in the hybrid log with enough space for the longer value, copies the incremented result, and updates the hash index to point to the new record. The old record is left in place for epoch-based reclamation. This means INCR is O(1) in the common case (same digit count) but incurs a copy when the digit count increases.

### 2.4 Timing-Critical Sections

1. **Epoch Hold Duration**: From `clusterSession?.AcquireCurrentEpoch()` to `ReleaseCurrentEpoch()` in the `finally` block. The entire batch of commands runs under a single epoch acquisition. If a session processes a very large batch, it can delay epoch advancement and thus delay memory reclamation.

2. **SpinLock Hold Duration**: The `GarnetTcpNetworkSender` SpinLock IS held for the entire `ProcessMessages` duration per connection. `EnterAndGetResponseObject` acquires the SpinLock at L123 and retrieves a response buffer, but does NOT release the lock. `ExitAndReturnResponseObject` (called in the `finally` block at Step 16) returns the buffer and calls `spinLock.Exit()` at L150. This means the lock spans the entire `TryConsumeMessages` -> `ProcessMessages` -> `ExitAndReturnResponseObject` flow. The consequence is that the `AsyncProcessor` CANNOT call `EnterAndGetResponseObject` while the main thread is processing messages -- it must wait for `ExitAndReturnResponseObject` in the `finally` block to release the lock. This is the serialization mechanism between the main processing thread and the async response sender. [Source: libs/common/Networking/GarnetTcpNetworkSender.cs:L120-L151]

3. **Send Buffer Lifetime**: Between `SendResponse` (which sets `responseObject = null`) and the SAEA completion callback (`SeaaBuffer_Completed`). During this window, the send buffer is "in flight" in the kernel's TCP stack. The throttle mechanism prevents too many buffers from being in flight simultaneously.

### 2.5 Pipelining

Garnet processes multiple commands per `TryConsumeMessages` call. The `ProcessMessages` while-loop continues as long as bytes remain, processing each command and writing its response into the same output buffer. All responses are sent as a single `Send()` call at the end:

```
Client sends:    *2\r\n$3\r\nGET\r\n$1\r\na\r\n*2\r\n$3\r\nGET\r\n$1\r\nb\r\n
Server receives: [entire batch in one TCP receive]
ProcessMessages:
  - Parse+execute GET a -> write $5\r\nhello\r\n to dcurr
  - Parse+execute GET b -> write $5\r\nworld\r\n to dcurr
Send:            [$5\r\nhello\r\n$5\r\nworld\r\n sent as one TCP send]
```

This pipelining is automatic and requires no special handling. The performance benefit is significant: syscall overhead is amortized across many commands.

### 2.6 Overflow Handling

When the response for a single command does not fit in the remaining send buffer:

1. `RespWriteUtils.TryWrite*` returns `false` (because `dcurr + needed > dend`)
2. The caller loops: `while (!RespWriteUtils.TryWriteError(..., ref dcurr, dend)) SendAndReset();`
3. `SendAndReset()` [Source: libs/server/Resp/RespServerSession.cs:L1255-L1272]:
   - Sends current buffer contents to network
   - Gets a new empty response buffer
   - Updates `dcurr` and `dend`
4. The `TryWrite*` call succeeds on retry with the fresh buffer

For very large responses (like `LRANGE` returning thousands of elements), `WriteDirectLarge` [Source: libs/server/Resp/RespServerSession.cs:L1317-L1344] streams the data across multiple send buffers in a tight loop.

## 3. Source File Map

| File | Purpose | Key Lines |
|------|---------|-----------|
| `libs/common/Networking/TcpNetworkHandlerBase.cs` | TCP receive handler | L196-L216 |
| `libs/common/Networking/NetworkHandler.cs` | Buffer management, Process() | L266-L500 |
| `libs/server/Resp/RespServerSession.cs` | TryConsumeMessages, ProcessMessages | L453-L667 |
| `libs/server/Resp/BasicCommands.cs` | GET, SET, INCR handlers | L23-L56 (GET) |
| `libs/server/Resp/AsyncProcessor.cs` | Async GET background processor | L48-L143 |
| `libs/server/Sessions/ServerSessionBase.cs` | Abstract base (IMessageConsumer) | L11-L55 |
| `libs/server/Storage/Session/MainStore/MainStoreOps.cs` | StorageSession.GET/SET | L21-L48 |
| `libs/server/Storage/Functions/MainStore/ReadMethods.cs` | SingleReader/ConcurrentReader | L16-L113 |
| `libs/server/Storage/Functions/MainStore/RMWMethods.cs` | RMW callbacks for SET/INCR | L25-L119 |
| `libs/common/Networking/GarnetTcpNetworkSender.cs` | SendResponse, throttle | L210-L329 |

## 4. Performance Notes

- **Direct-to-buffer response writing**: For GET, the `SpanByteAndMemory` output is initialized pointing at `dcurr`. If the response fits, Tsavorite's callback writes the RESP-formatted response directly into the TCP send buffer. Zero copies between storage and network.
- **In-place RMW**: For INCR on mutable-region records, the increment happens in-place on the log record. No allocation, no copy.
- **Batch epoch acquisition**: One epoch acquire/release per TCP receive batch, not per command. This amortizes the atomic operation cost.
- **Pipelining amortizes syscalls**: Multiple commands produce one `Send()` call, one `SendAsync()` syscall.

## 5. Rust Mapping Notes

- **Direct buffer writing** [Difficulty: Low]: Use a `&mut [u8]` slice of the send buffer with an offset cursor, returning the number of bytes written. The `dcurr`/`dend` pointer pair maps to `(&mut buf[cursor..], remaining)`. The `while (!TryWrite...) SendAndReset()` pattern maps to checking the return value and flushing + resetting the cursor when the buffer is full. Alternatively, implement `io::Write` on a `SendBuffer` struct for ergonomic RESP formatting. Performance parity expected since the write operations are simple memcpy-like byte writes.

- **Tsavorite RMW equivalent** [Difficulty: High]: Implement a `read_modify_write` method on the hash table that takes a closure `FnMut(&mut Value) -> Response`. The challenge is replicating Tsavorite's in-place update semantics: `InPlaceUpdater` attempts to update the record in the log without copying, falling back to `CopyUpdater` if the record needs to grow. In Rust, this requires careful lifetime management -- the closure must borrow the mutable record from the log while also writing the response to a different buffer. Use a two-phase approach: (1) `try_update_in_place` returns a `Result<Response, NeedsCopy>`, (2) on `NeedsCopy`, allocate new record and call `copy_update`.

- **Pipelining** [Difficulty: Low]: Process all commands from the receive buffer sequentially in a tight loop (matching Garnet's `ProcessMessages` while-loop), writing responses into the send buffer. Do NOT parse into a `Vec<Command>` first -- this would add allocation overhead. Instead, parse and execute each command inline, advancing the read cursor. Flush the send buffer once at the end of the batch. This matches Garnet's design where parsing and execution are interleaved.

- **Epoch** [Difficulty: Medium]: Wrap the entire batch processing in `crossbeam_epoch::pin()`. The `Guard` returned by `pin()` is `!Send`, which naturally prevents the batch from being migrated across threads mid-processing. This matches Garnet's design where the epoch is held for the entire `TryConsumeMessages` call. The cluster epoch (separate from storage epoch) maps to a second `crossbeam_epoch::Collector` instance with independent advancement.

- **SpinLock serialization** [Difficulty: Low]: The `GarnetTcpNetworkSender` SpinLock that serializes the main processing thread and `AsyncProcessor` maps to a `parking_lot::Mutex<ResponseBuffer>`. Since the lock is held for the entire `ProcessMessages` duration (not just a brief critical section), `parking_lot` will park the waiting thread rather than spinning, which is the correct behavior for this use case.

- **Scatter-gather GET** [Difficulty: Medium]: The `GET_WithPending` pattern (issue read, check if pending, continue parsing, then complete all pending) maps to issuing multiple `io_uring` SQEs for disk reads and then awaiting completions. The context parameter that carries the command index for out-of-order response assembly maps to `io_uring`'s `user_data` field on each SQE.

## 6. Kotlin/JVM Mapping Notes

- **Direct buffer writing** [Difficulty: Low]: Use `ByteBuffer.put()` to write into the send buffer, or Netty's `ByteBuf` for more ergonomic bulk writing. Netty's `ByteBuf.writeBytes()`, `writeInt()`, etc. handle bounds checking and growth internally. The `dcurr`/`dend` pointer pair maps to `ByteBuf.writerIndex()`/`ByteBuf.writableBytes()`. The `SendAndReset()` pattern maps to `ctx.writeAndFlush(buf); buf = allocator.buffer()`.

- **In-place update** [Difficulty: High, Risk: Performance]: JVM objects cannot be updated in place on a memory-mapped log because the JVM has no control over object layout in off-heap memory. Use off-heap `ByteBuffer` or `MemorySegment` for the log, with manual serialization/deserialization at every access point. The `InPlaceUpdater` optimization (which avoids copying the record) is difficult to replicate because the JVM cannot modify bytes in the log and interpret them as structured data simultaneously without `Unsafe`. This is a significant performance gap for write-heavy workloads.

- **Pipelining** [Difficulty: Low]: Same approach as Garnet -- read all commands from the receive buffer, execute each inline, write response to send buffer, flush once at end. Netty naturally supports this via `ChannelHandler.channelRead()` (called per message) + `channelReadComplete()` (flush point). However, Netty's event loop model means each `channelRead` call includes event loop overhead, whereas Garnet processes the entire batch in a single synchronous call.

- **Request lifecycle overhead** [Difficulty: N/A, Risk: Performance]: The end-to-end request lifecycle adds several JVM-specific overheads compared to C#: (a) no pointer-based RESP parsing (bounds-checked ByteBuffer access instead), (b) virtual dispatch on `ISessionFunctions` callbacks, (c) object allocation for `ArgSlice` equivalents, (d) no `SpanByte` for zero-copy key/value access. Cumulatively, expect 30-50% lower single-thread throughput for simple commands (GET/SET) vs. C#. The gap narrows for complex commands where storage I/O dominates.

## 7. Open Questions

1. What is the exact mechanism for `ParseCommand`'s command lookup? The efficiency of this lookup (O(1) vs O(n)) directly impacts per-command overhead.
2. For the GET scatter-gather path (`NetworkGET_SG`), how does it handle pending I/O operations that complete out of order?
3. What happens when `CompletePendingForSession` is called for a GET that hit disk? Does it block the session thread or use continuation-passing?
4. How does the AOF write interact with the response send? The `waitForAofBlocking` flag in `Send()` suggests synchronous AOF commit before response, which could significantly impact write latency.
