# Round 2 Review: Server/Network Documents (01,07-09,15)
## Reviewer: reviewer-blue

---

### Document: 01-architecture-overview.md

#### Critical Issues (must fix)

- [C1] **VectorStore completely missing from architecture.** The `BasicGarnetApi` type alias at `RespServerSession.cs:L23-L31` includes THREE store contexts: main store (SpanByte), object store (IGarnetObject), AND vector store (VectorSessionFunctions/VectorInput). The document only mentions two (main and object). GarnetApi itself is `GarnetApi<TContext, TObjectContext, TVectorContext>` with three type parameters (see `libs/server/API/GarnetApi.cs:L24-L27`). This is a significant architectural component with its own VectorManager subsystem (`libs/server/Resp/Vector/`) that is entirely omitted.

- [C2] **Type alias line references show only two contexts but actual code has three.** Section 2.2 and 4 reference `RespServerSession.cs:L23-L40` for the type aliases, but the actual code at those lines shows three BasicContext instantiations (including VectorStore at lines 29-31) and three LockableContext instantiations (lines 38-40). The document's claim that there are only main store and object store contexts is factually incomplete.

- [C3] **`RespServerSession` class declaration is wrong.** Section 2.5 says the class inherits via `IMessageConsumer`. Actually, `RespServerSession` extends `ServerSessionBase` (at `libs/server/Sessions/ServerSessionBase.cs`), which is the class that implements `IMessageConsumer`. The document's component dependency diagram shows `RespServerSess` directly, but never mentions `ServerSessionBase` as an intermediary, which is architecturally significant (it holds shared session state).

#### Major Issues (should fix)

- [M1] **Async GET path not mentioned.** The GET handler at `BasicCommands.cs:L26-L30` has TWO alternate paths that bypass the standard flow: `EnableScatterGatherGet` and `useAsync` (which dispatches to `NetworkGETAsync`). The `AsyncProcessor` subsystem at `libs/server/Resp/AsyncProcessor.cs` is never mentioned. An implementer following this document would miss the async processing capability entirely.

- [M2] **DatabaseManager not mentioned.** `StoreWrapper.cs:L41-L56` shows that store, objectStore, and appendOnlyFile all delegate through `databaseManager` (e.g., `this.databaseManager.MainStore`). The document describes `StoreWrapper` as directly wrapping `TsavoriteKV instances`, but the actual architecture has an intermediary `DatabaseManager` that manages multiple databases (the `GarnetDatabase` abstraction). This is important for multi-DB support.

- [M3] **IMessageConsumer signature slightly misquoted.** Section 2.5 says the signature is `TryConsumeMessages(byte* reqBuffer, int bytesRead) -> int`. The actual source at `IMessageConsumer.cs:L19` shows the parameter is named `bytesRead` (correct), but the doc adds "returning bytes consumed" which should clarify that this is the meaning of the return value (not explicit in the interface). Minor, but worth being precise.

- [M4] **Epoch model oversimplified.** Section 2.6 states "Garnet uses separate `LightEpoch` instances for different subsystems (store, AOF, pub/sub)." The source at `libs/host/GarnetServer.cs:L57` does not directly show this. The actual epoch sharing model was recently refactored (commit `ee8488294f` mentions "epoch sharing" changes). The document should reference where the epoch instances are actually created and how they're shared.

#### Minor Issues (nice to fix)

- [m1] **Line numbers may drift.** Many line references (e.g., `StoreWrapper.cs:L22-L27`, `NetworkHandler.cs:L19-L714`) are approximate. The actual `StoreWrapper` class starts at L31, not L22. Minor but could confuse readers trying to follow along.

- [m2] **Missing `libs/server/Sessions/` directory** in the project structure map. `ServerSessionBase.cs` lives there and is the base class for all sessions.

- [m3] **Open Question 2 is answerable.** The question about epoch instances (whether cluster session epoch and storage epoch are the same) can be answered by examining the code: `clusterSession?.AcquireCurrentEpoch()` acquires the cluster epoch, while Tsavorite operations use the store's internal epoch. They are separate instances.

#### Strengths
- Excellent high-level data flow trace (Section 2.4) that gives a clear mental model of the full request path
- Good identification of key interfaces (IMessageConsumer, INetworkSender, etc.)
- The component dependency diagram is clear and informative
- Rust and Kotlin mapping notes are practical and actionable

---

### Document: 07-network-layer.md

#### Critical Issues (must fix)

- [C1] **TLS state machine description uses wrong enum values.** Section 2.5 describes three states as "Rest", "Active", "Waiting". The actual enum at `libs/common/Networking/TlsReaderStatus.cs` should be verified. Looking at `NetworkHandler.cs:L95-L96`, the code references `TlsReaderStatus.Rest`, `TlsReaderStatus.Active`, and `TlsReaderStatus.Waiting`. This appears correct based on code usage at L287-L313. However, the description of how transitions work is incomplete -- it does not mention the `SslReaderAsync` fallback path at `NetworkHandler.cs:L392` which handles the case where `sslStream.ReadAsync` actually goes async. This path involves task-based continuation that is architecturally significant for TLS performance.

#### Major Issues (should fix)

- [M1] **Missing `NetworkSenderBase` from class hierarchy.** The class hierarchy in Section 2.3 shows `NetworkHandler<TServerHook, TNetworkSender>` as the root, but actually `NetworkHandler` extends `NetworkSenderBase` (see `NetworkHandler.cs:L19`). `NetworkSenderBase` implements `INetworkSender` and is important because when TLS is active, the `NetworkHandler` itself acts as the network sender (via `GetNetworkSender()` returning `this`). This dual role is mentioned in Section 2.9 but the class hierarchy omits the base class.

- [M2] **Buffer doubling description references wrong line numbers.** Section 2.6 says doubling is at `NetworkHandler.cs:L502-L510`. The actual code at L502-L510 shows `DoubleNetworkReceiveBuffer` which uses `networkPool.Get(networkReceiveBuffer.Length * 2)` and `Array.Copy`. The doc says "data copied, old buffer returned to pool" but the actual code calls `networkReceiveBufferEntry.Dispose()` which returns the `PoolEntry` to the pool, not just the buffer. This distinction matters because `PoolEntry` wraps both the array and the pointer.

- [M3] **`LimitedFixedBufferPool.Return` description inaccurate.** Section 2.7 says "If the level is full, the buffer is simply not returned (becomes GC garbage)". Need to verify this claim against the actual code. The doc also says `Array.Clear` is called on returned buffers. If the pool does clear buffers, this is a potentially important performance detail that should include timing impact.

- [M4] **Missing files from source file map.** The networking directory contains files not mentioned: `TaskToApm.cs` (APM pattern adapter), `NetworkSenderBase.cs` (base class for all senders), `INetworkHandler.cs` (handler interface), `WireFormat.cs` (protocol detection), `MaxSizeSettings.cs`, `BatchHeader.cs`. Several of these are architecturally relevant.

#### Minor Issues (nice to fix)

- [m1] **HandleNewConnection line reference slightly off.** The doc says `GarnetServerTcp.cs:L138-L203`. The actual method is at L138-L203 (correct), but some of the internal details described don't match. For example, the doc says "Atomically increments `activeHandlerCount` via `Interlocked.Increment`. If the count exceeds `networkConnectionLimit`, the socket is disposed immediately." The actual code at L154-L199 has a more nuanced check: it first checks `activeHandlerCount >= 0`, then increments, then checks both `> 0` and the connection limit. The `>= 0` check is related to shutdown detection.

- [m2] **Missing `AllocateNetworkReceiveBuffer` description.** The doc mentions buffer allocation from the pool but doesn't describe the `AllocateNetworkReceiveBuffer()` call in `TcpNetworkHandlerBase` constructor (L38). This is where the initial receive buffer is actually allocated.

- [m3] **Throttle semaphore type.** The doc says `SemaphoreSlim` is used for throttling, but the actual code at `GarnetTcpNetworkSender.cs:L37` shows `protected readonly SemaphoreSlim throttle = new(0)` initialized with count 0, not the throttle max. This is important: the semaphore starts at 0 and is released by send completions, meaning the first N sends are unthrottled and only block when the count exceeds ThrottleMax. The doc's description of the mechanism is correct in behavior but could be more precise about initialization.

#### Strengths
- Thorough coverage of both non-TLS and TLS paths
- Buffer lifecycle (allocation/shift/double/shrink) is well-documented
- The send throttle mechanism explanation is clear and practical
- Good attention to performance-relevant details (zero-copy, SAEA reuse)

---

### Document: 08-resp-parser.md

#### Critical Issues (must fix)

- [C1] **ArgSlice size is wrong.** Section 2.3 claims "ArgSlice layout (16 bytes on 64-bit)" with "4 bytes padding (alignment)". The actual source at `libs/server/ArgSlice/ArgSlice.cs:L18-L21` shows `[StructLayout(LayoutKind.Explicit, Size = 12)]` with `Size = 12` explicitly declared. The struct has `byte* ptr` at offset 0 (8 bytes) and `int length` at offset 8 (4 bytes), totaling exactly 12 bytes with NO padding. The constant `ArgSlice.Size = 12` is publicly declared at line 21.

- [C2] **Cache line calculation is wrong.** Because ArgSlice is 12 bytes (not 16), the doc's claim that "5 ArgSlices * 16 bytes = 80 bytes, close to one 64-byte cache line" is incorrect. The actual size is 5 * 12 = 60 bytes. Note that `SessionParseState.cs:L22` comments confirm this: `const int MinParams = 5; // 5 * 20 = 60; around one cache line of 64 bytes`. (The comment itself says "20" which appears to be a comment error -- the actual size is 12 per ArgSlice, 60 total, matching the "60" in the comment.)

- [C3] **MakeUpperCase description has wrong pointer offset.** Section 2.5 says `var firstUlong = *(ulong*)(ptr + 4)` but the actual code at `RespServerSession.cs:L690` does indeed use `ptr + 4`. However, the doc's description of the logic is incomplete. The actual code reads TWO ulongs: `firstUlong` at `ptr + 4` and `secondUlong` at `ptr + 4 + cmdLen` (line 691). The fast path checks BOTH, and only if `firstAllUpper && secondAllUpper` does it return early. The doc only describes the first ulong check. The second ulong is needed because the command bytes may span beyond the first 8 bytes.

#### Major Issues (should fix)

- [M1] **`ParseCommand` lookup mechanism not described.** Section 2.6 says "Maps the uppercase command name to a `RespCommand` enum value using a fast lookup (likely a hash or trie-based approach)". The word "likely" indicates the author didn't actually trace this code path. For a reimplementation document, the exact lookup mechanism needs to be determined and documented. The code in `RespCommand.cs` likely uses a combination of length-based switching and byte comparisons. Saying "likely" is not sufficient.

- [M2] **Response writing section is thin.** Section 2.8 describes `SendAndReset` at `RespServerSession.cs:L1255-L1272` but the actual code at L1255-L1272 includes an important error handling branch: if `dcurr - d <= 0` (no progress was made), it throws a `GarnetException` with `LogLevel.Critical`. The doc's pseudocode omits this case, which is important for understanding failure modes.

- [M3] **`RespCommand` enum organization is inaccurate.** Section 2.6 says read-only commands go "through `ZSCORE` (value ~117)" and write commands start from "APPEND (value ~121)". The actual enum at `RespCommand.cs` starts with `NONE = 0x00`, followed by read-only commands in alphabetical order starting with `BITCOUNT`. The enum values are implicit (auto-incrementing), so the exact values depend on the count of preceding entries. Citing approximate values with "~" is unreliable for reimplementation.

- [M4] **Missing inline command support description.** Open Question 2 asks about inline commands, but the code at `RespServerSession.cs:L581` checks `bytesRead - readHead >= 4` before parsing. Inline commands (like `PING\r\n`) ARE supported -- the `ParseCommand` method handles both array format (`*`) and inline format. This should be documented, not left as an open question.

#### Minor Issues (nice to fix)

- [m1] **SessionParseState has a `Read` method but the doc's line reference is wrong.** Section 2.3 references `SessionParseState.cs:L331-L358` for the `Read` method. This should be verified against current code as method positions may have shifted.

- [m2] **Missing `RespWriteUtils` details.** The doc references this file in Section 2.8 but provides no line numbers or method signatures. For reimplementation, the key methods (`TryWriteBulkString`, `TryWriteError`, `TryWriteInt64`, `TryWriteNull`, etc.) should be enumerated.

- [m3] **RESP3 coverage is insufficient.** The doc mentions RESP3 in the protocol format section and as Open Question 3, but RESP3 support is increasingly important. The `respProtocolVersion` field and its effects on response formatting should be documented.

#### Strengths
- The zero-copy parsing flow explanation is excellent and clearly shows how ArgSlice avoids allocation
- RESP protocol format description is clear and educational
- Partial message handling is well-explained with the readHead reset mechanism
- The bit-twiddling uppercase optimization is documented with references

---

### Document: 09-request-lifecycle.md

#### Critical Issues (must fix)

- [C1] **GET trace Step 8 line reference is wrong for current code.** The doc says `NetworkGET` is at `BasicCommands.cs:L23-L56`. The actual code at L23 shows `NetworkGET<TGarnetApi>`. However, the doc omits the first two conditional branches: `EnableScatterGatherGet` (L26-L27) and `useAsync` (L29-L30). These are not edge cases -- scatter-gather GET is a significant performance optimization that fundamentally changes the execution path. The doc's GET trace only covers the simplest synchronous case without mentioning these alternatives exist.

- [C2] **Step 9 (GarnetApi.GET) description is vague.** The doc says "The API method converts the `ArgSlice` key to `SpanByte` and delegates to `StorageSession`". Looking at the actual `StorageSession.GET` at `MainStoreOps.cs:L21`, the method signature takes `ref SpanByte key`, not `ArgSlice`. The conversion from `ArgSlice` to `SpanByte` happens in `GarnetApi.GET`, not `StorageSession.GET`. The doc should show this conversion step explicitly, as it's a key zero-copy bridge.

- [C3] **Step 12 (SingleReader) description has errors.** The doc says "For regular GET (cmd == NONE after the arg1 < 0 check): calls `CopyRespTo(ref value, ref dst, ...)`". The actual code at `ReadMethods.cs:L47-L56` shows that `input.arg1 < 0` is checked and cmd is reset to `NONE` only for `RespCommand.GET`. But before that, there are VectorSet type mismatch checks (lines 29-45) that the doc completely omits. For an implementer, missing these checks would mean incorrect behavior for VectorSet keys.

- [C4] **Step 10 `MainStoreOps.cs` has a WRONGTYPE return path not shown.** The actual `GET` at `MainStoreOps.cs:L39-L42` returns `GarnetStatus.WRONGTYPE` when `status.IsCanceled`. The doc's Step 10 pseudocode only shows `Found` and the `IsPending` path, missing `IsCanceled`/WRONGTYPE. Similarly, the GET handler at `BasicCommands.cs:L40-L41` handles `GarnetStatus.WRONGTYPE` by writing an error, but the doc's Step 13 only shows `OK` and `NOTFOUND`.

#### Major Issues (should fix)

- [M1] **SET trace lacks detail on actual code paths.** Section 2.2 says "SET maps to either `Upsert` (simple set) or `RMW` (conditional set with NX/XX/GET)". This is architecturally important but the doc provides no code references for where this routing decision is made. The actual NetworkSET handler is complex, with many conditional arguments (EX, PX, EXAT, PXAT, NX, XX, KEEPTTL, GET). An implementer needs to know the exact flow.

- [M2] **Section 2.4 claim about SpinLock hold duration is misleading.** The doc says "The `GarnetTcpNetworkSender.Enter()`/`Exit()` spinlock is held from `EnterAndGetResponseObject` to `ExitAndReturnResponseObject` -- the entire `ProcessMessages` call." But looking at the code, `EnterAndGetResponseObject` acquires the spinlock, gets a response buffer, and then releases the spinlock. It does NOT hold the spinlock for the entire ProcessMessages call. The `Enter`/`Exit` names suggest lock acquisition/release, but examining `GarnetTcpNetworkSender` would show the actual scope. If this claim is wrong, it significantly misrepresents the concurrency model.

- [M3] **Send path (Step 15) line references don't match.** The doc says `Send` is at `RespServerSession.cs:L1347-L1369`. The actual `Send` method is at L1347-L1369 (from my verification). The doc's pseudocode for `Send` omits the `waitForAofBlocking` check at L1360-L1364, which is critical for durability guarantees -- when AOF synchronous commit is enabled, the response is delayed until the AOF write completes.

- [M4] **Missing `WriteDirectLarge` in overflow handling.** Section 2.6 mentions `WriteDirectLarge` at `RespServerSession.cs:L1317-L1344` but doesn't explain that this is actually a specialized method for large ReadOnlySpan writes, distinct from the `SendAndReset(IMemoryOwner<byte>, int)` overload at L1275 that handles heap-allocated overflow results from Tsavorite.

#### Minor Issues (nice to fix)

- [m1] **Pipelining example in Section 2.5 uses simplified RESP.** The example shows `*2\r\n$3\r\nGET\r\n$1\r\na\r\n` but doesn't clarify that the `*2` means "2 elements total" (command + 1 arg), which could confuse readers unfamiliar with RESP.

- [m2] **Missing cleanup of `scratchBufferBuilder` and `scratchBufferAllocator`.** Step 16 shows these being reset in the finally block, but the doc never introduces what they are or when they're used during command processing.

#### Strengths
- The step-by-step trace format with file/line references is excellent for understanding
- Covering GET, SET, and INCR gives good coverage of read, write, and RMW paths
- The timing-critical sections (Section 2.4) highlight important concurrency concerns
- Pipelining and overflow handling are well-documented

---

### Document: 15-performance-engineering.md

#### Critical Issues (must fix)

- [C1] **HashBucket latch bit layout is wrong.** Section 2.8 claims `[63: X latch][62-49: S latch count (14 bits)][48-0: overflow address]`. The actual code at `HashBucket.cs:L15` computes `kSharedLatchBits = 63 - Constants.kAddressBits`. Since `kAddressBits = 48` (from `Constants.cs:L39`), `kSharedLatchBits = 15`, NOT 14. The actual layout is: `[bit 63: X latch][bits 62-48: S latch count (15 bits)][bits 47-0: overflow address (48 bits)]`. This matters for anyone implementing the lock-free hash table.

- [C2] **HashBucketEntry bit layout is wrong.** Section 2.15 claims `[tag: 14 bits][address: 48 bits][tentative: 1 bit][read-cache: 1 bit]`. The actual layout from `HashBucketEntry.cs:L9` is `[1-bit tentative][15-bit TAG][48-bit address]` in logical bit order, or `[48-bit address][15-bit TAG][1-bit tentative]` in physical little-endian memory. The tag is accessed via `kTagShift = 62 - 14 = 48`, so it occupies bits 48-61 (14 bits). The tentative bit is at position 63. There is also a Pending bit at position 62 and a ReadCache bit at position 47 (inside the address field). The doc's layout diagram is factually wrong. Specifically:
  - The tag is 14 bits (kTagSize = 14, bits 48-61), this is correct
  - But the doc claims "read-cache: 1 bit" as a separate field. Actually, `kReadCacheBitShift = 47` means the ReadCache bit is bit 47, which is INSIDE the 48-bit address field. It's not a separate top-level field.
  - The Pending bit (bit 62) is not mentioned at all

- [C3] **SpanByte size claim is wrong.** Section 2.3 says "keeping the total `SpanByte` size at 12 bytes (4 + 8 for the pointer)". Looking at `SpanByte.cs:L21`, the struct has `StructLayout(LayoutKind.Explicit, Pack = 4)` with `[FieldOffset(0)] int length` and `[FieldOffset(4)] IntPtr payload`. On 64-bit, this is 4 + 8 = 12 bytes. However, `Pack = 4` doesn't enforce a specific Size. The actual size depends on alignment, which could be 12 or 16 depending on the runtime. The doc should note this ambiguity or check the actual `sizeof(SpanByte)`. (Note: Looking more carefully, the struct does NOT have `Size = 12` unlike ArgSlice, so the size is determined by the runtime. With `Pack = 4`, it's likely 12 bytes, but this should be stated with less certainty.)

#### Major Issues (should fix)

- [M1] **LightEpoch ThreadStatic description is wrong about field location.** Section 2.7 shows the ThreadStatic fields as direct members of `LightEpoch`, but the actual code at `LightEpoch.cs:L50-L83` shows they are in a nested `private class Metadata`. This matters because the nesting scope affects visibility and is architecturally significant -- the fields are in a separate class to group them logically.

- [M2] **TryPromoteLatch description is inaccurate.** Section 2.8 says "Latch promotion: S->X upgrade via CAS that sets X bit and decrements S count atomically". The actual code at `HashBucket.cs:L113-L130` does this in a single CAS: `long new_word = (expected_word | kExclusiveLatchBitMask) - kSharedLatchIncrement`. This is correct. But the doc omits that after promotion, there's a spin-drain for other shared readers (L132-L138), and if draining fails, the promotion is reversed. This means promotion is NOT guaranteed to succeed even after the CAS, which is an important semantic detail.

- [M3] **Missing VectorSet bit in RecordInfo discussion.** The RecordInfo bit layout in Section 2.3 correctly shows the VectorSet bit at position 63, but Section 2.2 (cache-line alignment) and the broader performance document never discuss the performance implications of VectorSet checks that are now in the hot path (see `ReadMethods.cs:L29-L44` where VectorSet checks happen on every SingleReader call).

- [M4] **`AggressiveInlining` references may have shifted.** Section 2.4 lists specific line numbers for AggressiveInlining usage (e.g., `LimitedFixedBufferPool.cs:L85, L111`, `SessionParseState.cs:L72, L89, L102`). These line numbers should be verified and may not be stable. More importantly, the doc doesn't mention the `[MethodImpl(MethodImplOptions.AggressiveInlining)]` on `SendAndReset` at `RespServerSession.cs:L1254`, which is one of the most performance-critical inlining decisions.

- [M5] **Scatter-gather GET description is incomplete.** Section 2.10 briefly mentions scatter-gather GET at `BasicCommands.cs:L177-L200` but doesn't explain the mechanism: how pending operations are tracked, how completions are waited on, and how responses are assembled. For performance engineering, this is one of the most sophisticated optimizations.

#### Minor Issues (nice to fix)

- [m1] **"Thread-per-connection model" terminology may be misleading.** Doc 01 (Section 4) says "Thread-per-connection model with async I/O" but the actual model is event-driven with SAEA callbacks. There is NOT a dedicated thread per connection; instead, connections share the thread pool via async callbacks. "Thread-per-connection" implies dedicated threads, which is the opposite of what SAEA provides.

- [m2] **SpinLock vs Monitor timing claim.** Section 2.11 says "SpinLock avoids the overhead of `Monitor.Enter` (which involves checking thin lock, inflation to OS mutex, etc.) for critical sections that hold for < 1 microsecond." The `Monitor` thin lock path on modern .NET is quite fast (no inflation needed if uncontended). The claimed "~50ns per lock/unlock saved" in the performance table may be overstated for uncontended cases.

- [m3] **Missing `ReadOptimizedLock` implementation detail.** Section 2.12 mentions `ReadOptimizedLock` uses `[ThreadStatic]` counter but doesn't describe the actual mechanism. The file is at `libs/common/ReadOptimizedLock.cs` and should be checked to verify the claim.

- [m4] **"100x throughput improvement over Redis" claim.** Both docs 01 and 15 repeat this claim without qualification. The actual improvement depends heavily on workload, concurrency, data size, and hardware. The original Garnet paper likely qualifies this number; the docs should reference the specific benchmark conditions.

#### Strengths
- Comprehensive catalog of optimization techniques with specific code references
- The performance impact estimation table is very useful for prioritizing reimplementation effort
- RecordInfo bit layout is accurate and detailed
- Good coverage of unsafe code patterns with clear explanations of why each pattern is used
- The distinction between "always inline" and "intentionally NOT inline" strategies is insightful

---

### Cross-Document Issues

- [X1] **VectorStore omitted across all documents.** The VectorStore/VectorSessionFunctions is a major architectural component visible in the type aliases (RespServerSession.cs:L29-L31), the GarnetApi definition (3 type parameters), and has its own subsystem (`libs/server/Resp/Vector/`). None of the 5 documents mention it. This is a critical omission for reimplementation.

- [X2] **AsyncProcessor not covered.** The async processing capability (`libs/server/Resp/AsyncProcessor.cs`, `useAsync` flag in BasicCommands.cs) is not mentioned in any document. This is a significant feature that changes the GET execution path.

- [X3] **Inconsistent ArgSlice size.** Doc 08 says 16 bytes, but `ArgSlice.cs:L21` shows `Size = 12`. This error propagates: doc 08 calculates 80 bytes for 5 ArgSlices (should be 60). Doc 15 does not cite ArgSlice size. SessionParseState.cs:L22 comment says "5 * 20 = 60" which is also confusing (20 != 12) but at least gets the total (60) correct.

- [X4] **Thread model described inconsistently.** Doc 01 says "Thread-per-connection model with async I/O" (Section 4), doc 07 accurately describes the SAEA callback model, and doc 09 describes synchronous processing on the callback thread. These descriptions are not contradictory but the "thread-per-connection" label in doc 01 is misleading when docs 07 and 09 correctly show it's event-driven.

- [X5] **`waitForAofBlocking` not mentioned until doc 09 open questions.** The AOF synchronous commit behavior (where `Send` blocks until AOF write completes) is mentioned as an open question in doc 09 but is actually visible in the code at `RespServerSession.cs:L1360-L1364`. This should be documented in the request lifecycle (doc 09) and its performance impact analyzed in doc 15.

- [X6] **`ServerSessionBase` not mentioned in any document.** This base class at `libs/server/Sessions/ServerSessionBase.cs` sits between `IMessageConsumer` and `RespServerSession` and holds shared session infrastructure. Docs 01, 08, and 09 all discuss `RespServerSession` as if it directly implements `IMessageConsumer`, but the actual inheritance chain includes this intermediate class.

---

### Summary

| Document | Critical | Major | Minor |
|----------|----------|-------|-------|
| 01-architecture-overview.md | 3 | 4 | 3 |
| 07-network-layer.md | 1 | 4 | 3 |
| 08-resp-parser.md | 3 | 4 | 3 |
| 09-request-lifecycle.md | 4 | 4 | 2 |
| 15-performance-engineering.md | 3 | 5 | 4 |
| Cross-Document | 6 | - | - |
| **Total** | **20** | **21** | **15** |

**Overall Assessment:** The documents demonstrate strong conceptual understanding of Garnet's architecture and provide useful high-level traces of the major code paths. The writing quality is good, and the Rust/Kotlin mapping notes are practical. However, there are significant factual errors in data structure sizes (ArgSlice), bit layouts (HashBucketEntry, hash bucket latch), and the complete omission of the VectorStore subsystem. The most concerning pattern is that several claims appear to be inferred rather than verified against source code (e.g., "likely a hash or trie-based approach" for command lookup, approximate enum values). For a reimplementation document, every factual claim must be verified. The request lifecycle document (09) is the strongest of the five but still misses important code paths (WRONGTYPE, scatter-gather, async). The performance engineering document (15) has the most factual errors in bit layouts but provides the most value in its optimization catalog.

**Recommendation:** Fix all critical issues before proceeding. The VectorStore omission needs to be addressed across all documents. All bit-layout diagrams need to be verified against source code constants. The ArgSlice size error should be corrected as it affects cache-line analysis throughout the documents.
