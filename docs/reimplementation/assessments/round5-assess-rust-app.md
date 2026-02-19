# Feasibility Assessment: Rust x Server/Network
## Assessor: assess-rust-app

### Coverage Rating: A-

The documentation across the four documents is exceptionally detailed for a reimplementation effort. It covers the full lifecycle from TCP accept through command dispatch, response writing, and transaction coordination. The Rust-specific mapping notes embedded in each document further accelerate feasibility analysis. The minor gap that prevents a full A is the lack of detail on cluster-mode networking (cross-node forwarding, gossip protocol) and the async GET scatter-gather internals.

---

### Dimension Scores

| Dimension | Score (1-10) | Notes |
|-----------|-------------|-------|
| 1. Async I/O Mapping (SAEA -> io_uring/epoll) | 9 | SAEA pattern is thoroughly documented: accept loop, per-connection receive SAEA, synchronous completion fast path. Maps cleanly to io_uring (tokio-uring) or epoll (tokio/mio). The synchronous completion loop (ReceiveAsync returning false) maps to io_uring's batched completions or tokio's poll-based readiness. |
| 2. Zero-Copy Parsing | 9 | ArgSlice (ptr+len into receive buffer), SessionParseState with pinned arrays, SIMD uppercase detection, partial message handling -- all exhaustively documented. Rust's `&[u8]` slices are a natural fit. The bit-twiddling uppercase check can be ported verbatim. |
| 3. Buffer Pool Mapping | 9 | LimitedFixedBufferPool design is fully specified: tiered ConcurrentQueues, power-of-2 sizing, pinned allocation, clear-on-return. Maps to a custom slab allocator with mmap-backed pages or a tiered `crossbeam::ArrayQueue` pool. |
| 4. Command Dispatch | 8 | Command enum structure, length-based switching, jump table dispatch are described. The exact multi-byte comparison tricks in RespCommand.cs are noted as needing further analysis (Open Question #1 in doc 08). Rust `match` on enum + phf crate covers this well. |
| 5. Transaction Support | 8 | MULTI/EXEC state machine, sorted lock acquisition for deadlock freedom, WatchVersionMap optimistic concurrency, buffer replay mechanism, stored procedures -- all thoroughly documented. Rust-specific challenges around lifetime management for buffer replay are noted. |
| 6. TLS Integration | 7 | TLS state machine (Rest/Active/Waiting), SslStream wrapping, separate transport buffers documented. However, the async fallback path (`SslReaderAsync`) and exact handshake flow lack step-by-step detail. rustls mapping is straightforward but requires careful async integration. |
| 7. Pipelining | 9 | Batch processing is clearly described: ProcessMessages while-loop, single Send() at end, overflow handling via SendAndReset. The automatic nature (no special protocol handling) makes this trivially portable. |
| 8. Performance Parity | 8 | Rust can match or exceed C# networking. The zero-copy patterns, pinned buffers, and SAEA reuse translate directly. Rust advantages: no GC pauses, no pinned-object-heap overhead, io_uring kernel bypass. Rust challenges: async runtime overhead (tokio) vs SAEA's completion-port model. |

**Overall Feasibility Score: 8.4 / 10**

---

### Subsystem Analysis

#### Document 07: Network Layer

**Maps cleanly:**
- Accept loop with backlog=512 -> `TcpListener::bind` + `accept()` in a loop with tokio
- Per-connection receive loop with synchronous completion fast path -> tokio task per connection, or io_uring submission batching
- TCP NoDelay -> `TcpStream::set_nodelay(true)`
- Connection limit via atomic counter -> `AtomicUsize` with `fetch_add`
- Send throttle via semaphore -> `tokio::sync::Semaphore` with ThrottleMax permits
- LightConcurrentStack for buffer reuse -> `crossbeam::ArrayQueue<GarnetSaeaBuffer>`
- Buffer shift/double/shrink lifecycle -> manual management on `Vec<u8>` or mmap-backed buffers

**Needs creative solutions:**
- SAEA completion-based I/O model: .NET SAEA uses OS completion ports (Windows) or epoll (Linux via .NET runtime). Rust has two paradigms:
  - **tokio (epoll/kqueue)**: Readiness-based, requires read into user buffer. Good fit for the synchronous completion fast path.
  - **tokio-uring (io_uring)**: Completion-based like SAEA, with kernel-managed buffer pools (`IORING_OP_PROVIDE_BUFFERS`). Better architectural match but Linux 5.6+ only.
  - Recommendation: Use tokio for portability, with optional io_uring backend for Linux.
- NetworkHandler class hierarchy (5 levels deep): Rust doesn't have class inheritance. Remodel as composition with traits: `trait NetworkHandler`, `trait NetworkSender`, with concrete structs for TCP/TLS variants.
- GarnetTcpNetworkSender's SpinLock for response object lifecycle: `parking_lot::Mutex` or `std::sync::Mutex`. The critical section is very short (pop/push from stack), so `parking_lot::Mutex` is ideal.

**Missing information:**
- Unix domain socket handling specifics beyond the NoDelay skip
- Exact behavior when `networkConnectionLimit` is 0 (unlimited?)
- How `activeHandlers` dictionary cleanup works on connection close

#### Document 08: RESP Parser

**Maps cleanly:**
- ArgSlice (12 bytes: ptr + len) -> `(*const u8, u32)` or `&[u8]` slice (16 bytes on 64-bit, but the extra 4 bytes are negligible)
- SessionParseState with MinParams=5 inline -> `SmallVec<[(&[u8]); 5]>` or `tinyvec::ArrayVec`
- RespReadUtils pointer-based parsing -> Rust `unsafe` pointer arithmetic with `*const u8`, or safe parsing with slice indexing
- Integer parsing fast path (19 digits without overflow check) -> direct port of the same algorithm
- SIMD-like uppercase detection bit trick -> direct port or use `str::make_ascii_uppercase()`
- Command dispatch jump table -> Rust `match` on enum (compiler generates jump table for dense enums)
- Partial message handling (reset readHead) -> save and restore buffer position index

**Needs creative solutions:**
- The pinned GC array for SessionParseState: Rust has no GC, so pinning is a non-issue. Use a stack-allocated `ArrayVec` or a `Box<[ArgSlice]>` on the heap.
- RespCommand enum as ushort with range-based classification (OneIfWrite, OneIfRead): Rust enums can have explicit discriminants. Use `#[repr(u16)]` and implement range checks as const fn methods.
- Command name lookup using length-based switching + multi-byte comparison: Use `phf` for compile-time perfect hash, or replicate the length+byte approach with `match (len, first_bytes_as_u64)`.

**Missing information:**
- Full command-to-enum mapping logic (noted as Open Question)
- RESP3 parsing differences (mode switching, new type prefixes)
- Inline command support (non-RESP format like `PING\r\n`)
- Maximum command name length assumption for the uppercase optimization

#### Document 09: Request Lifecycle

**Maps cleanly:**
- TryConsumeMessages entry point -> function taking `&[u8]` receive buffer slice, returning bytes consumed
- Direct-to-buffer response writing (dcurr/dend pointers) -> `&mut [u8]` write cursor with position tracking
- Pipelining (process all commands, single send) -> parse loop + flush at end
- SendAndReset overflow pattern -> allocate new buffer, flush old, continue writing
- Epoch acquisition per batch -> `crossbeam_epoch::pin()` per batch
- GET scatter-gather and async paths -> documented at high level

**Needs creative solutions:**
- SpanByteAndMemory dual-mode output (inline SpanByte or heap Memory): In Rust, use an enum `enum Output { Inline { buf: &mut [u8], len: usize }, Heap(Vec<u8>) }` or write directly to the send buffer with a fallback to Vec.
- The GarnetApi generic type with three store contexts: Use Rust generics with trait bounds, but the 3-context pattern adds complexity. Consider a struct with three fields rather than triple-generic parameterization.
- AOF blocking wait before send (`waitForAofBlocking`): Use `tokio::sync::oneshot` or a `Notify` to await AOF flush completion.

**Missing information:**
- Scatter-gather GET internals (how pending I/Os are batched and completed)
- AsyncProcessor background task lifecycle details
- CompletePendingForSession blocking behavior
- Exact interaction between AOF write and response latency under load

#### Document 12: Transactions and Scripting

**Maps cleanly:**
- TxnState enum (None/Started/Running/Aborted) -> Rust enum, stored as `AtomicU8` or plain field (single-threaded per session)
- MULTI/EXEC state machine transitions -> match-based state transitions
- TxnKeyEntry 10-byte struct -> `#[repr(C)]` struct with explicit layout
- Sorted lock acquisition for deadlock freedom -> `sort_unstable_by` + sequential lock acquire
- WatchVersionMap (fixed-size AtomicI64 array) -> `Vec<AtomicI64>` with `Ordering::SeqCst`
- Buffer replay by rewinding readHead -> save byte offset, re-parse from saved position
- Stored procedure Prepare/Main/Finalize -> trait with three methods

**Needs creative solutions:**
- LockableContext from Tsavorite: This is deeply tied to Tsavorite's concurrency model. A Rust reimplementation needs its own lock table. Options:
  - Per-bucket `RwLock` with sorted acquisition order
  - Lock-free hash table with epoch-based reclamation (like crossbeam-skiplist)
  - Hybrid: optimistic reads with pessimistic writes
- Three API wrappers (GarnetWatchApi, LockableGarnetApi, BasicGarnetApi): Implement as three structs implementing a common `GarnetApi` trait, each wrapping different lock contexts.
- Buffer replay lifetime management: The queued commands reference the receive buffer. Must ensure the buffer is not shifted or freed during transaction execution. This is naturally handled if the buffer is owned by the session and not reallocated during a transaction.

**Missing information:**
- Tsavorite's `LockableContext` internal lock protocol (how Lock/Unlock work at the hash bucket level)
- Cross-database transaction design (currently blocked)
- Non-deterministic stored procedure replay concerns
- Maximum transaction size limits

---

### Missing Information (Blocking or Near-Blocking)

1. **Tsavorite LockableContext internals (Medium-blocking)**: The transaction system depends heavily on Tsavorite's `LockableContext.Lock/Unlock` and `CompareKeyHashes`. Without understanding the lock granularity (per-bucket? per-record? per-lock-table-slot?), implementing a Rust equivalent requires guesswork or source code reading.

2. **Cluster-mode networking (Non-blocking for single-node, blocking for cluster)**: No document covers cross-node request forwarding, MIGRATE, gossip protocol, or slot-based routing at the network level. Doc 09 briefly mentions `NetworkKeyArraySlotVerify` but doesn't explain the forwarding path.

3. **RESP3 mode specifics (Low-blocking)**: Parser documentation focuses on RESP2. RESP3 adds maps, sets, attributes, pushes -- the parsing differences are not detailed.

4. **AsyncProcessor lifecycle (Low-blocking)**: The async GET path and scatter-gather paths are mentioned but their full implementation details are missing. Not blocking for initial implementation but needed for feature parity.

5. **Connection teardown and error recovery (Low-blocking)**: What happens on read errors, write errors, connection resets? The Dispose path is mentioned but not detailed.

6. **Wire format detection (Low-blocking)**: `WireFormat.cs` is listed in the file map but not described. How does Garnet detect whether a connection speaks RESP vs. another protocol?

---

### Risk Analysis

| Risk | Severity | Mitigation |
|------|----------|------------|
| **Async runtime overhead**: tokio's task scheduling adds overhead vs SAEA's direct completion callbacks. The synchronous completion fast path (process data without returning to thread pool) is harder to replicate in tokio. | High | Use `tokio::task::spawn_blocking` for the processing loop, or consider `glommio`/`monoio` for thread-per-core model matching Garnet's SAEA pattern. Alternatively, use raw epoll with `mio` for maximum control. |
| **Lifetime complexity in buffer replay**: Transaction buffer replay requires the receive buffer to remain valid and unmodified during EXEC. Rust's borrow checker will enforce this, but the design must avoid borrowing conflicts between the parser and the transaction engine. | Medium | Copy queued commands into a transaction-owned buffer during MULTI phase, accepting the copy cost. Or use an `Arc<[u8]>` shared buffer with range tracking. |
| **Tsavorite lock protocol reimplementation**: The sorted-lock protocol depends on Tsavorite's `CompareKeyHashes` ordering. Getting this wrong causes deadlocks. | High | Define a clear, documented lock ordering (e.g., by hash bucket index, then by lock type) and add deadlock detection in debug builds. |
| **TLS performance regression**: rustls has different performance characteristics than .NET SslStream. The three-state TLS machine (Rest/Active/Waiting) is optimized for .NET's async model. | Medium | Benchmark rustls vs native-tls early. The Rest/Active/Waiting state machine may simplify in Rust since rustls operates on raw byte buffers without the SslStream abstraction. |
| **Command compatibility gaps**: 200+ commands need dispatch tables. Missing any command or getting edge cases wrong breaks Redis compatibility. | Medium | Use Redis's own test suite (redis-py tests, RESP integration tests) for validation. Build a compatibility matrix early. |
| **Pinned buffer equivalent in Rust**: Garnet uses GC-pinned arrays for zero-copy pointer stability. Rust's `Box` and `Vec` don't relocate, so this is naturally satisfied -- but custom allocators (jemalloc, mmap) need careful handling. | Low | Standard Rust heap allocations are stable. Use `Box<[u8]>` for buffers. For mmap-backed pools, use `memmap2` crate. |

---

### Recommended Rust Crates/Approaches

| Component | Recommended Crate | Alternative | Notes |
|-----------|------------------|-------------|-------|
| Async runtime | `tokio` (1.x) | `monoio`, `glommio` | tokio for ecosystem; monoio/glommio for thread-per-core matching SAEA model |
| io_uring | `tokio-uring` | `io-uring` (raw) | Optional Linux-specific backend for completion-based I/O |
| TCP listener | `tokio::net::TcpListener` | `mio` (raw) | mio gives more control over the accept loop |
| TLS | `rustls` (0.23+) | `native-tls` | rustls: pure Rust, no OpenSSL dependency, buffer-oriented API |
| RESP parsing | Custom (manual `&[u8]` parsing) | `nom`, `combine` | Custom is faster; nom adds ergonomics for complex parsing |
| Fast newline scan | `memchr` | -- | SIMD-accelerated `\r\n` search |
| Command lookup | `phf` (perfect hash) | `match` on `(len, u64)` | phf for compile-time, match for runtime flexibility |
| Buffer pool | Custom tiered pool | `slab`, `sharded-slab` | Custom pool matching LimitedFixedBufferPool's tiered design |
| Small vec (SessionParseState) | `smallvec` or `tinyvec` | `arrayvec` | 5-element inline optimization |
| Concurrent queue (buffer reuse) | `crossbeam::ArrayQueue` | `flume` | Bounded queue matching LightConcurrentStack |
| Epoch-based reclamation | `crossbeam-epoch` | `haphazard` | For batch epoch protection |
| Atomic operations | `std::sync::atomic` | `crossbeam-utils` | AtomicI64 for WatchVersionMap |
| Lock ordering | `parking_lot::RwLock` | `std::sync::RwLock` | parking_lot for shorter critical sections |
| Semaphore (backpressure) | `tokio::sync::Semaphore` | -- | For send throttle |
| Memory-mapped I/O | `memmap2` | -- | For mmap-backed buffer pools |
| Metrics/tracing | `tracing` | `metrics` | For latency tracking equivalent |

### Architectural Recommendation

The strongest architectural match for Garnet's SAEA-based networking in Rust is a **thread-per-core** model using `monoio` or `glommio`:

1. **Accept thread**: Distributes connections round-robin to core threads (matching Garnet's single accept SAEA)
2. **Core threads**: Each owns a set of connections, processes them with io_uring submissions (matching SAEA's completion-based model)
3. **No cross-thread sharing**: Each connection's state (session, parse state, buffers) lives on its core thread (matching Garnet's per-connection handler model)

This avoids tokio's work-stealing overhead and matches the tight synchronous-completion-loop pattern that Garnet relies on for performance.

If portability (macOS, Windows) is required, fall back to tokio with `current_thread` runtime per core, using `SO_REUSEPORT` for the accept distribution.

---

### Summary

The documentation provides an excellent foundation for a Rust reimplementation of Garnet's server/network layer. The zero-copy patterns (ArgSlice, direct buffer pointers, pinned arrays) are a natural fit for Rust's ownership model. The main engineering challenges are:

1. Choosing the right async I/O model (tokio vs thread-per-core)
2. Reimplementing Tsavorite's lock protocol for transactions
3. Managing buffer lifetimes during transaction replay

The documentation quality is high enough that an experienced Rust systems programmer could begin implementation with minimal source code consultation, except for the Tsavorite lock protocol details.
