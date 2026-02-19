# RESP Parser

## 1. Overview

Garnet's RESP (Redis Serialization Protocol) parser is a zero-allocation, pointer-based parser that operates directly on pinned receive buffers. It supports both RESP2 and RESP3 protocols and is designed to parse commands without creating any managed string objects on the hot path. The parser produces `ArgSlice` values -- lightweight pointer+length pairs that reference the original receive buffer -- which are stored in `SessionParseState`, a pinned array that fits approximately one cache line.

The parser must handle partial messages gracefully (a command may span multiple TCP receives) and integrates tightly with the command dispatch layer through the `RespCommand` enum.

## 2. Detailed Design

### 2.1 RESP Protocol Format

RESP2 is a text-based protocol where all commands are encoded as arrays of bulk strings:

```
*3\r\n          -- Array of 3 elements
$3\r\n          -- Bulk string of length 3
SET\r\n         -- "SET"
$5\r\n          -- Bulk string of length 5
mykey\r\n       -- "mykey"
$7\r\n          -- Bulk string of length 7
myvalue\r\n     -- "myvalue"
```

Responses use type prefixes: `+` (simple string), `-` (error), `:` (integer), `$` (bulk string), `*` (array). RESP3 adds additional types: `_` (null), `,` (double), `#` (boolean), `(` (big number), `%` (map), `~` (set), `|` (attribute), `>` (push).

### 2.2 Core Parsing Primitives

The fundamental parsing utilities are in `RespReadUtils` [Source: libs/common/RespReadUtils.cs:L16], a static class of unsafe methods operating on `byte*` pointers:

**`TryReadUnsignedLengthHeader(out int length, ref byte* ptr, byte* end)`**: Reads a `$<length>\r\n` header. This is the most-called parsing method and is critical for performance. It:
1. Expects `*ptr` to be `$`
2. Skips the `$`, reads digits into a `ulong` accumulator using the fast path for up to 19 digits
3. Validates `\r\n` terminator
4. Advances `ptr` past the `\r\n`

**`TryReadInt64(ref byte* ptr, byte* end, out long value, out ulong bytesRead)`**: Reads signed 64-bit integers from ASCII with overflow detection. Uses a two-phase approach [Source: libs/common/RespReadUtils.cs:L44-L101]:
- Fast path: Parse first 19 digits without overflow checking (19 digits always fit in uint64)
- Slow path: For 20+ digit numbers, check overflow on each digit

**`TryReadUInt64`**: The inner loop compiles to approximately 4 instructions per digit:
```c
nextDigit = *readHead - '0';
if (nextDigit > 9) goto Done;
value = 10 * value + nextDigit;
readHead++;
```

All parsing methods return `false` if the end of the buffer is reached before the parse is complete, enabling partial message handling without exceptions.

### 2.3 SessionParseState

`SessionParseState` is the central structure for managing parsed arguments [Source: libs/server/Resp/Parser/SessionParseState.cs:L17-L490]:

```
struct SessionParseState {
    int Count;                    // Number of accessible arguments
    ArgSlice* bufferPtr;          // Pointer to pinned argument array
    int rootCount;                // Count in root buffer
    ArgSlice[] rootBuffer;        // Pinned managed array
}
```

**ArgSlice layout** (12 bytes, explicitly sized) [Source: libs/server/ArgSlice/ArgSlice.cs:L18-L21]:
```
[StructLayout(LayoutKind.Explicit, Size = 12)]
struct ArgSlice {
    [FieldOffset(0)]  byte* ptr;     // 8 bytes - pointer into receive buffer
    [FieldOffset(8)]  int length;    // 4 bytes - length of argument data
}
// Total: exactly 12 bytes (ArgSlice.Size = 12, no padding)
```

**Initialization**: On session creation, `Initialize()` allocates a pinned array of `MinParams = 5` ArgSlices:
```csharp
rootBuffer = GC.AllocateArray<ArgSlice>(MinParams, true);  // pinned
bufferPtr = (ArgSlice*)Unsafe.AsPointer(ref rootBuffer[0]);
```
5 ArgSlices * 12 bytes = 60 bytes, fitting within one 64-byte cache line. [Source: libs/server/Resp/Parser/SessionParseState.cs:L22, L60-L66]

**Growth**: When a command has more than 5 arguments, `Initialize(count)` is called. If `count > rootBuffer.Length`, a new pinned array is allocated. The old array becomes GC garbage (not pooled, as this is rare).

**Zero-copy argument access**: The `Read(int i, ref byte* ptr, byte* end)` method [Source: libs/server/Resp/Parser/SessionParseState.cs:L331-L358] parses one RESP bulk string argument and stores the pointer and length directly:
```csharp
slice.ptr = ptr;           // Points INTO the receive buffer
slice.length = length;     // From the $<length> header
ptr += slice.length + 2;   // Skip past data + \r\n
```

No string is allocated. The argument data remains in the receive buffer until the buffer is shifted or reused.

### 2.4 Command Parsing Flow

Command parsing begins in `RespServerSession.ProcessMessages()` (see [Doc 09 Section 2.1](./09-request-lifecycle.md#21-complete-get-trace) Steps 6-7 for how parsing connects to dispatch and storage operations) [Source: libs/server/Resp/RespServerSession.cs:L572-L667]:

```
ProcessMessages():
  _origReadHead = readHead
  while bytesRead - readHead >= 4:
    cmd = ParseCommand(writeErrorOnFailure: true, out commandReceived)
    if !commandReceived:
      endReadHead = readHead = _origReadHead   // Reset to retry later
      break
    // Dispatch cmd...
    _origReadHead = readHead = endReadHead     // Advance past command
```

`ParseCommand` (defined in the Parser directory) performs:

1. **Read array header**: Expects `*<count>\r\n` where count is the number of elements
2. **Read command name**: Reads the first bulk string, converts to uppercase via `AsciiUtils.ToUpperInPlace` [Source: libs/server/Resp/RespServerSession.cs:L1180-L1213]
3. **Lookup command**: Maps the uppercase command name to a `RespCommand` enum value. The lookup uses length-based switching combined with byte comparisons on the command name to identify the command in O(1) time. [Source: libs/server/Resp/Parser/RespCommand.cs]
4. **Parse arguments**: Calls `parseState.Initialize(argCount - 1)` then `parseState.Read(i, ref ptr, end)` for each argument

### 2.5 Uppercase Optimization

The `MakeUpperCase` method [Source: libs/server/Resp/RespServerSession.cs:L670-L726] uses a SIMD-like bit trick to detect if any bytes need uppercasing. It reads TWO `ulong` values:

```csharp
var firstUlong = *(ulong*)(ptr + 4);
var secondUlong = *(ulong*)(ptr + 4 + cmdLen);

var firstAllUpper = (((firstUlong + (~0UL / 255 * (127 - 95))) | firstUlong) & (~0UL / 255 * 128)) == 0;
var secondAllUpper = (((secondUlong + (~0UL / 255 * (127 - 95))) | secondUlong) & (~0UL / 255 * 128)) == 0;

var allLower = firstAllUpper && secondAllUpper;
if (allLower) return false;  // nothing to uppercase, early exit
```

This tests 8 bytes at once by checking if any byte exceeds value 95 (lowercase ASCII starts at 97). The first ulong covers the RESP framing bytes and start of the command; the second ulong covers the command bytes themselves (at `ptr + 4 + cmdLen` where `cmdLen` is derived from the `$<len>` header). Both must pass the check for the fast path to be taken. If all bytes are already uppercase (the common case for well-behaved clients), the method returns immediately without touching any bytes. This is based on the classic "determine if a word has a byte greater than n" bit hack from Stanford's bit twiddling page. [Source: libs/server/Resp/RespServerSession.cs:L690-L698]

### 2.6 Command Dispatch Table

`RespCommand` is a `ushort` enum [Source: libs/server/Resp/Parser/RespCommand.cs:L18] organized into sections:
- **Read-only commands**: `NONE + 1` through `ZSCORE` (value ~117)
- **Write commands**: Starting from `APPEND` (value ~121)
- **Administrative/slow commands**: `AUTH`, `CLIENT_*`, `COMMAND_*`, etc.

The separation enables efficient classification: `cmd.OneIfWrite()` and `cmd.OneIfRead()` likely use range checks on the enum value for O(1) classification.

The dispatch in `ProcessBasicCommands` [Source: libs/server/Resp/RespServerSession.cs:L728-L793] is a switch expression, which the C# compiler optimizes into a jump table. This gives O(1) dispatch for the ~200+ supported commands.

### 2.7 Partial Message Handling

The parser handles incomplete messages by checking `ptr > end` at every read boundary. When a read method returns `false`, the entire command parse is abandoned and `readHead` is reset to `_origReadHead` -- the position before the current command started. The buffer retains the unparsed bytes, which will be combined with future receives:

```
[consumed bytes][incomplete command bytes]
                ^-- readHead reset to here
```

After `ProcessMessages` returns, `TryConsumeMessages` returns `readHead` as the number of consumed bytes. The network layer shifts unconsumed bytes to the start of the buffer via `ShiftNetworkReceiveBuffer()` and the next receive appends after them.

### 2.8 Response Writing

Response writing uses `RespWriteUtils` methods that write directly into the send buffer via `dcurr` and `dend` pointers:

```csharp
while (!RespWriteUtils.TryWriteError(errorMsg, ref dcurr, dend))
    SendAndReset();
```

The `TryWrite*` methods return `false` when the output buffer is full. `SendAndReset()` [Source: libs/server/Resp/RespServerSession.cs:L1255-L1272] flushes the current buffer to the network and obtains a fresh one:

```csharp
void SendAndReset():
  d = networkSender.GetResponseObjectHead()
  if dcurr - d > 0:
    Send(d)                              // Flush to network
    networkSender.GetResponseObject()    // Get new buffer
    dcurr = networkSender.GetResponseObjectHead()
    dend = networkSender.GetResponseObjectTail()
  else:  // dcurr - d <= 0
    throw GarnetException(LogLevel.Critical, ...)  // No progress made, fatal error
```

The `else` branch fires when `dcurr - d` is zero or negative (i.e., the write pointer has not advanced). Zero means the `TryWrite*` call made no progress at all -- the message being written cannot fit in the response buffer. Rather than looping forever, it throws a `GarnetException` at `LogLevel.Critical`. [Source: libs/server/Resp/RespServerSession.cs:L1255-L1272]

This loop-and-retry pattern means that even responses larger than the send buffer are handled correctly through multiple buffer flushes.

## 3. Source File Map

| File | Purpose | Key Lines |
|------|---------|-----------|
| `libs/common/RespReadUtils.cs` | Low-level RESP parsing primitives | L16-L200 |
| `libs/server/Resp/Parser/SessionParseState.cs` | Argument buffer management | L17-L490 |
| `libs/server/Resp/Parser/RespCommand.cs` | Command enum and lookup | L18-L200+ |
| `libs/server/Resp/Parser/ParseUtils.cs` | Helper parsing for typed values | - |
| `libs/server/Resp/RespServerSession.cs` | ProcessMessages, ParseCommand, MakeUpperCase | L453-L726 |
| `libs/server/Resp/CmdStrings.cs` | Constant RESP string literals | - |
| `libs/common/RespWriteUtils.cs` | Response writing utilities | - |
| `libs/server/Resp/BasicCommands.cs` | Individual command handlers | L23-L56 (GET) |

## 4. Performance Notes

- **No allocation on hot path**: `SessionParseState` uses a pinned `ArgSlice[]` allocated once per session. Arguments are pointer+length references into the receive buffer. No strings, no byte array copies.
- **Cache-line-fitted argument buffer**: 5 ArgSlices * 12 bytes = 60 bytes, fitting within a single 64-byte cache line. Most Redis commands have 1-4 arguments, so the common case fits in L1 cache.
- **SIMD-like uppercase detection**: The bit-twiddling trick in `MakeUpperCase` avoids per-byte branching in the common case where commands are already uppercase.
- **Jump table dispatch**: The C# switch expression over `RespCommand` compiles to an efficient jump table, giving O(1) command dispatch regardless of the number of supported commands.
- **Partial parse is cheap**: On incomplete messages, the parser simply resets `readHead` and returns. No exception is thrown, no cleanup is needed. The cost is approximately one failed bounds check.
- **Inline RESP formatting**: `RespWriteUtils.TryWrite*` methods are typically small and `AggressiveInlining`-attributed, allowing the JIT to inline response formatting into command handlers.

## 5. Rust Mapping Notes

- **ArgSlice** [Difficulty: Low]: Map to `&[u8]` slices borrowed from the receive buffer. Rust's borrow checker ensures the buffer is not freed while slices are outstanding, providing compile-time safety that C#'s `ArgSlice` (raw `byte*`) lacks. The 12-byte `ArgSlice` (8-byte pointer + 4-byte length) is structurally identical to Rust's fat pointer `&[u8]` (8-byte pointer + 8-byte length on 64-bit). For the `unsafe` path, `(*const u8, u32)` matches C#'s exact 12-byte layout.

- **SessionParseState** [Difficulty: Low]: Use `SmallVec<[&[u8]; 5]>` or `tinyvec::ArrayVec<[&[u8]; 5]>` for the inline-5 optimization. `SmallVec` stores up to 5 elements inline (on the stack) and spills to heap for larger commands, matching `SessionParseState`'s growth behavior. 5 slices * 16 bytes = 80 bytes on 64-bit (slightly larger than C#'s 60 bytes due to 8-byte length fields), still within two cache lines.

- **RespReadUtils** [Difficulty: Low]: Implement as `fn try_read_bulk_string(buf: &[u8]) -> Option<(&[u8], usize)>` returning the data slice and bytes consumed. Use `memchr` crate for fast `\r\n` scanning (SIMD-accelerated on x86_64). The integer parsing fast path (19 digits without overflow check) maps directly. Rust's bounds checking on slice access will be elided by LLVM in sequential access patterns where the compiler can prove the index is in bounds. Performance parity expected.

- **Command lookup** [Difficulty: Low]: Use `phf` (perfect hash function) crate for compile-time command lookup, or a `match` on the first 8 bytes reinterpreted as `u64` (matching Garnet's `*(ulong*)` comparison pattern). The `phf` approach generates a zero-collision hash table at compile time, providing O(1) lookup with a single hash computation. Alternatively, Rust's `match` on `&[u8]` patterns compiles to efficient jump tables.

- **Uppercase** [Difficulty: Low]: Use `slice::make_ascii_uppercase()` which LLVM auto-vectorizes on x86_64 (processes 16 bytes at a time with SSE2). Alternatively, replicate the bit-twiddling approach from `MakeUpperCase` for the fast detection path (check if uppercasing is needed before touching any bytes). Performance parity or improvement expected.

- **Partial message handling** [Difficulty: Low]: Rust's `Option<T>` return type naturally maps to the "return false on incomplete" pattern. `fn try_parse_command(buf: &[u8]) -> Option<(Command, usize)>` returns `None` when the buffer is incomplete, and `Some((cmd, consumed))` on success. The parser simply restores the read position on `None`.

## 6. Kotlin/JVM Mapping Notes

- **ArgSlice** [Difficulty: Medium, Risk: Performance]: Map to `ByteBuffer.slice()` or a custom `data class ArgSlice(val buffer: ByteBuffer, val offset: Int, val length: Int)`. The `data class` approach allocates a small object per argument (12 bytes of fields but ~32 bytes with JVM object header). Avoid creating `String` objects until absolutely necessary. For hot-path optimization, use a pre-allocated `IntArray` of (offset, length) pairs to avoid per-argument object allocation entirely -- but this loses type safety. Netty's `ByteBuf.slice()` returns a derived buffer without copying but still allocates a small object.

- **SessionParseState** [Difficulty: Low]: A pre-allocated `Array<ArgSlice>` of fixed size in the session constructor. Resize by creating a new array (same as C#'s approach). The JVM's 5-element array is ~72 bytes with header overhead (vs. C#'s 60-byte pinned array). Close to cache-line fit.

- **Parsing** [Difficulty: Medium, Risk: Performance]: Implement on `ByteBuffer` using `get()`/`position()`. JVM bounds checking adds ~1-2ns overhead per byte access. For sequential access patterns, the JIT can sometimes eliminate bounds checks, but this depends on the JIT's escape analysis succeeding. The critical `TryReadUnsignedLengthHeader` method (called per argument) compiles to ~8 instructions per digit in C# vs. ~12-15 on the JVM due to bounds checks and method call overhead. Expect 15-25% slower parsing throughput.

- **Command dispatch** [Difficulty: Low]: Use `when` (Kotlin switch) which compiles to `tableswitch`/`lookupswitch` bytecode for contiguous/sparse enum values respectively. `tableswitch` provides O(1) dispatch identical to C#'s jump table. Alternatively, use an `IntArray`-based jump table or `HashMap<String, CommandHandler>` for extensibility.

- **String avoidance** [Difficulty: Medium]: Use `ByteBuffer.compareTo()` or manual byte comparison instead of converting to `String` for command matching. The Netty `AsciiString` class provides a non-copying string abstraction with `hashCode()` and `equals()` operating on the underlying byte array. The SIMD-like uppercase detection trick cannot be replicated directly on the JVM without `Unsafe`; use `Byte.toInt() or 0x20` for per-byte lowercase conversion.

- **Response writing** [Difficulty: Medium]: The `while (!TryWrite...) SendAndReset()` pattern maps to Netty's `ByteBuf.ensureWritable()` + `ctx.writeAndFlush()`. Netty handles buffer growth internally. The critical difference is that Garnet writes directly into a pre-allocated send buffer via `byte*` pointer (`dcurr`/`dend`), while Netty uses method calls (`writeByte`, `writeBytes`). This adds per-byte method call overhead in the response formatting path.

## 7. Open Questions

1. The exact branching structure in `RespCommand.cs` for command-to-enum mapping uses length-based switching combined with byte-level comparisons. Further analysis is needed to determine the full set of multi-byte comparison tricks used (e.g., `*(ulong*)` comparisons for 8-byte command names).
2. What is the maximum supported inline command size (non-RESP format like `PING\r\n`)? The code checks for at least 4 bytes before parsing -- does it support inline commands at all?
3. How does RESP3 mode change the parsing flow? The `respProtocolVersion` field in `RespServerSession` defaults to RESP2; what additional parsing paths are activated for RESP3?
4. The `parseState` buffer is never shrunk back to `MinParams` after a large command. Could a client sending one large command followed by many small commands cause per-session memory waste?
