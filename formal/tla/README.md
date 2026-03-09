# TLA+ Quickstart (garnet)

This directory is the starting point for model checking owner-thread actor behavior.

## Prerequisites

- Java runtime (`java` in PATH). Java 17+ recommended.

## Install TLC (CLI)

From repository root:

```bash
./tools/tla/install_tlc.sh
```

Notes:

- Default pinned version is `v1.7.4`.
- Existing jar is reused by default. Use `--force` to redownload.
- To install latest release jar instead:

```bash
./tools/tla/install_tlc.sh --latest
```

- Combine with force when needed:

```bash
./tools/tla/install_tlc.sh --latest --force
```

## Run sample model

```bash
./tools/tla/run_tlc.sh formal/tla/specs/OwnerThreadKV.tla formal/tla/specs/OwnerThreadKV.cfg
```

## Modeling Rule (Repro First)

- First goal is **reproduction** of the currently running failing test trace.
- Do **not** split into `bug` / `fixed` model variants until a concrete causal path is confirmed by TLC traces.
- After the cause is identified, add `*_bug.cfg` and `*_fixed.cfg` as a second step.

## Source Annotation Rule

When a Rust code path is backed by a TLA+ model:

- Add a file-level comment identifying the related model/spec.
- Add comments on each critical section with the exact TLA+ action label:
  - `// TLA+ : <ActionName>`

The script stores TLC metadata under:

- `${TMPDIR:-/tmp}/garnet-tla/<timestamp-pid>/`

## Reproduce a blocking disconnect leak (counterexample)

This model captures a common blocking-path regression:

- a client disconnects while blocked
- waiter cleanup is skipped
- disconnected waiter remains in queue head and can break fairness/progress

Disconnect timing assumption in this model:

- `Disconnect(c)` is enabled for any `c` in `ACTIVE` or `BLOCKED` state at any step.
- This means disconnect is modeled as "can happen at arbitrary timing" against all interleavings.

Bug config (expected to fail with a counterexample):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/BlockingDisconnectLeak.tla formal/tla/specs/BlockingDisconnectLeak_bug.cfg
```

Fixed config (expected to pass):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/BlockingDisconnectLeak.tla formal/tla/specs/BlockingDisconnectLeak_fixed.cfg
```

Stress config (broader key/client space, expected to pass):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/BlockingDisconnectLeak.tla formal/tla/specs/BlockingDisconnectLeak_stress.cfg
```

## Reproduce scripting accept starvation vs fixed offload

This model captures a specific starvation pattern:

- one connection enters long-running scripting execution
- connection handler synchronously waits on completion
- a second connection cannot be accepted, so `KILL` never arrives

Bug config (expected to fail):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/ScriptExecutorAcceptProgress.tla formal/tla/specs/ScriptExecutorAcceptProgress_bug.cfg
```

Fixed config (expected to pass):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/ScriptExecutorAcceptProgress.tla formal/tla/specs/ScriptExecutorAcceptProgress_fixed.cfg
```

## Reproduce lazy-expire SYNC select re-arm race

This model captures the flaky test path:

- downstream replica subscribes via `SYNC`
- lazy-expire `GET` emits synthetic `DEL`
- a following mutating command emits `SET`
- required frame order is `SELECT -> DEL -> SET`

Bug config (expected to fail with counterexample):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/LazyExpireReplicationSyncSelectRace.tla formal/tla/specs/LazyExpireReplicationSyncSelectRace_bug.cfg
```

Fixed config (expected to pass):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/LazyExpireReplicationSyncSelectRace.tla formal/tla/specs/LazyExpireReplicationSyncSelectRace_fixed.cfg
```

## Reproduce linked BLMOVE chain intermediate-residue race

This model captures the chain tested by:

- `linked_blmove_chain_is_observable_without_intermediate_residue`
- waiter-1 blocks on `BLMOVE list1 -> list2`
- waiter-2 blocks on `BLMOVE list2 -> list3`
- producer `RPUSH list1 foo`, then inspector immediately checks list states

Current model knobs:

- `ObserveImmediatelyAfterAck = TRUE` to force post-ACK inspection on the next step
- `WaitForBlockingDrainBeforeAck = TRUE` for conservative guard (both waiters done)
- `WaitForTailDrainBeforeAck = TRUE` for minimal guard in this 2-hop chain (waiter-2 done)

Bug config (expected to fail with counterexample):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/LinkedBlmoveChainResidue.tla formal/tla/specs/LinkedBlmoveChainResidue_bug.cfg
```

Minimal fixed config (expected to pass):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/LinkedBlmoveChainResidue.tla formal/tla/specs/LinkedBlmoveChainResidue_minimal.cfg
```

Conservative fixed config (expected to pass):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/LinkedBlmoveChainResidue.tla formal/tla/specs/LinkedBlmoveChainResidue_fixed.cfg
```

## Reproduce `unit/multi` blocking-timeout-in-`EXEC` hang

This model maps `tests/unit/multi.tcl` test `Blocking commands ignores the timeout` directly:

- `XGROUP CREATE s{t} g $ MKSTREAM`
- `MULTI`
- queue `BLPOP`, `BRPOP`, `BRPOPLPUSH`, `BLMOVE`, `BZPOPMIN`, `BZPOPMAX`, `XREAD`, `XREADGROUP`
- `EXEC` expecting 8 empty replies (no blocking wait)

Bug config (expected to fail with a hang counterexample):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/MultiExecBlockingTimeout.tla formal/tla/specs/MultiExecBlockingTimeout_bug.cfg
```

Fixed config (expected to pass):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/MultiExecBlockingTimeout.tla formal/tla/specs/MultiExecBlockingTimeout_fixed.cfg
```

## Reproduce `unit/other` PIPELINING stresser timeout

This model maps `tests/unit/other.tcl` test `PIPELINING stresser (also a regression for the old epoll bug)` directly from runtime code flow:

- `connection_handler::handle_connection`:
  - read-and-drain from socket into `receive_buffer`
  - inline parse path (`InvalidArrayPrefix` -> `parse_inline_frame`)
  - per-frame execute and buffered response write
- `request_lifecycle::handle_set` / `handle_get`:
  - `SET` success/error response and key visibility
  - `GET` bulk/null response shape

- batch `SET key:i value` + `GET key:i` writes
- single `flush`
- read loop behavior from Tcl as-is:
  - first `gets` line is consumed but not validated (SET reply line)
  - second `gets` line is treated as `GET` length line
  - if that line is `$-1`, `read -1` blocks (hang/timeout)

Nominal config (expected to pass):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/PipelineStresserTimeout.tla formal/tla/specs/PipelineStresserTimeout_nominal.cfg
```

Reproduction config (expected to fail with counterexample):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/PipelineStresserTimeout.tla formal/tla/specs/PipelineStresserTimeout_repro.cfg
```

## Reproduce `CLIENT PAUSE`/`CLIENT UNBLOCK` resume race

This model captures the pause boundary race for blocking commands:

- a blocking command is pause-gated (`CLIENT PAUSE`)
- `CLIENT UNPAUSE` happens
- `CLIENT UNBLOCK` is requested before the blocking loop fully resumes
- bug variant clears pending unblock on resume and can lose the request

Bug config (expected to fail with counterexample):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/ClientPauseUnblockRace.tla formal/tla/specs/ClientPauseUnblockRace_bug.cfg
```

Fixed config (expected to pass):

```bash
./tools/tla/run_tlc.sh formal/tla/specs/ClientPauseUnblockRace.tla formal/tla/specs/ClientPauseUnblockRace_fixed.cfg
```

## Add new model

1. Add `<Name>.tla` and `<Name>.cfg` under `formal/tla/specs/`.
2. Keep constants small first (minutes, not hours).
3. Run model with `./tools/tla/run_tlc.sh`.
4. Capture failing traces and convert to deterministic Rust tests.

## Counterexample -> Rust Test Template

When TLC reports an error, convert the trace into a deterministic test plan first.

### 1) Capture the trace metadata

- Spec: `<spec>.tla`
- Config: `<spec>.cfg`
- TLC metadir from script output
- Exact failing action sequence from `The behavior up to this point`

### 2) Build a trace worksheet

Use this table format and fill one row per state transition:

| Step | TLC action label | Trigger client/task | Expected state delta |
|---|---|---|---|
| S1 | `Block(c1,k1)` | client-1 | waiter queue becomes `[c1]` |
| S2 | `Block(c2,k1)` | client-2 | waiter queue becomes `[c1,c2]` |
| S3 | `WakeHead(k1)` | writer task | client-1 wakes first (FIFO) |

### 3) Translate to deterministic Rust test

- One async task per trace actor (client-1/client-2/writer).
- Use explicit synchronization (`Barrier`, `oneshot`, `mpsc`) to enforce S1->S2->S3 order.
- Avoid random sleeps as ordering control.
- Assert both intermediate and final observable states.
- Add bounded timeouts so hangs fail fast.

Template skeleton:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tla_trace_blocking_fifo_case_001() {
    // Arrange: start server + clients, create barriers/channels.

    // S1: client-1 issues blocking op on key k1.
    // S2: client-2 issues blocking op on key k1.
    // S3: writer pushes one element to k1.

    // Assert: client-1 receives wakeup/result before client-2.
    // Assert: queue/state observations match the trace worksheet.
}
```

### 4) Record provenance in the test

Add a short comment near the test with:

- Spec path
- Config path
- TLC run date
- Trace step IDs consumed by this test
