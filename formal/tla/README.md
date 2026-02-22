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
