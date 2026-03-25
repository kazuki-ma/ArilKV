## 11.597 Full Command Context Surface

Before commit: `36069ae485`  
After commit: this commit

### Goal

Finish the `CommandContext` surface rollout so every `handle_*` command path takes an explicit `ctx` parameter.

This slice expands the first hot-path-only `CommandContext` introduction from `11.596` to the full request lifecycle command surface:

- all `handle_*` command methods in `request_lifecycle.rs` and `request_lifecycle/*_commands.rs` take `ctx`
- dispatch passes `ctx` to every command handler
- internal command-to-command calls carry the same `ctx`
- string hot paths and scripting entry continue to use `ctx` for real work, while leaf commands that do not need time yet accept `_ctx`

### Surface Audit

- `surface-audit.txt` reports `dispatch_missing_ctx=0`

### Validation

- `make fmt` PASS
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture` PASS
- `make test-server` PASS with verified counts:
  - `716 passed; 0 failed; 16 ignored`
  - `26 passed`
  - `1 passed`

### Benchmark

Owner-inline single-thread, `PIPELINE=1`, run 1:

- `SET -6.25%`
- `GET +3.60%`
- `SET p99 flat`
- `GET p99 -9.20%`

Owner-inline single-thread, `PIPELINE=1`, rerun:

- `SET -1.32%`
- `GET +2.42%`
- `SET p99 +10.13%`
- `GET p99 flat`

Owner-inline multithread client, `THREADS=8 CONNS=16 PIPELINE=4`:

- `SET +6.67%`
- `GET +2.68%`
- `SET p99 -7.43%`
- `GET p99 -1.44%`

### Decision

Keep.

Rationale:

- the rollout objective is structural and is now complete for the command surface
- the pressure shape improved clearly
- the narrow `PIPELINE=1` shape was mixed/noisy rather than showing a stable cliff

### Artifacts

- `comparison-owner-inline-p1.txt`
- `comparison-owner-inline-p1-r2.txt`
- `comparison-owner-inline-p4mt.txt`
- `surface-audit.txt`
- `summary.txt`
- `diff.patch`
