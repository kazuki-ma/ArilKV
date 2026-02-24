# Full External Runtest: Next Actions (2026-02-24)

## Baseline Run

- Command:
  - `cd garnet-rs/tests/interop`
  - `REDIS_REPO_ROOT=<redis-repo-root> RUNTEXT_TIMEOUT_SECONDS=120 ./build_compatibility_report.sh`
- Probe mode: `full` (no `--single` / `--only` / `--tags`)
- Artifacts:
  - `garnet-rs/tests/interop/results/compatibility-report-*/redis-runtest-external-full`
- Summary:
  - `ok=174`
  - `err=412`
  - `ignore=116`

## Failure Hotspots (by test unit)

| Unit | Failures |
|---|---:|
| `tests/unit/functions.tcl` | 94 |
| `tests/unit/expire.tcl` | 48 |
| `tests/unit/introspection-2.tcl` | 47 |
| `tests/unit/multi.tcl` | 43 |
| `tests/unit/introspection.tcl` | 32 |
| `tests/unit/keyspace.tcl` | 29 |
| `tests/unit/geo.tcl` | 26 |
| `tests/unit/hyperloglog.tcl` | 18 |
| `tests/unit/info-keysizes.tcl` | 17 |
| `tests/unit/bitops.tcl` | 14 |

## Dominant Error Patterns

- `ERR storage capacity exceeded (increase max in-memory pages)`: 116
- `ERR scripting is disabled in this server`: 92
- `ERR wrong number of arguments for 'command' command`: 74
- `ERR unknown command`: 40
- `ERR wrong number of arguments for 'expire' command`: 30

Interpretation:
- Current full baseline is useful, but still mixed with infra/profile noise (`max_in_memory_pages` too small and scripting disabled by default for this run).

## Next Actions (Priority Order)

1. Rebaseline full run under compatibility profile:
   - Set `GARNET_SCRIPTING_ENABLED=1`
   - Raise `GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES` (for example `4096` or higher)
   - Re-run full report and compare `err` delta by unit.
2. Fix `EXPIRE` family parity (`tests/unit/expire.tcl`):
   - option matrix (`NX/XX/GT/LT`), big integer/overflow, negative expiry, propagation/stat behavior.
3. Expand scripting/function semantics (`tests/unit/functions.tcl`):
   - close minimal-surface gaps toward Redis test expectations.
4. Fix introspection/transaction edge behavior:
   - `COMMAND`/`CLIENT`/`INFO`/`LATENCY` arity/state semantics.
   - `MULTI`/`EXEC`/`WATCH` edge cases.
5. Address secondary modules:
   - `geo`, `hyperloglog`, `keyspace`, `bitops`, `lazyfree`, `dump`.

## Tracking

- Backlog items are recorded in `TODO_AND_STATUS.md`:
  - `11.149` to `11.152`
