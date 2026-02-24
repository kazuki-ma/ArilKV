# Compatibility TODO Loop Log (2026-02)

This log records each compatibility TODO loop executed using
`docs/compatibility/compatibility-todo-execution-playbook.md`.

## Template

### Loop N

- TODO:
- Scope:
- Step 0 (tests first):
- Step 1 (target behavior):
- Step 2 (target implementation):
- Step 3 (current implementation findings):
- Step 4 (refactor/cleanup decisions):
- Step 5 (Garnet original references):
- Step 6 (Valkey references):
- Step 7 (essence integrated + follow-up TODOs):
- Step 8 (validation):
- Step 9 (TLA+):
- Result:
- Commit:

## Loops

### Loop 1

- TODO: `11.149`
- Scope: Rebaseline full compatibility run under compatibility profile to remove infra-noise failures.
- Step 0 (tests first): N/A (measurement/documentation loop; no behavior change).
- Step 1 (target behavior):
  - Full compatibility report should reflect semantic failures, not capacity/scripting-profile artifacts.
- Step 2 (target implementation):
  - Re-run compatibility report with `GARNET_SCRIPTING_ENABLED=1` and raised `GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES`.
- Step 3 (current implementation findings):
  - Previous baseline had large noise from:
    - `ERR storage capacity exceeded (increase max in-memory pages)`
    - `ERR scripting is disabled in this server`
- Step 4 (refactor/cleanup decisions):
  - Keep runtime code unchanged.
  - Update docs and TODO statuses using the new baseline.
- Step 5 (Garnet original references): N/A (measurement loop).
- Step 6 (Valkey references): N/A (measurement loop).
- Step 7 (essence integrated + follow-up TODOs):
  - Feed rebaselined counts into next semantic TODOs (`11.150`, `11.151`, `11.152`).
- Step 8 (validation):
  - `cd garnet-rs/tests/interop && REDIS_REPO_ROOT=<redis-repo-root> GARNET_SCRIPTING_ENABLED=1 GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES=4096 RUNTEXT_TIMEOUT_SECONDS=120 ./build_compatibility_report.sh`
- Step 9 (TLA+): Not required (no concurrency design change).
- Result:
  - Rebaseline run generated full-profile summary with reduced infra-noise surface:
    - `ok=98`, `err=97`, `ignore=36`
  - Result artifact:
    - `garnet-rs/tests/interop/results/compatibility-report-20260224-233319/redis-runtest-external-full/summary.csv`
- Commit: Pending.

### Loop 2

- TODO: `11.150`
- Scope: Implement Redis-compatible `EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT` condition options (`NX/XX/GT/LT`) and re-measure full runtest.
- Step 0 (tests first):
  - Added/used unit tests with upstream references in `request_lifecycle/tests.rs`:
    - `expire_condition_options_follow_redis_matrix`
    - `expire_condition_option_errors_follow_redis_messages`
    - `expire_condition_options_on_missing_and_negative_follow_redis_behavior`
- Step 1 (target behavior):
  - Accept optional `NX|XX|GT|LT` for all `EXPIRE*` variants.
  - Match Redis conflict rules and unsupported-option error text.
  - Preserve missing-key/negative-time semantics and GT/LT behavior against non-volatile keys.
- Step 2 (target implementation):
  - Shared option parser + shared condition predicate.
  - One code path for relative/absolute expiry target calculation.
  - Keep existing metadata/update path for string/object keys.
- Step 3 (current implementation findings):
  - Existing code required exact arity=3 and rejected all condition options.
- Step 4 (refactor/cleanup decisions):
  - Added compact helpers in `string_commands.rs`:
    - `parse_expire_condition_options`
    - `should_apply_expire_condition`
    - `compute_relative_expire_target_unix_millis`
    - `compute_absolute_expire_target_unix_millis`
- Step 5 (Garnet original references):
  - `libs/server/Resp/KeyAdminCommands.cs` (`NetworkEXPIRE` option parse + compatibility checks).
- Step 6 (Valkey references):
  - `src/expire.c`:
    - `parseExtendedExpireArgumentsOrReply`
    - `expireGenericCommand` (GT/LT on `current_expire=-1`, negative expiry handling).
- Step 7 (essence integrated + follow-up TODOs):
  - Integrated conflict and unsupported-option messages at RESP level for compatibility.
  - Follow-up: remaining `expire.tcl` failures now focus on non-option parity (large int/propagation/stats).
- Step 8 (validation):
  - Unit:
    - `cargo test -p garnet-server expire_condition_ -- --nocapture`
    - `cargo test -p garnet-server`
  - Full compatibility profile:
    - `cd garnet-rs/tests/interop && REDIS_REPO_ROOT=<redis-repo-root> GARNET_SCRIPTING_ENABLED=1 GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES=4096 RUNTEXT_TIMEOUT_SECONDS=120 ./build_compatibility_report.sh`
    - Latest summary: `ok=119`, `err=76`, `ignore=36`
    - `expire.tcl` failed tests: `26` (down from previous `48`)
- Step 9 (TLA+): Not required (no new concurrency ordering introduced).
- Result:
  - Option-surface parity for `EXPIRE*` closed at unit level.
  - Full-run error count reduced (`97 -> 76`) under compatibility profile baseline.
- Commit: Pending.
