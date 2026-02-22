# AGENT.md (`tests/interop`)

## First Step

Read `garnet-rs/tests/interop/README.md` before running or editing scripts in this directory.

## Expectations

- Keep script usage and output paths documented in `README.md`.
- When command surface changes, run and commit outputs from:
  - `redis_runtest_external_subset.sh`
  - `build_command_status_matrix.sh`
- Store notable run result directories under `tests/interop/results/` and reference them from iteration notes.
