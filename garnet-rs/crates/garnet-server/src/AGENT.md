# AGENT.md (`garnet-server/src`)

## First Step

Read `garnet-rs/crates/garnet-server/src/README.md` before editing files in this directory.

## Scope

This file applies to:

- `garnet-rs/crates/garnet-server/src/*.rs`
- `garnet-rs/crates/garnet-server/src/request_lifecycle/**`

## Expectations

- Keep command metadata (`command_spec.rs`) and runtime dispatch (`request_lifecycle.rs`) in sync.
- Add/refresh unit tests for every command behavior change.
- Run the validation flow documented in the local `README.md`.
