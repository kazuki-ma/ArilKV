# Garnet-rs

Garnet-rs is an independent Rust implementation inspired by Microsoft Garnet. It is a Redis-compatible server experiment focused on owner-thread execution, replication, persistence, formal modeling, and evidence-driven performance work.

This repository root is the Rust workspace. The original .NET Garnet source, website, CI, and sample assets are intentionally not carried here.

## Status

The project is experimental and under active development. Compatibility and performance-sensitive changes should be backed by targeted regression tests and benchmark evidence.

## Build And Test

Use the root `Makefile` as the default entrypoint:

```sh
make fmt-check
make test-server
make test
make clippy
```

The stable Rust toolchain is pinned by `rust-toolchain.toml`. Formatting uses the pinned nightly rustfmt version in `Makefile`.

## Layout

- `crates/`: Rust workspace crates.
- `benches/`: local benchmark and profiling harnesses.
- `tests/`: integration and Redis interop test harnesses.
- `formal/`: TLA+ models for modeled concurrency and command paths.
- `tools/tla/`: local TLC runner utilities.
- `third_party/`: vendored third-party source required by the Rust workspace.

## License

This project is licensed under the MIT License. See `LICENSE`.

Vendored third-party source carries its own license file under `third_party/`.
