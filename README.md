# Garnet-rs

Garnet-rs is an independent Rust implementation inspired by Microsoft Garnet. It is a Redis-compatible server experiment focused on owner-thread execution, replication, persistence, formal modeling, and evidence-driven performance work.

This repository root is the Rust workspace. The original .NET Garnet source, website, CI, and sample assets are intentionally not carried here.

## Current Performance Snapshot

Owner-inline execution is now the default Garnet server policy. In the latest
Docker/Linux single-server-core comparison on this machine, Garnet outperformed
Dragonfly and Valkey on simple string `SET`/`GET` workloads:

| Target | SET ops/sec | SET p99 | GET ops/sec | GET p99 |
|---|---:|---:|---:|---:|
| Garnet | 177,999.76 | 0.055 ms | 218,181.50 | 0.047 ms |
| Dragonfly | 150,864.72 | 0.055 ms | 176,304.03 | 0.055 ms |
| Valkey | 110,537.36 | 0.063 ms | 111,818.36 | 0.055 ms |

Run artifact:
`benches/results/linux-perf-diff-docker-20260510-042719/`.

See [PERFORMANCE.md](PERFORMANCE.md) for methodology, commands, caveats, and
historical context.

Pipeline scaling snapshot for Garnet on the same single-server-core shape:
`PIPELINE=16` reached SET `526,033.39` / GET `735,647.07` ops/sec, and
`PIPELINE=64` reached SET `778,973.94` / GET `1,345,967.48` ops/sec. In this
Docker Desktop run, GET crossed 1M ops/sec; SET did not.

When comparing with high-throughput Redis-compatible products, also compare the
enabled compatibility surface, not only `SET`/`GET` throughput. ArilKV keeps
Redis-style ACL checks, ACL audit logging, slow log tracking, command stats,
client tracking, replication wait semantics, and persistence-related command
surface in the same codebase; some benchmark-focused products document only a
smaller subset or a different equivalent. See
[Benchmarking Notes](https://kazuki-ma.github.io/ArilKV/benchmarking-notes.html)
for the public comparison checklist. The researched comparison claims are kept
in `docs/benchmarking/benchmarking-surface.yaml`, with references attached to
each claim.

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
