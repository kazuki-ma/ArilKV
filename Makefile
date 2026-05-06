.PHONY: help fmt fmt-check check test test-server clippy

RUSTFMT_NIGHTLY ?= nightly-2026-02-24
export RUST_BACKTRACE ?= 1

help:
	@echo "Available targets:"
	@echo "  make fmt         # format Rust workspace with pinned nightly rustfmt"
	@echo "  make fmt-check   # check Rust formatting with pinned nightly rustfmt"
	@echo "  make check       # cargo check --workspace (stable toolchain from rust-toolchain.toml)"
	@echo "  make test        # cargo test --workspace"
	@echo "  make test-server # cargo test -p garnet-server"
	@echo "  make clippy      # cargo clippy --workspace --all-targets -- -D warnings"

fmt:
	cargo +$(RUSTFMT_NIGHTLY) fmt --all

fmt-check:
	cargo +$(RUSTFMT_NIGHTLY) fmt --all -- --check

check:
	cargo check --workspace

test:
	cargo test --workspace

test-server:
	cargo test -p garnet-server

clippy:
	cargo clippy --workspace --all-targets -- -D warnings
