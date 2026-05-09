.PHONY: help fmt fmt-check check test test-server clippy

RUSTFMT_NIGHTLY ?= nightly-2026-02-24
RUST_TOOLCHAIN ?= 1.95.0
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
	rustup run $(RUSTFMT_NIGHTLY) cargo fmt --all

fmt-check:
	rustup run $(RUSTFMT_NIGHTLY) cargo fmt --all -- --check

check:
	rustup run $(RUST_TOOLCHAIN) cargo check --workspace

test:
	rustup run $(RUST_TOOLCHAIN) cargo test --workspace

test-server:
	rustup run $(RUST_TOOLCHAIN) cargo test -p garnet-server

clippy:
	rustup run $(RUST_TOOLCHAIN) cargo clippy --workspace --all-targets -- -D warnings
