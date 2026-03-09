.PHONY: help fmt fmt-check check test test-server clippy

RUST_WORKSPACE := garnet-rs
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
	cd $(RUST_WORKSPACE) && cargo +$(RUSTFMT_NIGHTLY) fmt --all

fmt-check:
	cd $(RUST_WORKSPACE) && cargo +$(RUSTFMT_NIGHTLY) fmt --all -- --check

check:
	cd $(RUST_WORKSPACE) && cargo check --workspace

test:
	cd $(RUST_WORKSPACE) && cargo test --workspace

test-server:
	cd $(RUST_WORKSPACE) && cargo test -p garnet-server

clippy:
	cd $(RUST_WORKSPACE) && cargo clippy --workspace --all-targets -- -D warnings
