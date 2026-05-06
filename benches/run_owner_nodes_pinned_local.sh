#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

OWNER_NODES="${OWNER_NODES:-4}"
BIND_ADDR="${BIND_ADDR:-127.0.0.1:6389}"
READ_BUFFER_SIZE="${READ_BUFFER_SIZE:-}"
OWNER_THREAD_CPU_SET="${OWNER_THREAD_CPU_SET:-0,1,2,3}"
ENABLE_APP_PINNING="${ENABLE_APP_PINNING:-1}"
USE_TASKSET="${USE_TASKSET:-0}"

export GARNET_BIND_ADDR="$BIND_ADDR"
export GARNET_OWNER_NODE_COUNT="$OWNER_NODES"

if [[ "$ENABLE_APP_PINNING" == "1" ]]; then
  export GARNET_OWNER_THREAD_PINNING=1
  export GARNET_OWNER_THREAD_CPU_SET="$OWNER_THREAD_CPU_SET"
fi

if [[ -n "$READ_BUFFER_SIZE" ]]; then
  export GARNET_READ_BUFFER_SIZE="$READ_BUFFER_SIZE"
fi

cd "$REPO_ROOT"

if [[ "$USE_TASKSET" == "1" ]]; then
  if command -v taskset >/dev/null 2>&1; then
    echo "[run_owner_nodes_pinned_local] launching with taskset cpus=$OWNER_THREAD_CPU_SET"
    exec taskset -c "$OWNER_THREAD_CPU_SET" cargo run -p garnet-server --release
  fi
  echo "[run_owner_nodes_pinned_local] taskset requested but not found; falling back to normal launch" >&2
fi

echo "[run_owner_nodes_pinned_local] launching garnet-server with app-side owner-thread pinning"
exec cargo run -p garnet-server --release
