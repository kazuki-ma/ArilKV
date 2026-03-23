#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export REDIS_RUNTEXT_MODE="${REDIS_RUNTEXT_MODE:-full}"
export RUNTEXT_ENABLE_LARGE_MEMORY="${RUNTEXT_ENABLE_LARGE_MEMORY:-1}"
export RUNTEXT_TIMEOUT_SECONDS="${RUNTEXT_TIMEOUT_SECONDS:-900}"
export RUNTEXT_WALL_TIMEOUT_SECONDS="${RUNTEXT_WALL_TIMEOUT_SECONDS:-14400}"
export GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES="${GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES:-524288}"

# By default run only the upstream large-memory-tagged cases in this dedicated
# lane. Callers can override RUNTEXT_EXTRA_ARGS to narrow or widen the probe.
export RUNTEXT_EXTRA_ARGS="${RUNTEXT_EXTRA_ARGS:---tags large-memory}"

exec "${SCRIPT_DIR}/redis_runtest_external_subset.sh"
