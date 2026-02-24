#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
GARNET_RS_ROOT="${WORKSPACE_ROOT}/garnet-rs"
COMPAT_DIR="${WORKSPACE_ROOT}/docs/compatibility"

REDIS_IMAGE="${REDIS_IMAGE:-redis:7.2-alpine}"
REDIS_PORT="${REDIS_PORT:-6397}"
GARNET_PORT="${GARNET_PORT:-6398}"
GARNET_SERVER_CMD="${GARNET_SERVER_CMD:-cargo run -p garnet-server --release}"

OUTPUT_CSV="${OUTPUT_CSV:-${COMPAT_DIR}/redis-command-status.csv}"
OUTPUT_SUMMARY="${OUTPUT_SUMMARY:-${COMPAT_DIR}/redis-command-status-summary.md}"
TMP_DIR="$(mktemp -d)"

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required command: $1" >&2
        exit 2
    fi
}

wait_for_ping() {
    local host="$1"
    local port="$2"
    for _ in $(seq 1 200); do
        if redis-cli -h "${host}" -p "${port}" PING >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

cleanup() {
    if [[ -n "${GARNET_PID:-}" ]]; then
        kill "${GARNET_PID}" >/dev/null 2>&1 || true
        wait "${GARNET_PID}" >/dev/null 2>&1 || true
    fi
    if [[ -n "${REDIS_CID:-}" ]]; then
        docker kill "${REDIS_CID}" >/dev/null 2>&1 || true
    fi
    rm -rf "${TMP_DIR}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

require_cmd docker
require_cmd redis-cli
require_cmd jq
require_cmd awk
require_cmd sort
require_cmd comm
require_cmd cargo

mkdir -p "${COMPAT_DIR}"

REDIS_CID="$(docker run -d --rm -p "${REDIS_PORT}:6379" "${REDIS_IMAGE}")"
if ! wait_for_ping 127.0.0.1 "${REDIS_PORT}"; then
    echo "redis failed to start on ${REDIS_PORT}" >&2
    exit 1
fi

(
    cd "${GARNET_RS_ROOT}"
    GARNET_BIND_ADDR="127.0.0.1:${GARNET_PORT}" bash -lc "${GARNET_SERVER_CMD}"
) >"${TMP_DIR}/garnet.log" 2>&1 &
GARNET_PID="$!"
if ! wait_for_ping 127.0.0.1 "${GARNET_PORT}"; then
    echo "garnet failed to start on ${GARNET_PORT}" >&2
    exit 1
fi

redis-cli -h 127.0.0.1 -p "${REDIS_PORT}" -2 --json COMMAND \
    | jq -r 'if (.[0] | type) == "array" then .[][0] else .[] end' \
    | tr '[:lower:]' '[:upper:]' \
    | sort -u > "${TMP_DIR}/redis-commands.txt"

redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" -2 --json COMMAND \
    | jq -r 'if (.[0] | type) == "array" then .[][0] else .[] end' \
    | tr '[:lower:]' '[:upper:]' \
    | sort -u > "${TMP_DIR}/garnet-commands.txt"

sort -u "${TMP_DIR}/redis-commands.txt" "${TMP_DIR}/garnet-commands.txt" \
    > "${TMP_DIR}/all-commands.txt"

{
    echo "command,status,redis_present,garnet_present,notes"
    while IFS= read -r command; do
        redis_present=0
        garnet_present=0
        if grep -Fxq "${command}" "${TMP_DIR}/redis-commands.txt"; then
            redis_present=1
        fi
        if grep -Fxq "${command}" "${TMP_DIR}/garnet-commands.txt"; then
            garnet_present=1
        fi
        status="UNKNOWN"
        if [[ "${redis_present}" == "1" && "${garnet_present}" == "1" ]]; then
            status="SUPPORTED_DECLARED"
        elif [[ "${redis_present}" == "1" && "${garnet_present}" == "0" ]]; then
            status="NOT_IMPLEMENTED"
        elif [[ "${redis_present}" == "0" && "${garnet_present}" == "1" ]]; then
            status="GARNET_EXTENSION"
        fi
        echo "${command},${status},${redis_present},${garnet_present},"
    done < "${TMP_DIR}/all-commands.txt"
} > "${OUTPUT_CSV}"

redis_total="$(wc -l < "${TMP_DIR}/redis-commands.txt" | tr -d ' ')"
garnet_total="$(wc -l < "${TMP_DIR}/garnet-commands.txt" | tr -d ' ')"
supported_declared="$(awk -F, 'NR>1 && $2=="SUPPORTED_DECLARED" {c++} END {print c+0}' "${OUTPUT_CSV}")"
not_implemented="$(awk -F, 'NR>1 && $2=="NOT_IMPLEMENTED" {c++} END {print c+0}' "${OUTPUT_CSV}")"
extensions="$(awk -F, 'NR>1 && $2=="GARNET_EXTENSION" {c++} END {print c+0}' "${OUTPUT_CSV}")"
coverage_pct="$(awk -v s="${supported_declared}" -v r="${redis_total}" 'BEGIN { if (r==0) print "0.00"; else printf "%.2f", (s*100.0)/r }')"

cat > "${OUTPUT_SUMMARY}" <<EOF
# Redis Command Status Summary

- Source Redis image: \`${REDIS_IMAGE}\`
- Redis command count: \`${redis_total}\`
- Garnet declared command count: \`${garnet_total}\`
- Supported (declared): \`${supported_declared}\`
- Not implemented: \`${not_implemented}\`
- Garnet extensions: \`${extensions}\`
- Coverage vs Redis baseline: \`${coverage_pct}%\`

## Files

- Matrix CSV: \`docs/compatibility/redis-command-status.csv\`
- This summary: \`docs/compatibility/redis-command-status-summary.md\`

## Status Semantics

- \`SUPPORTED_DECLARED\`: command appears in both Redis and Garnet \`COMMAND\` output.
- \`NOT_IMPLEMENTED\`: command appears in Redis \`COMMAND\` output but not in Garnet.
- \`GARNET_EXTENSION\`: command appears in Garnet \`COMMAND\` output but not in Redis.

## Update Command

\`\`\`bash
cd garnet-rs/tests/interop
./build_command_status_matrix.sh
\`\`\`
EOF

echo "wrote ${OUTPUT_CSV}"
echo "wrote ${OUTPUT_SUMMARY}"
