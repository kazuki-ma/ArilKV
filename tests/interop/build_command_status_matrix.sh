#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
GARNET_RS_ROOT="${WORKSPACE_ROOT}"
COMPAT_DIR="${WORKSPACE_ROOT}/docs/compatibility"

REDIS_IMAGE="${REDIS_IMAGE:-redis:7.2-alpine}"
REDIS_REPO_ROOT="${REDIS_REPO_ROOT:-/Users/kazuki-matsuda/dev/src/github.com/redis/redis}"
REDIS_COMMAND_SOURCE="${REDIS_COMMAND_SOURCE:-auto}"
REDIS_COMMANDS_DIR="${REDIS_COMMANDS_DIR:-${REDIS_REPO_ROOT}/src/commands}"
REDIS_DOCKER_START_TIMEOUT_SECONDS="${REDIS_DOCKER_START_TIMEOUT_SECONDS:-120}"
REDIS_PORT="${REDIS_PORT:-6397}"
GARNET_PORT="${GARNET_PORT:-6398}"
GARNET_SERVER_CMD="${GARNET_SERVER_CMD:-rustup run 1.95.0 cargo run -p garnet-server --release}"
RUST_BACKTRACE="${RUST_BACKTRACE:-1}"

OUTPUT_CSV="${OUTPUT_CSV:-${COMPAT_DIR}/redis-command-status.csv}"
OUTPUT_SUMMARY="${OUTPUT_SUMMARY:-${COMPAT_DIR}/redis-command-status-summary.md}"
TMP_DIR="$(mktemp -d)"
REDIS_SOURCE_NOTE=""

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required command: $1" >&2
        exit 2
    fi
}

wait_for_ping() {
    local host="$1"
    local port="$2"
    for _ in $(seq 1 1200); do
        if redis-cli -h "${host}" -p "${port}" PING >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

extract_redis_commands_from_local_json() {
    local commands_dir="$1"
    local output_file="$2"
    local file_count
    local raw_file="${TMP_DIR}/redis-commands-raw.txt"

    if [[ ! -d "${commands_dir}" ]]; then
        return 1
    fi

    file_count="$(find "${commands_dir}" -maxdepth 1 -type f -name '*.json' | wc -l | tr -d ' ')"
    if [[ "${file_count}" -eq 0 ]]; then
        return 1
    fi

    : > "${raw_file}"
    while IFS= read -r -d '' json_file; do
        jq -r '
            to_entries[]
            | if (.value | type) != "object" then
                empty
              elif .value.container? != null then
                .value.container
              else
                .key
              end
        ' "${json_file}" >> "${raw_file}"
    done < <(find "${commands_dir}" -maxdepth 1 -type f -name '*.json' -print0)

    tr '[:lower:]' '[:upper:]' < "${raw_file}" | sort -u > "${output_file}"
    [[ -s "${output_file}" ]]
}

extract_redis_commands_from_docker() {
    local output_file="$1"
    local docker_run_output

    require_cmd docker

    if command -v timeout >/dev/null 2>&1; then
        docker_run_output="$(timeout "${REDIS_DOCKER_START_TIMEOUT_SECONDS}s" docker run -d --rm -p "${REDIS_PORT}:6379" "${REDIS_IMAGE}")"
    else
        docker_run_output="$(docker run -d --rm -p "${REDIS_PORT}:6379" "${REDIS_IMAGE}")"
    fi
    REDIS_CID="${docker_run_output}"

    if ! wait_for_ping 127.0.0.1 "${REDIS_PORT}"; then
        echo "redis failed to start on ${REDIS_PORT}" >&2
        return 1
    fi

    redis-cli -h 127.0.0.1 -p "${REDIS_PORT}" -2 --json COMMAND \
        | jq -r 'if (.[0] | type) == "array" then .[][0] else .[] end' \
        | tr '[:lower:]' '[:upper:]' \
        | sort -u > "${output_file}"
}

build_redis_command_list() {
    local output_file="$1"

    case "${REDIS_COMMAND_SOURCE}" in
        auto)
            if extract_redis_commands_from_local_json "${REDIS_COMMANDS_DIR}" "${output_file}"; then
                REDIS_SOURCE_NOTE="local-json:${REDIS_COMMANDS_DIR}"
                return 0
            fi
            echo "warning: local redis command json not available; falling back to docker baseline" >&2
            extract_redis_commands_from_docker "${output_file}"
            REDIS_SOURCE_NOTE="docker:${REDIS_IMAGE}"
            ;;
        local-json)
            if ! extract_redis_commands_from_local_json "${REDIS_COMMANDS_DIR}" "${output_file}"; then
                echo "failed to build redis command list from local json: ${REDIS_COMMANDS_DIR}" >&2
                return 1
            fi
            REDIS_SOURCE_NOTE="local-json:${REDIS_COMMANDS_DIR}"
            ;;
        docker)
            extract_redis_commands_from_docker "${output_file}"
            REDIS_SOURCE_NOTE="docker:${REDIS_IMAGE}"
            ;;
        *)
            echo "invalid REDIS_COMMAND_SOURCE: ${REDIS_COMMAND_SOURCE} (expected: auto|local-json|docker)" >&2
            return 1
            ;;
    esac
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

require_cmd redis-cli
require_cmd jq
require_cmd awk
require_cmd sort
require_cmd cargo

mkdir -p "${COMPAT_DIR}"

if ! build_redis_command_list "${TMP_DIR}/redis-commands.txt"; then
    echo "failed to build redis command baseline" >&2
    exit 1
fi

(
    cd "${GARNET_RS_ROOT}"
    GARNET_BIND_ADDR="127.0.0.1:${GARNET_PORT}" RUST_BACKTRACE="${RUST_BACKTRACE}" bash -lc "${GARNET_SERVER_CMD}"
) >"${TMP_DIR}/garnet.log" 2>&1 &
GARNET_PID="$!"
if ! wait_for_ping 127.0.0.1 "${GARNET_PORT}"; then
    echo "garnet failed to start on ${GARNET_PORT}" >&2
    exit 1
fi

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

- Redis baseline source: \`${REDIS_SOURCE_NOTE}\`
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

- \`SUPPORTED_DECLARED\`: command appears in both Redis baseline and Garnet \`COMMAND\` output.
- \`NOT_IMPLEMENTED\`: command appears in Redis baseline but not in Garnet.
- \`GARNET_EXTENSION\`: command appears in Garnet \`COMMAND\` output but not in Redis baseline.

## Update Command

\`\`\`bash
cd tests/interop
./build_command_status_matrix.sh
\`\`\`
EOF

echo "wrote ${OUTPUT_CSV}"
echo "wrote ${OUTPUT_SUMMARY}"
