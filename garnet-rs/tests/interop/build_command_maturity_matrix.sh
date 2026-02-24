#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPAT_DIR="${WORKSPACE_ROOT}/docs/compatibility"

STATUS_CSV="${STATUS_CSV:-${COMPAT_DIR}/redis-command-status.csv}"
IMPLEMENTATION_YAML="${IMPLEMENTATION_YAML:-${COMPAT_DIR}/command-implementation-status.yaml}"
OUTPUT_CSV="${OUTPUT_CSV:-${COMPAT_DIR}/redis-command-maturity.csv}"
OUTPUT_SUMMARY="${OUTPUT_SUMMARY:-${COMPAT_DIR}/redis-command-maturity-summary.md}"
TMP_DIR="$(mktemp -d)"

cleanup() {
    rm -rf "${TMP_DIR}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required command: $1" >&2
        exit 2
    fi
}

require_cmd awk
require_cmd sort
require_cmd comm

if [[ ! -f "${STATUS_CSV}" ]]; then
    echo "status csv not found: ${STATUS_CSV}" >&2
    exit 1
fi
if [[ ! -f "${IMPLEMENTATION_YAML}" ]]; then
    echo "implementation yaml not found: ${IMPLEMENTATION_YAML}" >&2
    exit 1
fi

IMPLEMENTATION_CSV="${TMP_DIR}/implementation.csv"

awk '
function trim(value) {
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", value);
    return value;
}

{
    line = $0;
}

line ~ /^[[:space:]]*-[[:space:]]*command:[[:space:]]*/ {
    command = line;
    sub(/^[[:space:]]*-[[:space:]]*command:[[:space:]]*/, "", command);
    command = trim(command);
    gsub(/^"|"$/, "", command);
    next;
}

line ~ /^[[:space:]]*status:[[:space:]]*/ {
    status = line;
    sub(/^[[:space:]]*status:[[:space:]]*/, "", status);
    status = trim(status);
    gsub(/^"|"$/, "", status);
    next;
}

line ~ /^[[:space:]]*comment:[[:space:]]*/ {
    comment = line;
    sub(/^[[:space:]]*comment:[[:space:]]*/, "", comment);
    comment = trim(comment);
    gsub(/^"|"$/, "", comment);
    gsub(/,/, ";", comment);
    if (command == "" || status == "") {
        printf("invalid yaml entry near comment line: %s\n", line) > "/dev/stderr";
        exit 1;
    }
    printf("%s,%s,%s\n", command, status, comment);
    command = "";
    status = "";
    comment = "";
    next;
}

END {
    if (command != "" || status != "") {
        print "unterminated yaml command entry (missing comment line)" > "/dev/stderr";
        exit 1;
    }
}
' "${IMPLEMENTATION_YAML}" > "${IMPLEMENTATION_CSV}"

awk -F, '
{
    if ($2 != "FULL" && $2 != "PARTIAL_MINIMAL" && $2 != "DISABLED") {
        printf("invalid implementation status: %s (%s)\n", $1, $2) > "/dev/stderr";
        exit 1;
    }
}
' "${IMPLEMENTATION_CSV}"

awk -F, '
{
    if (seen[$1]++) {
        printf("duplicate command entry in yaml: %s\n", $1) > "/dev/stderr";
        exit 1;
    }
}
' "${IMPLEMENTATION_CSV}"

tail -n +2 "${STATUS_CSV}" | cut -d, -f1 | sort -u > "${TMP_DIR}/declared-commands.txt"
cut -d, -f1 "${IMPLEMENTATION_CSV}" | sort -u > "${TMP_DIR}/yaml-commands.txt"

comm -23 "${TMP_DIR}/declared-commands.txt" "${TMP_DIR}/yaml-commands.txt" > "${TMP_DIR}/missing.txt"
comm -13 "${TMP_DIR}/declared-commands.txt" "${TMP_DIR}/yaml-commands.txt" > "${TMP_DIR}/extra.txt"

if [[ -s "${TMP_DIR}/missing.txt" ]]; then
    echo "commands present in status csv but missing from yaml:" >&2
    cat "${TMP_DIR}/missing.txt" >&2
    exit 1
fi

if [[ -s "${TMP_DIR}/extra.txt" ]]; then
    echo "commands present in yaml but absent from status csv:" >&2
    cat "${TMP_DIR}/extra.txt" >&2
    exit 1
fi

awk -F, -v impl_csv="${IMPLEMENTATION_CSV}" '
BEGIN {
    while ((getline line < impl_csv) > 0) {
        split(line, fields, ",");
        command = fields[1];
        implementation_status = fields[2];
        comment = fields[3];
        impl_status[command] = implementation_status;
        impl_comment[command] = comment;
    }
}

NR == 1 {
    print "command,declaration_status,redis_present,garnet_present,implementation_status,comment";
    next;
}

{
    command = $1;
    declaration_status = $2;
    redis_present = $3;
    garnet_present = $4;
    implementation_status = impl_status[command];
    comment = impl_comment[command];
    printf("%s,%s,%s,%s,%s,%s\n", command, declaration_status, redis_present, garnet_present, implementation_status, comment);
}
' "${STATUS_CSV}" > "${OUTPUT_CSV}"

supported_total="$(awk -F, 'NR>1 && $2=="SUPPORTED_DECLARED" {c++} END {print c+0}' "${OUTPUT_CSV}")"
full_total="$(awk -F, 'NR>1 && $2=="SUPPORTED_DECLARED" && $5=="FULL" {c++} END {print c+0}' "${OUTPUT_CSV}")"
partial_total="$(awk -F, 'NR>1 && $2=="SUPPORTED_DECLARED" && $5=="PARTIAL_MINIMAL" {c++} END {print c+0}' "${OUTPUT_CSV}")"
disabled_total="$(awk -F, 'NR>1 && $2=="SUPPORTED_DECLARED" && $5=="DISABLED" {c++} END {print c+0}' "${OUTPUT_CSV}")"
coverage_full_pct="$(awk -v full="${full_total}" -v total="${supported_total}" 'BEGIN { if (total==0) print "0.00"; else printf "%.2f", (full*100.0)/total }')"

{
    echo "# Redis Command Maturity Summary"
    echo
    echo "- Generated at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- Status source: \`docs/compatibility/redis-command-status.csv\`"
    echo "- Implementation source: \`docs/compatibility/command-implementation-status.yaml\`"
    echo "- Maturity matrix: \`docs/compatibility/redis-command-maturity.csv\`"
    echo
    echo "- Supported declared commands: \`${supported_total}\`"
    echo "- \`FULL\`: \`${full_total}\`"
    echo "- \`PARTIAL_MINIMAL\`: \`${partial_total}\`"
    echo "- \`DISABLED\`: \`${disabled_total}\`"
    echo "- Full implementation ratio over declared commands: \`${coverage_full_pct}%\`"
    echo
    echo "## Non-Full Commands"
    echo
    echo "| Command | Maturity | Comment |"
    echo "|---|---|---|"
    awk -F, '
    NR>1 && $2=="SUPPORTED_DECLARED" && $5!="FULL" {
        comment = ($6 == "" ? "-" : $6);
        printf("| `%s` | `%s` | %s |\n", $1, $5, comment);
    }
    ' "${OUTPUT_CSV}"
} > "${OUTPUT_SUMMARY}"

echo "wrote ${OUTPUT_CSV}"
echo "wrote ${OUTPUT_SUMMARY}"
