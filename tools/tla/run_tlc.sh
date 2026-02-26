#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <spec.tla> [model.cfg] [extra tlc args...]" >&2
  exit 1
fi

SPEC_PATH="$1"
shift

CFG_PATH=""
if [[ $# -gt 0 && "$1" == *.cfg ]]; then
  CFG_PATH="$1"
  shift
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_PATH="${TLA_TOOLS_JAR:-${SCRIPT_DIR}/bin/tla2tools.jar}"
METADIR_ROOT="${TLA_METADIR_ROOT:-${TMPDIR:-/tmp}/garnet-tla}"
TLA_NOWARNING="${TLA_NOWARNING:-1}"

if ! command -v java >/dev/null 2>&1; then
  echo "java is required but was not found in PATH." >&2
  exit 1
fi

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "TLC jar was not found: ${JAR_PATH}" >&2
  echo "Run ./tools/tla/install_tlc.sh first." >&2
  exit 1
fi

mkdir -p "${METADIR_ROOT}"
run_id="$(date +%Y%m%d-%H%M%S)-$$"
metadir="${METADIR_ROOT}/${run_id}"
mkdir -p "${metadir}"

cmd=(java -cp "${JAR_PATH}" tlc2.TLC)
if [[ "${TLA_NOWARNING}" != "0" ]]; then
  cmd+=(-nowarning)
fi
cmd+=(-cleanup -metadir "${metadir}")

if [[ -n "${CFG_PATH}" ]]; then
  cmd+=(-config "${CFG_PATH}")
fi

cmd+=("${SPEC_PATH}")
if [[ $# -gt 0 ]]; then
  cmd+=("$@")
fi

echo "Running TLC:"
printf '  %q' "${cmd[@]}"
echo

"${cmd[@]}"

echo "TLC metadir: ${metadir}"
