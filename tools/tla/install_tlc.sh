#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST_DIR="${TLA_TOOLS_DIR:-${SCRIPT_DIR}/bin}"
JAR_PATH="${DEST_DIR}/tla2tools.jar"
VERSION="${TLA_TOOLS_VERSION:-v1.7.4}"
USE_LATEST=0
FORCE_DOWNLOAD=0

for arg in "$@"; do
  case "${arg}" in
    --latest)
      USE_LATEST=1
      ;;
    --force)
      FORCE_DOWNLOAD=1
      ;;
    *)
      echo "Unknown option: ${arg}" >&2
      echo "Usage: $0 [--latest] [--force]" >&2
      exit 1
      ;;
  esac
done

if ! command -v java >/dev/null 2>&1; then
  echo "java is required but was not found in PATH." >&2
  echo "Install a JDK first (Java 17+ recommended)." >&2
  exit 1
fi

if [[ "${USE_LATEST}" -eq 1 ]]; then
  URL="https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar"
else
  URL="https://github.com/tlaplus/tlaplus/releases/download/${VERSION}/tla2tools.jar"
fi

mkdir -p "${DEST_DIR}"

if [[ -f "${JAR_PATH}" && "${FORCE_DOWNLOAD}" -eq 0 ]]; then
  echo "Using existing: ${JAR_PATH}"
else
  tmp="${JAR_PATH}.tmp"
  echo "Downloading ${URL}"
  curl -fL --retry 3 --retry-delay 1 "${URL}" -o "${tmp}"
  mv "${tmp}" "${JAR_PATH}"
fi

echo "Validating TLC runtime..."
help_output="$(java -cp "${JAR_PATH}" tlc2.TLC -help 2>&1 || true)"
if ! printf '%s\n' "${help_output}" | grep -Eq "TLC|Version"; then
  echo "TLC validation failed. Output was:" >&2
  printf '%s\n' "${help_output}" >&2
  exit 1
fi

echo "Installed: ${JAR_PATH}"
echo "Try: ./tools/tla/run_tlc.sh formal/tla/specs/OwnerThreadKV.tla formal/tla/specs/OwnerThreadKV.cfg"
