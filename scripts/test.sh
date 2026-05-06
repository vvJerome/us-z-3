#!/usr/bin/env bash
# test.sh — run the test suite locally.
#
# Usage:
#   scripts/test.sh               # all tests
#   scripts/test.sh --unit        # tests/unit/ only (fast, no I/O)
#   scripts/test.sh --integration # tests/integration/ only
#   scripts/test.sh --e2e         # tests/e2e/ only (subprocess-level)
#   scripts/test.sh -q            # pass -q (quiet) to pytest
#   scripts/test.sh --unit -q     # combine: unit tests, quiet mode

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
VENV="$ROOT/.venv/bin/python"

if [[ ! -x "$VENV" ]]; then
  echo "error: .venv not found. Run: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi

SUITE="tests/"
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --unit)        SUITE="tests/unit/" ;;
    --integration) SUITE="tests/integration/" ;;
    --e2e)         SUITE="tests/e2e/" ;;
    *)             EXTRA_ARGS+=("$1") ;;
  esac
  shift
done

echo "[test] suite: $SUITE"
"$VENV" -m pytest "$SUITE" "${EXTRA_ARGS[@]}"
