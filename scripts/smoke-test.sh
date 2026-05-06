#!/usr/bin/env bash
# smoke-test.sh — quick local sanity check: dry-run discovery on 20 records,
# then print pipeline status. No API calls, no VPS required.
#
# Usage:
#   scripts/smoke-test.sh --input input/nc_retry_300k.jsonl
#   scripts/smoke-test.sh --input input/nc_retry_300k.jsonl --limit 50

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
VENV="$ROOT/.venv/bin/python"

if [[ ! -x "$VENV" ]]; then
  echo "error: .venv not found. Run: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi

INPUT=""
LIMIT=20

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input) INPUT="$2"; shift ;;
    --limit) LIMIT="$2"; shift ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
  shift
done

if [[ -z "$INPUT" ]]; then
  echo "error: --input <path.jsonl> is required" >&2
  exit 1
fi

if [[ ! -f "$INPUT" ]]; then
  echo "error: input file not found: $INPUT" >&2
  exit 1
fi

RUN_NAME="smoke_$(date +%Y%m%d_%H%M%S)"
DB_PATH="$ROOT/output/$RUN_NAME/pipeline.db"

echo "[smoke] running dry-run producer on $LIMIT records from $INPUT"
"$VENV" -m pipeline run \
  --input "$INPUT" \
  --limit "$LIMIT" \
  --dry-run \
  --producer-only \
  --name "$RUN_NAME"

echo ""
echo "[smoke] pipeline status:"
"$VENV" -m pipeline status --db "$DB_PATH"
echo "[smoke] done — output at output/$RUN_NAME/"
