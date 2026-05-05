#!/usr/bin/env bash
# reset.sh — re-queue failed records in a pipeline database.
#
# Usage:
#   scripts/reset.sh --db output/run_20260430/pipeline.db --status discovery_failed
#   scripts/reset.sh --db output/run_20260430/pipeline.db --status validation_failed --dry-run
#   scripts/reset.sh --db output/run_20260430/pipeline.db --status cost_skipped
#
# Valid --status values: discovery_failed | validation_failed | cost_skipped

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
VENV="$ROOT/.venv/bin/python"

if [[ ! -x "$VENV" ]]; then
  echo "error: .venv not found. Run: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt" >&2
  exit 1
fi

DB=""
STATUS=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)       DB="$2"; shift ;;
    --status)   STATUS="$2"; shift ;;
    --dry-run)  DRY_RUN=true ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
  shift
done

if [[ -z "$DB" ]]; then
  echo "error: --db <path> is required" >&2
  exit 1
fi

if [[ -z "$STATUS" ]]; then
  echo "error: --status <discovery_failed|validation_failed|cost_skipped> is required" >&2
  exit 1
fi

if [[ ! -f "$DB" ]]; then
  echo "error: database not found: $DB" >&2
  exit 1
fi

ARGS=("--db" "$DB" "--status" "$STATUS")
if [[ "$DRY_RUN" == "true" ]]; then
  ARGS+=("--dry-run")
  echo "[reset] dry-run: would re-queue $STATUS records in $DB"
else
  echo "[reset] re-queuing $STATUS records in $DB"
fi

"$VENV" -m pipeline reset "${ARGS[@]}"
