#!/usr/bin/env bash
# clean.sh — delete stale pipeline outputs
#
# Usage:
#   ./scripts/clean.sh           # dry-run: show what would be deleted
#   ./scripts/clean.sh --force   # actually delete
#   ./scripts/clean.sh --days 7  # change age threshold (default: 3 days)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

FORCE=false
DAYS=3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=true ;;
    --days)  DAYS="$2"; shift ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
  shift
done

echo "Scanning for outputs older than ${DAYS} day(s)…"
echo ""

DELETED=0
SKIPPED=0

_maybe_delete() {
  local path="$1"
  if [[ "$FORCE" == "true" ]]; then
    rm -rf "$path"
    echo "  deleted: $path"
    (( DELETED++ )) || true
  else
    echo "  would delete: $path"
    (( SKIPPED++ )) || true
  fi
}

# 1. Stale run directories under runs/
if [[ -d "$ROOT/runs" ]]; then
  while IFS= read -r -d '' dir; do
    _maybe_delete "$dir"
  done < <(find "$ROOT/runs" -mindepth 1 -maxdepth 1 -type d -mtime +"$DAYS" -print0)
fi

# 2. WAL/SHM artefacts left by a crashed pipeline (pipeline.db must still exist)
while IFS= read -r -d '' shm; do
  db="${shm%-shm}"
  if [[ ! -f "$db" ]]; then
    _maybe_delete "$shm"
  fi
done < <(find "$ROOT/output" "$ROOT/runs" -name "*.db-shm" -print0 2>/dev/null)

while IFS= read -r -d '' wal; do
  db="${wal%-wal}"
  if [[ ! -f "$db" ]]; then
    _maybe_delete "$wal"
  fi
done < <(find "$ROOT/output" "$ROOT/runs" -name "*.db-wal" -print0 2>/dev/null)

# 3. Empty log directories
while IFS= read -r -d '' logdir; do
  if [[ -z "$(ls -A "$logdir" 2>/dev/null)" ]]; then
    _maybe_delete "$logdir"
  fi
done < <(find "$ROOT" -type d -name "logs" -print0 2>/dev/null)

echo ""
if [[ "$FORCE" == "true" ]]; then
  echo "Done. Deleted ${DELETED} item(s)."
else
  echo "Dry-run complete. ${SKIPPED} item(s) would be deleted."
  echo "Re-run with --force to delete."
fi
