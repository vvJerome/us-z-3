#!/usr/bin/env bash
# pre-compact.sh
# Runs before Claude compacts the context window.
# Logs a snapshot of in-progress pipeline state so context can be reconstructed.

set -uo pipefail

SNAPSHOT_DIR=".claude/snapshots"
mkdir -p "$SNAPSHOT_DIR"

SNAPSHOT="$SNAPSHOT_DIR/pre-compact-$(date +%Y%m%d_%H%M%S).txt"

{
  echo "=== Pre-compact snapshot: $(date -u '+%Y-%m-%d %H:%M:%S UTC') ==="
  echo ""

  echo "--- Git status ---"
  git status --short 2>/dev/null || echo "(not a git repo)"
  echo ""

  echo "--- Recent output runs ---"
  ls -lt output/ 2>/dev/null | head -5 || echo "(no output dir)"
  echo ""

  echo "--- Latest pipeline DB stats ---"
  LATEST_DB=$(find output/ -name "pipeline.db" -newer output/ 2>/dev/null | head -1)
  if [[ -n "$LATEST_DB" ]]; then
    python3 -c "
import sqlite3
conn = sqlite3.connect('$LATEST_DB')
for row in conn.execute('SELECT record_state, COUNT(*) FROM records GROUP BY record_state'):
    print(f'  {row[0]}: {row[1]}')
conn.close()
" 2>/dev/null || echo "(could not query DB)"
  fi

  echo ""
  echo "--- Test status ---"
  .venv/bin/python -m pytest tests/ -q --tb=no 2>/dev/null | tail -3 || echo "(tests not run)"

} > "$SNAPSHOT" 2>&1

echo "Pre-compact snapshot saved: $SNAPSHOT" >&2
exit 0
