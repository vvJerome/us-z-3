#!/usr/bin/env bash
# stop-audit.sh — runs when Claude finishes responding.
# End-of-turn gate: full test suite + clean-up of transient artifacts, then a short
# audit of uncommitted changes. Advisory (exit 0); surfaces failures for the next
# turn. Tests run here (not per-edit) to avoid paying ~12s on every Edit.

set -uo pipefail

echo "=== Session audit: $(date -u '+%Y-%m-%d %H:%M:%S UTC') ===" >&2

# --- Full test suite (only when Python changed this session) ---
PY_DIRTY=$(git diff --name-only 2>/dev/null | grep -E '\.py$' || true)
PY_STAGED=$(git diff --cached --name-only 2>/dev/null | grep -E '\.py$' || true)
if [[ -n "$PY_DIRTY$PY_STAGED" && -x ".venv/bin/python" ]]; then
  echo "Running test suite (Python changed this session)…" >&2
  if ! .venv/bin/python -m pytest tests/ -q >/tmp/stop-pytest.log 2>&1; then
    echo "❌ TESTS FAILING — do not consider the task done:" >&2
    tail -15 /tmp/stop-pytest.log >&2
  else
    echo "✓ $(grep -Eo '[0-9]+ passed' /tmp/stop-pytest.log | tail -1)" >&2
  fi
fi

# --- Clean up transient artifacts ---
rm -f .coverage .coverage.* 2>/dev/null || true
find . -type d -name __pycache__ -not -path "./.venv/*" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name .pytest_cache -not -path "./.venv/*" -exec rm -rf {} + 2>/dev/null || true

# --- Uncommitted-change audit ---
CHANGED=$(git diff --name-only 2>/dev/null)
STAGED=$(git diff --cached --name-only 2>/dev/null)
UNTRACKED=$(git ls-files --others --exclude-standard 2>/dev/null | grep -v "output/\|runs/\|\.venv/\|__pycache__" || true)

[[ -n "$CHANGED" ]] && { echo "Modified (unstaged):" >&2; echo "$CHANGED" | sed 's/^/  /' >&2; }
[[ -n "$STAGED" ]] && { echo "Staged:" >&2; echo "$STAGED" | sed 's/^/  /' >&2; }
[[ -n "$UNTRACKED" ]] && { echo "Untracked:" >&2; echo "$UNTRACKED" | sed 's/^/  /' >&2; }
[[ -z "$CHANGED$STAGED$UNTRACKED" ]] && echo "  No uncommitted changes." >&2

# --- Flag any tracked CSVs (must never be committed) ---
CSV_TRACKED=$(git ls-files '*.csv' 2>/dev/null || true)
[[ -n "$CSV_TRACKED" ]] && { echo "⚠ CSV files are tracked (should not be committed):" >&2; echo "$CSV_TRACKED" | sed 's/^/  /' >&2; }

exit 0
