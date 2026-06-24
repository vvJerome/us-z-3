#!/usr/bin/env bash
# post-edit-lint.sh — PostToolUse(Edit|Write) on Python files.
# Fast per-file gate: flake8 + mypy + the 600-LOC modularization limit, then a
# light artifact cleanup. Advisory (always exit 0) so issues surface to the model
# without blocking the edit. The full test suite runs once per turn in stop-audit.sh.

set -uo pipefail

INPUT="$(cat)"
FILE_PATH=$(echo "$INPUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('file_path',''))" 2>/dev/null || echo "")

# Always tidy stray coverage artifacts (cheap, keeps the tree clean).
rm -f .coverage .coverage.* 2>/dev/null || true

# Only lint Python source files.
[[ "$FILE_PATH" == *.py ]] || exit 0
[[ -f "$FILE_PATH" ]] || exit 0

VENV=".venv/bin"

# 1. Style (flake8)
if [[ -x "$VENV/flake8" ]]; then
  OUT=$("$VENV/flake8" "$FILE_PATH" 2>&1 || true)
  [[ -n "$OUT" ]] && { echo "flake8 — $FILE_PATH"; echo "$OUT"; }
fi

# 2. Types (mypy) — per-file is ~0.6s and clean in this project.
if [[ -x "$VENV/mypy" ]]; then
  OUT=$("$VENV/mypy" "$FILE_PATH" 2>&1 | grep -vE "^Success|: note:" || true)
  [[ -n "$OUT" ]] && { echo "mypy — $FILE_PATH"; echo "$OUT"; }
fi

# 3. Modularization limit: no file may exceed 600 LOC — split it by responsibility.
LOC=$(wc -l < "$FILE_PATH" | tr -d ' ')
if (( LOC > 600 )); then
  echo "⚠ MODULARIZATION: $FILE_PATH is ${LOC} LOC (limit 600). Split it by responsibility (see .claude/rules/python.md)."
fi

exit 0
