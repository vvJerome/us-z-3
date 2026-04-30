#!/usr/bin/env bash
# post-edit-lint.sh
# Runs after every Edit or Write tool call on Python files.
# Checks for unused imports and obvious type errors using pyflakes.
# Non-zero exit is advisory only (exit 0 always so it doesn't block).

set -uo pipefail

INPUT="$(cat)"
FILE_PATH=$(echo "$INPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('file_path',''))" 2>/dev/null || echo "")

# Only lint Python files
if [[ "$FILE_PATH" != *.py ]]; then
  exit 0
fi

if [[ ! -f "$FILE_PATH" ]]; then
  exit 0
fi

# Use .venv pyflakes if available, fall back to system
PYFLAKES=".venv/bin/pyflakes"
if [[ ! -x "$PYFLAKES" ]]; then
  PYFLAKES="$(which pyflakes 2>/dev/null || echo '')"
fi

if [[ -n "$PYFLAKES" ]]; then
  OUTPUT=$("$PYFLAKES" "$FILE_PATH" 2>&1 || true)
  if [[ -n "$OUTPUT" ]]; then
    echo "pyflakes: $FILE_PATH"
    echo "$OUTPUT"
  fi
fi

exit 0
