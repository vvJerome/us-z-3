#!/usr/bin/env bash
# stop-audit.sh
# Runs when Claude finishes responding.
# Prints a short audit of files modified this session.

set -uo pipefail

echo "=== Session audit: $(date -u '+%Y-%m-%d %H:%M:%S UTC') ===" >&2

# Show uncommitted changes
CHANGED=$(git diff --name-only 2>/dev/null)
STAGED=$(git diff --cached --name-only 2>/dev/null)
UNTRACKED=$(git ls-files --others --exclude-standard 2>/dev/null | grep -v "output/\|runs/\|\.venv/\|__pycache__" || true)

if [[ -n "$CHANGED" ]]; then
  echo "Modified (unstaged):" >&2
  echo "$CHANGED" | sed 's/^/  /' >&2
fi

if [[ -n "$STAGED" ]]; then
  echo "Staged:" >&2
  echo "$STAGED" | sed 's/^/  /' >&2
fi

if [[ -n "$UNTRACKED" ]]; then
  echo "Untracked:" >&2
  echo "$UNTRACKED" | sed 's/^/  /' >&2
fi

if [[ -z "$CHANGED$STAGED$UNTRACKED" ]]; then
  echo "  No uncommitted changes." >&2
fi

exit 0
