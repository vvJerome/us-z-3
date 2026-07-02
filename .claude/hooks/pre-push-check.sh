#!/usr/bin/env bash
# pre-push-check.sh
# Runs before every Bash tool call. If the command is a `git push`, runs the full
# `make check` gate (pytest + mypy + coverage) first and blocks the push on failure.
# This is the local complement to the required `test` status check on GitHub — it
# catches a broken push before it ever leaves the machine, rather than after a
# round trip to CI. Exit 2 = block. Exit 0 = allow.

set -uo pipefail

INPUT="$(cat)"
COMMAND=$(echo "$INPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('command',''))" 2>/dev/null || echo "")

# Only gate actual `git push` invocations — word-boundary match so "git push" inside
# an unrelated string (e.g. an echo or commit message) doesn't false-positive.
echo "$COMMAND" | grep -qE '(^|[;&|]|\s)git\s+push(\s|$)' || exit 0

# Nothing to gate if this isn't the pipeline repo (no venv/Makefile) — don't block
# pushes in unrelated projects that happen to share this hook via global settings.
[[ -x ".venv/bin/python" && -f "Makefile" ]] || exit 0

echo "pre-push-check: running 'make check' before allowing git push..." >&2
if ! OUTPUT=$(make check 2>&1); then
  echo "BLOCKED: 'make check' failed — fix tests/mypy/coverage before pushing." >&2
  echo "$OUTPUT" | tail -40 >&2
  exit 2
fi

echo "pre-push-check: make check passed." >&2
exit 0
