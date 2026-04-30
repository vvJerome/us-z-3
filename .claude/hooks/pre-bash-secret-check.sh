#!/usr/bin/env bash
# pre-bash-secret-check.sh
# Runs before every Bash tool call.
# Blocks commands that would expose API keys or write to .env directly.
# Exit 2 = block the tool call. Exit 0 = allow.

set -euo pipefail

INPUT="$(cat)"
COMMAND=$(echo "$INPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('command',''))" 2>/dev/null || echo "")

# Block patterns that look like key exfiltration or accidental secret echo
BLOCKED_PATTERNS=(
  "echo.*API_KEY"
  "cat.*\.env"
  "curl.*SERPER_API_KEY"
  "curl.*ZUHAL_API_KEY"
  "printenv.*KEY"
)

for pattern in "${BLOCKED_PATTERNS[@]}"; do
  if echo "$COMMAND" | grep -qiE "$pattern"; then
    echo "BLOCKED: command matches secret-exposure pattern '$pattern'" >&2
    exit 2
  fi
done

# Warn (but allow) if piping to an external host and a key var name is nearby
if echo "$COMMAND" | grep -qE "curl|wget|nc " && echo "$COMMAND" | grep -qE "KEY|SECRET|TOKEN"; then
  echo "WARN: command sends data to external host and references a secret variable — review before proceeding" >&2
fi

exit 0
