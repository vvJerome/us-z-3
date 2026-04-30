#!/usr/bin/env bash
# pre-edit-guard.sh
# Runs before every Edit or Write tool call.
# Blocks edits to .env, warns on credentials files.
# Exit 2 = block. Exit 0 = allow.

set -euo pipefail

INPUT="$(cat)"
FILE_PATH=$(echo "$INPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('file_path',''))" 2>/dev/null || echo "")

# Never allow direct edits to the live .env file
if [[ "$FILE_PATH" == *"/.env" ]] || [[ "$FILE_PATH" == ".env" ]]; then
  echo "BLOCKED: direct edits to .env are not allowed — edit .env.example and apply manually" >&2
  exit 2
fi

# Warn on sensitive paths (allow but flag)
SENSITIVE_PATTERNS=("*secret*" "*/credentials*" "*private_key*" "*.pem" "*.p12")
for pat in "${SENSITIVE_PATTERNS[@]}"; do
  if [[ "$FILE_PATH" == $pat ]]; then
    echo "WARN: editing a potentially sensitive file: $FILE_PATH" >&2
    break
  fi
done

exit 0
