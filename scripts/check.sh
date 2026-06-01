#!/usr/bin/env bash
# check.sh — snapshot of the active pipeline run on VPS #3.
#
# Usage:
#   scripts/check.sh                  # current il_20260531 run
#   scripts/check.sh --run il_20260531
#   scripts/check.sh --lines 30       # more log tail lines (default: 20)
#   watch -n 30 scripts/check.sh      # auto-refresh every 30s

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RUN_NAME="il_20260531"
LOG_LINES=20

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run)   RUN_NAME="$2";  shift 2 ;;
    --lines) LOG_LINES="$2"; shift 2 ;;
    -h|--help) sed -n '1,10p' "$0"; exit 0 ;;
    *) echo "[check] unknown arg: $1" >&2; exit 2 ;;
  esac
done

SSH="ssh -i ~/.ssh/racknerd_egress -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR root@23.238.100.4"
DB="/root/us-z-3/output/${RUN_NAME}/pipeline.db"
LOG="/root/us-z-3/logs/pipeline_${RUN_NAME}.log"

$SSH bash -s <<REMOTE
set -euo pipefail

echo "=== tmux sessions ==="
tmux ls 2>/dev/null || echo "(none)"

echo ""
echo "=== record states [${RUN_NAME}] ==="
if [[ -f "${DB}" ]]; then
  sqlite3 "${DB}" \
    "SELECT record_state, COUNT(*) AS n FROM records GROUP BY record_state ORDER BY n DESC;"
  echo ""
  echo "=== cost ==="
  sqlite3 "${DB}" \
    "SELECT service, COUNT(*) AS calls FROM cost_events GROUP BY service;" \
    2>/dev/null || echo "(no cost events)"
  echo ""
  echo "=== errors (last 5 VALIDATION_FAILED) ==="
  sqlite3 "${DB}" \
    "SELECT unique_id, candidate_emails FROM records
     WHERE record_state='VALIDATION_FAILED' ORDER BY updated_at DESC LIMIT 5;" \
    2>/dev/null || true
else
  echo "(pipeline.db not found yet)"
fi

echo ""
echo "=== last ${LOG_LINES} log lines ==="
if [[ -f "${LOG}" ]]; then
  tail -${LOG_LINES} "${LOG}"
else
  echo "(log not found)"
fi

echo ""
echo "=== disk ==="
df -h / | tail -1
REMOTE
