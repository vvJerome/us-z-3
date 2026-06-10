#!/usr/bin/env bash
# check.sh — snapshot of the active pipeline run on VPS #3.
#             Also updates UST Runs - Illinois.csv with the latest counts.
#
# Usage:
#   scripts/check.sh                  # current il_20260531 run
#   scripts/check.sh --run il_20260531
#   scripts/check.sh --lines 30       # more log tail lines (default: 20)
#   scripts/check.sh --no-sheet       # skip CSV update
#   watch -n 30 scripts/check.sh      # auto-refresh every 30s

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RUN_NAME="il_20260531"
LOG_LINES=20
UPDATE_SHEET=1
SHEET_PATH="${SCRIPT_DIR}/../UST Runs - Illinois.csv"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run)       RUN_NAME="$2";  shift 2 ;;
    --lines)     LOG_LINES="$2"; shift 2 ;;
    --no-sheet)  UPDATE_SHEET=0; shift ;;
    -h|--help)   sed -n '1,11p' "$0"; exit 0 ;;
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

# --- update UST Runs - Illinois.csv ---
if [[ "${UPDATE_SHEET}" -eq 1 ]]; then
  STATS_FILE=$(mktemp)
  ssh -i ~/.ssh/racknerd_egress \
      -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=/dev/null \
      -o LogLevel=ERROR \
      root@23.238.100.4 bash <<REMOTE_STATS > "${STATS_FILE}" 2>/dev/null || true
sqlite3 "${DB}" <<'SQL'
SELECT 'valid='            || COUNT(*) FROM records WHERE record_state='VALIDATED' AND final_verdict='valid';
SELECT 'catch_all='        || COUNT(*) FROM records WHERE record_state='VALIDATED' AND final_verdict IN ('catch_all','catch-all');
SELECT 'DISCOVERED='       || COUNT(*) FROM records WHERE record_state='DISCOVERED';
SELECT 'NEEDS_ZUHAL='      || COUNT(*) FROM records WHERE record_state='NEEDS_ZUHAL';
SELECT 'VALIDATION_FAILED='|| COUNT(*) FROM records WHERE record_state='VALIDATION_FAILED';
SELECT 'VALIDATED='        || COUNT(*) FROM records WHERE record_state='VALIDATED';
SELECT 'DISCOVERY_FAILED=' || COUNT(*) FROM records WHERE record_state='DISCOVERY_FAILED';
SELECT 'ZUHAL_VALIDATING=' || COUNT(*) FROM records WHERE record_state='ZUHAL_VALIDATING';
SELECT 'VALIDATING='       || COUNT(*) FROM records WHERE record_state='VALIDATING';
SELECT 'TOTAL='            || COUNT(*) FROM records;
SQL
REMOTE_STATS

  if [[ -s "${STATS_FILE}" ]]; then
    "${SCRIPT_DIR}/../.venv/bin/python" - "${SHEET_PATH}" "${STATS_FILE}" <<'PYEOF'
import sys, re

sheet      = sys.argv[1]
stats_file = sys.argv[2]

stats = {}
with open(stats_file) as f:
    for line in f:
        line = line.strip()
        if "=" in line:
            k, v = line.split("=", 1)
            stats[k.strip()] = int(v.strip())

valid     = stats.get("valid", 0)
catch_all = stats.get("catch_all", 0)
confirmed = valid + catch_all
total_in  = stats.get("TOTAL", 484240)
pct       = f"{confirmed / total_in * 100:.2f}%" if total_in else "0%"

def fmt(n): return f'"{n:,.2f}"'

with open(sheet) as f:
    lines = f.readlines()

out = []
in_zuhal_section = False
for line in lines:
    if line.strip() == "Zuhal,,,,":
        in_zuhal_section = True
    elif line.strip() == "Zerobounce,,,,":
        in_zuhal_section = False
    if line.startswith("Jerome (full run),") and in_zuhal_section:
        line = f'Jerome (full run),{fmt(total_in)},{fmt(confirmed)},{fmt(valid)},{fmt(catch_all)}\n'
    elif line.startswith("Total (in progress),"):
        line = f'Total (in progress),{fmt(total_in)},{fmt(confirmed)},{fmt(valid)},{fmt(catch_all)}\n'
    elif line.startswith("% (in progress),"):
        line = f'% (in progress),{pct},,,\n'
    elif line.startswith("DISCOVERED,"):
        line = f'DISCOVERED,{fmt(stats.get("DISCOVERED", 0))},,,\n'
    elif line.startswith("NEEDS_ZUHAL,"):
        line = f'NEEDS_ZUHAL,{fmt(stats.get("NEEDS_ZUHAL", 0))},,,\n'
    elif line.startswith("VALIDATION_FAILED,"):
        line = f'VALIDATION_FAILED,{fmt(stats.get("VALIDATION_FAILED", 0))},,,\n'
    elif line.startswith("VALIDATED,") and ",,," in line:
        line = f'VALIDATED,{fmt(stats.get("VALIDATED", 0))},,,\n'
    elif line.startswith("DISCOVERY_FAILED,"):
        line = f'DISCOVERY_FAILED,{fmt(stats.get("DISCOVERY_FAILED", 0))},,,\n'
    elif line.startswith("ZUHAL_VALIDATING,"):
        line = f'ZUHAL_VALIDATING,{fmt(stats.get("ZUHAL_VALIDATING", 0))},,,\n'
    elif line.startswith("VALIDATING,") and ",,," in line:
        line = f'VALIDATING,{fmt(stats.get("VALIDATING", 0))},,,\n'
    elif re.match(r'^Total,"[\d,]+\.\d+",,,', line):
        line = f'Total,{fmt(total_in)},,,\n'
    out.append(line)

with open(sheet, "w") as f:
    f.writelines(out)

print(f"[check] sheet updated — {confirmed:,} confirmed ({pct}) | valid={valid:,} catch_all={catch_all:,}")
PYEOF
  fi
  rm -f "${STATS_FILE}"
fi
