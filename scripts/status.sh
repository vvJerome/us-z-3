#!/usr/bin/env bash
# status.sh — summary of VPS state: tmux sessions, latest run manifest,
# v2 record counts, disk/mem.  `--watch N` re-runs every N seconds.
#
# Usage:
#   scripts/status.sh
#   scripts/status.sh --watch 10

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

WATCH=0
if [[ "${1:-}" == "--watch" ]]; then
  WATCH="${2:-10}"
fi

query_once() {
  vps_ssh bash -s <<'REMOTE'
set -euo pipefail
cd /root/us-z-3 2>/dev/null || { echo "[vps] /root/us-z-3 missing"; exit 0; }

echo "=== tmux sessions ==="
tmux ls 2>/dev/null || echo "(no sessions)"

echo
echo "=== latest run ==="
LATEST=$(ls -1dt runs/*/ 2>/dev/null | head -1 || true)
if [[ -n "$LATEST" ]]; then
  echo "dir: $LATEST"
  if [[ -f "${LATEST}manifest.json" ]]; then
    cat "${LATEST}manifest.json"
  else
    echo "(no manifest.json yet)"
  fi
  echo
  echo "--- v2 status ---"
  if [[ -f "${LATEST}v2/pipeline.db" ]]; then
    python3 <<PY
import sqlite3
c = sqlite3.connect("${LATEST}v2/pipeline.db")
for state, n in c.execute("SELECT record_state, COUNT(*) FROM records GROUP BY record_state ORDER BY 2 DESC"):
    print(f"  record_state={state:<24} {n:>6}")
for zs, n in c.execute("SELECT zuhal_status, COUNT(*) FROM records WHERE zuhal_status IS NOT NULL GROUP BY zuhal_status ORDER BY 2 DESC"):
    print(f"  zuhal={zs:<26} {n:>6}")
PY
  else
    echo "(v2/pipeline.db not yet present)"
  fi
else
  echo "(no runs yet)"
fi

echo
echo "=== system ==="
free -h | awk 'NR==1 || NR==2'
df -h / | awk 'NR==1 || NR==2'
uptime
REMOTE
}

if [[ "$WATCH" != "0" ]]; then
  while true; do
    clear
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] us-z-3 status @ ${VPS_IP}"
    echo
    query_once
    sleep "$WATCH"
  done
else
  query_once
fi
