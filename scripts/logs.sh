#!/usr/bin/env bash
# logs.sh — tail -f logs on the VPS.  Default follows orchestrator output for
# the latest run; --which selects a different stream.
#
# Usage:
#   scripts/logs.sh                       # orchestrator (latest run)
#   scripts/logs.sh --which v2            # v2 stderr + bbops verify.log (latest run)
#   scripts/logs.sh --which tmux-orch     # attach to orchestrator tmux (Ctrl-b d to detach)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

WHICH="orchestrator"
if [[ "${1:-}" == "--which" ]]; then
  WHICH="${2:-orchestrator}"
fi

case "$WHICH" in
  orchestrator)
    vps_ssh "bash -c 'LATEST=\$(ls -1t /root/us-z-3/logs/orchestrator_*.log 2>/dev/null | head -1); \
      if [[ -z \"\$LATEST\" ]]; then echo \"(no orchestrator log yet)\"; exit 0; fi; \
      echo \"[logs] tailing \$LATEST\"; tail -n 100 -F \$LATEST'"
    ;;
  v2)
    vps_ssh "bash -c 'LATEST=\$(ls -1dt /root/us-z-3/runs/*/ 2>/dev/null | head -1); \
      if [[ -z \"\$LATEST\" ]]; then echo \"(no runs)\"; exit 0; fi; \
      F1=\"\${LATEST}v2/stderr.log\"; F2=/root/us-z-3/scraper/verify.log; \
      echo \"[logs] tailing \$F1 and \$F2\"; tail -n 100 -F \$F1 \$F2 2>/dev/null'"
    ;;
  tmux-orch)
    echo "[logs] attaching to orchestrator tmux. Detach: Ctrl-b d"
    sshpass -p "$VPS_PASS" ssh -t "${SSH_OPTS[@]}" "${VPS_USER}@${VPS_IP}" tmux attach -t orchestrator
    ;;
  *)
    echo "[logs] unknown --which: $WHICH" >&2
    echo "       valid: orchestrator | v2 | tmux-orch" >&2
    exit 2 ;;
esac
