#!/usr/bin/env bash
# stop.sh — kill the orchestrator tmux session on the VPS.
# Usage:
#   scripts/stop.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

vps_ssh bash -s <<REMOTE
set -euo pipefail

if tmux has-session -t orchestrator 2>/dev/null; then
  tmux kill-session -t orchestrator
  echo "[vps] killed orchestrator"
else
  echo "[vps] orchestrator not running"
fi
REMOTE
