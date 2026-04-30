#!/usr/bin/env bash
# start.sh — launch the orchestrator tmux session on the VPS.
#
# Sessions:
#   orchestrator   — `python -m orchestrator ...` with provided args
#
# Usage:
#   scripts/start.sh --input input/records.jsonl --run-name run_01
#   scripts/start.sh --input input/records.jsonl --run-name run_01 --limit 500
#   scripts/start.sh --input input/records.jsonl --run-name run_01 \
#                    --orchestrator-extra "--skip-preflight"

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

INPUT=""
RUN_NAME=""
EXTRA=""
LIMIT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)
      INPUT="$2"; shift 2 ;;
    --run-name)
      RUN_NAME="$2"; shift 2 ;;
    --limit)
      LIMIT="$2"; shift 2 ;;
    --orchestrator-extra)
      EXTRA="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,15p' "$0"; exit 0 ;;
    *)
      echo "[start] unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$INPUT"    ]] || { echo "[start] --input required" >&2; exit 2; }
[[ -n "$RUN_NAME" ]] || { echo "[start] --run-name required" >&2; exit 2; }

# If --limit N, slice the first N records into runs/<slug>_sliced.jsonl on the VPS
# and point orchestrator at that instead of the full file.
REMOTE_INPUT="${VPS_REMOTE_DIR}/${INPUT}"
ORCH_CMD="cd ${VPS_REMOTE_DIR} && source .venv/bin/activate && python -m orchestrator"

if [[ -n "$LIMIT" ]]; then
  vps_ssh bash -s <<REMOTE
set -euo pipefail
mkdir -p ${VPS_REMOTE_DIR}/runs
SLICE=${VPS_REMOTE_DIR}/runs/${RUN_NAME}_sliced.jsonl
head -n ${LIMIT} ${REMOTE_INPUT} > \$SLICE
echo "[vps] sliced ${LIMIT} records -> \$SLICE"
REMOTE
  REMOTE_INPUT="${VPS_REMOTE_DIR}/runs/${RUN_NAME}_sliced.jsonl"
fi

vps_ssh bash -s <<REMOTE
set -euo pipefail
cd ${VPS_REMOTE_DIR}

if tmux has-session -t orchestrator 2>/dev/null; then
  echo "[vps] orchestrator session already running — refusing to start a second one"
  echo "[vps] attach: tmux attach -t orchestrator   |   kill: scripts/stop.sh --orchestrator"
  exit 1
fi

mkdir -p logs
tmux new-session -d -s orchestrator -c ${VPS_REMOTE_DIR} "\
  source ${VPS_REMOTE_DIR}/.venv/bin/activate && \
  python -m orchestrator \
    --input ${REMOTE_INPUT} \
    --run-name ${RUN_NAME} \
    --skip-preflight \
    ${EXTRA} \
    2>&1 | tee ${VPS_REMOTE_DIR}/logs/orchestrator_${RUN_NAME}.log"

echo "[vps] orchestrator launched in tmux (session: orchestrator)"
echo "[vps]   attach:  tmux attach -t orchestrator"
echo "[vps]   logs:    tail -f ${VPS_REMOTE_DIR}/logs/orchestrator_${RUN_NAME}.log"
REMOTE
