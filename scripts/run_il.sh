#!/usr/bin/env bash
# run_il.sh — deploy + start a fresh Illinois pipeline run on VPS #3.
#
# Usage:
#   scripts/run_il.sh --run-name il_20260531
#   scripts/run_il.sh --run-name il_20260531 --max-cost 550 --deploy
#   scripts/run_il.sh --run-name il_20260531 --limit 1000   # test slice
#
# Flags:
#   --run-name NAME       Output goes to output/<NAME>/ on VPS (required)
#   --max-cost USD        Stop when cumulative cost reaches this ceiling (default: 550)
#   --limit N             Process only the first N records (optional)
#   --deploy              Rsync + install deps before starting (recommended on first run)
#   --input PATH          JSONL path relative to project root (default: input/il_scp_business_agent_joined_filtered_clean_names.jsonl)
#   -h|--help             Print usage

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

# Defaults
INPUT_REL="input/il_scp_business_agent_joined_filtered_clean_names.jsonl"
RUN_NAME=""
MAX_COST="550"
LIMIT=""
DEPLOY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-name)   RUN_NAME="$2";  shift 2 ;;
    --max-cost)   MAX_COST="$2";  shift 2 ;;
    --limit)      LIMIT="$2";     shift 2 ;;
    --deploy)     DEPLOY=1;       shift   ;;
    --input)      INPUT_REL="$2"; shift 2 ;;
    -h|--help)    sed -n '1,18p' "$0"; exit 0 ;;
    *) echo "[run_il] unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$RUN_NAME" ]] || { echo "[run_il] --run-name required" >&2; exit 2; }

INPUT_LOCAL="${PROJECT_ROOT}/${INPUT_REL}"
[[ -f "$INPUT_LOCAL" ]] || { echo "[run_il] input not found: $INPUT_LOCAL" >&2; exit 2; }

REMOTE_INPUT="${VPS_REMOTE_DIR}/${INPUT_REL}"

# Optional deploy step
if [[ "$DEPLOY" -eq 1 ]]; then
  echo "[run_il] deploying code to VPS..."
  bash "$SCRIPT_DIR/deploy.sh"
fi

# Upload input file if not already on VPS
echo "[run_il] checking input file on VPS..."
if vps_ssh "test -f ${REMOTE_INPUT}"; then
  echo "[run_il] input already present on VPS: ${REMOTE_INPUT}"
else
  echo "[run_il] uploading $(basename "$INPUT_LOCAL") to VPS..."
  vps_ssh "mkdir -p $(dirname "${REMOTE_INPUT}")"
  vps_rsync -az --progress "$INPUT_LOCAL" "${VPS_USER}@${VPS_IP}:${REMOTE_INPUT}"
  echo "[run_il] upload done"
fi

# Build pipeline command
# --racknerd-direct: pipeline runs ON the egress VPS so no SOCKS5 tunnel needed
PIPELINE_CMD="python -m pipeline run \
  -i ${REMOTE_INPUT} \
  --name ${RUN_NAME} \
  --max-cost ${MAX_COST} \
  --chunk-size 100 \
  --dispatch-concurrency 20 \
  --racknerd-direct"

if [[ -n "$LIMIT" ]]; then
  PIPELINE_CMD="${PIPELINE_CMD} --limit ${LIMIT}"
fi

# Launch in tmux on VPS
vps_ssh bash -s <<REMOTE
set -euo pipefail
cd ${VPS_REMOTE_DIR}

if tmux has-session -t il_run 2>/dev/null; then
  echo "[vps] il_run tmux session already exists — attach or kill it first"
  echo "[vps]   attach: ssh -i ~/.ssh/racknerd_egress root@${VPS_IP} tmux attach -t il_run"
  echo "[vps]   kill:   ssh -i ~/.ssh/racknerd_egress root@${VPS_IP} tmux kill-session -t il_run"
  exit 1
fi

mkdir -p logs

tmux new-session -d -s il_run -c ${VPS_REMOTE_DIR} "\
  source ${VPS_REMOTE_DIR}/.venv/bin/activate && \
  ${PIPELINE_CMD} \
  2>&1 | tee ${VPS_REMOTE_DIR}/logs/pipeline_${RUN_NAME}.log"

echo "[vps] pipeline launched in tmux session: il_run"
echo "[vps]   attach:  ssh -i ~/.ssh/racknerd_egress root@${VPS_IP} 'tmux attach -t il_run'"
echo "[vps]   logs:    ssh -i ~/.ssh/racknerd_egress root@${VPS_IP} 'tail -f ${VPS_REMOTE_DIR}/logs/pipeline_${RUN_NAME}.log'"
echo "[vps]   status:  python -m pipeline status --db ${VPS_REMOTE_DIR}/output/${RUN_NAME}/pipeline.db --watch 10"
REMOTE
