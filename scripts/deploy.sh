#!/usr/bin/env bash
# deploy.sh — rsync us-z-3/ → VPS:/root/us-z-3/ (excludes runtime artifacts),
# push .env, then create venv + install all three requirements.txt on the VPS.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

echo "[deploy] rsync -> ${VPS_USER}@${VPS_IP}:${VPS_REMOTE_DIR}"

vps_rsync -az --delete \
  --exclude='.venv/' \
  --exclude='.git/' \
  --exclude='__pycache__/' \
  --exclude='*.pyc' \
  --exclude='.pytest_cache/' \
  --exclude='runs/' \
  --exclude='input/' \
  --exclude='*.db' \
  --exclude='*.db-journal' \
  --exclude='*.db-wal' \
  --exclude='*.db-shm' \
  --exclude='.env' \
  --exclude='.DS_Store' \
  "${PROJECT_ROOT}/" "${VPS_USER}@${VPS_IP}:${VPS_REMOTE_DIR}/"

echo "[deploy] pushing .env"
vps_scp "${PROJECT_ROOT}/.env" "${VPS_USER}@${VPS_IP}:${VPS_REMOTE_DIR}/.env"

echo "[deploy] building venv + installing dependencies on VPS"
vps_ssh bash -s <<REMOTE
set -euo pipefail
cd ${VPS_REMOTE_DIR}

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi
source .venv/bin/activate

python -m pip install --upgrade pip wheel setuptools >/dev/null
python -m pip install -r requirements.txt

echo "[vps] dependencies installed"
python -m orchestrator --help >/dev/null && echo "[vps] orchestrator import OK"
REMOTE

echo
echo "[deploy] done. Next: scripts/start.sh --input input/records.jsonl --run-name run_01"
