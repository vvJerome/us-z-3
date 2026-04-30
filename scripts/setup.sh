#!/usr/bin/env bash
# setup.sh — wipe /root/us-z-3 and /root/universal-scraper on the VPS, install
# system deps, create a fresh virtualenv at /root/us-z-3/.venv. Idempotent:
# re-running cleanly recreates everything.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

echo "[setup] target: ${VPS_USER}@${VPS_IP}:${VPS_REMOTE_DIR}"
echo "[setup] this will rm -rf /root/us-z-3 and /root/universal-scraper"
echo

vps_ssh bash -s <<'REMOTE'
set -euo pipefail

echo "[vps] wiping previous deployment"
rm -rf /root/us-z-3

echo "[vps] apt update + installing system packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq \
    python3 python3-venv python3-pip python3-dev \
    tmux rsync dnsutils ca-certificates curl \
    build-essential libssl-dev libffi-dev

mkdir -p /root/us-z-3
echo "[vps] python3 version: $(python3 --version)"
echo "[vps] tmux version:    $(tmux -V)"
echo "[vps] setup complete (pre-deploy). Venv is created by deploy.sh post-rsync."
REMOTE

echo
echo "[setup] done. Next: scripts/deploy.sh"
