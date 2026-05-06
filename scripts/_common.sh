#!/usr/bin/env bash
# Sourced by all us-z-3 scripts. Loads .env and exposes SSH/RSYNC/SCP helpers.
#
# Auth priority (for deploy scripts):
#   1. VPS_PASS set  -> password auth via sshpass (requires: brew install sshpass)
#   2. VPS_PASS unset -> SSH key auth via RACKNERD_SSH_KEY
#
# Deploy user: VPS_DEPLOY_USER (defaults to RACKNERD_SSH_USER if not set)
# SMTP tunnel user: RACKNERD_SSH_USER (used by pipeline config, separate from deploy)

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[us-z-3] ERROR: $ENV_FILE not found. Copy .env.example and fill in keys." >&2
  exit 1
fi

# shellcheck disable=SC1090
set -a; source "$ENV_FILE"; set +a

: "${RACKNERD_HOST:?RACKNERD_HOST not set in .env}"
: "${RACKNERD_SSH_USER:?RACKNERD_SSH_USER not set in .env}"

VPS_IP="${RACKNERD_HOST}"
VPS_USER="${VPS_DEPLOY_USER:-${RACKNERD_SSH_USER}}"
VPS_REMOTE_DIR="${VPS_REMOTE_DIR:-/root/us-z-3}"

SSH_OPTS=(
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
  -o ServerAliveInterval=30
  -o ServerAliveCountMax=4
)

if [[ -n "${VPS_PASS:-}" ]]; then
  if ! command -v sshpass >/dev/null 2>&1; then
    echo "[us-z-3] ERROR: VPS_PASS is set but sshpass is not installed." >&2
    echo "  Install with: brew install sshpass" >&2
    exit 1
  fi
  vps_ssh()   { sshpass -p "$VPS_PASS" ssh "${SSH_OPTS[@]}" "${VPS_USER}@${VPS_IP}" "$@"; }
  vps_rsync() { sshpass -p "$VPS_PASS" rsync -e "ssh ${SSH_OPTS[*]}" "$@"; }
  vps_scp()   { sshpass -p "$VPS_PASS" scp "${SSH_OPTS[@]}" "$@"; }
else
  : "${RACKNERD_SSH_KEY:?RACKNERD_SSH_KEY not set in .env (required when VPS_PASS is unset)}"
  VPS_KEY="${RACKNERD_SSH_KEY/#\~/$HOME}"
  SSH_OPTS+=(-i "$VPS_KEY")
  vps_ssh()   { ssh "${SSH_OPTS[@]}" "${VPS_USER}@${VPS_IP}" "$@"; }
  vps_rsync() { rsync -e "ssh ${SSH_OPTS[*]}" "$@"; }
  vps_scp()   { scp "${SSH_OPTS[@]}" "$@"; }
fi

export PROJECT_ROOT VPS_IP VPS_USER VPS_REMOTE_DIR
export -f vps_ssh vps_rsync vps_scp
