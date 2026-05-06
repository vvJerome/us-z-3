#!/usr/bin/env bash
# Sourced by all us-z-3 scripts. Loads .env and exposes SSH/RSYNC/SCP helpers
# that use SSH key auth via RACKNERD_SSH_KEY.

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
: "${RACKNERD_SSH_KEY:?RACKNERD_SSH_KEY not set in .env}"

VPS_IP="${RACKNERD_HOST}"
VPS_USER="${RACKNERD_SSH_USER}"
VPS_KEY="${RACKNERD_SSH_KEY/#\~/$HOME}"
VPS_REMOTE_DIR="${VPS_REMOTE_DIR:-/root/us-z-3}"

SSH_OPTS=(
  -i "$VPS_KEY"
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
  -o ServerAliveInterval=30
  -o ServerAliveCountMax=4
)

vps_ssh() {
  ssh "${SSH_OPTS[@]}" "${VPS_USER}@${VPS_IP}" "$@"
}

vps_rsync() {
  rsync -e "ssh ${SSH_OPTS[*]}" "$@"
}

vps_scp() {
  scp "${SSH_OPTS[@]}" "$@"
}

export PROJECT_ROOT VPS_IP VPS_USER VPS_KEY VPS_REMOTE_DIR
export -f vps_ssh vps_rsync vps_scp
