#!/usr/bin/env bash
# Sourced by all us-z-3 scripts. Loads .env and exposes SSH/RSYNC/SCP helpers
# that use sshpass + VPS_PASS.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[us-z-3] ERROR: $ENV_FILE not found. Copy .env.example and fill in keys." >&2
  exit 1
fi

# shellcheck disable=SC1090
set -a; source "$ENV_FILE"; set +a

: "${VPS_IP:?VPS_IP not set in .env}"
: "${VPS_USER:?VPS_USER not set in .env}"
: "${VPS_PASS:?VPS_PASS not set in .env}"

if ! command -v sshpass >/dev/null 2>&1; then
  echo "[us-z-3] ERROR: sshpass not installed. On macOS: brew install hudochenkov/sshpass/sshpass" >&2
  exit 1
fi

VPS_REMOTE_DIR="${VPS_REMOTE_DIR:-/root/us-z-3}"

SSH_OPTS=(
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
  -o ServerAliveInterval=30
  -o ServerAliveCountMax=4
)

vps_ssh() {
  sshpass -p "$VPS_PASS" ssh "${SSH_OPTS[@]}" "${VPS_USER}@${VPS_IP}" "$@"
}

vps_rsync() {
  sshpass -p "$VPS_PASS" rsync -e "ssh ${SSH_OPTS[*]}" "$@"
}

vps_scp() {
  sshpass -p "$VPS_PASS" scp "${SSH_OPTS[@]}" "$@"
}

export PROJECT_ROOT VPS_IP VPS_USER VPS_PASS VPS_REMOTE_DIR
export -f vps_ssh vps_rsync vps_scp
