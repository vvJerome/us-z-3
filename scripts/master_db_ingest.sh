#!/usr/bin/env bash
# master_db_ingest.sh — ingest a completed run into the master verification DB
#
# Usage:
#   bash scripts/master_db_ingest.sh --run-dir output/run_20260625
#   bash scripts/master_db_ingest.sh --run-dir output/run_20260625 --db master.db --summary
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"
PYTHON="${ROOT}/.venv/bin/python"
exec "$PYTHON" -m pipeline.ops.master_db "$@"
