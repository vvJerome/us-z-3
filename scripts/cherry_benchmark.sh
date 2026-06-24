#!/usr/bin/env bash
# Autonomous Cherry fleet benchmark — provision, validate a dataset, tear down.
# All logic lives in pipeline.fleet.benchmark; this is a thin wrapper that adds a
# belt-and-suspenders teardown trap (backup to the Python finally) so no server leaks.
#
#   scripts/cherry_benchmark.sh --input input/mi_1k.jsonl --count 5 \
#       [--ground-truth path.csv] [--with-zuhal] [--dispatch-concurrency N]
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && { set -a; source .env; set +a; }

backup_teardown(){ .venv/bin/python -m pipeline.fleet teardown --yes >/dev/null 2>&1 || true; }
trap backup_teardown EXIT

.venv/bin/python -m pipeline.fleet benchmark "$@"
