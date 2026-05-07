#!/usr/bin/env bash
# run_parallel.sh — split a JSONL input into N parallel pipeline workers, then merge outputs.
#
# Usage:
#   bash scripts/run_parallel.sh --input FILE --name NAME [--workers N] [--max-cost USD]
#                                [--limit N] [--dry-run] [--racknerd-direct] [--no-racknerd]
#
# Each worker gets its own output dir (output/NAME_w0, output/NAME_w1, …).
# On completion, valid_emails.csv from all workers is merged into output/NAME_merged.csv.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

PYTHON="${ROOT}/.venv/bin/python"

# ── Argument parsing ──────────────────────────────────────────────────────────
INPUT=""
NAME=""
WORKERS=4
MAX_COST=""
LIMIT=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)      INPUT="$2";    shift 2 ;;
    --name)       NAME="$2";     shift 2 ;;
    --workers)    WORKERS="$2";  shift 2 ;;
    --max-cost)   MAX_COST="$2"; shift 2 ;;
    --limit)      LIMIT="$2";    shift 2 ;;
    --dry-run)         EXTRA_ARGS+=("--dry-run");         shift ;;
    --racknerd-direct) EXTRA_ARGS+=("--racknerd-direct"); shift ;;
    --no-racknerd)     EXTRA_ARGS+=("--no-racknerd");     shift ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

[[ -z "$INPUT" ]] && { echo "ERROR: --input is required" >&2; exit 1; }
[[ -z "$NAME"  ]] && { echo "ERROR: --name is required"  >&2; exit 1; }
[[ ! -f "$INPUT" ]] && { echo "ERROR: input file not found: $INPUT" >&2; exit 1; }

# ── Work out chunk sizes ──────────────────────────────────────────────────────
TOTAL=$(wc -l < "$INPUT")
[[ -n "$LIMIT" ]] && TOTAL=$((LIMIT < TOTAL ? LIMIT : TOTAL))
CHUNK=$(( (TOTAL + WORKERS - 1) / WORKERS ))

echo "[run_parallel] input=$INPUT  total=$TOTAL  workers=$WORKERS  chunk=$CHUNK"
[[ -n "$MAX_COST" ]] && echo "[run_parallel] max-cost=$MAX_COST per worker"

# ── Launch workers ────────────────────────────────────────────────────────────
PIDS=()
WORKER_NAMES=()

for i in $(seq 0 $((WORKERS - 1))); do
  OFFSET=$(( i * CHUNK ))
  [[ $OFFSET -ge $TOTAL ]] && break   # fewer records than workers

  WORKER_NAME="${NAME}_w${i}"
  WORKER_NAMES+=("$WORKER_NAME")
  LOG="output/${WORKER_NAME}_nohup.log"

  CMD=("$PYTHON" -m pipeline run
    -i "$INPUT"
    --start-offset "$OFFSET"
    --limit "$CHUNK"
    --name "$WORKER_NAME"
    --ignore-checkpoint
    "${EXTRA_ARGS[@]}"
  )
  [[ -n "$MAX_COST" ]] && CMD+=(--max-cost "$MAX_COST")

  echo "[run_parallel] starting worker $i → offset=$OFFSET limit=$CHUNK name=$WORKER_NAME"
  nohup "${CMD[@]}" > "$LOG" 2>&1 &
  PIDS+=($!)
done

echo "[run_parallel] launched ${#PIDS[@]} workers: ${PIDS[*]}"

# ── Wait for all workers ──────────────────────────────────────────────────────
FAILED=0
for idx in "${!PIDS[@]}"; do
  pid="${PIDS[$idx]}"
  if wait "$pid"; then
    echo "[run_parallel] worker $idx (pid=$pid) done"
  else
    echo "[run_parallel] worker $idx (pid=$pid) exited with error" >&2
    FAILED=$(( FAILED + 1 ))
  fi
done

# ── Merge valid_emails.csv ────────────────────────────────────────────────────
MERGED="output/${NAME}_merged.csv"
HEADER_WRITTEN=0
TOTAL_ROWS=0

for WORKER_NAME in "${WORKER_NAMES[@]}"; do
  CSV="output/${WORKER_NAME}/valid_emails.csv"
  [[ ! -f "$CSV" ]] && { echo "[run_parallel] no CSV for $WORKER_NAME, skipping"; continue; }

  ROWS=$(( $(wc -l < "$CSV") - 1 ))
  TOTAL_ROWS=$(( TOTAL_ROWS + ROWS ))

  if [[ $HEADER_WRITTEN -eq 0 ]]; then
    cat "$CSV" > "$MERGED"
    HEADER_WRITTEN=1
  else
    tail -n +2 "$CSV" >> "$MERGED"
  fi
done

echo ""
echo "[run_parallel] ── Complete ──────────────────────────────────"
echo "[run_parallel] merged CSV : $MERGED"
echo "[run_parallel] total rows : $TOTAL_ROWS"
[[ $FAILED -gt 0 ]] && echo "[run_parallel] WARNING: $FAILED worker(s) exited with errors"
