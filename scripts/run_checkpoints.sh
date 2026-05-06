#!/usr/bin/env bash
# scripts/run_checkpoints.sh — Run pipeline in 100-record batches with checkpoint reviews
#
# Usage:
#   scripts/run_checkpoints.sh [--dry-run] [--max-batches N]
#
# Runs up to 1,000 records (10 × 100) against nc_1k with a $1.00 cost ceiling.
# After each batch: prints a metrics report, appends it to checkpoints.log,
# and prompts whether to continue.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"

# ── Crash handler ─────────────────────────────────────────────────────────────
_on_error() {
  local lineno="$1"
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "" >&2
  echo "══════════════════════════════════════════════════════" >&2
  echo "PIPELINE FAILURE — ${ts}  (line ${lineno})" >&2
  echo "  Database:  ${DB_PATH}" >&2
  echo "  Log:       ${LOG_FILE}" >&2
  echo "══════════════════════════════════════════════════════" >&2
  mkdir -p "$(dirname "$LOG_FILE")"
  {
    echo ""
    echo "FAILURE — ${ts}  (line ${lineno})"
    echo "  Run 'python -m pipeline status --db ${DB_PATH}' for current state."
  } >> "$LOG_FILE" 2>/dev/null || true
}
trap '_on_error $LINENO' ERR

# ── Configuration ─────────────────────────────────────────────────────────────
BATCH_SIZE=100
MAX_BATCHES=10
PIPELINE_NAME="nc_1k"
INPUT_FILE="input/nc_retry_300k.jsonl"
OUTPUT_DIR="output/${PIPELINE_NAME}"
DB_PATH="${OUTPUT_DIR}/pipeline.db"
LOG_FILE="${OUTPUT_DIR}/checkpoints.log"
MAX_COST="1.00"
DRY_RUN=false
NO_RACKNERD=false
RACKNERD_DIRECT=false

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)          DRY_RUN=true; shift ;;
    --no-racknerd)      NO_RACKNERD=true; shift ;;
    --racknerd-direct)  RACKNERD_DIRECT=true; shift ;;
    --max-batches)      MAX_BATCHES="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $(basename "$0") [--dry-run] [--no-racknerd] [--racknerd-direct] [--max-batches N]"
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Preflight checks ──────────────────────────────────────────────────────────
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found. Copy .env.example and fill in keys." >&2
  exit 1
fi

if [[ ! -f "$PROJECT_ROOT/$INPUT_FILE" ]]; then
  echo "ERROR: Input file not found: $PROJECT_ROOT/$INPUT_FILE" >&2
  exit 1
fi

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "ERROR: sqlite3 not installed — required for checkpoint reports." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

# ── Disk space check ──────────────────────────────────────────────────────────
_check_disk_space() {
  local min_mb=500
  local available_mb
  available_mb=$(df -m "$OUTPUT_DIR" | awk 'NR==2 {print $4}')
  if [[ "$available_mb" -lt "$min_mb" ]]; then
    echo "ERROR: Only ${available_mb}MB free in ${OUTPUT_DIR} — need at least ${min_mb}MB." >&2
    exit 1
  fi
}

# ── Checkpoint report helper ──────────────────────────────────────────────────
_checkpoint_report() {
  local batch_num="$1"
  local db="$2"
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  # State counts (cumulative)
  local total validated val_failed disc_failed discovered validating cost_skipped
  total=$(sqlite3       "$db" "SELECT COUNT(*) FROM records" 2>/dev/null || echo 0)
  validated=$(sqlite3   "$db" "SELECT COUNT(*) FROM records WHERE record_state='VALIDATED'" 2>/dev/null || echo 0)
  val_failed=$(sqlite3  "$db" "SELECT COUNT(*) FROM records WHERE record_state='VALIDATION_FAILED'" 2>/dev/null || echo 0)
  disc_failed=$(sqlite3 "$db" "SELECT COUNT(*) FROM records WHERE record_state='DISCOVERY_FAILED'" 2>/dev/null || echo 0)
  discovered=$(sqlite3  "$db" "SELECT COUNT(*) FROM records WHERE record_state='DISCOVERED'" 2>/dev/null || echo 0)
  validating=$(sqlite3  "$db" "SELECT COUNT(*) FROM records WHERE record_state='VALIDATING'" 2>/dev/null || echo 0)
  cost_skipped=$(sqlite3 "$db" "SELECT COUNT(*) FROM records WHERE record_state='COST_SKIPPED'" 2>/dev/null || echo 0)

  # Discovery source (cumulative)
  local dns_hits serper_hits input_hits
  dns_hits=$(sqlite3    "$db" "SELECT COUNT(*) FROM records WHERE discovery_source='dns'" 2>/dev/null || echo 0)
  serper_hits=$(sqlite3 "$db" "SELECT COUNT(*) FROM records WHERE discovery_source='serper'" 2>/dev/null || echo 0)
  input_hits=$(sqlite3  "$db" "SELECT COUNT(*) FROM records WHERE discovery_source='input'" 2>/dev/null || echo 0)

  # Derived rates
  local terminal_n disc_n pct_validated pct_dns pct_serper pct_disc
  terminal_n=$(( validated + val_failed ))
  disc_n=$(( dns_hits + serper_hits + input_hits ))

  pct_validated="n/a"
  [[ $terminal_n -gt 0 ]] && pct_validated=$(awk "BEGIN{printf \"%.1f%%\", 100*$validated/$terminal_n}")

  pct_disc="n/a"
  [[ $total -gt 0 ]] && pct_disc=$(awk "BEGIN{printf \"%.1f%%\", 100*$disc_n/$total}")

  pct_dns="n/a"
  [[ $disc_n -gt 0 ]] && pct_dns=$(awk "BEGIN{printf \"%.1f%%\", 100*$dns_hits/$disc_n}")

  pct_serper="n/a"
  [[ $disc_n -gt 0 ]] && pct_serper=$(awk "BEGIN{printf \"%.1f%%\", 100*$serper_hits/$disc_n}")

  # Stats table — written at pipeline shutdown, reflects the most recent batch
  local sp_calls sd_calls zuhal_calls rk_probes bb_probes disagreements batch_cost
  sp_calls=$(sqlite3      "$db" "SELECT COALESCE(serper_producer_calls,0)   FROM stats LIMIT 1" 2>/dev/null || echo 0)
  sd_calls=$(sqlite3      "$db" "SELECT COALESCE(serper_dispatcher_calls,0) FROM stats LIMIT 1" 2>/dev/null || echo 0)
  zuhal_calls=$(sqlite3   "$db" "SELECT COALESCE(zuhal_calls,0)             FROM stats LIMIT 1" 2>/dev/null || echo 0)
  rk_probes=$(sqlite3     "$db" "SELECT COALESCE(racknerd_probes,0)         FROM stats LIMIT 1" 2>/dev/null || echo 0)
  bb_probes=$(sqlite3     "$db" "SELECT COALESCE(bbops_probes,0)            FROM stats LIMIT 1" 2>/dev/null || echo 0)
  disagreements=$(sqlite3 "$db" "SELECT COALESCE(backend_disagreements,0)   FROM stats LIMIT 1" 2>/dev/null || echo 0)
  batch_cost=$(sqlite3    "$db" "SELECT COALESCE(estimated_cost_usd,0.0)    FROM stats LIMIT 1" 2>/dev/null || echo "0.0000")
  batch_cost=$(awk "BEGIN{printf \"%.4f\", $batch_cost}")

  # Pct of total for validated/failed/disc_failed
  local pct_v pct_f pct_df
  pct_v="";  [[ $total -gt 0 ]] && pct_v=$(awk "BEGIN{printf \" (%.1f%%)\", 100*$validated/$total}")
  pct_f="";  [[ $total -gt 0 ]] && pct_f=$(awk "BEGIN{printf \" (%.1f%%)\", 100*$val_failed/$total}")
  pct_df=""; [[ $total -gt 0 ]] && pct_df=$(awk "BEGIN{printf \" (%.1f%%)\", 100*$disc_failed/$total}")

  cat <<REPORT
══════════════════════════════════════════════════════
Checkpoint ${batch_num}/${MAX_BATCHES} — ${total} records   ${ts}
══════════════════════════════════════════════════════
Records in DB:        ${total}
  VALIDATED:          ${validated}${pct_v}
  VALIDATION_FAILED:  ${val_failed}${pct_f}
  DISCOVERY_FAILED:   ${disc_failed}${pct_df}
  DISCOVERED:         ${discovered}  (queued for dispatch)
  VALIDATING:         ${validating}
  COST_SKIPPED:       ${cost_skipped}

Discovery (cumulative):
  DNS hits:           ${dns_hits}  ${pct_dns} of discovered
  Serper hits:        ${serper_hits}  ${pct_serper} of discovered
  Input (pre-filled): ${input_hits}
  Discovery rate:     ${pct_disc}

Validation (cumulative):
  Rate:               ${pct_validated}  (${validated} / ${terminal_n} terminal)

Last batch — API calls & cost:
  Serper producer:    ${sp_calls}
  Serper dispatcher:  ${sd_calls}
  Zuhal rescues:      ${zuhal_calls}
  Racknerd SMTP:      ${rk_probes}
  bbops probes:       ${bb_probes}
  Disagreements:      ${disagreements}
  Batch cost:         \$${batch_cost}
══════════════════════════════════════════════════════
REPORT
}

# ── Main ──────────────────────────────────────────────────────────────────────
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Checkpoint runner starting"
echo "  Batches:   ${MAX_BATCHES} × ${BATCH_SIZE} = $(( MAX_BATCHES * BATCH_SIZE )) records max"
echo "  Cost cap:  \$${MAX_COST}"
echo "  Dry-run:   ${DRY_RUN}"
echo "  Output:    ${OUTPUT_DIR}"
echo

# Initialise log header if first run
if [[ ! -f "$LOG_FILE" ]]; then
  {
    echo "# Pipeline checkpoint log: ${PIPELINE_NAME}"
    echo "# Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
  } > "$LOG_FILE"
fi

for batch_num in $(seq 1 "$MAX_BATCHES"); do
  start_record=$(( (batch_num - 1) * BATCH_SIZE + 1 ))
  end_record=$(( batch_num * BATCH_SIZE ))

  _check_disk_space

  echo "──────────────────────────────────────────────────────"
  echo "Batch ${batch_num}/${MAX_BATCHES}: records ${start_record}–${end_record}"
  echo "──────────────────────────────────────────────────────"

  PYTHON="${PROJECT_ROOT}/.venv/bin/python"
  [[ ! -x "$PYTHON" ]] && PYTHON="python3"

  pipeline_cmd=(
    "$PYTHON" -m pipeline run
    --input "${INPUT_FILE}"
    --limit "${BATCH_SIZE}"
    --name  "${PIPELINE_NAME}"
    --max-cost "${MAX_COST}"
  )
  if [[ "$DRY_RUN" == "true" ]]; then
    pipeline_cmd+=(--dry-run --no-racknerd)
  elif [[ "$NO_RACKNERD" == "true" ]]; then
    pipeline_cmd+=(--no-racknerd)
  elif [[ "$RACKNERD_DIRECT" == "true" ]]; then
    pipeline_cmd+=(--racknerd-direct)
  fi

  cd "$PROJECT_ROOT"
  if ! "${pipeline_cmd[@]}"; then
    echo "ERROR: Pipeline batch ${batch_num} exited with error." >&2
    exit 1
  fi

  echo
  report=$(_checkpoint_report "$batch_num" "$DB_PATH")
  echo "$report"
  echo "$report" >> "$LOG_FILE"
  echo >> "$LOG_FILE"

  # Warn if any records were cost-skipped
  skipped=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM records WHERE record_state='COST_SKIPPED'" 2>/dev/null || echo 0)
  if [[ $skipped -gt 0 ]]; then
    echo "⚠  WARNING: ${skipped} record(s) hit the cost ceiling — consider raising --max-cost."
  fi

  # Prompt to continue (skip after final batch)
  if [[ $batch_num -lt $MAX_BATCHES ]]; then
    echo
    read -r -p "Continue to batch $(( batch_num + 1 ))/${MAX_BATCHES}? [Y/n] " response
    response="${response:-Y}"
    if [[ "${response,,}" == "n" ]]; then
      echo "Stopped at batch ${batch_num}. Re-run this script to resume."
      exit 0
    fi
  fi

  echo
done

# ── Drain pass: clear any DISCOVERED records left by the final batch ──────────
discovered_remaining=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM records WHERE record_state='DISCOVERED'" 2>/dev/null || echo 0)
if [[ $discovered_remaining -gt 0 ]]; then
  echo "──────────────────────────────────────────────────────"
  echo "Drain pass: ${discovered_remaining} DISCOVERED record(s) still pending — running dispatcher-only pass"
  echo "──────────────────────────────────────────────────────"
  drain_cmd=(
    "$PYTHON" -m pipeline run
    --consumer-only
    --name  "${PIPELINE_NAME}"
    --max-cost "${MAX_COST}"
  )
  if [[ "$DRY_RUN" == "true" ]]; then
    drain_cmd+=(--dry-run --no-racknerd)
  elif [[ "$NO_RACKNERD" == "true" ]]; then
    drain_cmd+=(--no-racknerd)
  elif [[ "$RACKNERD_DIRECT" == "true" ]]; then
    drain_cmd+=(--racknerd-direct)
  fi
  cd "$PROJECT_ROOT"
  "${drain_cmd[@]}"
  echo
fi

echo "══════════════════════════════════════════════════════"
echo "All ${MAX_BATCHES} batches complete."
echo "  Database:         ${DB_PATH}"
echo "  Checkpoint log:   ${LOG_FILE}"
echo "  Valid emails CSV: ${OUTPUT_DIR}/valid_emails.csv"
echo "══════════════════════════════════════════════════════"
