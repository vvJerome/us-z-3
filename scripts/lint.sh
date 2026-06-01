#!/usr/bin/env bash
# lint.sh — code quality suite: style, types, coverage.
#
# Usage:
#   scripts/lint.sh              # all checks, composite score
#   scripts/lint.sh --style      # flake8 only
#   scripts/lint.sh --types      # mypy only
#   scripts/lint.sh --coverage   # pytest + coverage only
#   scripts/lint.sh --fix        # auto-remove unused imports (autoflake)
#   scripts/lint.sh --ci         # all checks, exit 1 on any failure

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
VENV="$ROOT/.venv/bin/python"

if [[ ! -x "$VENV" ]]; then
  echo "error: .venv not found. Run: python3 -m venv .venv && pip install -r requirements.txt -r requirements-dev.txt" >&2
  exit 1
fi

for tool in flake8 mypy pytest; do
  if ! "$VENV" -m "$tool" --version &>/dev/null; then
    echo "error: $tool not installed. Run: pip install -r requirements-dev.txt" >&2
    exit 1
  fi
done

MODE="all"
CI=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --style)    MODE="style" ;;
    --types)    MODE="types" ;;
    --coverage) MODE="coverage" ;;
    --fix)      MODE="fix" ;;
    --ci)       CI=1 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

STYLE_ISSUES=0
TYPE_ERRORS=0
COVERAGE_PCT=0
STYLE_OK=1
TYPES_OK=1
COVERAGE_OK=1

run_style() {
  echo "[lint] style (flake8) ..."
  STYLE_OUT=$("$VENV" -m flake8 pipeline/ --count --statistics 2>&1 || true)
  STYLE_ISSUES=$(echo "$STYLE_OUT" | grep -E '^[0-9]+$' | tail -1 || echo 0)
  STYLE_ISSUES=${STYLE_ISSUES:-0}
  if [[ "$STYLE_ISSUES" -gt 0 ]]; then
    echo "$STYLE_OUT"
    STYLE_OK=0
  else
    echo "[lint] style   OK — 0 issues"
  fi
}

run_types() {
  echo "[lint] types  (mypy) ..."
  TYPE_OUT=$("$VENV" -m mypy pipeline/ 2>&1 || true)
  TYPE_ERRORS=$(echo "$TYPE_OUT" | grep -c ": error:" || echo 0)
  if [[ "$TYPE_ERRORS" -gt 0 ]]; then
    echo "$TYPE_OUT"
    TYPES_OK=0
  else
    echo "[lint] types   OK — 0 errors"
  fi
}

run_coverage() {
  echo "[lint] coverage (pytest --cov) ..."
  COV_OUT=$("$VENV" -m pytest tests/ -q --tb=no \
    --cov=pipeline --cov-report=term-missing \
    --ignore=pipeline/ops 2>&1)
  COVERAGE_LINE=$(echo "$COV_OUT" | grep "^TOTAL" | awk '{print $NF}' | tr -d '%')
  COVERAGE_PCT=${COVERAGE_LINE:-0}
  echo "$COV_OUT" | tail -5
  if [[ "$COVERAGE_PCT" -lt 60 ]]; then
    COVERAGE_OK=0
  fi
}

run_fix() {
  echo "[lint] fix — removing unused imports (autoflake) ..."
  if ! "$VENV" -m autoflake --version &>/dev/null; then
    echo "error: autoflake not installed. Run: pip install -r requirements-dev.txt" >&2
    exit 1
  fi
  "$VENV" -m autoflake \
    --remove-all-unused-imports \
    --in-place \
    --recursive \
    pipeline/
  echo "[lint] done. Review changes with: git diff pipeline/"
}

if [[ "$MODE" == "fix" ]]; then
  run_fix
  exit 0
fi

[[ "$MODE" == "all" || "$MODE" == "style" ]]    && run_style
[[ "$MODE" == "all" || "$MODE" == "types" ]]    && run_types
[[ "$MODE" == "all" || "$MODE" == "coverage" ]] && run_coverage

if [[ "$MODE" == "all" ]]; then
  # Composite score: style 25%, types 30%, coverage 45%
  # Style: 100 - min(issues * 2, 100)
  # Types: 100 - min(errors * 5, 100)
  # Coverage: raw %
  STYLE_SCORE=$(( 100 - STYLE_ISSUES * 2 < 0 ? 0 : 100 - STYLE_ISSUES * 2 ))
  TYPE_SCORE=$(( 100 - TYPE_ERRORS * 5 < 0 ? 0 : 100 - TYPE_ERRORS * 5 ))
  COV_SCORE="$COVERAGE_PCT"
  COMPOSITE=$(echo "scale=1; ($STYLE_SCORE * 25 + $TYPE_SCORE * 30 + $COV_SCORE * 45) / 100" | bc)

  echo ""
  echo "┌─────────────────────────────────┐"
  echo "│        Code Quality Score       │"
  echo "├──────────────┬──────────────────┤"
  printf "│ Style        │ %3d%%  %-11s│\n" "$STYLE_SCORE" "(${STYLE_ISSUES} issues)"
  printf "│ Types        │ %3d%%  %-11s│\n" "$TYPE_SCORE"  "(${TYPE_ERRORS} errors)"
  printf "│ Coverage     │ %3d%%  %-11s│\n" "$COV_SCORE"   "(pipeline/)"
  echo "├──────────────┼──────────────────┤"
  printf "│ COMPOSITE    │ %4s%%            │\n" "$COMPOSITE"
  echo "└──────────────┴──────────────────┘"
fi

if [[ "$CI" -eq 1 ]]; then
  [[ "$STYLE_OK" -eq 1 && "$TYPES_OK" -eq 1 && "$COVERAGE_OK" -eq 1 ]] || exit 1
fi
