# /test

Run the pipeline test suite.

## Usage

```
/test [unit|integration|e2e|all]
```

Default: `all`

## Suites

| Suite | Command | Speed | Description |
|---|---|---|---|
| `unit` | `pytest tests/unit/ -q` | ~5s | Pure logic, no I/O |
| `integration` | `pytest tests/integration/ -q` | ~15s | Real SQLite, no API calls |
| `e2e` | `pytest tests/e2e/ -q` | ~90s | Subprocess pipeline runs |
| `all` | `pytest tests/ -q` | ~120s | Full suite (119 tests) |

## Implementation

```bash
SUITE="${1:-all}"

case "$SUITE" in
  unit)        .venv/bin/python -m pytest tests/unit/ -q ;;
  integration) .venv/bin/python -m pytest tests/integration/ -q ;;
  e2e)         .venv/bin/python -m pytest tests/e2e/ -q ;;
  all)         .venv/bin/python -m pytest tests/ -q ;;
  *)           echo "Unknown suite: $SUITE. Use: unit, integration, e2e, all" && exit 1 ;;
esac
```

## On failure

- Unit failures → logic bug, fix before anything else
- Integration failures → SQLite schema mismatch or db.py helper bug
- E2e failures → check if API keys are exhausted (use `--dry-run` flag in that test)
