# Git Rules

## Branches

Every change goes through a branch — never commit directly to `main`.

```
feat/  — new capability
fix/   — bug fix
refactor/ — no behavior change
chore/ — deps, config, tooling
docs/  — documentation only
test/  — tests only
```

Branch name: `<type>/<short-description>` — lowercase, hyphens, 3–6 words max.

## Commits

```
<type>: <imperative present tense description>
```

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `perf`

Examples:
```
feat: add discovery_method and validation_method columns to CSV
fix: create aiodns resolver once per producer instead of per record
refactor: rename stage_v2 to stage now that V1 is deleted
docs: split architecture doc into technical and overview
```

- One logical change per commit
- Describe what changes, not why (PR description covers why)
- Never `--no-verify` to skip hooks

## What never gets committed

- `.env` (real credentials)
- `output/` (pipeline run artifacts)
- `runs/` (orchestrator run directories)
- `*.db`, `*.db-shm`, `*.db-wal` (SQLite files)
- `.venv/` (virtual environment)
- `__pycache__/`, `*.pyc`

All of these are in `.gitignore`. If you accidentally stage one, run `git rm --cached <file>`.

## PR template

Every PR uses this structure (use `/review-pr` to generate):

```
## Problem
## Root Cause
## Solution
## Technical Details
## Testing Instructions
## Rollback Plan
## Notes
```
