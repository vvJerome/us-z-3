# Git Rules

## Branches

Every change goes through a branch — never commit directly to `main`.

```
feat/     — new capability
fix/      — bug fix
refactor/ — no behavior change
chore/    — deps, config, tooling
docs/     — documentation only
test/     — tests only
perf/     — performance improvement
ci/       — CI/CD pipeline changes
revert/   — reverting a previous commit
```

Branch name: `<type>/<short-description>` — lowercase, hyphens, 2–50 chars after the
slash. Enforced (advisory, not blocking) by `.github/workflows/pr-lint.yml` on every
PR — see "PR checks" below.

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
- `*.csv` — **never commit CSVs.** They are data/spec, not source. Keep them under `output/`, `runs/`, or `local/` (all gitignored). The `pre-edit-guard.sh` hook blocks creating a CSV anywhere else, and `stop-audit.sh` flags any tracked CSV.

All of these are in `.gitignore`. If you accidentally stage one, run `git rm --cached <file>`. If one slipped into the **tip** commit, `git rm --cached <file> && git commit --amend --no-edit` removes it from history; force-push with `--force-with-lease`.

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

## PR checks

Two workflows run on every PR:

- **`test.yml`** (job `test`) — `make check` (pytest + mypy + coverage gate). **Required**:
  `main` is branch-protected to block merge until this passes. No path filters, by
  design — a required check that can be *skipped* (path/branch filters, commit-message
  conditions) sits in "Pending" forever and blocks the merge permanently. Don't add
  filters to this workflow without re-checking that consequence first.
- **`pr-lint.yml`** (job `branch-name`) — validates the branch name against the
  convention above. **Advisory only, not required** — a bad branch name shows up as a
  failed check for visibility but never blocks a merge.

Locally, `.claude/hooks/pre-push-check.sh` runs the same `make check` gate before any
`git push` and blocks the push (exit 2) on failure — catches a broken push before it
reaches GitHub at all, rather than after a round trip to CI.

## Branch protection (`main`)

Configured via the GitHub API, not just documentation — verify with
`gh api repos/<owner>/<repo>/branches/main/protection`:

- Required status check: `test` (non-strict — doesn't force branches to be rebased
  onto latest `main` before merging).
- Pull request required before merging; 0 mandatory approvals (fits a solo/agent-heavy
  workflow — the gate is the test suite, not a review headcount).
- Force-push and branch deletion blocked.
- `enforce_admins: false` — the repo owner can still bypass in a genuine emergency;
  this is deliberately not maximally strict.

## Releases and versioning

A release is a **manual, deliberate benchmarking checkpoint** — a git tag (`vX.Y.Z`)
plus a GitHub Release with auto-generated notes. It never fires automatically on
merge, and nothing in the pipeline depends on one existing; it exists so a given
run's `results.json` can be correlated to an exact, named point in history, and so
there's a clear rollback reference if a change silently degrades output quality
(the kind of bug that passes CI — see the DNS-retry fix in `dns_probe.py`'s history
for a real example).

Trigger with `/release [major|minor|patch]` (default `patch`), or directly:
`gh workflow run release.yml -f bump=minor`.

Version meaning — tied to checkable facts, not judgment calls, because there's no
public API here to version against:

| Bump | Meaning |
|---|---|
| **MAJOR** | `SCHEMA_VERSION` (`pipeline/db/schema.py`) bumped, or the CSV column contract in `pipeline/output.py` changed — breaks `ops/ingest_zerobounce.py`, `ops/master_db.py`, and anything else parsing `valid_emails.csv` by column name. |
| **MINOR** | New feature/capability. |
| **PATCH** | Fix, refactor, or docs. |

`/release` checks whether `SCHEMA_VERSION` or the CSV header changed since the last
tag and warns (doesn't block) if the requested bump isn't `major` despite that.

The Docker image built by `build.yml` also gets tagged `:vX.Y.Z` on a release, in
addition to its usual `:latest`/`:<sha>` tags — kept minimal since the actual
production deploy path is `scripts/deploy.sh` (rsync to a VPS), not the container;
the versioned image exists for the Kestra/Docker path as secondary coverage.
