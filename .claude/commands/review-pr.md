# /review-pr

Generate a structured PR description from the current branch diff.

## Usage

```
/review-pr [BASE_BRANCH]
```

Default base branch: `main`

## Output format

Produces a PR body using the project template:

```
## Problem
-

## Root Cause
-

## Solution
-

## Technical Details
-

## Testing Instructions
1.
2.
3.

## Rollback Plan
-

## Notes
-
```

## Implementation

```bash
BASE="${1:-main}"

echo "=== Commits on this branch ==="
git log "$BASE"...HEAD --oneline

echo ""
echo "=== Files changed ==="
git diff "$BASE"...HEAD --name-only

echo ""
echo "=== Full diff ==="
git diff "$BASE"...HEAD --stat

# Claude then drafts the PR body based on the diff
```

## Notes

- Always verify Technical Details section — Claude describes intent, not always exact behavior
- Testing Instructions should reference `pytest tests/ -q` and a specific dry-run command
- Rollback Plan: `git revert <commit>` or restore from last known-good `pipeline.db`
