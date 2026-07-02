# /release

Cut a release: tag `main` at its current HEAD and publish a GitHub Release with
auto-generated notes. Manual and deliberate — this never fires on its own, and
nothing else in the repo depends on a release existing. It marks a benchmarking
checkpoint (see CLAUDE.md "Versioning"), not a software publish event.

## Usage

```
/release [major|minor|patch]
```

Default: `patch` if not specified.

## What it does

1. Confirms the working tree is on `main`, clean, and up to date with `origin/main`
   — refuses to release a stale or dirty checkout.
2. Checks whether `SCHEMA_VERSION` (`pipeline/db/schema.py`) or the CSV column list
   (`pipeline/output.py`'s `write_outputs` header row) changed since the last tag.
   If either changed and the requested bump isn't `major`, warns and asks for
   confirmation before proceeding — a schema/contract change is the checkable
   definition of MAJOR here, not a judgment call.
3. Triggers `.github/workflows/release.yml` via `gh workflow run`, which computes
   the next `vX.Y.Z`, creates and pushes the tag, and publishes a GitHub Release
   with `--generate-notes` (auto-compiled from merged PRs since the last tag).
4. Prints the new release URL once the workflow completes.

## Implementation

```bash
BUMP="${1:-patch}"
case "$BUMP" in major|minor|patch) ;; *) echo "bump must be major, minor, or patch"; exit 1;; esac

BRANCH=$(git branch --show-current)
if [[ "$BRANCH" != "main" ]]; then
  echo "Refusing to release from '$BRANCH' — switch to main first."
  exit 1
fi
if [[ -n "$(git status --porcelain)" ]]; then
  echo "Working tree is dirty — commit or stash before releasing."
  exit 1
fi
git fetch origin main -q
if [[ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]]; then
  echo "Local main is behind origin/main — pull first."
  exit 1
fi

LATEST_TAG=$(git tag -l 'v*' --sort=-v:refname | head -1)
if [[ -n "$LATEST_TAG" ]]; then
  SCHEMA_CHANGED=$(git diff "$LATEST_TAG" HEAD -- pipeline/db/schema.py | grep -c "^[+-]SCHEMA_VERSION" || true)
  CSV_HEADER_CHANGED=$(git diff "$LATEST_TAG" HEAD -- pipeline/output.py | grep -c '^\s*[+-]\s*"' || true)
  if [[ "$SCHEMA_CHANGED" != "0" || "$CSV_HEADER_CHANGED" != "0" ]] && [[ "$BUMP" != "major" ]]; then
    echo "SCHEMA_VERSION or the CSV column contract changed since $LATEST_TAG, but bump=$BUMP."
    echo "That's the checkable definition of a MAJOR release here (see .claude/rules/git.md)."
    read -p "Continue with bump=$BUMP anyway? [y/N] " -n 1 -r; echo
    [[ "$REPLY" =~ ^[Yy]$ ]] || exit 1
  fi
fi

gh workflow run release.yml -f bump="$BUMP"
echo "Release workflow triggered (bump=$BUMP). Watch it: gh run watch"
```
