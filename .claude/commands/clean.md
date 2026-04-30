# /clean

Delete stale pipeline output directories and WAL artifacts.

## Usage

```
/clean [--days N] [--force]
```

Default: dry-run (shows what would be deleted), 3-day age threshold.

## What it removes

- `runs/` subdirectories older than N days
- Orphaned `.db-shm` / `.db-wal` files from crashed runs
- Empty `logs/` directories

## Implementation

```bash
DAYS="${1:-3}"
FORCE=""

# Parse flags
for arg in "$@"; do
  case "$arg" in
    --force) FORCE="--force" ;;
    --days=*) DAYS="${arg#--days=}" ;;
  esac
done

bash scripts/clean.sh --days "$DAYS" $FORCE
```

## Safety

Without `--force`, only prints what would be deleted.  
Always safe to run — active databases are never touched.
