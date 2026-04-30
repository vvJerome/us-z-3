# /pipeline-status

Show the current state of a pipeline run from its SQLite database.

## Usage

```
/pipeline-status [DB_PATH]
```

Default DB path: `output/pipeline.db`

## What it shows

- Record state distribution (DISCOVERED, VALIDATED, DISCOVERY_FAILED, etc.)
- API call counts and estimated cost
- Producer checkpoint (offset + done flag)
- Last heartbeat timestamps
- Recent failures by phase

## Implementation

```bash
DB="${1:-output/pipeline.db}"

if [[ ! -f "$DB" ]]; then
  echo "No database found at: $DB"
  echo "Available databases:"
  find output/ runs/ -name "pipeline.db" 2>/dev/null | head -10
  exit 1
fi

python -m pipeline status --db "$DB"
```

To watch live (refresh every 5s):
```bash
python -m pipeline status --db "$DB" --watch 5
```
