# /dry-run

Run a pipeline dry run with mocked API calls to validate wiring and output schema.

## Usage

```
/dry-run [--limit N] [--name NAME]
```

Default: 50 records, name = `dryrun_<timestamp>`

## What it does

1. Runs `python -m pipeline run -i input/nc_retry_300k.jsonl --limit $LIMIT --dry-run --name $NAME`
2. Confirms exit code 0
3. Shows the CSV headers and first 3 rows
4. Shows `results.json` summary
5. Queries the DB for state distribution

## Implementation

```bash
LIMIT=${1:-50}
NAME="dryrun_$(date +%Y%m%d_%H%M%S)"

python -m pipeline run \
  -i input/nc_retry_300k.jsonl \
  --limit "$LIMIT" \
  --dry-run \
  --name "$NAME"

echo ""
echo "=== CSV preview ==="
head -4 "output/$NAME/valid_emails.csv"

echo ""
echo "=== results.json ==="
cat "output/$NAME/results.json"

echo ""
echo "=== DB state distribution ==="
python3 -c "
import sqlite3
conn = sqlite3.connect('output/$NAME/pipeline.db')
for row in conn.execute('SELECT record_state, COUNT(*) FROM records GROUP BY record_state'):
    print(f'  {row[0]}: {row[1]}')
conn.close()
"
```
