# us-z-3

Hybrid V1 + V2 email discovery & validation orchestrator.

Runs V1 (`universal-scraper`) end-to-end first, then feeds every record V1 could not validate into V2 (`universal-scraper-v2`) for a second-chance pass via Serper re-discovery → bbops.io SMTP → Zuhal fallback. Final output is a single merged CSV tagged with the source stage (`v1` vs `v2_rescue`).

## Layout

```
us-z-3/
├── vendor/
│   ├── universal-scraper/        # V1, vendored unchanged
│   └── universal-scraper-v2/     # V2, vendored unchanged
├── orchestrator/                 # thin Python driver
├── input/                        # source JSONLs (gitignored in practice)
├── runs/                         # per-run artifacts (gitignored)
├── .env.example
└── requirements.txt
```

## Setup

1. Install deps for the orchestrator and both vendored pipelines:

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    pip install -r vendor/universal-scraper/requirements.txt
    pip install -r vendor/universal-scraper-v2/requirements.txt
    ```

2. Copy `.env.example` → `.env` and fill in keys.

3. Bring up the V1 SMTP verifier on the VPS and tunnel port 8025:

    ```bash
    # one-time VPS setup (from vendor/universal-scraper/)
    cd vendor/universal-scraper && python -m vps.provision

    # from the laptop, open the SSH tunnel:
    ssh -fN -L 8025:localhost:8025 root@142.171.178.179
    curl -s http://localhost:8025/health
    ```

4. Drop input JSONLs into `input/`:

    ```
    input/fl_scp_joined_business_agent_officers_only.jsonl
    input/fl_scp_joined_business_agent_all.jsonl
    ```

## Running

One command per dataset; the orchestrator runs V1, extracts non-valid rows, runs V2 (producer + bbops + Zuhal rescue), and writes the merged CSV.

```bash
python -m orchestrator \
    --input input/fl_scp_joined_business_agent_officers_only.jsonl \
    --run-name officers_only

python -m orchestrator \
    --input input/fl_scp_joined_business_agent_all.jsonl \
    --run-name all
```

Each run lands at `runs/<run_name>_<UTC_timestamp>/`:

```
runs/officers_only_20260421T180000Z/
├── v1/
│   ├── results.jsonl
│   ├── valid_emails.csv
│   └── report.json
├── v2_input.jsonl          # V1 non-valid records, transformed to V2 schema
├── v2/
│   ├── pipeline.db
│   ├── bbops_valid_emails.csv
│   └── output/             # V2 native outputs (results.jsonl, report.json, summary.md)
├── merged_valid_emails.csv # final deliverable
└── manifest.json           # step completion state (for --resume)
```

### Resuming

If V2 dies mid-run, resume the same run dir:

```bash
python -m orchestrator --resume runs/officers_only_20260421T180000Z
```

`manifest.json` tracks `v1_done` and `v2_done` flags. `--skip-v1` / `--skip-v2` force-skip a stage; `--skip-preflight` bypasses the SMTP + bbops.io reachability check.

## What goes into the V2 stage

Any V1 record with `status != "completed"` — i.e., `no_valid_email`, `catch_all_unverified`, `greylisted`, `failed`, `no_candidates`, `no_domain`, `skipped`. These are fed into V2's producer, which does fresh DNS + Serper discovery, then:

1. `verify_emails.py` → bbops.io SMTP (port-25 worker pool). Writes `zuhal_status ∈ {valid, catch_all, invalid, error}`.
2. Rescue SQL: `UPDATE records SET status='pending_validation', zuhal_status=NULL WHERE zuhal_status='error'` — these are the "SMTP blocked / Spamhaus / network" bucket.
3. V2 consumer runs Zuhal on the flipped records. Rate-limited to 200 calls/hour (the vendored V2 config; override via its own CLI flags if needed).

## Output

`merged_valid_emails.csv`:

| column | notes |
| --- | --- |
| `unique_id` | `raw_unique_id` from input |
| `business_name`, `agent_name`, `state` | pass-through |
| `email` | validated email |
| `domain` | discovered domain |
| `source` | `v1` (V1 completed) or `v2_rescue` (V2 post-Zuhal validated) |
| `validation_status` | V1 `email_source` or V2 `zuhal_status` |
| `confidence_tier` | `high` / `medium` / `low` |

Dedupe key is `(unique_id, email)`; V1 wins on conflict.
