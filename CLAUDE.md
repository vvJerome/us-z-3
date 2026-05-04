# us-z-3 — Email Contact Collector (ECC)

## What this project does

Discovers and validates business email addresses from NC Secretary of State filing records.
Input: JSONL of business + registered-agent records.
Output: `valid_emails.csv` (one row per confirmed email), `results.json` (run summary), `pipeline.db` (full audit trail).

---

## Quick setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Fill in SERPER_API_KEY and ZUHAL_API_KEY
```

---

## Running the pipeline

```bash
# Dry run — no API calls, confirms wiring is correct
python -m pipeline run -i input/nc_retry_300k.jsonl --limit 50 --dry-run --name test

# Live run with a cost ceiling
python -m pipeline run -i input/nc_retry_300k.jsonl --limit 500 --max-cost 1.00 --name run_$(date +%Y%m%d)

# Check status of a running or finished run
python -m pipeline status --db output/run_20260430/pipeline.db --watch 5

# Re-queue discovery failures for a retry
python -m pipeline reset --db output/run_20260430/pipeline.db --status discovery_failed
```

Via the orchestrator (wraps the pipeline with per-officer ID generation and output merging):

```bash
python -m orchestrator --input input/nc_retry_300k.jsonl --run-name nc_full
python -m orchestrator --resume runs/nc_full_20260430T120000Z/   # resume from manifest
```

---

## Directory layout

```
us-z-3/
├── pipeline/               # Core async pipeline package
│   ├── __main__.py         # Entry point: cmd_run / cmd_status / cmd_reset
│   ├── producer.py         # DNS probe + Serper enrichment → fills DISCOVERED queue
│   ├── consumer.py         # MS probe + Zuhal validation → VALIDATED/VALIDATION_FAILED
│   ├── bbops.py            # SMTP batch verification via bbops.io (runs between stages)
│   ├── db.py               # SQLite schema, State machine, all DB helpers
│   ├── models.py           # InputRecord, EnrichmentResult, ValidationResult dataclasses
│   ├── config.py           # PipelineConfig (pydantic-settings, reads .env)
│   ├── cli.py              # argparse definitions
│   ├── constants.py        # API costs, backoff params, DNS TLDs, fallback blocklist
│   ├── metrics.py          # Prometheus /metrics endpoint (port 9090)
│   └── utils/
│       ├── dns_probe.py    # aiodns MX probe, shared resolver, parallel TLD gather
│       ├── serper_client.py# Google search enrichment, enrichment_cache integration
│       ├── zuhal_client.py # Zuhal SMTP validation, aiobreaker circuit breaker
│       ├── ms_verify.py    # MS GetCredentialType probe (free, no Zuhal cost)
│       ├── email_patterns.py # Pattern generation + per-MX ranking from pattern_stats
│       ├── text.py         # Name parsing, domain stem generation, strategy assignment
│       ├── cost_tracker.py # Per-service cost accumulator with ceiling check
│       ├── rate_limiter.py # TokenBucket async rate limiter
│       ├── backoff.py      # Generic exponential backoff with jitter
│       ├── notify.py       # Named-pipe IPC (producer → consumer wake signal)
│       └── logger.py       # Structured JSON logging setup
│
├── orchestrator/           # Top-level run coordinator
│   ├── __main__.py         # Stages: input prep → pipeline → merge
│   ├── stage.py            # Calls pipeline producer, bbops, consumer as subprocesses
│   ├── merge_outputs.py    # Deduplicates and merges validated records to merged_valid_emails.csv
│   └── config.py           # RunPaths, Env dataclasses
│
├── tests/
│   ├── unit/               # Pure-logic tests (email patterns, scoring, rate limiter, MS verify)
│   ├── integration/        # SQLite schema + pipeline flow tests
│   └── e2e/                # Subprocess-level full pipeline tests
│
├── input/                  # Source JSONL files
├── output/                 # Per-run output (pipeline.db, results.json, valid_emails.csv)
├── runs/                   # Orchestrator run directories (managed automatically)
├── scripts/
│   ├── clean.sh            # Delete stale output dirs (--force to actually delete)
│   ├── deploy.sh           # Install deps on server
│   ├── start.sh            # Launch orchestrator in tmux
│   ├── stop.sh             # Kill orchestrator session
│   ├── logs.sh             # Tail pipeline logs
│   └── status.sh           # Show DB status summary
├── requirements.txt
├── pytest.ini
└── .env.example
```

---

## Record state machine

```
RAW → DISCOVERING → DISCOVERY_FAILED
           ↓
       DISCOVERED → VALIDATING → VALIDATED
                              ↘ VALIDATION_FAILED
                              ↘ COST_SKIPPED
```

| State | Meaning |
|---|---|
| `RAW` | Loaded from input, not yet processed |
| `DISCOVERING` | Transient error during discovery; eligible for retry |
| `DISCOVERED` | Domain/email candidates found; ready for validation |
| `VALIDATING` | Claimed by consumer; in-flight |
| `VALIDATED` | Confirmed deliverable email found |
| `VALIDATION_FAILED` | All candidates exhausted without a valid result |
| `DISCOVERY_FAILED` | Neither DNS nor Serper found a domain |
| `COST_SKIPPED` | Cost ceiling reached before this record could be validated |

---

## Output CSV columns

| Column | Description |
|---|---|
| `unique_id` | Composite key: `{filing_id}__{agent_id}` |
| `business_name` | Legal business name from filing |
| `agent_name` | Registered agent / officer name |
| `state` | State abbreviation (e.g. `NC`) |
| `email` | Confirmed deliverable email address |
| `zuhal_status` | Raw API verdict: `valid`, `accept-all`, `ms_valid`, `bbops_valid` |
| `confidence_tier` | `high` / `medium` / `low` (scoring details below) |
| `verified` | `True` if individually confirmed (`valid`, `ms_valid`, `bbops_valid`); `False` for catch-all (`accept-all`, `catch_all`) |
| `discovery_method` | How the email was found: `dns`, `serper`, `input` |
| `validation_method` | Which service validated it: `zuhal`, `ms_probe`, `bbops` |

**Confidence scoring** (additive):

- Domain match (+1): email domain fuzzy-matches the candidate domain
- Strategy `with`: name match (+1), not a generic prefix (+1), not catch-all (+1)
- Strategy `without`: IS a generic prefix (+1), not catch-all (+1)
- High ≥ 3, medium = 2, low ≤ 1

---

## Environment variables

| Variable | Required | Default |
|---|---|---|
| `SERPER_API_KEY` | Yes | — |
| `ZUHAL_API_KEY` | Yes | — |
| `BBOPS_BASE_URL` | No | `https://email-verifier.bbops.io` |

---

## Key CLI flags

| Flag | Default | Effect |
|---|---|---|
| `--limit N` | none | Process only first N records |
| `--dry-run` | off | Mock all API calls; no cost |
| `--max-cost USD` | none | Stop when cumulative cost reaches limit |
| `--name NAME` | none | Output to `output/NAME/` |
| `--producer-only` | off | Run discovery only |
| `--consumer-only` | off | Run validation only |
| `--chunk-size N` | 100 | Records per concurrent batch |
| `--dns-concurrency N` | 100 | Parallel DNS semaphore size |

---

## Running tests

```bash
pytest tests/ -q                    # all 119 tests
pytest tests/unit/ -q               # fast unit tests only
pytest tests/e2e/ -q                # end-to-end subprocess tests
```

---

## Costs (live runs)

| Service | Per call | Notes |
|---|---|---|
| Serper | $0.001 | 1 call per record, always |
| Zuhal | $0.0005 | Only for DISCOVERED records; skipped by MS probe / bbops |
| MS probe | $0 | Free; covers all Microsoft 365 / Exchange Online domains |
| bbops | Per contract | SMTP batch; runs between producer and consumer |

Typical: $0.00116/record, ~$348 for 300k records.
