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
# Fill in SERPER_API_KEY, ZUHAL_API_KEY, RACKNERD_HOST, RACKNERD_SSH_USER, RACKNERD_SSH_KEY
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
│   ├── dispatcher.py       # Three-backend coordinator (Racknerd + bbops + Zuhal rescue)
│   ├── db.py               # SQLite schema, State machine, all DB helpers
│   ├── models.py           # InputRecord, EnrichmentResult, ValidationResult dataclasses
│   ├── config.py           # PipelineConfig (pydantic-settings, reads .env)
│   ├── cli.py              # argparse definitions
│   ├── constants.py        # API costs, backoff params, DNS TLDs, fallback blocklist
│   ├── metrics.py          # Prometheus /metrics endpoint (port 9090)
│   ├── consumers/
│   │   ├── racknerd.py     # Direct SMTP via SSH SOCKS5 tunnel (Backend 1)
│   │   └── bbops_async.py  # Async bbops.io batch verifier (Backend 2)
│   ├── tunnels/
│   │   └── ssh_socks.py    # SSH SOCKS5 tunnel supervisor with auto-restart
│   └── utils/
│       ├── dns_probe.py    # aiodns MX probe, shared resolver, parallel TLD gather
│       ├── serper_client.py# Google search enrichment, enrichment_cache integration
│       ├── zuhal_client.py # Zuhal rescue backend (runs only when both SMTP backends reject)
│       ├── ms_verify.py    # MS GetCredentialType probe (free, short-circuits Microsoft domains)
│       ├── email_patterns.py # Pattern generation + per-MX ranking from pattern_stats
│       ├── text.py         # Name parsing, domain stem generation, strategy assignment
│       ├── cost_tracker.py # Per-service cost accumulator with ceiling check
│       ├── rate_limiter.py # TokenBucket async rate limiter
│       ├── backoff.py      # Generic exponential backoff with jitter
│       ├── notify.py       # Named-pipe IPC (producer → dispatcher wake signal)
│       └── logger.py       # Structured JSON logging setup
│
├── orchestrator/           # Top-level run coordinator
│   ├── __main__.py         # Stages: input prep → pipeline → merge
│   ├── stage.py            # Calls pipeline producer and dispatcher as subprocesses
│   ├── merge_outputs.py    # Deduplicates and merges validated records to merged_valid_emails.csv
│   └── config.py           # RunPaths, Env dataclasses
│
├── tests/
│   ├── unit/               # Pure-logic tests (reconciliation, scoring, SMTP, ssh, bbops)
│   ├── integration/        # SQLite schema + dispatcher + bbops flow tests
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
| `final_verdict` | Reconciled verdict: `valid`, `catch_all`, `invalid` |
| `confidence_tier` | `high` / `medium` / `low` (scoring details below) |
| `verified` | `True` if `valid` or `catch_all`; `False` otherwise |
| `discovery_method` | How the email was found: `dns`, `serper`, `input` |
| `validation_method` | Which backend validated it: `ms_probe`, `racknerd+bbops`, `zuhal_rescue` |
| `racknerd_status` | Racknerd SMTP verdict: `valid`, `invalid`, `catch_all`, `error`, `not_run`, `ms_valid` |
| `bbops_status` | bbops.io verdict: `valid`, `invalid`, `catch_all`, `error`, `not_run` |

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
| `ZUHAL_API_KEY` | Yes (dispatcher) | — |
| `RACKNERD_HOST` | Yes (dispatcher) | — |
| `RACKNERD_SSH_USER` | No | `egress` |
| `RACKNERD_SSH_KEY` | No | `~/.ssh/racknerd_egress` |
| `BBOPS_BASE_URL` | No | `https://email-verifier.bbops.io` |

---

## Key CLI flags

| Flag | Default | Effect |
|---|---|---|
| `--limit N` | none | Process only first N records |
| `--dry-run` | off | Mock all API calls; no cost |
| `--max-cost USD` | none | Stop when cumulative cost reaches limit |
| `--name NAME` | none | Output to `output/NAME/` |
| `--producer-only` | off | Run discovery only (no SSH tunnel, no Racknerd) |
| `--consumer-only` | off | Run dispatcher only |
| `--chunk-size N` | 100 | Records per producer batch |
| `--dns-concurrency N` | 100 | Parallel DNS semaphore size |
| `--dispatch-concurrency N` | 20 | Parallel dispatcher workers |
| `--dispatch-backend-timeout-s S` | 60.0 | Per-backend timeout for Racknerd + bbops |
| `--dispatch-chunk-size N` | 50 | Records fetched per dispatcher poll cycle |
| `--racknerd-host HOST` | — | VPS hostname for SSH tunnel (required for dispatcher) |
| `--racknerd-concurrency N` | 10 | Parallel SMTP connections via tunnel |
| `--no-racknerd` | off | Disable Racknerd backend (bbops + Zuhal only) |
| `--bbops-base-url URL` | bbops.io | Override bbops API base URL |
| `--max-consecutive-errors N` | 10 | Halt after N consecutive producer errors |

---

## Running tests

```bash
.venv/bin/python -m pytest tests/ -q    # all 194 tests
.venv/bin/python -m pytest tests/unit/ -q               # fast unit tests only
.venv/bin/python -m pytest tests/e2e/ -q                # end-to-end subprocess tests
```

---

## Costs (live runs)

| Service | Per call | Notes |
|---|---|---|
| Serper | $0.001 | 1 call per record (producer), always |
| Racknerd SMTP | $0 | Fixed VPS cost; no per-probe fee |
| bbops | Per contract | Async batch verifier; probes all non-MS records |
| MS probe | $0 | Free; short-circuits all Microsoft 365 / Exchange Online domains |
| Zuhal | $0.0005 | Rescue only — runs when both Racknerd + bbops return `invalid` |

Typical Serper-only cost: ~$0.001/record, ~$300 for 300k records. Zuhal rescue adds ~$0.0005 per record that fails both SMTP backends (typically 5–15% of records).
