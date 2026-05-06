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
│   ├── deploy.sh           # rsync project to VPS + install deps
│   ├── run_checkpoints.sh  # 10×100-record batched run with interactive checkpoint reviews
│   ├── start.sh            # Launch orchestrator in tmux
│   ├── stop.sh             # Kill orchestrator session
│   ├── logs.sh             # Tail pipeline logs
│   └── status.sh           # Show DB status summary
├── requirements.txt
├── pytest.ini
└── .env.example
```

---

## Record processing flow

### Stage 1 — Producer

```
InputRecord (RAW)
    ├─ DNS probe (aiodns, shared resolver, TLD variants)
    │     hit  → candidate_emails ranked by pattern_stats win rate
    │     miss → Serper enrichment (cached by business+agent+state+provider)
    │               hit  → candidate_emails
    │               miss → DISCOVERY_FAILED
    └─ DISCOVERED (candidate_emails, candidate_domain, mx_provider written to DB)
```

### Stage 2 — Dispatcher (per candidate_email, in rank order)

```
[1] MS probe (free, Microsoft MX only)
        valid   → VALIDATED  ✓
        invalid → skip candidate
        unknown → continue

[2] Racknerd SMTP + bbops.io  (asyncio.gather, concurrent)
        reconcile():
          valid / catch_all (either backend) → VALIDATED  ✓
          both invalid                        → [3]
          mixed error / tunnel down           → re-queue (no attempt burned)

[3] Zuhal rescue  (sequential, only when both SMTP → invalid)
        valid / accept-all → VALIDATED  ✓
        else               → try next candidate

All candidates exhausted → VALIDATION_FAILED
Cost ceiling before Zuhal → COST_SKIPPED
```

### OR-of-valids reconciliation conditions

| Racknerd | bbops | Outcome | Note |
|---|---|---|---|
| `valid` | any | VALIDATED `valid` | |
| any | `valid` | VALIDATED `valid` | |
| `catch_all` | any | VALIDATED `catch_all` | |
| any | `catch_all` | VALIDATED `catch_all` | |
| `invalid` | `invalid` | Zuhal rescue | |
| `invalid` | `error`/`not_run` | re-queue | Can't trust single invalid |
| `error`/`not_run` | `invalid` | re-queue | Can't trust single invalid |
| `error` | `error` | re-queue | Both inconclusive |
| tunnel down | any | re-queue | `"tunnel not up"` in Racknerd message |

**Re-queue** = record returns to `DISCOVERED`. `dispatch_attempts` only increments on terminal verdicts; transient failures do not count against the attempt budget.

---

## Status fields reference

### `racknerd_verdict` (DB column: `racknerd_status`)

| Value | Meaning |
|---|---|
| `valid` | RCPT accepted (250) |
| `invalid` | RCPT rejected (5xx) |
| `catch_all` | Domain accepts all addresses |
| `error` | Network error, timeout, or SMTP protocol failure |
| `blocked` | Spamhaus / reputation block detected |
| `not_run` | Skipped — MS probe short-circuited |
| `ms_valid` | Confirmed via MS probe (not direct SMTP) |

### `bbops_verdict` (DB column: `bbops_status`)

| Value | Meaning |
|---|---|
| `valid` | bbops confirmed deliverable |
| `invalid` | bbops rejected |
| `catch_all` | Domain accepts all (bbops signal) |
| `error` | API error, timeout, or poll failure |
| `not_run` | Skipped — MS probe short-circuited |

### `final_verdict`

| Value | Written when |
|---|---|
| `valid` | Either backend (or Zuhal) confirmed valid |
| `catch_all` | Either backend returned catch_all (none returned valid) |
| `invalid` | All backends rejected; only set on VALIDATION_FAILED records |

### `zuhal_status`

| Value | Meaning |
|---|---|
| `valid` / `accept-all` | Zuhal rescue succeeded |
| `invalid` / `error` | Zuhal also rejected or errored |
| `circuit_open` | Zuhal circuit breaker open; record re-queued as DISCOVERED (auto-heal, no attempt burned) |
| `dual_valid` / `dual_catch_all` / `dual_invalid` | Zuhal did NOT run; encodes the SMTP reconciliation result |
| `ms_valid` | MS probe short-circuited; Zuhal not called |

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
| `VALIDATING` | Claimed by dispatcher; in-flight |
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
| `final_verdict` | Reconciled verdict: `valid` or `catch_all` |
| `confidence_tier` | `high` / `medium` / `low` (from `confidence_score`) |
| `confidence_score` | Raw additive pattern score 0–4 |
| `verified` | `True` if `valid` or `catch_all`; `False` otherwise |
| `discovery_method` | How the email was found: `dns`, `serper`, `input` |
| `validation_method` | Which backend validated: `ms_probe`, `smtp_both`, `smtp_racknerd`, `smtp_bbops`, `zuhal_rescue` |
| `racknerd_verdict` | Racknerd SMTP verdict for this email |
| `bbops_verdict` | bbops.io verdict for this email |
| `zuhal_verdict` | Zuhal rescue verdict, or `not_run` if Zuhal was not invoked |

**Confidence scoring** (additive):

- Domain match (+1): email domain fuzzy-matches the candidate domain
- Strategy `with`: name match (+1), not a generic prefix (+1), verdict=`valid` (+1)
- Strategy `without`: IS a generic prefix (+1), verdict=`valid` (+1)
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
| `--racknerd-direct` | off | Skip SOCKS5 tunnel; connect directly to MX servers (use when running on the egress VPS) |
| `--bbops-base-url URL` | bbops.io | Override bbops API base URL |
| `--max-consecutive-errors N` | 10 | Halt after N consecutive producer errors |

---

## Running tests

```bash
.venv/bin/python -m pytest tests/ -q    # all 201 tests
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
