# us-z-3 — Email Contact Collector (ECC)

Discovers and validates business email addresses from NC Secretary of State filing records using a three-backend async pipeline.

Input: JSONL of business + registered-agent records.
Output: `valid_emails.csv` (confirmed emails), `results.json` (run summary), `pipeline.db` (full audit trail).

---

## Documentation

[UST tracking sheet](https://docs.google.com/spreadsheets/d/1rxTe-nIAU35-6p1OOlVvS0GT6zpYvK0XYngoy0AI7wQ/edit?usp=sharing) - run totals, enhancement docs, and related reports.

---

## Quick setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Required: SERPER_API_KEY, ZUHAL_API_KEY, RACKNERD_HOST, RACKNERD_SSH_USER, RACKNERD_SSH_KEY
```

---

## Running

```bash
# Dry run — no API calls, confirms wiring
python -m pipeline run -i input/nc_retry_300k.jsonl --limit 50 --dry-run --producer-only --name test

# Live run with cost ceiling
python -m pipeline run -i input/nc_retry_300k.jsonl --limit 500 --max-cost 1.00 --name run_$(date +%Y%m%d)

# 1,000-record run with interactive checkpoint review after every 100 records
bash scripts/run_checkpoints.sh

# Check status (add --watch 5 to poll every 5s)
python -m pipeline status --db output/run_20260430/pipeline.db

# Re-queue failures for retry
python -m pipeline reset --db output/run_20260430/pipeline.db --status discovery_failed
```

---

## Step-by-step processing flow

### Stage 1 — Producer (`producer.py`)

```
InputRecord (RAW)
    │
    ├─ DNS probe: try domain stems + TLD variants (.com, .net, .org, .us, .info) → MX lookup
    │     hit  → candidate_emails (ranked by pattern_stats win rate per MX provider)
    │     miss → Serper enrichment (uses normalize_business_name — no quoted legal names)
    │               primary query → site: scoped → agent-name → short-name (4+ words)
    │               hit  → candidate_emails
    │               miss → DISCOVERY_FAILED
    │
    └─ DISCOVERED (candidate_emails + candidate_domain + mx_provider written to DB)
```

- One `aiodns.DNSResolver` per run; Serper results cached by `(business_name, agent_name, state, provider)`; bypass with `--ignore-cache`.
- Serper starts with an empty token bucket (`initial_tokens=0`) — prevents 429 bursts on startup.
- Serper credit exhaustion (HTTP 400 "Not enough credits") degrades to `DISCOVERY_FAILED` rather than halting the run.
- Email patterns ranked by historical win rate from `pattern_stats` table.
- Domains appearing as first-organic fallback for 2+ different businesses are promoted to the runtime blocklist.

### Stage 2 — Dispatcher (`dispatcher.py`)

The dispatcher claims DISCOVERED records and loops through each `candidate_email` in ranked order:

```
For each candidate_email:
    │
    ├─ [1] MS probe (free, Microsoft-managed MX only)
    │         valid   → VALIDATED  ✓ done
    │         invalid → skip candidate, try next
    │         unknown → fall through to SMTP
    │
    ├─ [2] Racknerd SMTP + bbops.io  ──── asyncio.gather (concurrent)
    │         ┌─────────────────────────────────────────────────────────────┐
    │         │  reconcile(racknerd_verdict, bbops_verdict)                 │
    │         │                                                             │
    │         │  valid / catch_all (either backend) → VALIDATED  ✓         │
    │         │  both invalid                        → Phase 3 (Zuhal)     │
    │         │  Racknerd blocked (IP-level)         → re-queue, no burn   │
    │         │  mixed error / tunnel down           → re-queue, no burn   │
    │         └─────────────────────────────────────────────────────────────┘
    │
    └─ [3] Zuhal rescue  (sequential, only when both SMTP → invalid)
              valid / accept-all → VALIDATED  ✓ done
              circuit_open       → re-queue, no burn (auto-heal)
              error / invalid    → try next candidate

All candidates exhausted → VALIDATION_FAILED
Cost ceiling hit before Zuhal → COST_SKIPPED
```

#### Why Zuhal is sequential, not concurrent

Zuhal costs $0.0005/call. Running it alongside SMTP for every email would spend credits on records Racknerd or bbops already confirm — roughly 3× the Zuhal spend at 300k-record scale (~$45 extra). Rescue-only limits Zuhal to the subset both SMTP backends explicitly rejected.

#### Why `blocked` re-queues without calling Zuhal

`blocked` means our egress IP is flagged by Spamhaus — not that the email is invalid. Calling Zuhal would waste $0.0005 confirming a valid email we can't deliver from our IP anyway. The record re-queues and retries after SpamhausGuard cooldown (300s) expires.

---

## Validation logic

### OR-of-valids reconciliation

| Racknerd | bbops | Result | Action |
|---|---|---|---|
| `valid` | any | `valid` | VALIDATED |
| any | `valid` | `valid` | VALIDATED |
| `catch_all` | any | `catch_all` | VALIDATED |
| any | `catch_all` | `catch_all` | VALIDATED |
| `invalid` | `invalid` | — | Zuhal rescue |
| `blocked` | any | — | re-queue, no burn |
| `invalid` | `error`/`not_run` | `unknown` | re-queue, no burn |
| `error`/`not_run` | `invalid` | `unknown` | re-queue, no burn |
| `error` | `error` | `unknown` | re-queue, no burn |
| tunnel not up | any | `unknown` | re-queue, no burn |

**Re-queue** means the record returns to `DISCOVERED`. `dispatch_attempts` is only incremented on terminal verdicts (`valid`, `invalid`, `catch_all`) — transient failures do not count against the attempt budget.

---

## Status fields reference

### `record_state`

```
RAW → DISCOVERING → DISCOVERY_FAILED
           ↓
       DISCOVERED → VALIDATING → VALIDATED
                              ↘ VALIDATION_FAILED
                              ↘ COST_SKIPPED
```

| State | Meaning |
|---|---|
| `RAW` | Loaded from input, not yet touched |
| `DISCOVERING` | In-flight discovery; retried on restart |
| `DISCOVERED` | Candidate emails found; queued for validation |
| `VALIDATING` | Claimed by dispatcher; in-flight |
| `VALIDATED` | At least one email confirmed deliverable |
| `VALIDATION_FAILED` | All candidates exhausted, none valid |
| `DISCOVERY_FAILED` | No domain or email found via DNS or Serper |
| `COST_SKIPPED` | Cost ceiling hit before Zuhal rescue ran |

### `racknerd_status`

| Value | Meaning |
|---|---|
| `valid` | RCPT accepted (250) |
| `invalid` | RCPT rejected (5xx) |
| `catch_all` | Domain accepts all addresses |
| `error` | Network error, timeout, or SMTP protocol failure |
| `blocked` | Spamhaus/reputation IP-level block; re-queued without Zuhal |
| `not_run` | Skipped (MS probe short-circuited) |
| `ms_valid` | Confirmed via MS probe, not direct SMTP |

### `bbops_status`

| Value | Meaning |
|---|---|
| `valid` | bbops confirmed deliverable |
| `invalid` | bbops rejected |
| `catch_all` | Domain accepts all (bbops signal) |
| `error` | API error, timeout, or poll failure |
| `not_run` | Skipped (MS probe short-circuited) |

### `final_verdict`

| Value | Written when |
|---|---|
| `valid` | Either backend (or Zuhal) confirmed valid |
| `catch_all` | Either backend returned catch_all (none returned valid) |
| `invalid` | All backends rejected; only on VALIDATION_FAILED records |

### `zuhal_status`

| Value | Meaning |
|---|---|
| `valid` / `accept-all` | Zuhal rescue succeeded |
| `invalid` / `error` | Zuhal also rejected or errored |
| `circuit_open` | Zuhal circuit open; re-queued without burning attempt |
| `dual_valid` / `dual_catch_all` / `dual_invalid` | Zuhal NOT run; encodes SMTP result |
| `ms_valid` | MS probe short-circuited; Zuhal not called |

---

## Output

### `valid_emails.csv`

| Column | Description |
|---|---|
| `unique_id` | Composite key: `{filing_id}__{agent_id}` |
| `business_name` | Legal business name from filing |
| `agent_name` | Registered agent / officer name |
| `state` | State abbreviation (e.g. `NC`) |
| `email` | Confirmed deliverable email address |
| `final_verdict` | `valid` or `catch_all` |
| `confidence_tier` | `high` / `medium` / `low` |
| `confidence_score` | Additive score 0–4 |
| `verified` | `True` if valid or catch_all |
| `discovery_method` | `dns`, `serper`, or `input` |
| `validation_method` | `ms_probe`, `smtp_both`, `smtp_racknerd`, `smtp_bbops`, `zuhal_rescue` |
| `racknerd_verdict` | Racknerd SMTP verdict |
| `bbops_verdict` | bbops.io verdict |
| `zuhal_verdict` | Zuhal verdict, or `not_run` |

**Confidence scoring** (additive):
- Domain match (+1): email domain fuzzy-matches `candidate_domain`
- Strategy `with`: name match (+1), not generic prefix (+1), verdict=`valid` (+1)
- Strategy `without`: IS generic prefix (+1), verdict=`valid` (+1)
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

`--producer-only` runs without `RACKNERD_HOST` or `ZUHAL_API_KEY`.

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
| `--ignore-cache` | off | Bypass Serper enrichment cache (forces live API call) |
| `--chunk-size N` | 100 | Records per producer batch |
| `--dns-concurrency N` | 100 | Parallel DNS semaphore size |
| `--dispatch-concurrency N` | 20 | Parallel dispatcher workers |
| `--dispatch-backend-timeout-s S` | 60.0 | Per-backend timeout for Racknerd + bbops |
| `--dispatch-chunk-size N` | 50 | Records fetched per dispatcher poll cycle |
| `--racknerd-host HOST` | — | VPS hostname for SSH tunnel |
| `--racknerd-concurrency N` | 10 | Parallel SMTP connections via tunnel |
| `--no-racknerd` | off | Disable Racknerd (bbops + Zuhal only) |
| `--racknerd-direct` | off | Skip SOCKS5 tunnel; probe MX directly (use when on the egress VPS) |
| `--bbops-base-url URL` | bbops.io | Override bbops API base URL |
| `--max-consecutive-errors N` | 10 | Halt after N consecutive producer errors |

---

## Costs

| Service | Per call | Notes |
|---|---|---|
| Serper | $0.001 | DNS-miss path (producer); fallback after patterns exhausted (dispatcher) |
| Racknerd SMTP | $0 | Fixed VPS cost; no per-probe fee |
| bbops | Per contract | Async batch verifier; probes all non-MS records |
| MS probe | $0 | Free; short-circuits Microsoft 365 / Exchange Online domains |
| Zuhal | $0.0005 | Rescue only — both SMTP backends returned `invalid` |

---

## Running tests

```bash
.venv/bin/python -m pytest tests/ -q        # 250 tests, 0 failures
.venv/bin/python -m pytest tests/unit/ -q   # fast unit tests only
.venv/bin/python -m pytest tests/e2e/ -q    # end-to-end subprocess tests
```
