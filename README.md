# us-z-3 — Email Contact Collector (ECC)

Discovers and validates business email addresses from NC Secretary of State filing records using a three-backend async pipeline.

Input: JSONL of business + registered-agent records.
Output: `valid_emails.csv` (confirmed emails), `results.json` (run summary), `pipeline.db` (full audit trail).

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

# Check status (add --watch 5 to poll every 5s)
python -m pipeline status --db output/run_20260430/pipeline.db

# Re-queue failures for retry
python -m pipeline reset --db output/run_20260430/pipeline.db --status discovery_failed
```

Via the orchestrator (per-officer ID generation + output merging):

```bash
python -m orchestrator --input input/nc_retry_300k.jsonl --run-name nc_full
python -m orchestrator --resume runs/nc_full_20260430T120000Z/
```

---

## Step-by-step processing flow

Each input record moves through two workers:

### Stage 1 — Producer (`producer.py`)

```
InputRecord (RAW)
    │
    ├─ DNS probe: try domain stems + TLD variants → MX lookup
    │     hit  → candidate_emails (ranked by pattern_stats win rate)
    │     miss → Serper enrichment (Google search for business email)
    │               hit  → candidate_emails
    │               miss → DISCOVERY_FAILED
    │
    └─ DISCOVERED (candidate_emails + candidate_domain + mx_provider written to DB)
```

- One `aiodns.DNSResolver` per run; Serper results cached by `(business_name, agent_name, state, provider)`.
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
    │         ┌─────────────────────────────────────────────────────┐
    │         │  reconcile(racknerd_verdict, bbops_verdict)         │
    │         │                                                     │
    │         │  valid / catch_all (either backend) → VALIDATED ✓  │
    │         │  both invalid                        → Phase 2      │
    │         │  mixed error / tunnel down           → re-queue     │
    │         └─────────────────────────────────────────────────────┘
    │
    └─ [3] Zuhal rescue  (sequential, only when both SMTP → invalid)
              valid / accept-all → VALIDATED  ✓ done
              error / invalid    → try next candidate

All candidates exhausted → VALIDATION_FAILED
Cost ceiling hit before Zuhal → COST_SKIPPED
```

#### Why Zuhal is sequential, not concurrent

Zuhal costs $0.0005/call. Running it alongside SMTP for every email would spend credits on records Racknerd or bbops already confirm — roughly 3× the Zuhal spend at 300k-record scale (~$45 extra). Rescue-only limits Zuhal to the subset both SMTP backends explicitly rejected.

---

## Validation logic

### OR-of-valids reconciliation

`reconcile(racknerd, bbops)` in [pipeline/dispatcher.py](pipeline/dispatcher.py) applies this policy:

| Racknerd status | bbops status | Result | Action |
|----------------|-------------|--------|--------|
| `valid` | any | `valid` | VALIDATED |
| any | `valid` | `valid` | VALIDATED |
| `catch_all` | any | `catch_all` | VALIDATED |
| any | `catch_all` | `catch_all` | VALIDATED |
| `invalid` | `invalid` | — | Zuhal rescue (Phase 2) |
| `invalid` | `error` / `not_run` | `unknown` | re-queue, no attempt burned |
| `error` / `not_run` | `invalid` | `unknown` | re-queue, no attempt burned |
| `error` | `error` | `unknown` | re-queue, no attempt burned |
| tunnel not up | any | `unknown` | re-queue, no attempt burned |

**Re-queue** means the record returns to `DISCOVERED` state. `dispatch_attempts` is only incremented on terminal verdicts (`valid`, `invalid`, `catch_all`) — transient network failures do not count against the attempt budget.

### Conditions that trigger each path

| Condition | What fires |
|-----------|------------|
| `is_microsoft_mx(mx_provider) == True` | MS probe pre-filter |
| MS probe returns `valid` | VALIDATED immediately, no SMTP |
| MS probe returns `invalid` | Skip this candidate, try next |
| MS probe returns `unknown` / `error` | Fall through to SMTP backends |
| Either SMTP backend returns `valid` or `catch_all` | VALIDATED, Zuhal not called |
| Both SMTP backends return `invalid` | Zuhal rescue |
| Either SMTP backend returns `error` | Re-queue without burning attempt |
| `"tunnel not up"` in Racknerd message | Re-queue without burning attempt |
| `cost_tracker.ceiling_reached()` before Zuhal | COST_SKIPPED |
| All candidate_emails exhausted | VALIDATION_FAILED |

---

## Status fields reference

### `record_state` — lifecycle state machine

```
RAW → DISCOVERING → DISCOVERY_FAILED
           ↓
       DISCOVERED → VALIDATING → VALIDATED
                              ↘ VALIDATION_FAILED
                              ↘ COST_SKIPPED
```

| State | Set by | Meaning |
|---|---|---|
| `RAW` | db init | Loaded from input, not yet touched |
| `DISCOVERING` | producer (transient) | In-flight discovery; retried on restart |
| `DISCOVERED` | producer | Candidate emails found; queued for validation |
| `VALIDATING` | dispatcher | Claimed by dispatcher; in-flight |
| `VALIDATED` | dispatcher | At least one email confirmed deliverable |
| `VALIDATION_FAILED` | dispatcher | All candidates exhausted, none valid |
| `DISCOVERY_FAILED` | producer | No domain or email found via DNS or Serper |
| `COST_SKIPPED` | dispatcher | Cost ceiling hit before Zuhal rescue ran |

### `racknerd_verdict` — direct SMTP backend verdict

| Value | Meaning |
|---|---|
| `valid` | RCPT accepted (250) |
| `invalid` | RCPT rejected (5xx) |
| `catch_all` | Domain accepts all addresses |
| `error` | Network error, timeout, or SMTP protocol failure |
| `blocked` | Spamhaus / reputation block detected |
| `not_run` | Backend skipped (MS probe short-circuited) |
| `ms_valid` | Record confirmed via MS probe, not direct SMTP |

### `bbops_verdict` — async batch SMTP verdict

| Value | Meaning |
|---|---|
| `valid` | bbops confirmed deliverable |
| `invalid` | bbops rejected |
| `catch_all` | Domain accepts all (bbops signal) |
| `error` | API error, timeout, or poll failure |
| `not_run` | Backend skipped (MS probe short-circuited) |

### `final_verdict` — reconciled result written at terminal state

| Value | Written when |
|---|---|
| `valid` | Either backend (or Zuhal) confirmed valid |
| `catch_all` | Either backend returned catch_all (and none returned valid) |
| `invalid` | All backends rejected and Zuhal rescue also failed |

### `zuhal_status` — Zuhal API response or SMTP encoding tag

| Value | Meaning |
|---|---|
| `valid` | Zuhal rescue confirmed deliverable |
| `accept-all` | Zuhal rescue returned catch-all |
| `invalid` | Zuhal rescue also rejected |
| `error` | Zuhal API call failed |
| `circuit_open` | Zuhal circuit breaker open — record re-queued without burning attempt |
| `dual_valid` / `dual_catch_all` / `dual_invalid` | Zuhal NOT called; tag encodes the SMTP reconciliation result |
| `ms_valid` | Record confirmed by MS probe, Zuhal not called |

`zuhal_verdict` in the CSV strips the `dual_*` / `ms_valid` tags and shows `not_run` when Zuhal was not invoked.

---

## Output

### `valid_emails.csv` (written at shutdown, VALIDATED records only)

| Column | Description |
|---|---|
| `unique_id` | Composite key: `{filing_id}__{agent_id}` |
| `business_name` | Legal business name from filing |
| `agent_name` | Registered agent / officer name |
| `state` | State abbreviation (e.g. `NC`) |
| `email` | Confirmed deliverable email address |
| `final_verdict` | `valid` or `catch_all` |
| `confidence_tier` | `high` / `medium` / `low` (from `confidence_score`) |
| `confidence_score` | Raw additive pattern score 0–4 |
| `verified` | `True` if `valid` or `catch_all` |
| `discovery_method` | `dns`, `serper`, or `input` |
| `validation_method` | Which backend validated: `ms_probe`, `smtp_both`, `smtp_racknerd`, `smtp_bbops`, or `zuhal_rescue` |
| `racknerd_verdict` | Racknerd SMTP verdict for this email |
| `bbops_verdict` | bbops.io verdict for this email |
| `zuhal_verdict` | Zuhal rescue verdict, or `not_run` if Zuhal was not called |

**Confidence scoring** (additive, determines `confidence_tier`):
- Domain match (+1): email domain fuzzy-matches `candidate_domain`
- Strategy `with`: name match (+1), not generic prefix (+1), verdict=`valid` (+1)
- Strategy `without`: IS generic prefix (+1), verdict=`valid` (+1)
- High ≥ 3, medium = 2, low ≤ 1

### `results.json`

Run summary: total records, state counts, verdict counts, cost, timestamps. Written once at shutdown.

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

`--producer-only` runs without `RACKNERD_HOST`, `ZUHAL_API_KEY`. Use this for discovery-only runs.

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
| `--racknerd-direct` | off | Skip SOCKS5 tunnel; connect to MX servers directly (use when running on the egress VPS) |
| `--bbops-base-url URL` | bbops.io | Override bbops API base URL |
| `--max-consecutive-errors N` | 10 | Halt after N consecutive producer errors |

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

---

## Running tests

```bash
.venv/bin/python -m pytest tests/ -q            # all 201 tests
.venv/bin/python -m pytest tests/unit/ -q       # fast unit tests only
.venv/bin/python -m pytest tests/e2e/ -q        # end-to-end subprocess tests
```

---

## Architecture comparison

| Dimension | Before | After (fused) |
|---|---|---|
| Validation backend | Zuhal API only | Racknerd + bbops concurrent, Zuhal rescue |
| SMTP access | Via Zuhal's infra | Direct via SSH SOCKS5 to our VPS |
| bbops role | Standalone sync batch script | Async Dispatcher backend with crash recovery |
| Failure handling | Stalled run on Zuhal outage | Re-queue without burning attempt on errors |
| Zuhal role | Primary and only validator | Rescue only — after both SMTP say no |
| DB verdict storage | Single `zuhal_status` column | `racknerd_*` + `bbops_*` + `zuhal_status` + `confidence_score` |
| Schema version | v3 | v6 |
| Zuhal circuit failure | VALIDATION_FAILED written | Record re-queued as DISCOVERED (auto-heal) |
