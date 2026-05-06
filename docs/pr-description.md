## Problem

The ECC pipeline relied on a single paid validation backend (Zuhal) with no redundancy, and
bbops ran as a disconnected pre-pass batch script. The scraper.md architecture showed a
stronger pattern: two independent SMTP backends with OR-of-valids reconciliation, eliminating
single-backend failure modes and reducing per-email cost.

## Root Cause

The original design was sequential and single-path: one service to validate, one outcome.
When Zuhal had a bad day, the whole run stalled. When bbops ran pre-pass, any records it
would have caught went through Zuhal anyway. There was no fan-out, no fallback, no tunnel.
Discovery also relied on quoted legal business names in Serper queries — almost never matching
real search results — and only probed .com/.net/.org, missing fire departments and nonprofits
on .us and .info.

## Solution

Fused the best of both architectures into a three-backend Dispatcher with layered discovery
improvements:

- **Racknerd** (backend 1): Direct SMTP probe via SSH SOCKS5 tunnel to our VPS. Connects
  through python-socks → aiosmtplib → MX:25, runs EHLO/MAIL FROM/RCPT TO/RSET.
  SpamhausGuard tracks blocks in a 60s window and triggers a 300s cooldown. IP-level
  Spamhaus blocks (`blocked` status) re-queue the record immediately — Zuhal is not called
  since the block is against our IP, not the email. SSH tunnel supervisor auto-restarts with
  exponential backoff (2s → 60s). `--racknerd-direct` mode bypasses the tunnel for runs
  launched from the egress VPS itself.

- **bbops.io** (backend 2): Promoted from sync batch script to an async Dispatcher backend.
  Queue-based batching with asyncio.Future per email, adaptive drain, crash recovery via
  a `bbops_jobs` SQLite table persisted before polling begins.

- **Zuhal** (rescue backend): Runs only when both SMTP backends return definitively `invalid`.
  Zuhal is now required (not opt-in). A circuit breaker re-queues without burning
  `dispatch_attempts` when the circuit is open.

- **Discovery improvements**: Serper queries now use `normalize_business_name()` — no quoted
  legal names, no comma-format agent names. A 4th short-name fallback fires for businesses
  with 4+ significant words. DNS probes `.us` and `.info` in addition to `.com`/`.net`/`.org`.
  Organic domain fuzzy-match threshold lowered from 85% to 75%. Serper starts with an empty
  token bucket (`initial_tokens=0`) to prevent startup 429 bursts. Credit exhaustion
  (HTTP 400 "Not enough credits") now degrades gracefully to `DISCOVERY_FAILED` instead of
  halting the run; a `_credits_exhausted` flag prevents retry storms.

- **Operational hardening**: `run_checkpoints.sh` orchestrates 10×100-record batches with an
  interactive checkpoint review after each. ERR trap with structured failure output, disk
  space preflight (≥500MB), venv python detection, and a drain pass for any DISCOVERED
  records left after the final batch.

---

## Step-by-step flow

### Stage 1 — Producer

```
InputRecord (RAW)
    ├─ DNS probe: try domain stems + TLD variants (.com, .net, .org, .us, .info) → MX lookup
    │     hit  → candidate_emails (ranked by pattern_stats win rate per MX provider)
    │     miss → Serper enrichment
    │               query: normalize_business_name() — no quoted legal names
    │               fallbacks: site: scoped → agent-name → short-name (4+ words)
    │               hit  → candidate_emails
    │               miss → DISCOVERY_FAILED
    └─ DISCOVERED (candidate_emails, candidate_domain, mx_provider written to DB)
```

- One `aiodns.DNSResolver` per run. Serper results cached by `(business_name, agent_name, state, provider)`; bypass with `--ignore-cache`.
- Serper rate: 2 calls/sec sustained, burst cap 5, starts empty to prevent startup spike.
- Domains appearing as first-organic fallback for 2+ different businesses are promoted to the runtime blocklist.
- Confidence scoring (`confidence_score` 0–4) assigned at validation time; `confidence_tier` = high ≥ 3, medium = 2, low ≤ 1.

### Stage 2 — Dispatcher (per record, loops through each candidate email in rank order)

```
For each candidate_email:
    │
    ├─ [1] MS probe  (free, only when is_microsoft_mx(mx_provider) == True)
    │         valid   → VALIDATED  ✓
    │         invalid → skip candidate, try next
    │         unknown → fall through to SMTP
    │
    ├─ [2] Racknerd SMTP + bbops.io  ──── asyncio.gather (concurrent)
    │         ┌──────────────────────────────────────────────────────────────────┐
    │         │  reconcile(racknerd_verdict, bbops_verdict)                     │
    │         │                                                                  │
    │         │  valid / catch_all (either backend) → VALIDATED        ✓        │
    │         │  both invalid                        → Phase 3 (Zuhal)          │
    │         │  Racknerd blocked                    → re-queue, no burn        │
    │         │  mixed error / tunnel down           → re-queue, no burn        │
    │         └──────────────────────────────────────────────────────────────────┘
    │
    └─ [3] Zuhal rescue  (sequential, only when both SMTP returned invalid)
              valid / accept-all → VALIDATED          ✓
              circuit_open       → re-queue, no burn
              anything else      → try next candidate

All candidates exhausted → VALIDATION_FAILED
Cost ceiling hit before Zuhal → COST_SKIPPED
```

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
| `blocked` | any | — | re-queue, no burn (IP block, not email verdict) |
| `invalid` | `error`/`not_run` | `unknown` | re-queue, no burn |
| `error`/`not_run` | `invalid` | `unknown` | re-queue, no burn |
| `error` | `error` | `unknown` | re-queue, no burn |
| tunnel not up | any | `unknown` | re-queue, no burn |

**Re-queue without burn** = record returns to DISCOVERED; `dispatch_attempts` is not incremented.
Only terminal verdicts (valid, invalid, catch_all) increment the counter.

---

## Fields and status reference

### `record_state`

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

### CSV output columns

| Column | Description |
|---|---|
| `email` | Confirmed deliverable address |
| `final_verdict` | `valid` or `catch_all` |
| `confidence_tier` | `high` / `medium` / `low` |
| `confidence_score` | Additive score 0–4 (domain match, name match, generic prefix, verdict) |
| `verified` | `True` if valid or catch_all |
| `discovery_method` | `dns`, `serper`, or `input` |
| `validation_method` | `ms_probe`, `smtp_both`, `smtp_racknerd`, `smtp_bbops`, `zuhal_rescue` |
| `racknerd_verdict` | Raw Racknerd verdict |
| `bbops_verdict` | Raw bbops verdict |
| `zuhal_verdict` | Raw Zuhal verdict, or `not_run` |

---

## Architecture comparison

| Dimension | Before | After |
|---|---|---|
| Validation backend | Zuhal only | Racknerd + bbops concurrent, Zuhal rescue |
| SMTP access | Via Zuhal's infra | Direct via SSH SOCKS5 tunnel |
| bbops role | Standalone sync batch script | Async Dispatcher backend with crash recovery |
| IP block handling | Zuhal called anyway | `blocked` re-queues immediately, no Zuhal |
| Serper queries | Quoted full legal names | `normalize_business_name()`, no quotes |
| Serper startup | Full bucket → 429 burst | `initial_tokens=0`, throttled from start |
| Serper credit exhaustion | `PipelineHaltError` kills run | Degrades to `DISCOVERY_FAILED`, run continues |
| DNS TLDs probed | `.com`, `.net`, `.org` | + `.us`, `.info` |
| Zuhal role | Primary validator | Rescue only — after both SMTP say invalid |
| Schema version | v3 | v7 (additive migrations only; safe rollback) |
| Confidence field | `zuhal_score` | `confidence_score` (renamed; v7 migration backfills) |
| Dispatcher fallback costs | Not tracked | `_fallback_calls` reset and charged per record |

---

## Files changed

| File | Change |
|---|---|
| `pipeline/dispatcher.py` | New — three-backend coordinator |
| `pipeline/consumers/racknerd.py` | New — SSH SOCKS5 + direct SMTP prober |
| `pipeline/consumers/bbops_async.py` | New — async bbops backend with crash recovery |
| `pipeline/tunnels/ssh_socks.py` | New — supervised SSH tunnel with auto-restart |
| `pipeline/db.py` | Schema v7, versioned migrations, `update_record_dual()` |
| `pipeline/config.py` | `zuhal_api_key` required; Racknerd/dispatch/ignore_cache fields |
| `pipeline/models.py` | `BackendVerdict`, `ReconcileResult`, `confidence_score` |
| `pipeline/constants.py` | DNS TLDs expanded; `SERVICE_BACKOFF` + racknerd |
| `pipeline/__main__.py` | Wire dispatcher, tunnel, consumers; `_validation_method`, `_zuhal_verdict` |
| `pipeline/cli.py` | `--racknerd-host`, `--racknerd-direct`, `--dispatch-*`, `--ignore-cache` |
| `pipeline/producer.py` | Serper burst fix, `initial_tokens=0`, `ignore_cache` wired |
| `pipeline/utils/serper_client.py` | Normalized queries, 4th fallback, credit exhaustion, `ignore_cache` |
| `pipeline/utils/rate_limiter.py` | `initial_tokens` parameter |
| `pipeline/metrics.py` | `racknerd_probes`, `bbops_probes`, `backend_disagreements` |
| `scripts/run_checkpoints.sh` | New — 10×100 checkpoint runner with ERR trap + disk preflight |
| `scripts/reset.sh` | New — re-queue helper |
| `scripts/smoke-test.sh` | New — quick wiring check |
| `pipeline/consumer.py` | Deleted — replaced by `dispatcher.py` + `consumers/` |
| `pipeline/bbops.py` | Deleted — replaced by `consumers/bbops_async.py` |

---

## Test coverage

| File | Type | What it covers |
|---|---|---|
| `tests/unit/test_reconcile.py` | Unit | OR-of-valids combinations (15 cases) |
| `tests/unit/test_racknerd.py` | Unit | SpamhausGuard, tunnel-down, DNS failure, catch_all |
| `tests/unit/test_bbops_async.py` | Unit | Health state machine, Future resolution, failure paths |
| `tests/unit/test_ssh_socks.py` | Unit | `is_up()`, port probe, missing binary, `stop()` |
| `tests/unit/test_rate_limiter.py` | Unit | `initial_tokens=0`, custom start, default full |
| `tests/unit/test_serper_client.py` | Unit | Cache bypass, normalized queries, 4th fallback, credit exhaustion |
| `tests/unit/test_output_helpers.py` | Unit | `_validation_method`, `_zuhal_verdict`, DNS TLDs, migration warnings |
| `tests/integration/test_dispatcher.py` | Integration | Reconciliation, blocked re-queue, fallback cost accounting |
| `tests/integration/test_bbops_async.py` | Integration | Submit→poll cycle, crash recovery, DB failures |
| `tests/integration/test_pipeline_flow.py` | Integration | State transitions, `update_record_dual`, pattern stats |
| `tests/e2e/test_full_run.py` | E2E | Producer subprocess runs, `--limit`, `--db` |

**250 tests, 0 failures.**

---

## Technical Details

- MS probe short-circuits before any paid backend for Microsoft-managed MX domains (free)
- `blocked` Racknerd verdict re-queues immediately without calling Zuhal — the block is against our egress IP, not the email address; Zuhal would waste credits confirming a valid email we can't deliver from our IP
- Serper `_fallback_calls` tracked and charged to `cost_tracker` in both producer and dispatcher after each record; counter reset to 0 so records don't inherit prior counts
- `_credits_exhausted` flag on `SerperClient` — once set, all subsequent `enrich()` calls return empty immediately without HTTP; prevents retry storms after credit depletion
- Schema migrations are versioned (`migration_sets` list of `(target_version, stmts)` tuples); fresh installs and old DBs both handled without spurious warnings
- Stop-event safety: dispatcher skips writing verdicts on graceful shutdown; stale `VALIDATING` rows recovered via `recover_stale_validating()` on next start
- Cost ceiling checked before Zuhal only — Racknerd and bbops have no per-call cost
- Pattern learning: after every terminal verdict, `email_to_template()` + `record_pattern_result()` updates `pattern_stats`; next run for same MX provider ranks patterns by historical win rate

## Testing Instructions

```bash
# 1. Install
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

# 2. Full test suite
.venv/bin/python -m pytest tests/ -q
# → 250 tests, 0 failures

# 3. Dry run (no SSH tunnel or API keys needed)
python -m pipeline run -i input/nc_retry_300k.jsonl --limit 10 \
  --dry-run --producer-only --name test_pr

# 4. Verify DB state
python -m pipeline status --db output/test_pr/pipeline.db

# 5. Live 1k-record run with checkpoint reviews
bash scripts/run_checkpoints.sh

# 6. Verify three-backend verdicts
sqlite3 output/nc_1k/pipeline.db \
  "SELECT racknerd_status, bbops_status, zuhal_status, final_verdict FROM records LIMIT 20"
```

## Rollback Plan

Schema migrations are additive (ALTER TABLE only, no drops). Reverting this branch leaves
extra nullable columns that old code ignores. The `bbops_jobs` table can be dropped safely.
`consumer.py` and `bbops.py` are deleted — restore from `main` if rollback is needed.
`git revert` the merge commit restores the full pre-fused state.

## Notes

- `ZUHAL_API_KEY` is now required at startup (not opt-in); use `--dry-run` or `--producer-only` to bypass
- `--ignore-cache` bypasses Serper enrichment cache — useful when re-running against fresh data
- `--racknerd-direct` skips the SSH tunnel — use when running the pipeline from the egress VPS itself
- Prometheus metrics at `:9090/metrics` expose `racknerd_probes`, `bbops_probes`, `backend_disagreements` in addition to existing state/cost counters
