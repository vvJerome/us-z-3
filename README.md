# us-z-3 — Email Contact Collector (ECC)

Discovers and validates business email addresses from US state business-filing records
(NC / MI / IL / …) using an async pipeline whose SMTP layer is a **self-managing fleet of
Cherry Servers** running **concurrently with bbops.io** under an OR-of-valids policy, backed
by a free Microsoft probe and a paid Zuhal rescue.

Input: JSONL of business + registered-agent records.
Output: `valid_emails.csv` (confirmed emails), `results.json` (run summary), `pipeline.db` (full audit trail).

---

## Quick setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Minimum to run: SERPER_API_KEY  +  one SMTP egress source:
#   - a RackNerd VPS:        RACKNERD_HOST (+ optional RACKNERD_SSH_USER / RACKNERD_SSH_KEY)
#   - a Cherry fleet:        CHERRY_AUTH_TOKEN + CHERRY_PROJECT_ID, run with --cherry-enabled
#   - explicit worker IPs:   --smtp-hosts a.b.c.d e.f.g.h
# ZUHAL_API_KEY is optional (empty = rescue disabled). ZEROBOUNCE_API_KEY is only for the
# post-pipeline ground-truth ingest.
```

---

## Running

```bash
# Dry run — no API calls, confirms wiring
python -m pipeline run -i input/nc_retry_300k.jsonl --limit 50 --dry-run --name test

# Live run against the Cherry fleet, with website harvest and a cost ceiling
python -m pipeline run -i input/<file> --cherry-enabled --harvest \
  --max-cost 1.00 --name run_$(date +%Y%m%d)

# Live run against a single RackNerd VPS (SSH SOCKS5 tunnel)
python -m pipeline run -i input/<file> --racknerd-host <host> --name run_$(date +%Y%m%d)

# Dispatcher only (re-run validation without re-discovery)
python -m pipeline run --consumer-only --cherry-enabled --name <run>

# Check status (add --watch 5 to poll every 5s)
python -m pipeline status --db output/<run>/pipeline.db

# Re-queue discovery failures for retry
python -m pipeline reset --db output/<run>/pipeline.db --status discovery_failed

# Patient retry pass: re-queue ONLY the unverified (timed-out / no-answer) failures, leaving
# definitive-invalid records terminal, then re-run the dispatcher with a longer timeout.
python -m pipeline reset --db output/<run>/pipeline.db --status validation_failed --unverified-only
RACKNERD_SMTP_TIMEOUT_S=25 python -m pipeline run --consumer-only --cherry-enabled \
  --name <run> --max-dispatch-attempts 5
```

Via the orchestrator (wraps the pipeline with per-officer ID generation and output merging):

```bash
python -m orchestrator --input input/<file> --run-name nc_full
python -m orchestrator --resume runs/nc_full_<ts>/   # resume from manifest
```

---

## SMTP egress — Cherry Servers fleet

The SMTP verification layer migrated from a single RackNerd VPS to a **self-managing fleet of
Cherry Servers** (hourly, API-provisioned). Architecture: a central coordinator opens one SSH
SOCKS5 tunnel per worker; **each worker is just a stateless SMTP egress IP** (sshd + outbound
port 25). All authoritative state stays in the coordinator's `pipeline.db`, so no state lives
on any VPS. The fleet implements the same `verify(email)` dispatcher seam, so the rest of the
pipeline is unchanged.

- **Two co-equal checkers:** the Cherry fleet and bbops run **concurrently** (`asyncio.gather`)
  under OR-of-valids — bbops is not a fallback. Their independence (separate infra / IPs / job
  state) is the redundancy. RackNerd is retained only as an optional failover worker; Zuhal
  rescue is unchanged.
- **Live self-management** (`pipeline/fleet/`): monitors each worker's IP-reputation/health and
  **auto-heals** a degraded worker (drain → terminate → reprovision a fresh IP → reattach)
  without pausing the run; **load-balances** to the least-loaded healthy worker; **scales** via
  a control file or `--fleet-autoscale`. Guards: a credit floor and a per-run reprovision cap.
- **Per-(worker, provider) telemetry** in `smtp_outcomes` drives provider-aware routing and
  reroute-on-block.

```bash
# Provision a 4-worker fleet (last one in a reserve region for cross-region redundancy)
python -m pipeline.fleet provision --count 4 --reserve-region US-Chicago
python -m pipeline.fleet status
# Run the pipeline against the fleet
python -m pipeline run -i input/<file> --cherry-enabled
# Scale mid-run / tear down
echo '{"scale_to": 6}' > output/fleet/control.json
python -m pipeline.fleet teardown --yes        # deletes only fleet-provisioned servers
```

### Autonomous benchmark (provision → validate → tear down)

```bash
# Verdict distribution only
python -m pipeline.fleet benchmark --input input/<file> --count 5
# With a deliverability-accuracy score (email,zb_status CSV ground truth)
python -m pipeline.fleet benchmark --input input/<file> --count 5 --ground-truth gt.csv
```

Always tears the fleet down (a `finally` plus a backup trap, so a crash/Ctrl-C never leaks
servers). Zuhal rescue is off by default in the benchmark; pass `--with-zuhal` to keep it on.

### Throughput tuning (≥5k records/hour on a 5-worker fleet)

The dominant limiter is per-recipient-domain serialization (free-mail domains dominate real
data — gmail alone is ~30% of some sets). Knobs (env or flag):

| Setting | Default | High-throughput | Effect |
|---|---|---|---|
| `FLEET_DOMAIN_CONCURRENCY` | `10` | `10–15` | unblock gmail/yahoo; too high → provider 421s on cold IPs |
| `FLEET_BLOCK_COOLDOWN_S` | `120` | `60` | a 421-blocked worker recovers fast instead of stalling the fleet |
| `--dispatch-backend-timeout-s` | `60` | `30` | caps the bbops-rescue wait on fleet-non-valid records |
| `--dispatch-concurrency` | `50` | `100` | keep moderate — over-driving cold IPs *lowers* throughput |

The ceiling is provider rate-limiting of **cold** IPs; to go higher, scale out to more IPs
(`--count` / `scale_to`) rather than driving each IP harder.

### Single RackNerd VPS (alternative)

```bash
python -m pipeline run -i input/<file> --racknerd-host <host>   # SSH SOCKS5 tunnel
python -m pipeline run -i input/<file> --racknerd-direct        # when running ON the egress VPS
```

`RACKNERD_HELO_HOSTNAME` overrides the SMTP HELO/MAIL FROM FQDN; the fleet otherwise falls back
to each worker's rDNS/PTR.

Durable state backup to R2/S3 is available behind `BACKUP_ENABLED=true` (`pipeline/storage/`).

---

## Observability

During a run the dispatcher serves Prometheus metrics at `http://localhost:9090/metrics`
(plain-text exposition format; best-effort — if the port is busy the run logs a warning and
continues without it). For a live DB summary use:

```bash
python -m pipeline status --db output/<run>/pipeline.db --watch 5
```

---

## Step-by-step processing flow

### Stage 1 — Producer (`producer.py`)

```
InputRecord (RAW)
    │
    ├─ DNS probe: domain stems + TLD variants (.com .net .org .us .info) → MX lookup
    │     hit  → candidate_emails (ranked by pattern_stats win rate per MX provider)
    │     miss → Serper enrichment (normalize_business_name — no quoted legal names)
    │               primary → site: scoped → agent-name → short-name (4+ words)
    │               hit → candidate_emails    miss → DISCOVERY_FAILED
    │
    └─ DISCOVERED (candidate_emails, candidate_domain, mx_provider,
                   domain_confidence, owner_confidence written to DB)
```

- One `aiodns.DNSResolver` per run; Serper cached by `(business_name, agent_name, state, provider)`; bypass with `--ignore-cache`.
- Serper starts with an empty token bucket (`initial_tokens=0`) — no startup 429 bursts; credit exhaustion degrades to `DISCOVERY_FAILED` rather than halting.
- Domains appearing as first-organic fallback for 2+ businesses are promoted to a runtime blocklist.
- `owner_confidence` (registered-agent → owner likelihood) and `domain_confidence` (business↔domain match) are heuristic scores computed at discovery.

### Stage 2 — Dispatcher (`dispatcher.py` + `reconcile.py` + `dispatch_probes.py` + `dispatch_verdicts.py`)

The dispatcher claims DISCOVERED records and loops through each `candidate_email` in ranked order:

```
For each candidate_email:
    │
    ├─ [1] MS probe (free, Microsoft-managed MX only)
    │         valid → VALIDATED ✓   invalid → skip candidate   unknown → fall through
    │
    ├─ [2] Cherry fleet (SMTP) + bbops.io  ──── asyncio.gather (concurrent, co-equal)
    │         reconcile() applies OR-of-valids:
    │           valid / catch_all (either)  → VALIDATED ✓ (short-circuits on first valid)
    │           both invalid                 → try next candidate
    │                                           (Zuhal rescue is opt-in: --zuhal-on-both-invalid)
    │           SMTP inconclusive            → Zuhal rescue if configured, else re-queue
    │             (1 invalid + 1 error;         (no attempt burned unless a backend was definitive)
    │              both error/blocked)
    │           tunnel down                   → re-queue, no attempt burned
    │
    ├─ pattern candidates exhausted → free website harvest (--harvest) → paid Serper fallback
    │
    └─ [3] Zuhal rescue (paid) — by default rescues the SMTP-INCONCLUSIVE records (not the
              both-invalid ones) via a decoupled NEEDS_ZUHAL worker pool (zuhal_decoupled, default on).
              valid / accept-all → VALIDATED ✓   circuit_open → re-queue, no burn

All candidates exhausted → VALIDATION_FAILED      Cost ceiling hit before Zuhal → COST_SKIPPED
```

- **Free harvest before paid Serper:** when every pattern candidate fails SMTP, the dispatcher
  scrapes the business's own site (`pipeline/harvest/`, opt-in via `--harvest`) for emails and
  officers — learning the house local-part convention — before paying for the Serper fallback.
- **Zuhal rescues what SMTP can't decide, not what it rejects:** the paid Zuhal pass runs on the
  *inconclusive* set (one backend invalid + the other errored, or both inconclusive), not on
  records both backends definitively rejected. Add `--zuhal-on-both-invalid` to also rescue
  both-invalid records. With `ZUHAL_API_KEY` empty, inconclusive records re-queue instead.
- `dispatch_attempts` increments only when a backend gave a definitive verdict; pure-infra
  outcomes (tunnel down, Zuhal circuit-open, both-inconclusive) re-queue without burning the
  attempt budget (`--max-dispatch-attempts`, default 5; `--max-requeue-count`, default 15).

---

## Validation logic — OR-of-valids reconciliation

`reconcile()` (`pipeline/reconcile.py`) is pure decision logic. `not_run` means a backend was
intentionally disabled and is treated as definitive; `error`/`blocked` are inconclusive.

| Cherry/RackNerd SMTP | bbops | `reconcile()` | Default action |
|---|---|---|---|
| `valid` | any | `valid` | VALIDATED |
| any | `valid` | `valid` | VALIDATED |
| `catch_all` | any | `catch_all` | VALIDATED (gated by `catch_all_min_confidence`) |
| any | `catch_all` | `catch_all` | VALIDATED |
| `invalid` | `invalid` / `not_run` | `invalid` | try next candidate → VALIDATION_FAILED (Zuhal only with `--zuhal-on-both-invalid`) |
| `invalid` / `not_run` | `invalid` | `invalid` | same as above |
| `invalid` | `error`/`blocked` | `unknown` | Zuhal rescue if configured, else re-queue (no burn) |
| `error`/`blocked` | `invalid` | `unknown` | Zuhal rescue if configured, else re-queue (no burn) |
| `error`/`blocked` | `error`/`blocked` | `unknown` | Zuhal rescue if configured, else re-queue (no burn) |
| tunnel not up | any (no positive) | `unknown` | re-queue, no burn |

A `valid`/`catch_all` from **either** backend wins even when the other's tunnel is down — their
independence is the redundancy. **Re-queue** returns the record to `DISCOVERED`; the attempt
budget is charged only when at least one backend returned a definitive verdict.

---

## Canonical verdicts

Read `canonical_status` for the standardized outcome — never branch on raw per-service columns.
Every provider status is normalized in one place (`pipeline/verdicts.py::normalize_verdict()`).

| Column | Meaning |
|---|---|
| `canonical_status` | Single normalized verdict: `valid`, `invalid`, `catch_all`, `unknown`, `do_not_mail`, `abuse`, `disposable` |
| `canonical_source` | Which service set it: `zerobounce` (ground truth) > `zuhal` > `smtp` > `ms_probe` |
| `canonical_sub_status` | Provider sub-status (mainly ZeroBounce `role`/`toxic`/…) |
| `reconciliation_path` | Holds the `dual_*`/`ms_valid` SMTP-reconciliation signal when Zuhal didn't run |

ZeroBounce is the ground-truth final layer and runs as a separate script
(`pipeline.ops.ingest_zerobounce`); its ingest overrides `canonical_status`/`canonical_source`
for matched records and feeds unambiguous `valid`/`invalid` verdicts back into `pattern_stats`
(continuous learning). Ingest a given ZB CSV once — the pattern feedback is not idempotent.

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

### `racknerd_status` (Cherry/RackNerd SMTP; DB column `racknerd_status`)

| Value | Meaning |
|---|---|
| `valid` | RCPT accepted (250) |
| `invalid` | RCPT rejected (5xx) |
| `catch_all` | Domain accepts all addresses |
| `error` | Network error, timeout, or SMTP protocol failure |
| `blocked` | Spamhaus/reputation IP-level block; re-queued |
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
| `dual_valid` / `dual_catch_all` / `dual_invalid` | Zuhal NOT run; encodes SMTP result (see `reconciliation_path`) |
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
| `canonical_status` | Standardized verdict (`valid`/`catch_all`/…); read this, not per-service columns |
| `canonical_source` | Which service set it (`zerobounce`/`zuhal`/`smtp`/`ms_probe`) |
| `final_verdict` | Reconciled SMTP/Zuhal verdict: `valid` or `catch_all` |
| `confidence_tier` / `confidence_score` | `high`/`medium`/`low` and the raw additive score 0–4 |
| `domain_confidence` / `domain_confidence_tier` | 0–1 business↔domain match + its tier |
| `owner_confidence` / `owner_confidence_tier` | 0–1 agent-is-owner likelihood + its tier |
| `verified` | `True` if `valid` or `catch_all` |
| `discovery_method` | `dns`, `serper`, `serper_fallback`, `input` |
| `validation_method` | `ms_probe`, `smtp_both`, `smtp_racknerd`, `smtp_bbops`, `zuhal_rescue` |
| `racknerd_verdict` / `bbops_verdict` / `zuhal_verdict` | Per-backend verdicts (`not_run` if not invoked) |
| `zb_status` / `zb_sub_status` | ZeroBounce verdict (blank until the ZB ingest runs) |

**Confidence scoring** (additive): domain match (+1); strategy `with` → name match (+1), not a
generic prefix (+1), verdict=`valid` (+1); strategy `without` → IS a generic prefix (+1),
verdict=`valid` (+1). High ≥ 3, medium = 2, low ≤ 1.

**Owner-confidence** (`pipeline/utils/owner_inference.py`, additive, capped at 1.0): commercial
registered-agent → `0.0`; org agent → `0.1`; named individual base `0.2` + surname∈business name
(+0.4) + owner-ish position (+0.3) + has website (+0.1). Tiers: high ≥ 0.6, medium ≥ 0.3, low < 0.3.

`valid_emails.csv` and `results.json` are written once at shutdown; `pipeline.db` is the
authoritative record and the CSV/JSON are derived views.

---

## Environment variables

All live in `.env` (gitignored); see `.env.example`. Only `SERPER_API_KEY` plus **one** SMTP
egress source (RackNerd host, `SMTP_HOSTS`, or a Cherry fleet) are required.

**Core**

| Variable | Required | Default |
|---|---|---|
| `SERPER_API_KEY` | Yes | — |
| `ZUHAL_API_KEY` | For Zuhal rescue (empty = disabled) | — |
| `ZEROBOUNCE_API_KEY` | For the post-pipeline ZB ingest only | — |
| `BBOPS_BASE_URL` | No | `https://email-verifier.bbops.io` |

**SMTP egress — single RackNerd VPS / per-worker SMTP tuning**

| Variable | Required | Default |
|---|---|---|
| `RACKNERD_HOST` | Yes* | — |
| `RACKNERD_SSH_USER` | No | `egress` |
| `RACKNERD_SSH_KEY` | No | `~/.ssh/racknerd_egress` |
| `RACKNERD_HELO_HOSTNAME` | No — SMTP HELO/MAIL FROM FQDN; fleet falls back to worker rDNS/PTR | — |
| `RACKNERD_CONCURRENCY` | No | `25` |
| `RACKNERD_SMTP_TIMEOUT_S` | No | `8.0` |

\* Not required when `--cherry-enabled`, `--smtp-hosts`, or `--racknerd-direct` is set.

**Cherry Servers SMTP fleet**

| Variable | Required | Default |
|---|---|---|
| `CHERRY_AUTH_TOKEN` | For provisioning / auto-heal | — |
| `CHERRY_PROJECT_ID` | For the fleet | — |
| `CHERRY_TEAM_ID` | For the auto-heal credit guard | — |
| `CHERRY_REGION` | No | `EU-Nord-1` |
| `CHERRY_PLAN` | No | `B2-1-1gb-20s-shared` |
| `CHERRY_SSH_KEY` | No — private key; `<path>.pub` is registered with Cherry | `~/.ssh/cherry_fleet` |
| `SMTP_HOSTS` | No — explicit worker IPs (JSON list); overrides inventory | `[]` |
| `FLEET_CREDIT_FLOOR_EUR` | No | `0.10` |
| `FLEET_MAX_REPROVISIONS` | No | `10` |
| `FLEET_SCALE_MAX` / `FLEET_DOMAIN_CONCURRENCY` / `FLEET_BLOCK_COOLDOWN_S` | No | `10` / `10` / `120` |

**Durable state backup to R2/S3 (off by default)**

| Variable | Required | Default |
|---|---|---|
| `BACKUP_ENABLED` | No — master switch | `false` |
| `BACKUP_R2_ENDPOINT` | If `BACKUP_ENABLED` — S3-compatible endpoint incl. bucket | — |
| `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` | If backing up to R2 | — |
| `BACKUP_INTERVAL_S` | No | `300` |

---

## Key CLI flags

Run `python -m pipeline run --help` for the full list (bbops/Zuhal tuning, etc.).

| Flag | Default | Effect |
|---|---|---|
| `--limit N` | none | Process only first N records |
| `--dry-run` | off | Mock all API calls; no cost |
| `--max-cost USD` | none | Stop when cumulative cost reaches limit |
| `--name NAME` | none | Output to `output/NAME/` |
| `--producer-only` / `--consumer-only` | off | Discovery only / dispatcher only |
| `--strategy {auto,with,without}` | auto | Email local-part strategy (drives confidence scoring) |
| `--start-offset N` | 0 | Skip the first N input lines (sharding / resume) |
| `--harvest` | off | Scrape the business website (free) before the paid Serper fallback |
| `--ignore-cache` | off | Bypass the Serper enrichment cache |
| `--chunk-size N` | 100 | Records per producer batch |
| `--dns-concurrency N` | 100 | Parallel DNS semaphore size |
| `--dispatch-concurrency N` | 50 | Parallel dispatcher workers |
| `--dispatch-backend-timeout-s S` | 60.0 | Per-backend timeout (Cherry/RackNerd + bbops) |
| `--dispatch-chunk-size N` | 50 | Records claimed per dispatcher poll cycle |
| `--max-dispatch-attempts N` | 5 | Real-verdict attempts before VALIDATION_FAILED |
| `--max-requeue-count N` | 15 | Total re-queue safety valve against infra loops |
| `--cherry-enabled` | off | Use the Cherry Servers SMTP fleet as the SMTP backend |
| `--smtp-hosts IP …` | — | Explicit worker IPs (overrides single `--racknerd-host`) |
| `--cherry-fleet-size N` / `--cherry-region R` | 4 / `EU-Nord-1` | Fleet target size / region |
| `--fleet-autoscale` | off | Queue-depth autoscaling of the fleet |
| `--racknerd-host HOST` | — | Single VPS hostname for the SSH tunnel |
| `--racknerd-direct` | off | Skip SOCKS5 tunnel; probe MX directly (when on the egress VPS) |
| `--racknerd-concurrency N` | 25 | Parallel SMTP connections per worker |
| `--racknerd-helo FQDN` | — | SMTP EHLO/MAIL FROM domain (real FQDN; IP literals are rejected) |
| `--no-racknerd` | off | Disable the RackNerd backend |
| `--bbops-base-url URL` | bbops.io | Override the bbops API base URL |

`reset` flags: `--status {discovery_failed,validation_failed,cost_skipped}`, `--phase {dns,serper}`,
`--unverified-only` (re-queue only verdict-less failures), `--dry-run`.

---

## Costs

| Service | Per call | Notes |
|---|---|---|
| Serper | $0.001 | 1 call per record (producer); fallback after patterns exhausted (dispatcher) |
| Cherry / RackNerd SMTP | $0 per probe | Hourly/fixed VPS cost; no per-probe fee |
| bbops | Per contract | Async batch verifier; co-equal SMTP checker |
| MS probe | $0 | Free; short-circuits Microsoft 365 / Exchange Online domains |
| Zuhal | $0.0005 | Rescue for SMTP-inconclusive records (decoupled queue); both-invalid only with `--zuhal-on-both-invalid` |

Typical Serper-only cost: ~$0.001/record (~$300 for 300k). Zuhal rescue adds ~$0.0005 per record
SMTP can't decide (typically 5–15% of records).

---

## Running tests

```bash
.venv/bin/python -m pytest tests/ -q        # 669 tests, 0 failures
.venv/bin/python -m pytest tests/unit/ -q   # fast unit tests only
.venv/bin/python -m pytest tests/e2e/ -q    # end-to-end subprocess tests
```
