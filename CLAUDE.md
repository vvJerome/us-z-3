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

# Patient retry pass: re-queue ONLY the "couldn't verify" failures (timed out / no
# answer), leaving definitive-invalid records terminal, then re-run the dispatcher
# with a longer timeout + more attempts so greylisting holds get a fair retry.
python -m pipeline reset --db output/<run>/pipeline.db --status validation_failed --unverified-only
RACKNERD_SMTP_TIMEOUT_S=25 python -m pipeline run --consumer-only --cherry-enabled \
  --name <run> --max-dispatch-attempts 5
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
│   ├── dispatcher.py       # Backend coordinator: MS/Racknerd/bbops + candidate loop
│   ├── reconcile.py        # OR-of-valids policy + greylisting (pure decision logic)
│   ├── dispatch_probes.py  # Backend probe wrappers (ms/zuhal/serper/racknerd/bbops)
│   ├── dispatch_verdicts.py# Zuhal-rescue verdict handling for the candidate loop
│   ├── verdicts.py         # Canonical verdict vocabulary (normalize_verdict + sources)
│   ├── harvest/            # Website harvester: free email/officer scrape (--harvest)
│   │   ├── __init__.py     # harvest(domain) → HarvestResult orchestration
│   │   ├── fetch.py        # curl_cffi AsyncSession + robots.txt + rate limit
│   │   ├── extract.py      # PURE: email/officer extraction + house-convention inference
│   │   └── models.py       # HarvestResult dataclass
│   ├── db/                 # SQLite data layer, split by responsibility
│   │   ├── __init__.py     # Re-exports the full surface (from pipeline import db)
│   │   ├── schema.py       # DDL, migrations, State machine, init_db
│   │   ├── records.py      # Record lifecycle + verdict writes (update_record_dual)
│   │   ├── zuhal_queue.py  # NEEDS_ZUHAL handoff/claim/recover helpers
│   │   ├── meta.py         # checkpoints, stats, failures, heartbeats, status summary
│   │   ├── patterns.py     # pattern_stats read/write
│   │   ├── enrichment.py   # enrichment_cache + serper_enriched flag
│   │   └── bbops_jobs.py   # in-flight bbops batch job tracking
│   ├── manifest.py         # SQLite email-state store + CSV ingest helpers
│   ├── models.py           # InputRecord, EnrichmentResult, ValidationResult dataclasses
│   ├── config.py           # PipelineConfig (pydantic-settings, reads .env)
│   ├── cli.py              # argparse definitions
│   ├── constants.py        # API costs, backoff, DNS/Serper tuning, provider lists, blocklist
│   ├── metrics.py          # Prometheus /metrics endpoint (port 9090)
│   ├── ops/                # Operator-facing tools (post-pipeline workflows)
│   │   ├── manifest_init.py        # Backfill manifest from existing CSV outputs
│   │   ├── passoff_watcher.py      # Drip-feed daemon: ingest results → append to combined CSV
│   │   ├── zuhal_bulk.py           # Submit NEEDS_ZUHAL CSVs to Zuhal Bulk API
│   │   ├── zb_zuhaled.py           # Submit /zuhaled CSVs to ZeroBounce (--min-confidence gate)
│   │   ├── ingest_zerobounce.py    # Join /zerobounced CSV back to records (ZB = ground truth)
│   │   ├── zuhal_rescue.py         # Standalone Zuhal rescue pass over VALIDATION_FAILED
│   │   ├── normalize_zuhaled.py    # Upgrade legacy {Email,Status} zuhaled files
│   │   ├── requeue_zuhal_429_burns.py  # Recover records burned by Zuhal 429 bug
│   │   └── build_summary.py        # Write summary_counts.csv (hardcoded May 2026 run)
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
├── scripts/                # Shell entry points only — all logic lives in pipeline/ops/
│   ├── _common.sh          # Shared SSH/rsync helpers (sourced by other scripts)
│   ├── check.sh            # Snapshot of active pipeline run on VPS
│   ├── clean.sh            # Delete stale output dirs (--force to actually delete)
│   ├── deploy.sh           # rsync project to VPS + install deps
│   ├── logs.sh             # Tail pipeline logs
│   ├── manifest_init.sh    # → pipeline.ops.manifest_init
│   ├── normalize_zuhaled.sh# → pipeline.ops.normalize_zuhaled
│   ├── passoff_watcher.sh  # → pipeline.ops.passoff_watcher
│   ├── requeue_zuhal_429_burns.sh # → pipeline.ops.requeue_zuhal_429_burns
│   ├── reset.sh            # Re-queue failed records helper
│   ├── run_checkpoints.sh  # 10×100-record batched run with interactive checkpoint reviews
│   ├── run_il.sh           # Deploy + start Illinois pipeline run on VPS
│   ├── run_parallel.sh     # Split input into N parallel workers + merge outputs
│   ├── setup.sh            # Wipe and re-provision VPS from scratch
│   ├── smoke-test.sh       # Quick wiring check (dry-run 10 records)
│   ├── start.sh            # Launch orchestrator in tmux
│   ├── status.sh           # Show DB status summary
│   ├── stop.sh             # Kill orchestrator session
│   ├── zb_zuhaled.sh       # → pipeline.ops.zb_zuhaled
│   ├── zuhal_bulk.sh       # → pipeline.ops.zuhal_bulk
│   └── zuhal_rescue.sh     # → pipeline.ops.zuhal_rescue
├── requirements.txt
├── pytest.ini
└── .env.example
```

---

## Record processing flow

### Stage 1 — Producer

```
InputRecord (RAW)
    ├─ DNS probe (aiodns, shared resolver, TLD variants: .com .net .org .us .info)
    │     hit  → candidate_emails ranked by pattern_stats win rate
    │     miss → Serper enrichment (normalize_business_name — no quoted legal names)
    │               primary → site: scoped → agent-name → short-name (4+ words)
    │               hit  → candidate_emails
    │               miss → DISCOVERY_FAILED
    └─ DISCOVERED (candidate_emails, candidate_domain, mx_provider written to DB)
```

- Serper starts empty (`initial_tokens=0`) — no startup 429 bursts.
- Serper credit exhaustion degrades to `DISCOVERY_FAILED`; run continues.
- Use `--ignore-cache` to bypass enrichment_cache on re-runs against fresh data.

### Stage 2 — Dispatcher (per candidate_email, in rank order)

```
[1] MS probe (free, Microsoft MX only)
        valid   → VALIDATED  ✓
        invalid → skip candidate
        unknown → continue

[2] Racknerd SMTP first; bbops only when Racknerd can't decide (sequential, lazy)
        Racknerd valid / catch_all          → VALIDATED ✓ (bbops skipped, bbops_status=not_run)
        Racknerd tunnel down                → re-queue, no burn
        else (blocked/error/invalid)        → run bbops, then reconcile():
          valid / catch_all (either backend) → VALIDATED  ✓
          blocked (Racknerd)                 → re-queue, no burn (IP block, not email verdict)
          both invalid                        → [3]
          mixed error / tunnel down           → re-queue (no attempt burned)

[3] Zuhal rescue  (sequential, only when both SMTP → invalid)
        valid / accept-all → VALIDATED  ✓
        circuit_open       → re-queue, no burn (auto-heal)
        else               → try next candidate

All pattern candidates fail → harvest (free, --harvest) → Serper fallback (paid) → more candidates
All candidates exhausted → VALIDATION_FAILED
Cost ceiling before Zuhal → COST_SKIPPED
```

### Website harvesting (`--harvest`, opt-in)

When every generated pattern candidate fails SMTP, the dispatcher scrapes the business's
own domain (`pipeline/harvest/`) **before** paying for the Serper fallback — free local work
first. It fetches `HARVEST_PATHS` (homepage + contact/about/team/…) via curl_cffi with a
browser TLS fingerprint, respects `robots.txt`, and is throttled by one global rate bucket.

- **Convention learning (item 2):** a scraped name paired to a harvested address (e.g. `john.smith@acme.com` + "John Smith") reveals the house template (`first.last`), which then generates *our* officer's address as a top-ranked candidate.
- **Officers (item 4):** names found near role keywords (Owner/Founder/President/…) feed extra candidates.
- Harvest spends no API budget; its candidates are tried before Serper, which is only called if harvest finds nothing.

### OR-of-valids reconciliation conditions

| Racknerd | bbops | Outcome | Note |
|---|---|---|---|
| `valid` | any | VALIDATED `valid` | |
| any | `valid` | VALIDATED `valid` | |
| `catch_all` | any | VALIDATED `catch_all` | |
| any | `catch_all` | VALIDATED `catch_all` | |
| `blocked` | any | re-queue | IP-level block; skip Zuhal |
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
| `dual_valid` / `dual_catch_all` / `dual_invalid` | Zuhal did NOT run; encodes the SMTP reconciliation result (legacy; see `reconciliation_path`) |
| `ms_valid` | MS probe short-circuited; Zuhal not called |

### `canonical_status` (the standardized verdict — read this, not the per-service columns)

Normalized in one place (`pipeline/verdicts.py`) across all services. One of:
`valid`, `invalid`, `catch_all`, `unknown`, `do_not_mail`, `abuse`, `disposable`.

| Column | Meaning |
|---|---|
| `canonical_status` | Single normalized verdict (`normalize_verdict()` collapses `accept-all`/`catch-all`→`catch_all`, `ms_valid`→`valid`, etc.) |
| `canonical_source` | Which service set it: `zerobounce` (ground truth) > `zuhal` > `smtp` > `ms_probe` |
| `canonical_sub_status` | Provider sub-status (mainly ZeroBounce's `role`/`toxic`/…) |
| `reconciliation_path` | De-overloads `zuhal_status`: holds `dual_*`/`ms_valid` when Zuhal didn't run |
| `domain_confidence` | 0–1 business-to-domain match confidence, computed at discovery |
| `zb_status` / `zb_sub_status` | ZeroBounce verdict, ingested by `pipeline.ops.ingest_zerobounce` |

ZeroBounce is the ground-truth final layer and runs as a separate script; its
ingest overrides `canonical_status`/`canonical_source` for matched records, and
feeds its unambiguous `valid`/`invalid` verdicts back into `pattern_stats`
(continuous learning) — `catch_all`/`unknown`/`do_not_mail`/etc. are skipped as
inconclusive for the naming convention. Ingest a given ZB CSV once (the pattern
feedback is not idempotent).

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
| `canonical_status` | Standardized verdict (`valid`/`catch_all`/…); read this, not per-service columns |
| `canonical_source` | Which service set canonical_status (`zerobounce`/`zuhal`/`smtp`/`ms_probe`) |
| `final_verdict` | Reconciled SMTP/Zuhal verdict: `valid` or `catch_all` |
| `confidence_tier` | `high` / `medium` / `low` (from `confidence_score`) |
| `confidence_score` | Raw additive pattern score 0–4 |
| `domain_confidence` / `domain_confidence_tier` | 0–1 business-to-domain match + its tier |
| `owner_confidence` / `owner_confidence_tier` | 0–1 likelihood the agent is the business owner + its tier (computed at discovery) |
| `zb_status` / `zb_sub_status` | ZeroBounce verdict (blank until the ZB ingest runs) |
| `verified` | `True` if `valid` or `catch_all`; `False` otherwise |
| `discovery_method` | How the email was found: `dns`, `serper`, `serper_fallback`, `input` |
| `validation_method` | Which backend validated: `ms_probe`, `smtp_both`, `smtp_racknerd`, `smtp_bbops`, `zuhal_rescue` |
| `racknerd_verdict` | Racknerd SMTP verdict for this email |
| `bbops_verdict` | bbops.io verdict for this email |
| `zuhal_verdict` | Zuhal rescue verdict, or `not_run` if Zuhal was not invoked |

**Confidence scoring** (additive):

- Domain match (+1): email domain fuzzy-matches the candidate domain
- Strategy `with`: name match (+1), not a generic prefix (+1), verdict=`valid` (+1)
- Strategy `without`: IS a generic prefix (+1), verdict=`valid` (+1)
- High ≥ 3, medium = 2, low ≤ 1

**Owner-confidence scoring** (`pipeline/utils/owner_inference.py`, additive, capped at 1.0):

- Commercial registered-agent service (`constants.COMMERCIAL_AGENT_NAMES`) → `0.0` (never the owner)
- Organization agent (`is_org_agent`) → `0.1`
- Otherwise a named individual: base `0.2` + surname∈business name (+0.4) + owner-ish `position_type` (+0.3) + has website (+0.1)
- Tiers: high ≥ 0.6, medium ≥ 0.3, low < 0.3
- Heuristic baseline (no ML); principal-address match from the spec is omitted — not in the NC input.

---

## Environment variables

All live in `.env` (gitignored); see `.env.example` for a copy-paste template. Only
`SERPER_API_KEY` plus one SMTP egress source (RackNerd host, `SMTP_HOSTS`, or a Cherry
fleet) are needed to run.

**Core**

| Variable | Required | Default |
|---|---|---|
| `SERPER_API_KEY` | Yes | — |
| `ZUHAL_API_KEY` | For Zuhal rescue (empty = rescue disabled) | — |
| `ZEROBOUNCE_API_KEY` | For the post-pipeline ZB ingest only | — |
| `BBOPS_BASE_URL` | No | `https://email-verifier.bbops.io` |

**SMTP egress — single RackNerd VPS / per-worker SMTP tuning**

| Variable | Required | Default |
|---|---|---|
| `RACKNERD_HOST` | Yes* | — |
| `RACKNERD_SSH_USER` | No | `egress` |
| `RACKNERD_SSH_KEY` | No | `~/.ssh/racknerd_egress` |
| `RACKNERD_HELO_HOSTNAME` | No — overrides SMTP HELO/MAIL FROM FQDN; fleet falls back to each worker's rDNS/PTR | — |
| `RACKNERD_CONCURRENCY` | No — parallel SMTP channels per worker | `25` |
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
| `SMTP_HOSTS` | No — explicit worker IPs as a JSON list; overrides the inventory | `[]` |
| `FLEET_CREDIT_FLOOR_EUR` | No — auto-heal refuses to provision below this | `0.10` |
| `FLEET_MAX_REPROVISIONS` | No — per-run auto-heal cap | `10` |
| `FLEET_SCALE_MAX` | No | `10` |

**Durable state backup to R2/S3 (off by default)**

| Variable | Required | Default |
|---|---|---|
| `BACKUP_ENABLED` | No — master switch | `false` |
| `BACKUP_R2_ENDPOINT` | If `BACKUP_ENABLED` — S3-compatible endpoint incl. bucket | — |
| `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` | If backing up to R2 | — |
| `BACKUP_DIR` | No — optional local copy alongside R2 | — |
| `BACKUP_INTERVAL_S` | No | `300` |

---

## Cherry Servers SMTP fleet (migration)

The SMTP layer migrated from a single RackNerd VPS to a **self-managing fleet of Cherry
Servers** (hourly, API-provisioned). Architecture: a central coordinator opens one SSH
SOCKS5 tunnel per worker; **each worker is just a stateless SMTP egress IP** (sshd +
outbound port 25). All authoritative state stays in the coordinator's `pipeline.db`, so
no state lives on any VPS (item 2). The fleet implements the same `verify(email)`
dispatcher seam, so the rest of the pipeline is unchanged.

- **Two co-equal checkers:** the Cherry fleet and bbops run **concurrently** under
  OR-of-valids — bbops is not a fallback. RackNerd is retained only as an optional
  failover worker. Zuhal rescue is unchanged.
- **Live self-management** (`pipeline/fleet/control.py`): monitors each worker's
  IP-reputation/health and **auto-heals** a degraded worker (drain → terminate →
  reprovision a fresh IP → reattach) without pausing the run; **load-balances** to the
  least-loaded healthy worker; **scales** via `scale_to` / a control file. Guards: a
  credit floor and a per-run reprovision cap.
- **Per-(worker, provider) telemetry** in `smtp_outcomes` drives provider-aware routing
  and reroute-on-block (item 5).

```bash
# Provision a 4-worker fleet (last one in a reserve region for item 6)
python -m pipeline.fleet provision --count 4 --reserve-region US-Chicago
python -m pipeline.fleet status
# Run the pipeline against the fleet
python -m pipeline run -i input/<file> --cherry-enabled
# Scale mid-run / tear down
echo '{"scale_to": 6}' > output/fleet/control.json
python -m pipeline.fleet teardown --yes        # deletes only fleet-provisioned servers
```

### Autonomous benchmark (provision → validate → tear down)

One command, default config — point it at a dataset and it provisions a fresh fleet,
waits for sshd, runs the real validation path, prints the SMTP verdict distribution, and
**always tears the fleet down** (a `finally` plus a backup trap, so a crash/`kill`/Ctrl-C
never leaks servers). No per-run scripts.

```bash
# Verdict distribution only
python -m pipeline.fleet benchmark --input input/<file> --count 5
# With a deliverability-accuracy score (email,zb_status CSV ground truth)
python -m pipeline.fleet benchmark --input input/<file> --count 5 --ground-truth gt.csv
scripts/cherry_benchmark.sh --input input/<file> --count 5   # thin wrapper, same flags
```

Zuhal rescue is off by default (it measures the SMTP fleet; pass `--with-zuhal` to keep
the paid rescue on). `summarize()` reports per-record **decisive accuracy** (definitive
verdicts that match ground-truth deliverability) and **coverage** (decided / attempted).

### Throughput tuning (≥5k records/hour on a 5-worker fleet)

The dispatcher short-circuits the SMTP fan-out on the first `valid` (a record the fleet
validates directly no longer waits on the batched bbops backend). Beyond that, the dominant
limiter is **per-recipient-domain serialization**: one shared semaphore caps concurrent
probes per domain across the whole fleet, and free-mail domains dominate real data (gmail
alone is ~30% of the Michigan set). The knobs (defaults already raised for fleets):

| Setting | Default | High-throughput | Effect |
|---|---|---|---|
| `FLEET_DOMAIN_CONCURRENCY` | `10` | `10–15` | unblock gmail/yahoo; too high → provider 421-rate-limits cold IPs |
| `FLEET_BLOCK_COOLDOWN_S` | `120` | `60` | a 421-blocked worker recovers fast instead of collapsing the fleet |
| `--dispatch-backend-timeout-s` | `60` | `30` | caps the bbops-rescue wait on fleet-non-valid records (trades a little coverage) |
| `--dispatch-concurrency` | `50` | `100` | keep moderate — over-driving cold IPs *lowers* throughput |

```bash
FLEET_DOMAIN_CONCURRENCY=10 FLEET_BLOCK_COOLDOWN_S=60 \
  python -m pipeline run -i input/<file> --cherry-enabled \
    --dispatch-concurrency 100 --dispatch-backend-timeout-s 30
```

Sustained ~6k/hour on 5 cold IPs with this. The ceiling is provider rate-limiting of
cold IPs (gmail throttles low-reputation IPs) — to go higher, scale out to more IPs
(`--count`/`scale_to`) rather than driving each IP harder.

Fleet package: `pipeline/fleet/` (cherry_client, provisioner, worker, health, balancer,
manager, control, wiring, benchmark, `__main__` = the provision/status/teardown/benchmark
CLI). Durable backup: `pipeline/storage/` (R2/S3 via SigV4, no boto3), enabled with
`BACKUP_ENABLED=true`.

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
| `--harvest` | off | Scrape the business website for emails/officers (free) before the paid Serper fallback |
| `--chunk-size N` | 100 | Records per producer batch |
| `--dns-concurrency N` | 100 | Parallel DNS semaphore size |
| `--dispatch-concurrency N` | 50 | Parallel dispatcher workers |
| `--dispatch-backend-timeout-s S` | 60.0 | Per-backend timeout for Racknerd + bbops |
| `--dispatch-chunk-size N` | 50 | Records fetched per dispatcher poll cycle |
| `--racknerd-host HOST` | — | VPS hostname for SSH tunnel (required for dispatcher) |
| `--racknerd-concurrency N` | 25 | Parallel SMTP connections via tunnel |
| `--no-racknerd` | off | Disable Racknerd backend (bbops + Zuhal only) |
| `--racknerd-direct` | off | Skip SOCKS5 tunnel; connect directly to MX servers (use when running on the egress VPS) |
| `--bbops-base-url URL` | bbops.io | Override bbops API base URL |

---

## Running tests

```bash
.venv/bin/python -m pytest tests/ -q    # all 648 tests
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
