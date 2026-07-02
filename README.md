# us-z-3 — Email Contact Collector (ECC)

Discovers and validates business email addresses from Secretary of State filing records using a multi-backend async pipeline. Designed for high-volume B2B contact enrichment with full auditability, cost controls, and continuous learning from verified outcomes.

**Input:** JSONL of business + registered-agent records.
**Output:** `valid_emails.csv` (confirmed emails), `results.json` (run summary), `pipeline.db` (full audit trail).

---

## Documentation

[UST tracking sheet](https://docs.google.com/spreadsheets/d/1rxTe-nIAU35-6p1OOlVvS0GT6zpYvK0XYngoy0AI7wQ/edit?usp=sharing) - run totals, enhancement docs, and related reports.

---

## What This System Does (Executive Summary)

Given a list of businesses from public filing records, this pipeline:

1. **Finds the business domain** — tries DNS first (free), falls back to Google search
2. **Generates email candidates** — creates plausible address patterns ranked by historical win rates
3. **Verifies each candidate** — contacts the business's mail server directly to confirm the mailbox exists, without sending any email
4. **Escalates unresolved records** — uses paid third-party verifiers only when free methods are inconclusive
5. **Learns from every outcome** — feeds results back into ranking so future runs get smarter and cheaper

---

## Infrastructure Overview

```
┌─────────────────────────────────────────────────────┐
│              HETZNER VPS (runs all pipeline code)   │
│                                                     │
│  Producer → SQLite DB ← Dispatcher                  │
│                │                                    │
│                ├── MS API (direct HTTP, free)       │
│                ├── Serper API (domain search)       │
│                ├── BBops API (HTTP, batch verify)   │
│                └── Zuhal API (HTTP, rescue only)    │
│                                                     │
│         SSH SOCKS5 tunnels (one per worker)         │
└──────────┬────────────┬────────────┬────────────────┘
           │            │            │
    ┌──────▼──┐  ┌──────▼──┐  ┌─────▼───┐
    │Cherry W1│  │Cherry W2│  │Cherry W3│   ← clean egress IPs
    │  (LT)   │  │  (NL)   │  │  (LT)   │     port 25 open
    └──────┬──┘  └──────┬──┘  └─────┬───┘
           └────────────┴────────────┘
                        │ TCP port 25
                        ▼
              TARGET MAIL SERVERS
           (Gmail / Microsoft / Proofpoint)
```

**Why Hetzner for the pipeline, Cherry Servers for SMTP:**
The pipeline code runs on Hetzner. All SMTP probes exit through Cherry Servers, whose IP addresses have clean reputations — mail servers accept connections from them. Hetzner's own IP is never exposed to mail servers.

> **Cherry Servers fleet** is opt-in (`--cherry-enabled` / `--smtp-hosts`). It provisions, monitors, and tears down Cherry egress workers automatically via the Cherry Servers API. See [Cherry Fleet](#cherry-servers-smtp-fleet) below.

---

## Quick Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Required: SERPER_API_KEY, ZUHAL_API_KEY
# Required for SMTP: RACKNERD_HOST (or Cherry fleet — see below)
# Optional: BBOPS_BASE_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
```

---

## Running the Pipeline

```bash
# Dry run — no API calls, confirms wiring is correct
python -m pipeline run -i input/nc_retry_300k.jsonl --limit 50 --dry-run --name test

# Live run with a cost ceiling
python -m pipeline run -i input/nc_retry_300k.jsonl --limit 500 --max-cost 1.00 --name run_$(date +%Y%m%d)

# Check status with 3 rolling throughput windows
python -m pipeline status --db output/run_20260430/pipeline.db --watch 5

# Re-queue specific failure states for retry
python -m pipeline reset --db output/run_20260430/pipeline.db --status discovery_failed
```

---

## Pipeline Architecture

### Stage 1 — Producer (Domain Discovery)

**What it does:** Figures out what domain a business operates on, then generates ranked email address candidates for that business's officers.

**Why it matters:** A wrong domain means every downstream verification step wastes money confirming addresses that belong to a different business. Getting the domain right — and scoring confidence in that match — is the highest-leverage step in the pipeline.

```
InputRecord (RAW)
    │
    ├─ DNS probe (.com / .net / .org / .us / .info TLD variants)
    │     hit  → candidate_emails ranked by pattern_stats win rate per MX provider
    │     miss ↓
    │
    ├─ Serper enrichment (Google search — $0.001/record)
    │     Tries: site: scoped → agent-name → short-name (4+ words)
    │     hit  → domain match scored → candidate_emails
    │     miss → DISCOVERY_FAILED
    │
    └─ DISCOVERED → written to SQLite queue
```

**Domain match scoring** (`domain_match_score`, 0.0–1.0): Every Serper-returned domain is scored against the business name using word overlap and fuzzy matching. A domain that scores below the threshold caps the record's confidence tier at `low` regardless of whether the email verifies — because a verified email at the wrong domain is a false positive.

**Pattern ranking** (`pattern_stats`): Every verified outcome (valid/invalid) is fed back into a per-MX-provider win-rate table. On the next run, `john.smith@` is tried before `j.smith@` if history shows it wins more often for that mail provider.

**Website harvesting** (free, opt-in via `--harvest`): Before paying for Serper, the dispatcher scrapes the business's own website for existing email addresses. One known address reveals the company's naming convention (e.g., `john.smith@` → try `mary.jones@` for another officer) — free intelligence that improves candidate quality.

---

### Stage 2 — Dispatcher (Email Verification)

**What it does:** For each candidate email, runs a sequence of verification backends in cost order — free first, paid only as a last resort.

**Why it matters:** Each verification step has a different cost and reliability profile. Running them in strict sequence (not in parallel) prevents paying for Zuhal on records that Racknerd or bbops would have resolved for free.

```
For each candidate_email (in ranked order):
    │
    ├─ [1] MS Probe (free) — Microsoft 365 / Exchange Online domains only
    │         valid   → VALIDATED ✓  stop here
    │         invalid → skip candidate, try next
    │         error   → fall through to SMTP
    │
    ├─ [2] Racknerd SMTP via Cherry IP (free per-probe)
    │         valid / catch_all → VALIDATED ✓  bbops skipped
    │         blocked           → re-queue (IP-level block, not email verdict)
    │         invalid / error   ↓
    │
    ├─ [2b] BBops API (contract cost, only when Racknerd is inconclusive)
    │         reconcile(racknerd, bbops):
    │           valid / catch_all (either)  → VALIDATED ✓
    │           both invalid                → go to Zuhal
    │           mixed error / tunnel down   → re-queue, no attempt burned
    │
    └─ [3] Zuhal rescue (paid, $0.0005 — only when both SMTP backends return invalid)
              Checks email_verification_cache first → free if already seen
              valid / accept-all → VALIDATED ✓
              circuit_open       → re-queue, no attempt burned
              invalid / error    → try next candidate

All candidates exhausted → VALIDATION_FAILED
Cost ceiling hit before Zuhal → COST_SKIPPED
```

**Key design decision — sequential backends:** BBops only runs when Racknerd can't give a clear answer. Zuhal only runs when both SMTP backends explicitly reject. This keeps Zuhal spend to 5–15% of records rather than 100%.

**Zuhal credit check at startup:** The pipeline calls the Zuhal API at boot to confirm credits are available before processing begins. This prevents discovering a depleted account mid-run after thousands of records have already been processed.

---

### OR-of-Valids Reconciliation

| Racknerd | BBops | Outcome | Note |
|---|---|---|---|
| `valid` | any | VALIDATED `valid` | |
| any | `valid` | VALIDATED `valid` | |
| `catch_all` | any | VALIDATED `catch_all` | |
| any | `catch_all` | VALIDATED `catch_all` | |
| `invalid` | `invalid` | → Zuhal rescue | Both confirmed: email doesn't exist |
| `invalid` | `not_run` | → Zuhal rescue | BBops disabled; treat as definitive |
| `blocked` | any | re-queue | IP reputation block — not an email verdict |
| `invalid` | `error` | re-queue | Can't trust a single invalid with an error |
| `error` | `invalid` | re-queue | Same |
| `error` | `error` | re-queue | Both inconclusive |
| tunnel down | any | re-queue | Infrastructure issue; no attempt burned |

**Re-queue** means the record returns to `DISCOVERED`. `dispatch_attempts` only increments on terminal verdicts — transient infrastructure failures do not count against the retry budget.

---

## Key Enhancements

### Domain & Identity Confidence

**Business-to-domain confidence scoring** — Every discovered domain gets a `domain_confidence` score (0.0–1.0) computed from word overlap and fuzzy matching between the business name and the domain. DNS hits start at high confidence. Serper hits are scored — a weak match caps the record's `confidence_tier` even if the email later verifies as deliverable. This prevents a technically valid email at the wrong business from being treated as a high-quality contact.

**Owner-confidence scoring** (`owner_confidence`, 0.0–1.0) — Estimates whether the registered agent is likely the actual business owner (vs. a paid commercial agent like CT Corporation). Scored from: commercial-agent detection → 0.0; individual name with surname overlap in the business name → up to 1.0. Feeds into candidate prioritization and confidence tier.

**Website harvesting for email patterns** — When all generated candidates fail SMTP, the dispatcher scrapes the business's own domain (homepage, /contact, /about, /team) for existing email addresses. A single found address reveals the company's naming convention and generates a high-confidence new candidate — for free, before paying for Serper.

**Officer crawling** — Names found near role keywords (Owner, Founder, President, CEO, etc.) on harvested pages generate additional candidates for people who may not appear in the filing record.

**Canonical verdict fields** — All per-service verdicts are normalized through a single function (`normalize_verdict()`) into a canonical set: `valid`, `invalid`, `catch_all`, `unknown`, `do_not_mail`, `abuse`, `disposable`. The `canonical_status` column is the single source of truth for downstream consumers — never the raw per-service columns.

**ZeroBounce ground-truth feedback** — After a ZeroBounce pass, `valid` and `invalid` outcomes are fed back into `pattern_stats`. The pipeline learns which naming patterns actually deliver vs. bounce, improving candidate ranking for future runs. Each ZB CSV should be ingested once (the pattern feedback is not idempotent).

**Identity and deliverability scoring before paid stages** — Candidates are ranked and filtered by confidence score before reaching Zuhal. Low-confidence candidates skip Zuhal rescue, preventing spend on records where the business-domain match itself is uncertain.

---

### Operational Reliability

**Per-MX SpamhausGuard** — SMTP cooldowns are isolated per recipient MX provider. A Spamhaus block on one provider (e.g., Proofpoint) no longer freezes probes to unrelated providers (e.g., Google Workspace). Each provider has an independent sliding-window block counter and cooldown timer.

**Verifier agreement tracking** (`verifier_agreement` column) — Every validated record records which backend(s) confirmed it: `both`, `racknerd_only`, `bbops_only`, `ms_only`, or `zuhal_only`. This lets post-run analysis identify which verifier combinations correlate with actual deliverability vs. false positives — especially important for catch-all domains.

**Email verification cache** (`email_verification_cache` table) — Before calling Zuhal, the pipeline checks whether that email was already verified in a previous run. A cache hit costs nothing. Results are written back to the cache after every terminal verdict. Prevents duplicate Zuhal charges across reruns and across multiple states where the same officer appears.

**Zuhal job tracking** (`zuhal_jobs` table) — Every bulk Zuhal API job gets an audit row before polling starts, recording the job ID, email count, status, and completion timestamp. No more lost bulk jobs when a process restarts mid-poll.

**MS probe failure monitoring** — A rolling 100-probe error tracker monitors the free Microsoft API. When the error rate hits 50%+, `logger.error` fires — so Microsoft domain failures are visible immediately rather than silently inflating SMTP costs.

**Domain match scoring** (`domain_match_score` column, 0.0–1.0) — Scores how well the Serper-returned domain actually matches the business name. A wrong domain is expensive: Zuhal or ZeroBounce may confirm a deliverable mailbox that belongs to a different company. Low match scores cap the confidence tier.

**Enhanced status command** — The `pipeline status` output now shows:
- Three throughput windows: 1min / 5min / 15min side by side (spot acceleration or degradation)
- Per-state breakdown: validated/min, failed/min, discovery_failed/min
- Zuhal queue size + drain rate shown independently
- Pending split into fresh vs. retries with accurate ETA based on the best available window

**Zuhal spot-check tool** (`pipeline/ops/zuhal_spot_check.py`) — Samples a set of ZeroBounce-validated emails and re-verifies them through Zuhal. Catches cases where ZeroBounce returns `valid` but Zuhal disagrees. Used for QA after a full run to measure false-negative rates.

**Master DB** (`pipeline/ops/master_db.py`) — Stores confirmed valid emails with validity periods across runs. Defines result expiry windows by status (shorter for `catch_all` and `unknown`) so stale records can be flagged for re-verification.

**Exponential re-queue backoff** — Infrastructure re-queues (tunnel down, bbops error) use an exponential schedule (5min → 15min → 45min) rather than immediate retry, preventing hot-loop churn when a backend is degraded.

---

## Cherry Servers SMTP Fleet

**The IP reputation problem:** Budget VPS providers (Racknerd, Contabo, Hostinger) have IP ranges that end up in Spamhaus blocklists because other customers on the same network send spam. When the pipeline's egress IP is blocklisted, Gmail, Microsoft, and Proofpoint reject connections before even seeing the RCPT command.

**The solution:** Cherry Servers provides dedicated bare metal servers with clean, isolated IP addresses. The pipeline code still runs on Hetzner — Cherry servers are just clean exit points for SMTP traffic, used via SSH SOCKS5 tunnels. Cherry never runs any pipeline code.

### How it works

```
python -m pipeline.fleet provision --count 3
→ Creates 3 Cherry servers via Cherry API
→ Waits for them to boot and become SSH-accessible
→ Writes IPs to output/fleet/hosts.json
→ Each worker becomes an SSH tunnel endpoint on a different local port

python -m pipeline run -i input/nc.jsonl --smtp-hosts <ips>
→ FleetManager routes each SMTP probe to the least-loaded worker
→ IP-blocked probes are rerouted to a different worker automatically
→ FleetSupervisor watches in background; replaces degraded IPs with fresh servers

python -m pipeline.fleet teardown --yes
→ Deletes all managed servers
→ Billing stops (Cherry charges hourly)
```

### Fleet capabilities

**Load balancing** — `FleetManager` routes each probe to the least-loaded worker, with per-recipient-domain concurrency caps (max 3 simultaneous probes to the same mail provider) to avoid triggering 421 rate-limit responses.

**Greylist affinity** — If a mail server says "try again later" (greylisting), the retry must come from the same IP. The fleet remembers which worker handled each greylisted email and sticks to it.

**Auto-heal** — `FleetSupervisor` monitors every worker every 15 seconds. If a worker's IP becomes reputation-degraded (too many blocks), it provisions a fresh Cherry server with a new clean IP, swaps it into the pool, and deletes the old server — without pausing the pipeline.

**Elastic scaling** — Write `{"scale_to": 5}` to `output/fleet/control.json` to grow or shrink the pool mid-run without restarting.

**Durable state backup (R2)** — The SQLite database is backed up periodically to Cloudflare R2 (S3-compatible) during long runs. If the Hetzner machine crashes, the run can be resumed from the last backup.

**rDNS as HELO hostname** — Each Cherry worker's PTR record is automatically used as its SMTP EHLO/MAIL FROM domain. Mail servers require the HELO hostname to match the connecting IP's PTR — this is set correctly per-worker.

**Autonomous benchmark** — One command to validate accuracy:
```bash
python -m pipeline.fleet benchmark \
  --input input/nc_sample.jsonl \
  --count 3 \
  --ground-truth zb_results.csv
# Provisions fleet → runs pipeline → prints accuracy report → tears down fleet
```

### Fleet environment variables

| Variable | Required | Notes |
|---|---|---|
| `CHERRY_AUTH_TOKEN` | Yes | Cherry Servers API token |
| `CHERRY_PROJECT_ID` | Yes | Project to provision servers into |
| `CHERRY_PLAN` | No | Default: `B2-1-1gb-20s-shared` |
| `CHERRY_REGION` | No | Default: `EU-Nord-1` (Lithuania) |
| `R2_ACCESS_KEY_ID` | No | Cloudflare R2 backup credentials |
| `R2_SECRET_ACCESS_KEY` | No | |
| `R2_ENDPOINT` | No | R2 bucket endpoint URL |

### Recommended infrastructure stack

| Server | Role | Cost |
|---|---|---|
| Hetzner Dedicated (AX41) | Runs the pipeline | €39/month |
| Cherry Servers Lithuania (AS16125) | SMTP egress worker 1 | $66/month (or hourly) |
| Cherry Servers Netherlands (AS59642) | SMTP egress worker 2 | $66/month (or hourly) |

Two Cherry workers = two different ASNs = different IP reputations viewed by different mail providers. Cherry bills hourly — a 3-day run costs 3 days, not a full month.

---

## State Machine

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
| `DISCOVERING` | In-flight discovery; eligible for retry on restart |
| `DISCOVERED` | Domain + email candidates found; queued for validation |
| `VALIDATING` | Claimed by dispatcher; in-flight |
| `VALIDATED` | At least one email confirmed deliverable |
| `VALIDATION_FAILED` | All candidates exhausted without a valid result |
| `DISCOVERY_FAILED` | Neither DNS nor Serper found a domain |
| `COST_SKIPPED` | Cost ceiling reached before Zuhal rescue ran |

---

## Status Fields Reference

### Verdict columns

| Column | Meaning |
|---|---|
| `canonical_status` | Single normalized verdict — read this, not raw per-service columns. Values: `valid`, `invalid`, `catch_all`, `unknown`, `do_not_mail`, `abuse`, `disposable` |
| `canonical_source` | Which service set it: `zerobounce` > `zuhal` > `smtp` > `ms_probe` |
| `final_verdict` | SMTP/Zuhal reconciled result: `valid` or `catch_all` |
| `racknerd_verdict` | Racknerd SMTP verdict for this email |
| `bbops_verdict` | bbops.io verdict |
| `zuhal_verdict` | Zuhal rescue verdict, or `not_run` |
| `verifier_agreement` | Which backend(s) confirmed: `both`, `racknerd_only`, `bbops_only`, `ms_only`, `zuhal_only` |
| `zb_status` / `zb_sub_status` | ZeroBounce verdict (blank until ZB ingest runs) |

### Confidence columns

| Column | Meaning |
|---|---|
| `confidence_score` | Additive 0–4: domain match (+1), name match (+1), non-generic pattern (+1), verdict=valid (+1) |
| `confidence_tier` | `high` ≥ 3, `medium` = 2, `low` ≤ 1 |
| `domain_confidence` | 0–1 business-to-domain match confidence, computed at discovery |
| `domain_confidence_tier` | `high` / `medium` / `low` tier of the above |
| `domain_match_score` | 0–1 word-overlap + fuzzy match score between business name and domain |
| `owner_confidence` | 0–1 likelihood the registered agent is a business owner |
| `owner_confidence_tier` | `high` ≥ 0.6, `medium` ≥ 0.3, `low` < 0.3 |

---

## Output CSV Columns

| Column | Description |
|---|---|
| `unique_id` | Composite key: `{filing_id}__{agent_id}` |
| `business_name` | Legal business name from filing |
| `agent_name` | Registered agent / officer name |
| `state` | State abbreviation |
| `email` | Confirmed deliverable email address |
| `canonical_status` | Normalized verdict (`valid`, `catch_all`, etc.) |
| `canonical_source` | Which service set canonical_status |
| `final_verdict` | SMTP/Zuhal reconciled verdict |
| `confidence_tier` | `high` / `medium` / `low` |
| `confidence_score` | Raw additive score 0–4 |
| `domain_confidence` / `domain_confidence_tier` | Business-to-domain match |
| `domain_match_score` | Serper domain name fuzzy match score |
| `owner_confidence` / `owner_confidence_tier` | Agent-as-owner likelihood |
| `verifier_agreement` | Which backend(s) confirmed the email |
| `verified` | `True` if `valid` or `catch_all` |
| `discovery_method` | `dns`, `serper`, `serper_fallback`, or `input` |
| `validation_method` | `ms_probe`, `smtp_both`, `smtp_racknerd`, `smtp_bbops`, `zuhal_rescue` |
| `racknerd_verdict` | Racknerd SMTP verdict |
| `bbops_verdict` | bbops.io verdict |
| `zuhal_verdict` | Zuhal verdict, or `not_run` |
| `zb_status` / `zb_sub_status` | ZeroBounce verdict (post-ingest) |

---

## Environment Variables

| Variable | Required | Default |
|---|---|---|
| `SERPER_API_KEY` | Yes | — |
| `ZUHAL_API_KEY` | Yes (dispatcher) | — |
| `RACKNERD_HOST` | Yes (SMTP, non-fleet) | — |
| `RACKNERD_SSH_USER` | No | `egress` |
| `RACKNERD_SSH_KEY` | No | `~/.ssh/racknerd_egress` |
| `RACKNERD_HELO_HOSTNAME` | Recommended | Falls back to system FQDN |
| `BBOPS_BASE_URL` | No | `https://email-verifier.bbops.io` |
| `CHERRY_AUTH_TOKEN` | Fleet only | — |
| `CHERRY_PROJECT_ID` | Fleet only | — |
| `R2_ACCESS_KEY_ID` | Fleet backup only | — |
| `R2_SECRET_ACCESS_KEY` | Fleet backup only | — |

`--producer-only` runs without `RACKNERD_HOST` or `ZUHAL_API_KEY`.

---

## Key CLI Flags

| Flag | Default | Effect |
|---|---|---|
| `--limit N` | none | Process only first N records |
| `--dry-run` | off | Mock all API calls; no cost |
| `--max-cost USD` | none | Stop when cumulative cost reaches limit |
| `--name NAME` | none | Output to `output/NAME/` |
| `--producer-only` | off | Discovery only (no SMTP, no Zuhal) |
| `--consumer-only` | off | Dispatcher only |
| `--harvest` | off | Scrape business websites before paid Serper fallback |
| `--ignore-cache` | off | Bypass Serper enrichment cache |
| `--dns-concurrency N` | 100 | Parallel DNS semaphore size |
| `--dispatch-concurrency N` | 50 | Parallel dispatcher workers |
| `--racknerd-concurrency N` | 25 | Parallel SMTP connections via tunnel |
| `--no-racknerd` | off | Disable Racknerd (bbops + Zuhal only) |
| `--racknerd-direct` | off | Skip SOCKS5 tunnel (use when running on the egress VPS) |

---

## Costs

| Service | Per call | Notes |
|---|---|---|
| Serper | $0.001 | DNS-miss path (producer) + dispatcher fallback |
| DNS probe | $0 | Free; always tried first |
| MS probe | $0 | Free; short-circuits all Microsoft 365 / Exchange Online domains |
| Website harvest | $0 | Free scrape before paid Serper fallback (`--harvest`) |
| Racknerd / Cherry SMTP | $0 per probe | Fixed VPS cost only |
| bbops | Per contract | Only runs when Racknerd is inconclusive |
| Zuhal | $0.0005 | Rescue only — both SMTP backends returned `invalid` (5–15% of records) |
| ZeroBounce | Per contract | Post-pipeline ground-truth pass; ingest once |

**Typical 300k-record run:** ~$300 Serper + $15–75 Zuhal + fixed infrastructure costs.

---

## Post-Pipeline Operations

| Script | Purpose |
|---|---|
| `python -m pipeline.ops.zuhal_bulk` | Submit NEEDS_ZUHAL records to Zuhal Bulk API |
| `python -m pipeline.ops.zb_zuhaled` | Submit Zuhal-validated emails to ZeroBounce |
| `python -m pipeline.ops.ingest_zerobounce` | Join ZB results back (sets canonical_status, feeds pattern_stats) |
| `python -m pipeline.ops.zuhal_spot_check` | Sample VALIDATION_FAILED records and re-check via ZB (false-negative QA) |
| `python -m pipeline.ops.passoff_watcher` | Drip-feed daemon: append validated results to combined CSV |
| `python -m pipeline.ops.master_db` | Persist confirmed emails to master DB with validity periods |
| `python -m pipeline.ops.normalize_zuhaled` | Upgrade legacy zuhaled CSV format |
| `python -m pipeline.ops.requeue_zuhal_429_burns` | Recover records burned by Zuhal 429 bug |

---

## Running Tests

```bash
.venv/bin/python -m pytest tests/ -q        # all tests, 0 failures
.venv/bin/python -m pytest tests/unit/ -q   # fast unit tests only
.venv/bin/python -m pytest tests/e2e/ -q    # end-to-end subprocess tests
```
