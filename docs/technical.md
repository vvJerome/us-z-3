# Technical Architecture

## System Summary

The Email Contact Collector (ECC) is a Python 3.12 async pipeline that discovers and validates business email addresses from NC Secretary of State filing records. Input is a JSONL file of business and registered-agent records. Output is a CSV of confirmed deliverable emails backed by a full SQLite audit trail.

The pipeline runs as a single-process async application with two concurrent workers — a producer and a dispatcher — communicating through a shared SQLite database and a named pipe for wake signals.

## Runtime Model

The pipeline uses `asyncio` throughout. There is no threading. All I/O — DNS, HTTP, SMTP, and database — is non-blocking. The two workers run as `asyncio.Task` objects inside a single event loop, sharing a single `aiosqlite` connection.

SQLite is opened with `isolation_level=None` (autocommit disabled at the driver level). The pipeline manages all transactions explicitly with `BEGIN` / `COMMIT` / `ROLLBACK`. WAL mode is enabled so the producer and dispatcher can read and write concurrently without blocking each other.

## Module Map

```
pipeline/
  __main__.py        Entry point. Wires workers, tunnels, and clients. Writes outputs at shutdown.
  producer.py        Stage 1 worker. DNS probe → Serper enrichment → DISCOVERED state.
  dispatcher.py      Stage 2 worker. Three-backend coordinator. Writes terminal states.
  db.py              Schema (v6), state machine constants, all DB helper functions.
  models.py          InputRecord, EnrichmentResult, ValidationResult, BackendVerdict dataclasses.
  config.py          PipelineConfig (pydantic-settings). Reads .env and CLI overrides.
  cli.py             argparse definitions for run / status / reset subcommands.
  constants.py       API_COSTS, SERVICE_BACKOFF, DNS_TLDS, FALLBACK_DOMAIN_BLOCKLIST.
  metrics.py         Prometheus /metrics endpoint on port 9090.
  consumers/
    racknerd.py      Direct SMTP prober with SOCKS5 and direct-TCP modes.
    bbops_async.py   Async batch verifier against bbops.io HTTP API.
  tunnels/
    ssh_socks.py     SSH SOCKS5 tunnel supervisor with exponential backoff autorestart.
  utils/
    dns_probe.py     aiodns MX resolver with TLD enumeration and shared resolver instance.
    serper_client.py Google Serper search enrichment with enrichment_cache integration.
    zuhal_client.py  Zuhal rescue backend. Circuit breaker (fail_max=5, timeout=600s).
    ms_verify.py     Microsoft GetCredentialType probe via requests.post in asyncio.to_thread.
    email_patterns.py Pattern generation and per-MX win-rate ranking from pattern_stats.
    text.py          Name parsing, domain stem generation, strategy assignment.
    cost_tracker.py  Per-service cost accumulator with ceiling check.
    rate_limiter.py  TokenBucket async rate limiter.
    backoff.py       Generic exponential backoff with jitter.
    notify.py        Named-pipe IPC: producer signals dispatcher on new DISCOVERED records.
    logger.py        Structured JSON logging setup.
```

## Database Schema (v6)

### `records` table — one row per input record

| Column | Type | Description |
|---|---|---|
| `unique_id` | TEXT PK | `{filing_id}__{agent_id}` |
| `record_state` | TEXT | Lifecycle state (RAW → VALIDATED or terminal) |
| `business_name` | TEXT | Legal business name |
| `agent_name` | TEXT | Registered agent or officer |
| `state` | TEXT | State abbreviation |
| `strategy` | TEXT | `with` (name-specific patterns) or `without` (generic) |
| `candidate_email` | TEXT | The email that passed validation |
| `candidate_emails` | TEXT | JSON array of all ranked candidates |
| `candidate_domain` | TEXT | Domain identified during discovery |
| `discovery_source` | TEXT | `dns`, `serper`, or `input` |
| `discovery_attempts` | INTEGER | How many discovery cycles ran |
| `mx_provider` | TEXT | MX host or provider label |
| `zuhal_status` | TEXT | Zuhal response or SMTP encoding tag |
| `confidence_score` | REAL | Additive pattern score 0–4 |
| `racknerd_status` | TEXT | Racknerd SMTP verdict |
| `racknerd_message` | TEXT | Full SMTP response string |
| `racknerd_verified_at` | TEXT | ISO timestamp of Racknerd probe |
| `bbops_status` | TEXT | bbops.io verdict |
| `bbops_message` | TEXT | Full bbops response string |
| `bbops_verified_at` | TEXT | ISO timestamp of bbops response |
| `final_verdict` | TEXT | Reconciled terminal verdict |
| `dispatch_attempts` | INTEGER | Count of terminal verdict cycles |
| `process_trace` | TEXT | JSON array of per-stage outcome entries |
| `serper_enriched` | INTEGER | 1 if Serper was called for this record |

### Supporting tables

| Table | Purpose |
|---|---|
| `checkpoints` | Per-run producer offset and state (enables resume) |
| `stats` | Aggregated run metrics written at shutdown |
| `failures` | Per-record failure details with phase and message |
| `pattern_stats` | Per-template win/loss counts used to rank candidates |
| `enrichment_cache` | Serper result cache keyed by `(business_name_norm, agent_name_norm, state, provider)` |
| `bbops_jobs` | Tracks in-flight and completed bbops batch jobs |

## Stage 1 — Producer

The producer reads input records in chunks (default 100) and advances a `producer_offset` checkpoint atomically so the same `--name` run can resume after interruption.

For each record:

1. Generates domain stem candidates from `business_name` and `agent_name` using `text.py`.
2. Probes each stem across `.com`, `.net`, `.org` with `aiodns.DNSResolver.query(domain, "MX")`. A single resolver instance is shared across all records in the run.
3. On DNS hit: builds ranked `candidate_emails` from `email_patterns.py`, writes DISCOVERED with source `dns`.
4. On DNS miss: calls Serper with a business-name query. If `strategy == "with"` and no result is found, falls back to an agent-name query. On Serper hit: writes DISCOVERED with source `serper`. On miss: writes DISCOVERY_FAILED.
5. Serper results are cached in `enrichment_cache` (TTL: 30 days). Cache key is normalized with `.lower().strip()`.
6. Domains appearing as first-organic Serper result for two or more different businesses in the same run are added to `_fallback_blocklist` and rejected on subsequent records.

The producer signals the dispatcher via a named pipe after each DISCOVERED batch so the dispatcher wakes immediately rather than polling.

## Stage 2 — Dispatcher

The dispatcher polls for DISCOVERED records via atomic `UPDATE records SET record_state='VALIDATING' WHERE record_state='DISCOVERED' LIMIT ? RETURNING *`. This prevents any record from being claimed twice.

For each claimed record the dispatcher iterates `candidate_emails` in ranked order:

### Step 1 — MS Probe (free)

Applies only when `mx_provider` indicates a Microsoft-managed domain (Office 365, Exchange Online, Outlook.com, Hotmail). Calls `ms_verify.check_sync()` in `asyncio.to_thread` to hit the Microsoft GetCredentialType endpoint.

- `valid` → write VALIDATED, stop.
- `invalid` → skip this candidate, try next.
- `unknown` / `error` → fall through to SMTP backends.

### Step 2 — Concurrent SMTP (Racknerd + bbops)

`asyncio.gather(racknerd.verify(email), bbops.submit_and_wait(email))` runs both backends simultaneously.

**Racknerd** (`consumers/racknerd.py`):
- In SOCKS5 mode: opens a TCP socket through the SSH SOCKS5 tunnel to `mx_host:25`.
- In direct mode (`--racknerd-direct`): connects to `mx_host:25` directly (used when the pipeline runs on the egress VPS itself).
- Runs EHLO → MAIL FROM → RCPT TO sequence. Interprets 2xx/3xx as `valid`, 5xx as `invalid` or `blocked`, 4xx as `error` (try next MX host).
- MX records are cached for 1 hour per domain.
- A sliding-window SpamhausGuard detects 100+ block events in 60 seconds and triggers a 300-second cooldown across all SMTP probes.

**bbops** (`consumers/bbops_async.py`):
- Submits emails in batches to `https://email-verifier.bbops.io`.
- Polls for results asynchronously. On crash recovery, re-submits any in-flight batches found in `bbops_jobs` at startup.

### Step 3 — Reconciliation

`reconcile(rk_verdict, bb_verdict)` applies OR-of-valids:

- Either backend returns `valid` → outcome `valid`.
- Either backend returns `catch_all` and neither returned `valid` → outcome `catch_all`.
- Both return `invalid` → outcome `invalid` (proceed to Zuhal).
- Any other combination (error, not_run, tunnel down) → outcome `unknown` → re-queue as DISCOVERED without incrementing `dispatch_attempts`.

### Step 4 — Zuhal Rescue (paid, sequential)

Runs only when reconciliation produces `invalid`. Zuhal is a third-party email verification API at $0.0005/call.

`ZuhalClient` wraps the call with:
- An `aiobreaker.CircuitBreaker` (fail_max=5, timeout=600s). On circuit open, raises `ZuhalCircuitOpenError` and the dispatcher re-queues the record as DISCOVERED without burning `dispatch_attempts`. The circuit self-resets after 600 seconds.
- A `TokenBucket` rate limiter.
- Exponential backoff with jitter (base=1s, max=64s, retryable: 429, 500, 503).
- A random anti-fingerprinting delay of 0.5–2.5s before each call.

If Zuhal returns `valid` or `accept-all` → write VALIDATED. Otherwise try the next candidate.

### Candidate exhaustion

If all candidates are tried without a valid result, `dispatch_attempts` is incremented and the record is written as VALIDATION_FAILED with the last observed `racknerd_status` and `bbops_status` values. NULL is never written for backend verdicts at exhaustion.

### Cost ceiling

`cost_tracker.ceiling_reached()` is checked immediately before the Zuhal rescue call. If the ceiling is reached, the record is written as COST_SKIPPED. The ceiling is never checked before Racknerd or bbops calls — only before the paid Zuhal call.

## Pattern Ranking

`email_patterns.py` generates candidate emails by applying name templates to the discovered domain. Templates include `firstname.lastname`, `flastname`, `info`, `contact`, and 17 others.

`pattern_stats` records wins and losses per template per MX provider. `generate_ranked_candidates()` sorts templates by `wins / (wins + losses)` descending, falling back to a deterministic default order for unseen templates. This means the pipeline learns from each run which patterns are most likely to succeed for a given mail host.

After every terminal verdict (valid, invalid, or catch_all) the winning or losing template is recorded via `email_to_template()` → `update_pattern_stats()`.

## Concurrency Limits

| Resource | Concurrency |
|---|---|
| DNS probes | 100 semaphore slots (configurable) |
| Serper calls | 1 (sequential) |
| Dispatcher workers | 20 concurrent records (configurable) |
| Racknerd SMTP connections | 10 concurrent (configurable) |
| Zuhal calls | 5 concurrent semaphore slots |
| bbops batch inflight | 3 concurrent batches |

## Cost Tracking

`CostTracker` accumulates per-service call counts and computes `total_cost` from `API_COSTS`. Only Serper and Zuhal increment the tracker. Racknerd SMTP, bbops, and MS probe are free.

| Service | Cost per call |
|---|---|
| Serper (producer) | $0.001 |
| Serper (dispatcher fallback) | $0.001 |
| Zuhal rescue | $0.0005 |

## Resumability

Each run is identified by `--name`. The `checkpoints` table stores `producer_offset` — the number of input records already ingested. On restart with the same name, the producer skips to `producer_offset` and continues. The dispatcher resumes naturally by finding any DISCOVERED or VALIDATING records still in the database.

At startup, `recover_stale_validating()` resets any records stuck in VALIDATING state (from a previous crash) back to DISCOVERED so they are re-processed.

## SSH SOCKS5 Tunnel

`SshSocksTunnel` spawns an `ssh -D {socks_port} -N` subprocess to forward a local SOCKS5 port through the egress VPS. It monitors the process, detects failures, and restarts with exponential backoff (base=2s, max=60s). The dispatcher checks `tunnel.is_up()` before each SMTP probe and re-queues if the tunnel is down rather than burning a dispatch attempt.

When `--racknerd-direct` is set, no tunnel is started. `RacknerdConsumer` is initialized with `tunnel=None` and `direct=True`, and connects to MX servers via plain TCP port 25.
