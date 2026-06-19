# Pipeline Rules

## State machine

Valid transitions only. Never set `record_state` to an arbitrary string — always use `db.State.*` constants.

```
RAW → DISCOVERING | DISCOVERED | DISCOVERY_FAILED
DISCOVERING → DISCOVERED | DISCOVERY_FAILED      (retry only)
DISCOVERED → VALIDATING                           (atomic UPDATE RETURNING — dispatcher only)
VALIDATING → VALIDATED | VALIDATION_FAILED | COST_SKIPPED
```

Any code that writes `record_state` directly via a string literal is wrong — use `State.VALIDATED`, etc.

## Database

- `isolation_level=None` on the connection (set in `init_db`) — do not change this.
- Batch inserts always use explicit `BEGIN` / `COMMIT` / `ROLLBACK`.
- `conn.commit()` after every single-row write in the dispatcher (not batched — rows must be visible to concurrent readers immediately).
- Never add new SQL without a parameterized `?` placeholder. No f-strings in SQL.
- All new tables need an index on the primary lookup column.

## Producer

- One `aiodns.DNSResolver` per producer run — created in `__init__`, passed to every `probe_domains()` call.
- Serper enrichment_cache is keyed by `(business_name_norm, agent_name_norm, state, provider)` — normalize with `.lower().strip()` before any cache lookup or write.
- Fallback domain blocklist: any domain that appears as first-organic fallback for 2+ different businesses is promoted to `_fallback_blocklist` at runtime. Static seed is in `constants.FALLBACK_DOMAIN_BLOCKLIST`.
- `process_trace` must have an entry for every stage that ran: `dns`, `patterns`, `serper`, and (if applicable) `input`.
- `owner_confidence` (registered-agent → owner likelihood) is computed at discovery via `owner_inference.score_owner_confidence(record, has_website=bool(effective_domain))` and written like `domain_confidence`. It's a heuristic baseline, not ML — commercial-agent detection uses a light normalize (never `normalize_business_name`, which strips the very service-name tokens).

## Dispatcher

`_process_record()` order per email candidate:
1. MS probe pre-filter (free) — only when `is_microsoft_mx(mx_provider)` is True; short-circuits on `valid`/`invalid`, falls through on `error`/`unknown`
2. Racknerd SMTP first; on `valid`/`catch_all` (catch_all gated by confidence) return immediately with `bbops_status=not_run`. bbops runs **only** when Racknerd returns `blocked`/`error`/`invalid` — sequential and lazy, not a concurrent fan-out, so a confirmed Racknerd hit never spends a bbops call.
3. OR-of-valids reconciliation (`reconcile()`) → `valid`, `catch_all`, `invalid`, or `unknown`
4. If `unknown` (tunnel down / both inconclusive) → re-queue as DISCOVERED without burning `dispatch_attempts`
5. If `invalid` and `self.zuhal is not None` → Zuhal rescue (sequential, paid)
6. If Zuhal raises `ZuhalCircuitOpenError` → re-queue as DISCOVERED without burning `dispatch_attempts` (auto-heal)
7. `email_to_template()` called after every terminal verdict to update `pattern_stats`

Other rules:
- Fallback order when pattern candidates are exhausted: **free harvest first, then paid Serper.** `_inject_harvest_fallback` runs only when `harvest_enabled`; `fb_boundary` moves out as it injects so harvested candidates are tried before Serper is called. Serper only fires when harvest adds nothing.
- `recover_stale_validating()` called at startup before the poll loop.
- `cost_tracker.ceiling_reached()` checked before the Zuhal rescue call, not before SMTP backends.
- Racknerd and bbops do NOT increment the cost tracker — only Zuhal does.
- `last_rk` / `last_bb` tracked throughout the candidate loop so the final write always reflects the last actual backend verdict, not NULL.

## Harvest (`pipeline/harvest/`)

- Opt-in only (`--harvest` / `config.harvest_enabled`). Off by default — never fetches a site unless enabled.
- `extract.py` is pure (no I/O) — all network lives in `fetch.py`. Keep it that way so extraction stays unit-testable without mocks.
- Harvest spends **no** API budget — never call `cost_tracker.record_call` from the harvest path.
- Always go through `urllib.robotparser` (fail-open: a missing/erroring robots.txt means proceed) and the shared global `TokenBucket`. Never bypass the rate limiter.
- curl_cffi fingerprint is `constants.HARVEST_IMPERSONATE`; paths are `constants.HARVEST_PATHS`. Don't inline either.
- `infer_templates` normalizes scraped names with `.lower()` before `email_to_template` — `parse_name` output is lowercase and the reverse-map is case-sensitive.

## Field naming

- DB columns: `racknerd_status`, `bbops_status`, `confidence_score` (not `zuhal_score`)
- CSV headers: `racknerd_verdict`, `bbops_verdict`, `zuhal_verdict`, `confidence_score`, `confidence_tier`
- `validation_method` values: `ms_probe`, `smtp_both`, `smtp_racknerd`, `smtp_bbops`, `zuhal_rescue`, `unknown`
- Never use the old names `zuhal_score`, `racknerd+bbops`, `zuhal_fallback` — they are gone

## Canonical verdicts

- Read `canonical_status` for the standardized outcome — never branch on raw per-service values.
- Normalize every provider status through `pipeline.verdicts.normalize_verdict()` (the single source of truth). Canonical set: `valid`, `invalid`, `catch_all`, `unknown`, `do_not_mail`, `abuse`, `disposable`.
- `canonical_source` precedence: `zerobounce` (ground truth) > `zuhal` > `smtp` > `ms_probe`. `update_record_dual` sets it for every verdict write; `pipeline.ops.ingest_zerobounce` overrides it on ZB ingest.
- The `dual_*`/`ms_valid` SMTP-reconciliation signal lives in `reconciliation_path`, not `zuhal_status`.
- `ingest_zerobounce` feeds ground-truth `valid`/`invalid` back into `pattern_stats` via `_feed_pattern_stats` (continuous learning). Only those two canonical outcomes count — `catch_all`/`unknown`/`do_not_mail`/`abuse`/`disposable` are inconclusive for the local-part convention and are skipped. The verdict write is idempotent; the pattern feedback is not — ingest each ZB CSV once.

## Output

- `valid_emails.csv` is written in `_write_outputs()` only — never written mid-run.
- `results.json` reflects final state — written once at shutdown, never incrementally.
- `pipeline.db` is the authoritative record — CSV and JSON are derived views.

## Cost

- `API_COSTS` in `constants.py` is the single source of pricing truth.
- Every paid API call must `cost_tracker.record_call(service)` immediately after the call succeeds.
- MS probe (`ms_verify.py`) and bbops do NOT increment the cost tracker.
