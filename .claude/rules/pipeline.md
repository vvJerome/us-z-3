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

## Dispatcher

`_process_record()` order per email candidate:
1. MS probe pre-filter (free) — only when `is_microsoft_mx(mx_provider)` is True; short-circuits on `valid`/`invalid`, falls through on `error`/`unknown`
2. Fan out: Racknerd SMTP + bbops concurrently via `asyncio.gather`
3. OR-of-valids reconciliation (`reconcile()`) → `valid`, `catch_all`, `invalid`, or `unknown`
4. If `unknown` (tunnel down / both inconclusive) → re-queue as DISCOVERED without burning `dispatch_attempts`
5. If `invalid` and `self.zuhal is not None` → Zuhal rescue (sequential, paid)
6. `email_to_template()` called after every terminal verdict to update `pattern_stats`

Other rules:
- `recover_stale_validating()` called at startup before the poll loop.
- `cost_tracker.ceiling_reached()` checked before the Zuhal rescue call, not before SMTP backends.
- Racknerd and bbops do NOT increment the cost tracker — only Zuhal does.

## Output

- `valid_emails.csv` is written in `_write_outputs()` only — never written mid-run.
- `results.json` reflects final state — written once at shutdown, never incrementally.
- `pipeline.db` is the authoritative record — CSV and JSON are derived views.

## Cost

- `API_COSTS` in `constants.py` is the single source of pricing truth.
- Every paid API call must `cost_tracker.record_call(service)` immediately after the call succeeds.
- MS probe (`ms_verify.py`) and bbops do NOT increment the cost tracker.
