# Pipeline Rules

## State machine

Valid transitions only. Never set `record_state` to an arbitrary string — always use `db.State.*` constants.

```
RAW → DISCOVERING | DISCOVERED | DISCOVERY_FAILED
DISCOVERING → DISCOVERED | DISCOVERY_FAILED      (retry only)
DISCOVERED → VALIDATING                           (atomic UPDATE RETURNING — consumer only)
VALIDATING → VALIDATED | VALIDATION_FAILED | COST_SKIPPED
```

Any code that writes `record_state` directly via a string literal is wrong — use `State.VALIDATED`, etc.

## Database

- `isolation_level=None` on the connection (set in `init_db`) — do not change this.
- Batch inserts always use explicit `BEGIN` / `COMMIT` / `ROLLBACK`.
- `conn.commit()` after every single-row write in consumer (not batched — rows must be visible to concurrent readers immediately).
- Never add new SQL without a parameterized `?` placeholder. No f-strings in SQL.
- All new tables need an index on the primary lookup column.

## Producer

- One `aiodns.DNSResolver` per producer run — created in `__init__`, passed to every `probe_domains()` call.
- Serper enrichment_cache is keyed by `(business_name_norm, agent_name_norm, state, provider)` — normalize with `.lower().strip()` before any cache lookup or write.
- Fallback domain blocklist: any domain that appears as first-organic fallback for 2+ different businesses is promoted to `_fallback_blocklist` at runtime. Static seed is in `constants.FALLBACK_DOMAIN_BLOCKLIST`.
- `process_trace` must have an entry for every stage that ran: `dns`, `patterns`, `serper`, and (if applicable) `input`.

## Consumer

- `recover_stale_validating()` must be called at startup before the poll loop.
- MS probe runs first — only for records where `is_microsoft_mx(mx_provider)` is True.
- `cost_tracker.ceiling_reached()` checked before every Zuhal call, not after.
- `email_to_template()` called after every Zuhal verdict to update `pattern_stats`.
- Circuit breaker (`self._breaker`) is per-instance — never shared across workers.

## Output

- `valid_emails.csv` is written in `_write_outputs()` only — never written mid-run.
- `results.json` reflects final state — written once at shutdown, never incrementally.
- `pipeline.db` is the authoritative record — CSV and JSON are derived views.

## Cost

- `API_COSTS` in `constants.py` is the single source of pricing truth.
- Every paid API call must `cost_tracker.record_call(service)` immediately after the call succeeds.
- MS probe (`ms_verify.py`) and bbops do NOT increment the cost tracker.
