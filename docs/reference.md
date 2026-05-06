# Reference

## Record States

A record moves through these states in order. Only forward transitions are valid except for re-queue paths noted below.

| State | Set by | Meaning |
|---|---|---|
| `RAW` | DB init | Loaded from input, not yet touched |
| `DISCOVERING` | Producer (transient) | In-flight during discovery; reset to RAW on restart |
| `DISCOVERED` | Producer | Candidate domain and emails found; queued for validation |
| `VALIDATING` | Dispatcher (atomic) | Claimed by a dispatcher worker; in-flight |
| `VALIDATED` | Dispatcher | At least one email confirmed deliverable |
| `VALIDATION_FAILED` | Dispatcher | All candidate emails exhausted without confirmation |
| `DISCOVERY_FAILED` | Producer | Neither DNS nor Serper found a usable domain |
| `COST_SKIPPED` | Dispatcher | Cost ceiling reached before Zuhal rescue could run |

Re-queue paths (record returns to `DISCOVERED` without burning `dispatch_attempts`):
- Racknerd tunnel is down at probe time
- Reconciliation outcome is `unknown` (mixed error, single-backend inconclusive)
- Zuhal circuit breaker is open (`ZuhalCircuitOpenError`)

## `racknerd_status` Values

Stored in the `racknerd_status` DB column. Written for every dispatched record.

| Value | Meaning |
|---|---|
| `valid` | RCPT TO accepted (2xx response) |
| `invalid` | RCPT TO rejected (5xx response) |
| `catch_all` | Domain accepts all addresses indiscriminately |
| `error` | Network error, timeout, 4xx response, or protocol failure |
| `blocked` | Spamhaus or reputation block keyword in SMTP response |
| `not_run` | Skipped because MS probe short-circuited validation |
| `ms_valid` | Record was confirmed by MS probe; direct SMTP not attempted |

## `bbops_status` Values

Stored in the `bbops_status` DB column. Written for every dispatched record.

| Value | Meaning |
|---|---|
| `valid` | bbops confirmed deliverable |
| `invalid` | bbops rejected the address |
| `catch_all` | Domain accepts all (bbops signal) |
| `error` | bbops API error, timeout, or poll failure |
| `not_run` | Skipped because MS probe short-circuited validation |

## `zuhal_status` Values

Stored in the `zuhal_status` DB column. Encodes either the actual Zuhal API response or a tag summarizing how validation completed without Zuhal.

| Value | Meaning |
|---|---|
| `valid` | Zuhal rescue confirmed deliverable |
| `accept-all` | Zuhal returned accept-all (treated as `catch_all`) |
| `invalid` | Zuhal rescue also rejected |
| `error` | Zuhal API call failed |
| `circuit_open` | Zuhal circuit breaker was open; record re-queued, no attempt burned |
| `dual_valid` | Zuhal not called; both SMTP backends reconciled to `valid` |
| `dual_catch_all` | Zuhal not called; both SMTP backends reconciled to `catch_all` |
| `dual_invalid` | Zuhal not called but Zuhal rescue was attempted after both returned `invalid` |
| `ms_valid` | MS probe short-circuited; Zuhal not called |

## `final_verdict` Values

Written only when a record reaches a terminal validation state.

| Value | Written when |
|---|---|
| `valid` | Any backend confirmed the email as deliverable |
| `catch_all` | A backend returned catch-all and no backend returned valid |
| `invalid` | All backends rejected all candidates; written on VALIDATION_FAILED only |

## `confidence_score` and `confidence_tier`

`confidence_score` is an additive integer score (0–4) computed at validation time from observable signals. It is not a probability and is not sourced from any API.

| Signal | Points | Condition |
|---|---|---|
| Domain match | +1 | Email domain fuzzy-matches the discovered `candidate_domain` (≥85% similarity) |
| Name match | +1 | Email local part contains a token from `agent_name` (strategy `with` only) |
| Non-generic prefix | +1 | Email is not a generic address like `info`, `contact`, `admin` (strategy `with` only) |
| Valid verdict | +1 | `final_verdict` is `valid` rather than `catch_all` (strategy `with` only) |
| Generic prefix | +1 | Email IS a generic address (strategy `without` only) |

`confidence_tier` maps the raw score:

| Score | Tier |
|---|---|
| ≥ 3 | `high` |
| 2 | `medium` |
| ≤ 1 | `low` |

## `validation_method` Values

Written to the CSV output column. Describes which backend or path produced the terminal verdict.

| Value | Meaning |
|---|---|
| `ms_probe` | Microsoft GetCredentialType probe confirmed the address |
| `smtp_both` | Both Racknerd and bbops returned valid or catch_all |
| `smtp_racknerd` | Racknerd returned valid or catch_all; bbops did not |
| `smtp_bbops` | bbops returned valid or catch_all; Racknerd did not |
| `zuhal_rescue` | Zuhal rescue confirmed after both SMTP backends returned invalid |
| `unknown` | Could not determine the validation path from stored fields |

## `discovery_source` Values

| Value | Meaning |
|---|---|
| `dns` | Domain discovered via DNS MX lookup |
| `serper` | Domain discovered via Serper web search |
| `input` | Email provided directly in the input record |

## CSV Output Columns

Written to `valid_emails.csv` at run shutdown. Contains only VALIDATED records.

| Column | Description |
|---|---|
| `unique_id` | Composite key: `{filing_id}__{agent_id}` |
| `business_name` | Legal business name from filing |
| `agent_name` | Registered agent or officer name |
| `state` | State abbreviation |
| `email` | The confirmed deliverable email address |
| `final_verdict` | `valid` or `catch_all` |
| `confidence_tier` | `high`, `medium`, or `low` |
| `confidence_score` | Raw additive score (integer 0–4) |
| `verified` | `True` for `valid` or `catch_all`; `False` otherwise |
| `discovery_method` | `dns`, `serper`, or `input` |
| `validation_method` | Which backend path confirmed the email |
| `racknerd_verdict` | Racknerd SMTP verdict for the confirmed email |
| `bbops_verdict` | bbops verdict for the confirmed email |
| `zuhal_verdict` | Zuhal verdict, or `not_run` if Zuhal was not called |

## OR-of-Valids Reconciliation Table

Applied after concurrent Racknerd + bbops probes complete.

| Racknerd | bbops | Outcome | Action |
|---|---|---|---|
| `valid` | any | `valid` | VALIDATED |
| any | `valid` | `valid` | VALIDATED |
| `catch_all` | any | `catch_all` | VALIDATED |
| any | `catch_all` | `catch_all` | VALIDATED |
| `invalid` | `invalid` | `invalid` | Zuhal rescue |
| `invalid` | `error` / `not_run` | `unknown` | Re-queue, no attempt burned |
| `error` / `not_run` | `invalid` | `unknown` | Re-queue, no attempt burned |
| `error` | `error` | `unknown` | Re-queue, no attempt burned |
| tunnel down | any | `unknown` | Re-queue, no attempt burned |

## Environment Variables

| Variable | Required | Default |
|---|---|---|
| `SERPER_API_KEY` | Yes | — |
| `ZUHAL_API_KEY` | Yes (dispatcher) | — |
| `RACKNERD_HOST` | Yes (SOCKS5 mode) | — |
| `RACKNERD_SSH_USER` | No | `egress` |
| `RACKNERD_SSH_KEY` | No | `~/.ssh/racknerd_egress` |
| `BBOPS_BASE_URL` | No | `https://email-verifier.bbops.io` |

## CLI Flags

| Flag | Default | Effect |
|---|---|---|
| `--limit N` | none | Process only the first N input records |
| `--dry-run` | off | Mock all API calls; no cost incurred |
| `--max-cost USD` | none | Halt when cumulative spend reaches this amount |
| `--name NAME` | none | Namespace output to `output/NAME/`; enables resume |
| `--producer-only` | off | Run Stage 1 only; no SMTP probing |
| `--consumer-only` | off | Run Stage 2 only; skip producer |
| `--chunk-size N` | 100 | Records per producer batch |
| `--dns-concurrency N` | 100 | Parallel DNS resolver slots |
| `--dispatch-concurrency N` | 20 | Parallel dispatcher workers |
| `--dispatch-backend-timeout-s S` | 60.0 | Per-backend timeout for Racknerd and bbops |
| `--dispatch-chunk-size N` | 50 | Records claimed per dispatcher poll cycle |
| `--racknerd-host HOST` | — | VPS hostname for SSH tunnel (SOCKS5 mode) |
| `--racknerd-concurrency N` | 10 | Parallel SMTP connections |
| `--no-racknerd` | off | Disable Racknerd; bbops and Zuhal only |
| `--racknerd-direct` | off | Skip SOCKS5; connect directly to MX servers |
| `--bbops-base-url URL` | bbops.io | Override bbops API base URL |
| `--max-consecutive-errors N` | 10 | Halt after N consecutive producer errors |

## API Costs

| Service | Cost per call | Notes |
|---|---|---|
| Serper (producer) | $0.001 | DNS-miss path; always one call per undiscovered record |
| Serper (dispatcher) | $0.001 | Fallback after all patterns exhausted without a valid email |
| Zuhal | $0.0005 | Rescue only; called after both SMTP backends return invalid |
| Racknerd SMTP | $0 | Fixed VPS cost; no per-probe fee |
| bbops | per contract | Async batch verifier |
| MS probe | $0 | Free Microsoft API; no quota impact |

## Backoff Parameters

| Service | Base delay | Max delay |
|---|---|---|
| DNS | 0.5s | 8s |
| Serper | 1.0s | 32s |
| Zuhal | 1.0s | 64s |
| bbops | 1.0s | 60s |
| Racknerd | 1.0s | 32s |

Zuhal circuit breaker: fail_max=5 consecutive failures, reset timeout=600s.

## Racknerd SpamhausGuard

| Parameter | Value |
|---|---|
| Detection window | 60 seconds |
| Block threshold | 100 events in window |
| Cooldown on trigger | 300 seconds |
| Scope | All SMTP probes in the process |
