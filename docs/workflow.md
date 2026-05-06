# Full Workflow

## 1. Setup

Copy `.env.example` to `.env` and fill in the required keys.

```
SERPER_API_KEY=...
ZUHAL_API_KEY=...
RACKNERD_HOST=...         # only needed in SOCKS5 mode; omit with --racknerd-direct
RACKNERD_SSH_KEY=...      # path to ed25519 key for egress VPS
```

Create the Python virtual environment and install dependencies.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Prepare Input

Input is a JSONL file where each line is a JSON object with at minimum:

```json
{"unique_id": "filing123__agent456", "business_name": "Smith Electric LLC", "agent_name": "John Smith", "state": "NC"}
```

The file lives in `input/`. Place it there before running.

## 3. Validate Wiring (Dry Run)

Before spending API credits, confirm the pipeline starts correctly.

```bash
python -m pipeline run \
  -i input/nc_retry_300k.jsonl \
  --limit 10 \
  --dry-run \
  --name test
```

Dry run mode mocks all external calls. The pipeline should complete with no errors and write `output/test/pipeline.db` and `output/test/valid_emails.csv`.

## 4. Run Live

Start a live run with a cost ceiling. The `--name` flag namespaces all output and enables resume.

```bash
python -m pipeline run \
  -i input/nc_retry_300k.jsonl \
  --limit 1000 \
  --max-cost 2.00 \
  --name nc_1k \
  --racknerd-direct
```

Use `--racknerd-direct` when the pipeline is running on the egress VPS itself. Omit it and add `--racknerd-host $RACKNERD_HOST` when running from a separate machine that needs the SSH SOCKS5 tunnel.

The pipeline starts two concurrent workers and logs to stdout. Producer log lines show `BUCKET pv` (discovered) or `BUCKET df` (discovery failed). Dispatcher log lines show verdicts per email.

## 5. Monitor Progress

In a second terminal, watch the live state counts.

```bash
python -m pipeline status \
  --db output/nc_1k/pipeline.db \
  --watch 5
```

This polls every 5 seconds and prints state counts, verdict distribution, and cumulative cost.

Alternatively, query the database directly.

```bash
sqlite3 output/nc_1k/pipeline.db \
  "SELECT record_state, COUNT(*) FROM records GROUP BY record_state"
```

## 6. Checkpoint-Based Batched Run

For large runs where you want to review progress at intervals, use the checkpoint script.

```bash
bash scripts/run_checkpoints.sh \
  --racknerd-direct \
  --max-batches 10
```

This runs 10 batches of 100 records each. After every batch it prints a report showing state counts, discovery rates, backend call counts, and batch cost. It then prompts whether to continue. Reports are also appended to `output/nc_1k/checkpoints.log`.

At the end it checks for any DISCOVERED records left over from the final batch and runs a drain pass (`--consumer-only`) to clear them.

## 7. Producer Flow (per record)

The producer claims each RAW record and runs these steps in order.

1. Generate domain stem candidates from `business_name` and `agent_name` by removing stop words, abbreviations, and legal suffixes.

2. For each stem, try `.com`, `.net`, and `.org` in parallel via DNS MX lookup.

3. If a domain with a mail server is found:
   - Generate ranked candidate emails by applying name templates to the domain.
   - Templates include patterns like `firstname.lastname`, `flastname`, `firstname`, `f.lastname`, `info`, `contact`, etc.
   - Templates are ranked by historical win rate from `pattern_stats` for the discovered MX provider.
   - Write the record as DISCOVERED with `discovery_source=dns` and the ranked `candidate_emails` list.

4. If DNS finds nothing:
   - Build a Serper search query: `"{business_name}" {state} email`.
   - Call the Serper API (Google search). Parse organic results for domain and email signals.
   - If `strategy == "with"` and no result is found, run a second query using `"{agent_name}" {state} email`.
   - Check the result domain against the fallback blocklist (known directory and aggregator sites). Reject blocklisted domains.
   - On a valid result: write DISCOVERED with `discovery_source=serper`.
   - On no result: write DISCOVERY_FAILED.

5. Serper results are cached in the database for 30 days per `(business_name_norm, agent_name_norm, state, provider)` key. A cached hit does not consume an API call.

6. After each batch, signal the dispatcher via a named pipe so it wakes immediately.

## 8. Dispatcher Flow (per record)

The dispatcher atomically claims DISCOVERED records and iterates each `candidate_email` in ranked order.

For each candidate:

**Step 1 — MS probe (free)**

Check whether the domain's MX provider is Microsoft-managed (Office 365, Exchange Online, Outlook, Hotmail). If yes:

- Call the Microsoft GetCredentialType endpoint with the email address.
- Response `valid` → write VALIDATED, stop iterating candidates.
- Response `invalid` → skip this candidate, move to the next one.
- Response `unknown` / `error` → fall through to Step 2.

**Step 2 — Concurrent SMTP probe**

Run both backends simultaneously via `asyncio.gather`.

Racknerd: connect to the domain's highest-priority MX host on port 25. Send EHLO, MAIL FROM, RCPT TO. Interpret the response code: 2xx–3xx = `valid`, 5xx with known-invalid keywords = `invalid`, 5xx with spamhaus keywords = `blocked`, 4xx = `error` (try next MX host if available).

bbops: submit the email to the bbops.io API as part of an async batch. Poll for the result. Return `valid`, `invalid`, `catch_all`, or `error`.

**Step 3 — Reconciliation**

Compare the two results using OR-of-valids logic:
- If either says `valid` → outcome `valid`.
- If either says `catch_all` and neither said `valid` → outcome `catch_all`.
- If both say `invalid` → outcome `invalid`, proceed to Step 4.
- Any other combination → outcome `unknown`: re-queue this record as DISCOVERED without burning a dispatch attempt. The record will be retried on the next poll cycle.

If the outcome is `valid` or `catch_all` → write VALIDATED with the email address and stop. Do not call Zuhal.

**Step 4 — Zuhal rescue (paid)**

Only reached when both SMTP backends returned `invalid`.

Check whether the cost ceiling has been reached. If yes → write COST_SKIPPED and stop.

Call the Zuhal API with the email address. Zuhal charges $0.0005 per call.

- If the Zuhal circuit breaker is open (five consecutive failures within the breaker window) → re-queue as DISCOVERED without burning a dispatch attempt. The circuit resets automatically after 600 seconds.
- Response `valid` or `accept-all` → write VALIDATED, stop.
- Response `invalid` or `error` → move to the next candidate email.

**Candidate exhaustion**

If all candidate emails have been tried without a valid result: write VALIDATION_FAILED with the final `racknerd_status` and `bbops_status` from the last probe attempt. `dispatch_attempts` is incremented.

After any terminal verdict (valid, invalid, or catch_all from any backend), record the result in `pattern_stats` for the template used by that email. This updates the ranking for future records with the same MX provider.

## 9. Output Files

Output is written at pipeline shutdown, not during the run. All files go to `output/{name}/`.

**`pipeline.db`** — SQLite database. Full audit trail. Every record, every state, every backend verdict, every timestamp. This is the authoritative source.

**`valid_emails.csv`** — One row per VALIDATED record. Columns: `unique_id`, `business_name`, `agent_name`, `state`, `email`, `final_verdict`, `confidence_tier`, `confidence_score`, `verified`, `discovery_method`, `validation_method`, `racknerd_verdict`, `bbops_verdict`, `zuhal_verdict`.

**`results.json`** — Run summary: state counts, verdict counts, discovery rates, validation rate, cumulative cost. Same data as `python -m pipeline status` but written once to disk.

## 10. Resume an Interrupted Run

If the run is interrupted, restart it with the same `--name`. The producer reads `producer_offset` from the `checkpoints` table and skips records already ingested. The dispatcher finds any DISCOVERED or VALIDATING records and continues from there.

At startup, `recover_stale_validating()` resets any VALIDATING records from the previous session back to DISCOVERED so they are re-processed cleanly.

```bash
python -m pipeline run \
  -i input/nc_retry_300k.jsonl \
  --limit 1000 \
  --max-cost 2.00 \
  --name nc_1k \
  --racknerd-direct
```

The command is identical to the original. Resume is automatic.

## 11. Reset Failed Records

To retry records that failed discovery or validation:

```bash
# Re-queue discovery failures
python -m pipeline reset \
  --db output/nc_1k/pipeline.db \
  --status discovery_failed

# Re-queue validation failures
python -m pipeline reset \
  --db output/nc_1k/pipeline.db \
  --status validation_failed
```

Use `--dry-run` to preview the count before committing the reset.

## 12. Deploy to VPS and Run Remotely

The `scripts/deploy.sh` script rsyncs the project to the egress VPS and installs dependencies.

```bash
bash scripts/deploy.sh
```

Then SSH into the VPS and run the pipeline there. On the VPS, use `--racknerd-direct` because the VPS is the egress IP — no SOCKS5 tunnel is needed.

```bash
ssh -i ~/.ssh/racknerd_egress root@{VPS_HOST}
cd /root/us-z-3
bash scripts/run_checkpoints.sh --racknerd-direct --max-batches 10
```

The checkpoint script runs inside the shell. To run it unattended, start it in a tmux session.

```bash
tmux new-session -d -s pipeline
tmux send-keys -t pipeline \
  'bash scripts/run_checkpoints.sh --racknerd-direct --max-batches 10 2>&1 | tee output/run.log' \
  Enter
```

Monitor from your local machine:

```bash
ssh -i ~/.ssh/racknerd_egress root@{VPS_HOST} \
  "tail -f /root/us-z-3/output/run.log"
```

## 13. Inspect Results

Query the database for a high-level summary:

```bash
sqlite3 output/nc_1k/pipeline.db "
  SELECT record_state, COUNT(*)
  FROM records
  GROUP BY record_state
"
```

Query for validated records by backend path:

```bash
sqlite3 output/nc_1k/pipeline.db "
  SELECT zuhal_status, COUNT(*)
  FROM records
  WHERE record_state = 'VALIDATED'
  GROUP BY zuhal_status
"
```

Query for discovery source breakdown:

```bash
sqlite3 output/nc_1k/pipeline.db "
  SELECT discovery_source, COUNT(*)
  FROM records
  WHERE discovery_source IS NOT NULL
  GROUP BY discovery_source
"
```

Open the CSV in any spreadsheet tool to filter by `confidence_tier`, `validation_method`, or `final_verdict`.
