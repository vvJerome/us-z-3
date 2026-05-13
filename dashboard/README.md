# Live pipeline dashboard

Single-page dashboard for monitoring a running pipeline. Reads `pipeline.db` in
read-only mode and refreshes every 2 seconds.

## Run

```bash
.venv/bin/python -m dashboard --db output/run_part2_20260509/pipeline.db --port 8765
```

Flags:

| Flag | Default | Effect |
|---|---|---|
| `--db PATH` | required | path to `pipeline.db` |
| `--host` | `127.0.0.1` | bind address |
| `--port` | `8765` | bind port |
| `--poll-ttl` | `1.5` | server cache TTL (seconds) |
| `--cost-ceiling` | `600.0` | for the cost gauge |

## View from another host (e.g. Fedora)

```bash
ssh -L 8765:127.0.0.1:8765 devonly@49.12.127.119
# then open http://127.0.0.1:8765 locally
```

## Panels

- **State machine** — live counts for every `record_state`
- **Throughput (60 min)** — per-minute terminal verdicts as a bar chart, with
  peak / avg / last-15m / trend stats below
- **Cost** — spent vs ceiling, per-service breakdown (Serper / Zuhal), and a
  projected total based on current % complete
- **Backend verdicts (30 min)** — Racknerd / bbops / Zuhal distribution + error %
- **Discovery (cumulative)** — DNS vs Serper vs failed, with share bar and
  overall hit-rate label
- **Run timeline (per hour)** — stacked area of every terminal verdict
  (valid / catch_all / invalid / error / discovery_failed) across the full run,
  with a thin cumulative-valid line underneath for at-a-glance progress
- **Recent validations** — latest 30 VALIDATED rows with per-backend pills
- **Top errors (1 h)** — most-frequent error messages from Racknerd + bbops

## Efficiency notes

- Cache: server holds a snapshot for ~1.5 s; client polls every 2 s. DB cost is
  bounded regardless of viewer count.
- DB access is `file:…?mode=ro` URI with `busy_timeout=2000` — read-only opens
  cannot block the writer; WAL mode keeps reads lock-free.
- The wire payload is ~2 KB gzipped per refresh.
- Adding `CREATE INDEX IF NOT EXISTS idx_records_updated_at` is recommended —
  drops the 30-minute window scan from ~190 ms to <20 ms.

## Endpoints

| Path | Purpose |
|---|---|
| `GET /` | dashboard HTML |
| `GET /api/snapshot` | full panel JSON, cached |
| `GET /api/health` | `{"db_ok": true, "rows": N}` |
