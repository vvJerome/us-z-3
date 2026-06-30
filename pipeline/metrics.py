"""Prometheus metrics endpoint.

Serves /metrics on port 9090 (plain text Prometheus exposition format).
Reads live data from the SQLite stats and records tables.
Start as a background asyncio task from __main__.cmd_run().
"""

from __future__ import annotations

import asyncio
import logging

import aiohttp.web
import aiosqlite

from pipeline.constants import METRICS_PORT

logger = logging.getLogger("pipeline.metrics")

_PORT = METRICS_PORT


async def _handle(request: aiohttp.web.Request) -> aiohttp.web.Response:
    conn: aiosqlite.Connection = request.app["conn"]
    lines: list[str] = []

    # Record state counters
    async with conn.execute(
        "SELECT record_state, COUNT(*) FROM records GROUP BY record_state"
    ) as cur:
        async for row in cur:
            state = (row[0] or "unknown").replace("-", "_").lower()
            lines.append(f'pipeline_records_total{{state="{state}"}} {row[1]}')

    # Cost + API call counts
    async with conn.execute(
        "SELECT estimated_cost_usd, serper_producer_calls, serper_dispatcher_calls, "
        "zuhal_calls, racknerd_probes, bbops_probes, backend_disagreements, serper_cache_hits "
        "FROM stats ORDER BY rowid DESC LIMIT 1"
    ) as cur:
        stats_row = await cur.fetchone()
        if stats_row:
            lines.append(f"pipeline_cost_usd {stats_row[0] or 0:.6f}")
            serper_total = (stats_row[1] or 0) + (stats_row[2] or 0)
            for svc, n in (
                ("serper_producer", stats_row[1]),
                ("serper_dispatcher", stats_row[2]),
                ("serper", serper_total),
                ("zuhal", stats_row[3]),
                ("racknerd", stats_row[4]),
                ("bbops", stats_row[5]),
            ):
                lines.append(f'pipeline_api_calls_total{{service="{svc}"}} {n or 0}')
            lines.append(f"pipeline_backend_disagreements_total {stats_row[6] or 0}")
            lines.append(f"pipeline_serper_cache_hits_total {stats_row[7] or 0}")

    body = "\n".join(lines) + "\n"
    return aiohttp.web.Response(text=body, content_type="text/plain")


async def serve_metrics(conn: aiosqlite.Connection, stop_event: asyncio.Event) -> None:
    """Run the metrics HTTP server until stop_event is set."""
    app = aiohttp.web.Application()
    app["conn"] = conn
    app.router.add_get("/metrics", _handle)

    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "0.0.0.0", _PORT)
    try:
        await site.start()
    except OSError:
        logger.warning("Metrics port %d already in use — running without metrics", _PORT)
        await runner.cleanup()
        await stop_event.wait()
        return
    logger.info("Metrics endpoint running on :%d/metrics", _PORT)

    await stop_event.wait()
    await runner.cleanup()
    logger.info("Metrics endpoint stopped")
