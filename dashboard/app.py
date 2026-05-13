from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import queries


RUN_HISTORY_TTL = 30.0


def create_app(db_path: str, poll_ttl: float = 1.5, cost_ceiling: float | None = None) -> FastAPI:
    app = FastAPI(title="us-z-3 dashboard", docs_url=None, redoc_url=None)
    app.add_middleware(GZipMiddleware, minimum_size=512)

    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    conn = queries.open_ro(db_path)

    cache: dict[str, object] = {"as_of": 0.0, "payload": None}
    rh_cache: dict[str, object] = {"as_of": 0.0, "data": None}
    lock = asyncio.Lock()

    def _build_snapshot() -> tuple[dict, float]:
        t0 = time.perf_counter()
        sc = queries.state_counts(conn)
        payload = {
            "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "build_ms": None,
            "run_id": queries.run_id(conn),
            "states": sc["states"],
            "totals": {"all": sc["total"], "terminal": sc["terminal"], "pending": sc["pending"]},
            "rate": queries.rate(conn, sc["pending"]),
            "throughput_60min": queries.throughput_60min(conn),
            "backends": queries.backends(conn),
            "discovery": queries.discovery_detail(conn),
            "cost": queries.cost(conn, cost_ceiling),
            "cost_breakdown": queries.cost_breakdown(conn),
            "run_history": rh_cache["data"] or [],
            "recent_validated": queries.recent_validated(conn, limit=30),
            "top_recent_errors": queries.top_recent_errors(conn, limit=10),
        }
        return payload, time.perf_counter() - t0

    def _refresh_run_history_sync() -> list:
        return queries.throughput_full_run(conn)

    async def _refresh_if_stale() -> dict:
        now = time.monotonic()
        if cache["payload"] is not None and (now - cache["as_of"]) < poll_ttl:
            return cache["payload"]  # type: ignore[return-value]
        async with lock:
            now = time.monotonic()
            if cache["payload"] is not None and (now - cache["as_of"]) < poll_ttl:
                return cache["payload"]  # type: ignore[return-value]
            if rh_cache["data"] is None or (now - rh_cache["as_of"]) >= RUN_HISTORY_TTL:
                rh_cache["data"] = await asyncio.to_thread(_refresh_run_history_sync)
                rh_cache["as_of"] = now
            payload, elapsed = await asyncio.to_thread(_build_snapshot)
            payload["build_ms"] = round(elapsed * 1000, 1)
            cache["payload"] = payload
            cache["as_of"] = now
            return payload

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/api/snapshot")
    async def snapshot() -> JSONResponse:
        payload = await _refresh_if_stale()
        return JSONResponse(payload)

    @app.get("/api/health")
    async def health() -> dict:
        row = conn.execute("SELECT COUNT(*) FROM records").fetchone()
        return {"db_ok": True, "rows": row[0]}

    return app
