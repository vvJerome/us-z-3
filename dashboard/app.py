from __future__ import annotations

import asyncio
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import queries


RUN_HISTORY_TTL = 30.0
RESCAN_INTERVAL = 60.0
MIN_RECORDS = 1_000


def create_app(
    output_dir: str,
    extra_dbs: list[str] | None = None,
    poll_ttl: float = 1.5,
    cost_ceiling: float | None = None,
) -> FastAPI:
    app = FastAPI(title="us-z-3 dashboard", docs_url=None, redoc_url=None)
    app.add_middleware(GZipMiddleware, minimum_size=512)

    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    out_dir = Path(output_dir)
    _extra: list[str] = extra_dbs or []

    # run_id → open read-only connection
    conns: dict[str, sqlite3.Connection] = {}
    # run_id → {cache, rh_cache, lock}
    run_state: dict[str, dict] = {}

    def _scan_new() -> list[tuple[str, sqlite3.Connection]]:
        """Scan output_dir and extra_dbs; return (run_id, conn) for runs not yet open."""
        result: list[tuple[str, sqlite3.Connection]] = []
        candidates: list[Path] = sorted(out_dir.glob("*/pipeline.db"))
        for extra in _extra:
            p = Path(extra)
            if p.is_file() and p not in candidates:
                candidates.append(p)
        for db_path in candidates:
            if not db_path.is_file():
                continue
            try:
                conn = queries.open_ro(str(db_path))
                total = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
                if total < MIN_RECORDS:
                    conn.close()
                    continue
                rid = queries.run_id(conn) or db_path.parent.name
                if rid in conns:
                    conn.close()
                    continue
                result.append((rid, conn))
            except Exception:
                pass
        return result

    def _most_recent_run_id() -> str | None:
        if not conns:
            return None

        def _lu(rid: str) -> str:
            try:
                row = conns[rid].execute("SELECT MAX(updated_at) FROM records").fetchone()
                return row[0] or ""
            except Exception:
                return ""

        return max(conns.keys(), key=_lu)

    def _build_snapshot(run_id: str) -> tuple[dict, float]:
        conn = conns[run_id]
        state = run_state[run_id]
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
            "run_history": state["rh_cache"]["data"] or [],
            "recent_validated": queries.recent_validated(conn, limit=30),
            "top_recent_errors": queries.top_recent_errors(conn, limit=10),
        }
        return payload, time.perf_counter() - t0

    async def _get_snapshot(run_id: str) -> dict:
        state = run_state[run_id]
        cache = state["cache"]
        rh_cache = state["rh_cache"]
        lock = state["lock"]
        now = time.monotonic()
        if cache["payload"] is not None and (now - cache["as_of"]) < poll_ttl:
            return cache["payload"]  # type: ignore[return-value]
        async with lock:
            now = time.monotonic()
            if cache["payload"] is not None and (now - cache["as_of"]) < poll_ttl:
                return cache["payload"]  # type: ignore[return-value]
            if rh_cache["data"] is None or (now - rh_cache["as_of"]) >= RUN_HISTORY_TTL:
                rh_cache["data"] = await asyncio.to_thread(
                    queries.throughput_full_run, conns[run_id]
                )
                rh_cache["as_of"] = now
            payload, elapsed = await asyncio.to_thread(_build_snapshot, run_id)
            payload["build_ms"] = round(elapsed * 1000, 1)
            cache["payload"] = payload
            cache["as_of"] = now
            return payload

    async def _register(new_runs: list[tuple[str, sqlite3.Connection]]) -> None:
        for rid, conn in new_runs:
            conns[rid] = conn
            run_state[rid] = {
                "cache": {"as_of": 0.0, "payload": None},
                "rh_cache": {"as_of": 0.0, "data": None},
                "lock": asyncio.Lock(),
            }

    @app.on_event("startup")
    async def _startup() -> None:
        new_runs = await asyncio.to_thread(_scan_new)
        await _register(new_runs)

        async def _rescan_loop() -> None:
            while True:
                await asyncio.sleep(RESCAN_INTERVAL)
                new = await asyncio.to_thread(_scan_new)
                if new:
                    await _register(new)

        asyncio.create_task(_rescan_loop(), name="db-rescanner")

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/api/runs")
    async def list_runs() -> JSONResponse:
        result = []
        for rid, conn in list(conns.items()):
            try:
                summary = await asyncio.to_thread(queries.run_summary, conn, cost_ceiling)
                summary["run_id"] = rid
                result.append(summary)
            except Exception:
                pass
        result.sort(key=lambda x: x.get("last_updated") or "", reverse=True)
        return JSONResponse({"runs": result})

    @app.get("/api/snapshot")
    async def snapshot(run_id: str | None = None) -> JSONResponse:
        rid = run_id or _most_recent_run_id()
        if not rid or rid not in conns:
            return JSONResponse({"error": "run not found"}, status_code=404)
        payload = await _get_snapshot(rid)
        return JSONResponse(payload)

    @app.get("/api/health")
    async def health() -> dict:
        ok = 0
        for conn in conns.values():
            try:
                conn.execute("SELECT COUNT(*) FROM records").fetchone()
                ok += 1
            except Exception:
                pass
        return {"db_ok": ok > 0, "runs": len(conns), "ok_runs": ok}

    return app
