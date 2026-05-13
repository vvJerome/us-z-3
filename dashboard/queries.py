from __future__ import annotations

import sqlite3
from typing import Any

TERMINAL_STATES = ("VALIDATED", "VALIDATION_FAILED", "COST_SKIPPED")
PENDING_STATES = ("DISCOVERED", "VALIDATING", "NEEDS_ZUHAL", "ZUHAL_VALIDATING")


def open_ro(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 2000")
    return conn


def state_counts(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT record_state, COUNT(*) AS n FROM records GROUP BY record_state"
    ).fetchall()
    states = {r["record_state"]: r["n"] for r in rows}
    total = sum(states.values())
    terminal = sum(states.get(s, 0) for s in TERMINAL_STATES) + states.get("DISCOVERY_FAILED", 0)
    pending = sum(states.get(s, 0) for s in PENDING_STATES)
    return {"states": states, "total": total, "terminal": terminal, "pending": pending}


def rate(conn: sqlite3.Connection, pending: int) -> dict[str, Any]:
    last_15 = conn.execute(
        """SELECT COUNT(*) FROM records
           WHERE record_state IN ('VALIDATED','VALIDATION_FAILED')
             AND updated_at > datetime('now', '-15 minutes')"""
    ).fetchone()[0]
    per_hour = last_15 * 4
    eta_hours = (pending / per_hour) if per_hour > 0 else None
    return {
        "last_15min": last_15,
        "per_hour": per_hour,
        "eta_hours": round(eta_hours, 2) if eta_hours is not None else None,
    }


def throughput_60min(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT strftime('%H:%M', updated_at) AS minute, COUNT(*) AS n
           FROM records
           WHERE record_state IN ('VALIDATED','VALIDATION_FAILED')
             AND updated_at > datetime('now', '-60 minutes')
           GROUP BY 1
           ORDER BY 1"""
    ).fetchall()
    return [{"minute": r["minute"], "count": r["n"]} for r in rows]


def _verdict_dist(conn: sqlite3.Connection, column: str) -> dict[str, int]:
    rows = conn.execute(
        f"""SELECT {column} AS v, COUNT(*) AS n FROM records
            WHERE {column} IS NOT NULL
              AND updated_at > datetime('now', '-30 minutes')
            GROUP BY 1"""  # column is a hardcoded identifier, not user input
    ).fetchall()
    return {r["v"]: r["n"] for r in rows}


def backends(conn: sqlite3.Connection) -> dict[str, Any]:
    rk = _verdict_dist(conn, "racknerd_status")
    bb = _verdict_dist(conn, "bbops_status")
    zu = _verdict_dist(conn, "zuhal_status")

    def err_pct(d: dict[str, int]) -> float:
        total = sum(d.values())
        return round(d.get("error", 0) / total * 100, 1) if total else 0.0

    return {
        "racknerd": {**rk, "error_pct": err_pct(rk), "total": sum(rk.values())},
        "bbops": {**bb, "error_pct": err_pct(bb), "total": sum(bb.values())},
        "zuhal": {**zu, "error_pct": err_pct(zu), "total": sum(zu.values())},
    }


def discovery(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        """SELECT
              SUM(CASE WHEN discovery_source='dns'    THEN 1 ELSE 0 END) AS dns,
              SUM(CASE WHEN discovery_source='serper' THEN 1 ELSE 0 END) AS serper,
              SUM(CASE WHEN record_state='DISCOVERY_FAILED' THEN 1 ELSE 0 END) AS failed
           FROM records"""
    ).fetchone()
    return {"dns": rows["dns"] or 0, "serper": rows["serper"] or 0, "failed": rows["failed"] or 0}


def discovery_detail(conn: sqlite3.Connection) -> dict[str, Any]:
    base = discovery(conn)
    total = base["dns"] + base["serper"] + base["failed"]
    hit = base["dns"] + base["serper"]
    return {
        **base,
        "total_input": total,
        "hit_rate_pct": round(hit / total * 100, 1) if total else 0.0,
    }


def cost_breakdown(conn: sqlite3.Connection) -> dict[str, Any]:
    from pipeline.constants import API_COSTS

    row = conn.execute(
        """SELECT serper_producer_calls, serper_dispatcher_calls, zuhal_calls
           FROM stats LIMIT 1"""
    ).fetchone()
    if not row:
        return {"services": []}
    sp = row["serper_producer_calls"] or 0
    sd = row["serper_dispatcher_calls"] or 0
    zu = row["zuhal_calls"] or 0
    serper_cost = sp * API_COSTS["serper_producer"] + sd * API_COSTS["serper_dispatcher"]
    zuhal_cost = zu * API_COSTS["zuhal"]
    return {
        "services": [
            {"name": "serper", "calls": sp + sd, "cost_usd": round(serper_cost, 4)},
            {"name": "zuhal",  "calls": zu,     "cost_usd": round(zuhal_cost,  4)},
        ]
    }


def throughput_full_run(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT strftime('%Y-%m-%dT%H:00', updated_at) AS hour,
                  SUM(CASE WHEN final_verdict='valid'      THEN 1 ELSE 0 END) AS valid,
                  SUM(CASE WHEN final_verdict='catch_all'  THEN 1 ELSE 0 END) AS catch_all,
                  SUM(CASE WHEN final_verdict='invalid'    THEN 1 ELSE 0 END) AS invalid,
                  SUM(CASE WHEN final_verdict IN ('error','unknown') THEN 1 ELSE 0 END) AS errored,
                  SUM(CASE WHEN record_state='DISCOVERY_FAILED' THEN 1 ELSE 0 END) AS disc_failed
           FROM records
           WHERE record_state IN ('VALIDATED','VALIDATION_FAILED','DISCOVERY_FAILED')
             AND updated_at IS NOT NULL
           GROUP BY 1
           ORDER BY 1"""
    ).fetchall()
    return [dict(r) for r in rows]


def cost(conn: sqlite3.Connection, ceiling: float | None) -> dict[str, Any]:
    row = conn.execute("SELECT estimated_cost_usd FROM stats LIMIT 1").fetchone()
    spent = round(row["estimated_cost_usd"], 4) if row and row["estimated_cost_usd"] else 0.0
    out: dict[str, Any] = {"spent_usd": spent, "ceiling_usd": ceiling}
    if ceiling:
        out["pct"] = round(spent / ceiling * 100, 2)
    return out


def recent_validated(conn: sqlite3.Connection, limit: int = 30) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT unique_id, candidate_email, racknerd_status, bbops_status, zuhal_status,
                  final_verdict, updated_at
           FROM records
           WHERE record_state = 'VALIDATED'
           ORDER BY updated_at DESC, id DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def top_recent_errors(conn: sqlite3.Connection, limit: int = 10) -> list[dict[str, Any]]:
    rk = conn.execute(
        """SELECT 'racknerd' AS source, racknerd_message AS message, COUNT(*) AS n
           FROM records
           WHERE racknerd_status = 'error'
             AND racknerd_message IS NOT NULL AND racknerd_message != ''
             AND updated_at > datetime('now', '-60 minutes')
           GROUP BY racknerd_message"""
    ).fetchall()
    bb = conn.execute(
        """SELECT 'bbops' AS source, bbops_message AS message, COUNT(*) AS n
           FROM records
           WHERE bbops_status = 'error'
             AND bbops_message IS NOT NULL AND bbops_message != ''
             AND updated_at > datetime('now', '-60 minutes')
           GROUP BY bbops_message"""
    ).fetchall()
    merged = [dict(r) for r in rk] + [dict(r) for r in bb]
    merged.sort(key=lambda x: x["n"], reverse=True)
    for m in merged:
        if m["message"] and len(m["message"]) > 140:
            m["message"] = m["message"][:137] + "..."
    return merged[:limit]


def run_id(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT run_id FROM stats LIMIT 1").fetchone()
    return row["run_id"] if row else None
