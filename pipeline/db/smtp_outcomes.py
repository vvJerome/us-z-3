"""Per-(worker, provider) SMTP outcome counters.

Feeds provider-aware routing, IP-reputation degradation detection, and failover
in the SMTP fleet (Improve-Existing item 5). Mirrors the pattern_stats upsert.
"""
from __future__ import annotations

import aiosqlite

# Counter columns in commit order; ms_valid rolls up into `valid`. Statuses absent
# from this map (e.g. not_run) are not telemetry — the worker did not actually probe.
_COUNTER_COLUMNS: tuple[str, ...] = ("valid", "invalid", "catch_all", "blocked", "error")
_STATUS_TO_COUNTER: dict[str, str] = {
    "valid": "valid",
    "ms_valid": "valid",
    "invalid": "invalid",
    "catch_all": "catch_all",
    "blocked": "blocked",
    "error": "error",
}


async def record_smtp_outcome(
    conn: aiosqlite.Connection,
    worker_id: str,
    provider: str,
    status: str,
) -> None:
    """Increment the per-(worker, provider) counter for a terminal SMTP probe status."""
    target = _STATUS_TO_COUNTER.get(status)
    if target is None:
        return
    inc = tuple(1 if col == target else 0 for col in _COUNTER_COLUMNS)
    await conn.execute(
        """
        INSERT INTO smtp_outcomes (worker_id, provider, valid, invalid, catch_all, blocked, error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(worker_id, provider) DO UPDATE SET
            valid     = valid + ?,
            invalid   = invalid + ?,
            catch_all = catch_all + ?,
            blocked   = blocked + ?,
            error     = error + ?,
            updated_at = datetime('now')
        """,
        (worker_id, provider, *inc, *inc),
    )
    await conn.commit()


async def get_worker_provider_stats(
    conn: aiosqlite.Connection,
    worker_id: str | None = None,
) -> list[dict]:
    """Return per-(worker, provider) counters; filter to one worker if given."""
    sql = (
        "SELECT worker_id, provider, valid, invalid, catch_all, blocked, error, updated_at "
        "FROM smtp_outcomes"
    )
    params: tuple[str, ...] = ()
    if worker_id is not None:
        sql += " WHERE worker_id = ?"
        params = (worker_id,)
    sql += " ORDER BY worker_id, provider"
    async with conn.execute(sql, params) as cursor:
        return [
            {
                "worker_id": row[0], "provider": row[1], "valid": row[2],
                "invalid": row[3], "catch_all": row[4], "blocked": row[5],
                "error": row[6], "updated_at": row[7],
            }
            async for row in cursor
        ]
