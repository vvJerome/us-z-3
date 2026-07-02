from __future__ import annotations

import aiosqlite

from pipeline.db.schema import State, UPSERT_CHECKPOINT_SQL


async def get_checkpoint(conn: aiosqlite.Connection, key: str) -> str | None:
    async with conn.execute(
        "SELECT value FROM checkpoints WHERE key = ?", (key,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else None


async def upsert_checkpoint(conn: aiosqlite.Connection, key: str, value: str) -> None:
    await conn.execute(UPSERT_CHECKPOINT_SQL, (key, value))


async def insert_failure(
    conn: aiosqlite.Connection,
    unique_id: str,
    phase: str,
    attempt: int,
    error_type: str,
    error_detail: str,
) -> None:
    await conn.execute(
        "INSERT INTO failures (unique_id, phase, attempt, error_type, error_detail) "
        "VALUES (?, ?, ?, ?, ?)",
        (unique_id, phase, attempt, error_type, error_detail),
    )
    await conn.commit()


async def upsert_stats(
    conn: aiosqlite.Connection,
    run_id: str,
    **fields: object,
) -> None:
    cols = ["run_id"]
    vals: list[object] = [run_id]
    updates = ["updated_at = datetime('now')"]

    for col, val in fields.items():
        cols.append(col)
        vals.append(val)
        updates.append(f"{col} = excluded.{col}")

    placeholders = ", ".join(["?"] * len(cols))
    col_str = ", ".join(cols)
    update_str = ", ".join(updates)

    sql = (
        f"INSERT INTO stats ({col_str}) VALUES ({placeholders}) "
        f"ON CONFLICT(run_id) DO UPDATE SET {update_str}"
    )
    await conn.execute(sql, vals)
    await conn.commit()


async def upsert_producer_heartbeat(conn: aiosqlite.Connection) -> None:
    await conn.execute(
        """
        UPDATE stats SET last_producer_heartbeat = datetime('now'), updated_at = datetime('now')
        WHERE rowid = (SELECT MAX(rowid) FROM stats)
        """
    )
    await conn.commit()


async def upsert_dispatcher_heartbeat(conn: aiosqlite.Connection) -> None:
    await conn.execute(
        """
        UPDATE stats SET last_dispatcher_heartbeat = datetime('now'), updated_at = datetime('now')
        WHERE rowid = (SELECT MAX(rowid) FROM stats)
        """
    )
    await conn.commit()


async def get_status_summary(conn: aiosqlite.Connection) -> dict:
    summary: dict = {}

    async with conn.execute(
        "SELECT record_state, COUNT(*) FROM records GROUP BY record_state"
    ) as cursor:
        summary["records_by_state"] = {row[0]: row[1] async for row in cursor}

    async with conn.execute("SELECT COUNT(*) FROM records") as cursor:
        row = await cursor.fetchone()
        summary["total_records"] = row[0] if row else 0

    offset = await get_checkpoint(conn, "producer_offset")
    summary["producer_offset"] = int(offset) if offset else 0

    done = await get_checkpoint(conn, "producer_done")
    summary["producer_done"] = done == "true"

    async with conn.execute("SELECT * FROM stats LIMIT 1") as cursor:
        row = await cursor.fetchone()
        if row:
            summary["stats"] = dict(row)

    async with conn.execute(
        "SELECT phase, COUNT(*) FROM failures GROUP BY phase"
    ) as cursor:
        summary["failures_by_phase"] = {row[0]: row[1] async for row in cursor}

    async with conn.execute(
        "SELECT final_verdict, COUNT(*) FROM records WHERE final_verdict IS NOT NULL GROUP BY final_verdict"
    ) as cursor:
        summary["records_by_verdict"] = {row[0]: row[1] async for row in cursor}

    # Rolling throughput across three windows
    _TERMINAL = "('VALIDATED', 'VALIDATION_FAILED', 'COST_SKIPPED', 'DISCOVERY_FAILED')"
    for minutes, key in ((1, "terminal_last_1min"), (5, "terminal_last_5min"), (15, "terminal_last_15min")):
        async with conn.execute(
            f"SELECT COUNT(*) FROM records"
            f" WHERE record_state IN {_TERMINAL}"
            f" AND updated_at >= datetime('now', '-{minutes} minutes')"
        ) as cursor:
            row = await cursor.fetchone()
            summary[key] = row[0] if row else 0

    # Per-state terminal rate (last 5 min)
    async with conn.execute(
        f"SELECT record_state, COUNT(*) FROM records"
        f" WHERE record_state IN {_TERMINAL}"
        f" AND updated_at >= datetime('now', '-5 minutes')"
        f" GROUP BY record_state"
    ) as cursor:
        summary["terminal_by_state_5min"] = {row[0]: row[1] async for row in cursor}

    # Zuhal queue drain rate (last 5 min)
    async with conn.execute(
        "SELECT COUNT(*) FROM records"
        " WHERE record_state IN ('VALIDATED', 'VALIDATION_FAILED')"
        " AND zuhal_status NOT IN ('dual_valid', 'dual_catch_all', 'dual_invalid', 'ms_valid')"
        " AND zuhal_status IS NOT NULL"
        " AND updated_at >= datetime('now', '-5 minutes')"
    ) as cursor:
        row = await cursor.fetchone()
        summary["zuhal_terminal_last_5min"] = row[0] if row else 0

    # Retry backlog: DISCOVERED records already attempted at least once
    async with conn.execute(
        "SELECT COUNT(*) FROM records"
        " WHERE record_state = 'DISCOVERED' AND dispatch_attempts > 0"
    ) as cursor:
        row = await cursor.fetchone()
        summary["retry_backlog"] = row[0] if row else 0

    return summary


async def reset_failed_records(
    conn: aiosqlite.Connection,
    record_state: str = State.DISCOVERY_FAILED,
    phase: str | None = None,
    unverified_only: bool = False,
) -> int:
    if phase:
        sql = """
            UPDATE records SET record_state = 'RAW', discovery_attempts = 0, updated_at = datetime('now')
            WHERE record_state = ? AND unique_id IN (
                SELECT DISTINCT unique_id FROM failures WHERE phase = ?
            )
        """
        cursor = await conn.execute(sql, (record_state, phase))
    else:
        if record_state in (State.VALIDATION_FAILED, State.COST_SKIPPED):
            sql = """
                UPDATE records SET record_state = 'DISCOVERED', dispatch_attempts = 0,
                updated_at = datetime('now')
                WHERE record_state = ?
            """
            # Re-queue only "couldn't verify" failures (timed out / no answer); leave
            # definitive-invalid records (final_verdict set) terminal so a patient retry
            # pass does not re-probe addresses already known to be bad.
            if unverified_only:
                sql += " AND final_verdict IS NULL"
        else:
            sql = """
                UPDATE records SET record_state = 'RAW', discovery_attempts = 0,
                updated_at = datetime('now')
                WHERE record_state = ?
            """
        cursor = await conn.execute(sql, (record_state,))

    await conn.commit()
    return cursor.rowcount
