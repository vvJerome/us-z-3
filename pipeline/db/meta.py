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

    return summary


async def reset_failed_records(
    conn: aiosqlite.Connection,
    record_state: str = State.DISCOVERY_FAILED,
    phase: str | None = None,
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
        else:
            sql = """
                UPDATE records SET record_state = 'RAW', discovery_attempts = 0,
                updated_at = datetime('now')
                WHERE record_state = ?
            """
        cursor = await conn.execute(sql, (record_state,))

    await conn.commit()
    return cursor.rowcount
