from __future__ import annotations

from typing import cast

import aiosqlite


async def handoff_to_zuhal(
    conn: aiosqlite.Connection,
    unique_id: str,
    *,
    racknerd_status: str | None,
    racknerd_message: str | None,
    racknerd_verified_at: str | None,
    bbops_status: str | None,
    bbops_message: str | None,
    bbops_verified_at: str | None,
    candidate_email: str,
) -> None:
    """Persist SMTP verdicts and route the record to the Zuhal queue.

    Atomic: SMTP-side outcome lands on the row in the same UPDATE that moves
    state to NEEDS_ZUHAL. Does NOT write final_verdict (Zuhal worker owns that)
    and does NOT increment dispatch_attempts (the handoff is a transition, not
    a retry; the SMTP attempt is already counted).
    """
    await conn.execute(
        """
        UPDATE records
           SET record_state = 'NEEDS_ZUHAL',
               racknerd_status = ?,
               racknerd_message = ?,
               racknerd_verified_at = ?,
               bbops_status = ?,
               bbops_message = ?,
               bbops_verified_at = ?,
               candidate_email = ?,
               updated_at = datetime('now')
         WHERE unique_id = ?
        """,
        (
            racknerd_status,
            racknerd_message,
            racknerd_verified_at,
            bbops_status,
            bbops_message,
            bbops_verified_at,
            candidate_email,
            unique_id,
        ),
    )
    await conn.commit()


async def fetch_pending_zuhal(
    conn: aiosqlite.Connection,
    limit: int = 10,
) -> list[aiosqlite.Row]:
    """Atomically claim up to `limit` NEEDS_ZUHAL rows by setting them to ZUHAL_VALIDATING."""
    async with conn.execute(
        """
        UPDATE records
           SET record_state = 'ZUHAL_VALIDATING', updated_at = datetime('now')
         WHERE id IN (
             SELECT id FROM records WHERE record_state = 'NEEDS_ZUHAL' LIMIT ?
         )
        RETURNING *
        """,
        (limit,),
    ) as cursor:
        return cast(list[aiosqlite.Row], await cursor.fetchall())


async def has_pending_zuhal(conn: aiosqlite.Connection) -> bool:
    """Non-claiming check: True if any NEEDS_ZUHAL or ZUHAL_VALIDATING rows exist."""
    async with conn.execute(
        "SELECT 1 FROM records "
        "WHERE record_state IN ('NEEDS_ZUHAL', 'ZUHAL_VALIDATING') LIMIT 1"
    ) as cursor:
        return await cursor.fetchone() is not None


async def count_needs_zuhal(conn: aiosqlite.Connection) -> int:
    """Return current size of the NEEDS_ZUHAL backlog (non-claiming)."""
    async with conn.execute(
        "SELECT COUNT(*) FROM records WHERE record_state = 'NEEDS_ZUHAL'"
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else 0


async def touch_zuhal_validating(conn: aiosqlite.Connection, unique_ids: list[str]) -> None:
    """Refresh updated_at on ZUHAL_VALIDATING rows to prevent stale-recovery eviction."""
    if not unique_ids:
        return
    placeholders = ",".join("?" * len(unique_ids))
    await conn.execute(
        f"UPDATE records SET updated_at = datetime('now') "
        f"WHERE unique_id IN ({placeholders}) AND record_state = 'ZUHAL_VALIDATING'",
        unique_ids,
    )
    await conn.commit()


async def recover_stale_zuhal_validating(
    conn: aiosqlite.Connection,
    timeout_minutes: int = 5,
) -> int:
    """Return rows orphaned in ZUHAL_VALIDATING back to NEEDS_ZUHAL for retry."""
    cursor = await conn.execute(
        """
        UPDATE records
           SET record_state = 'NEEDS_ZUHAL',
               updated_at = datetime('now')
         WHERE record_state = 'ZUHAL_VALIDATING'
           AND updated_at < datetime('now', ?)
        """,
        (f"-{timeout_minutes} minutes",),
    )
    await conn.commit()
    return cursor.rowcount


async def requeue_zuhal(conn: aiosqlite.Connection, unique_id: str) -> None:
    """Return a ZUHAL_VALIDATING row to NEEDS_ZUHAL (circuit-open or 429 re-queue)."""
    await conn.execute(
        """
        UPDATE records
           SET record_state = 'NEEDS_ZUHAL',
               updated_at = datetime('now')
         WHERE unique_id = ?
        """,
        (unique_id,),
    )
    await conn.commit()


async def create_zuhal_job(conn: aiosqlite.Connection, job_id: str, email_count: int) -> None:
    """Persist a new bulk job before polling starts (audit trail + crash recovery marker)."""
    await conn.execute(
        "INSERT OR IGNORE INTO zuhal_jobs (job_id, email_count, status) VALUES (?, ?, 'polling')",
        (job_id, email_count),
    )
    await conn.commit()


async def update_zuhal_job_status(conn: aiosqlite.Connection, job_id: str, status: str) -> None:
    """Update bulk job status to 'complete' or 'failed'."""
    await conn.execute(
        """UPDATE zuhal_jobs
              SET status = ?,
                  completed_at = CASE WHEN ? IN ('complete', 'failed') THEN datetime('now') ELSE completed_at END
            WHERE job_id = ?""",
        (status, status, job_id),
    )
    await conn.commit()


async def lookup_email_cache(conn: aiosqlite.Connection, email: str) -> str | None:
    """Return cached verdict for this email (normalized), or None if not cached."""
    async with conn.execute(
        "SELECT verdict FROM email_verification_cache WHERE email_norm = ?",
        (email.lower().strip(),),
    ) as cur:
        row = await cur.fetchone()
    return row[0] if row else None


async def write_email_cache(conn: aiosqlite.Connection, email: str, verdict: str, provider: str) -> None:
    """Cache a verified email result to prevent re-paying on duplicates."""
    await conn.execute(
        """INSERT OR REPLACE INTO email_verification_cache (email_norm, verdict, provider, verified_at)
           VALUES (?, ?, ?, datetime('now'))""",
        (email.lower().strip(), verdict, provider),
    )
    await conn.commit()
