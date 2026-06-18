from __future__ import annotations

import aiosqlite


async def insert_bbops_jobs(
    conn: aiosqlite.Connection,
    jobs: list[dict],
) -> None:
    """Persist bbops job mappings BEFORE polling — enables crash recovery."""
    for job in jobs:
        await conn.execute(
            """
            INSERT OR IGNORE INTO bbops_jobs
                (record_id, email, job_id, batch_id, submitted_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            """,
            (job["record_id"], job["email"], job.get("job_id", ""), job["batch_id"]),
        )
    await conn.commit()


async def mark_bbops_job_done(
    conn: aiosqlite.Connection,
    job_id: str,
    result_status: str,
    result_message: str,
) -> None:
    await conn.execute(
        """
        UPDATE bbops_jobs
           SET status = 'done', result_status = ?, result_message = ?,
               completed_at = datetime('now')
         WHERE job_id = ?
        """,
        (result_status, result_message, job_id),
    )
    await conn.commit()


async def fetch_inflight_bbops_batches(
    conn: aiosqlite.Connection,
) -> dict[str, list[dict]]:
    """Return all submitted-but-not-done batches grouped by batch_id for crash recovery."""
    async with conn.execute(
        """
        SELECT batch_id, record_id, email
          FROM bbops_jobs
         WHERE status = 'submitted'
        """
    ) as cursor:
        batches: dict[str, list[dict]] = {}
        async for row in cursor:
            batches.setdefault(row[0], []).append(
                {"record_id": row[1], "email": row[2]}
            )
    return batches
