from __future__ import annotations

import json
import logging

import aiosqlite

from pipeline.db.schema import State, INSERT_RECORD_SQL, UPSERT_CHECKPOINT_SQL
from pipeline.verdicts import canonical_from_smtp, canonical_from_zuhal

_log = logging.getLogger("pipeline.db")


async def insert_records_batch(
    conn: aiosqlite.Connection,
    records: list[dict],
    new_offset: int,
) -> None:
    """Atomically insert a batch of records and advance the producer checkpoint."""
    async with conn.cursor() as cur:
        await cur.execute("BEGIN")
        try:
            inserted = 0
            for r in records:
                await cur.execute(INSERT_RECORD_SQL, (
                    r["unique_id"],
                    r.get("business_name", ""),
                    r.get("agent_name", ""),
                    r.get("state", ""),
                    r.get("jurisdiction", ""),
                    r.get("position_type", ""),
                    r.get("name_entity_type", ""),
                    r.get("candidate_email"),
                    r.get("candidate_emails"),
                    r.get("subdomain_emails"),
                    r.get("candidate_domain"),
                    r.get("discovery_source"),
                    r.get("discovery_attempts", 0),
                    r.get("strategy"),
                    1 if r.get("is_org_agent") else 0,
                    r.get("mx_provider"),
                    r.get("domain_confidence"),
                    r.get("owner_confidence"),
                    r.get("record_state", State.RAW),
                    r.get("process_trace"),
                    1 if r.get("serper_enriched") else 0,
                ))
                inserted += cur.rowcount
            if inserted < len(records):
                dropped = len(records) - inserted
                _log.warning(
                    "insert_records_batch: %d record(s) dropped — duplicate unique_id in input",
                    dropped,
                )
            await cur.execute(UPSERT_CHECKPOINT_SQL, ("producer_offset", str(new_offset)))
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


async def fetch_pending_validation(
    conn: aiosqlite.Connection,
    limit: int = 10,
) -> list[aiosqlite.Row]:
    """Atomically claim up to `limit` DISCOVERED rows by setting them to VALIDATING.

    Skips records whose retry_after timestamp has not yet passed (greylisting hold).
    """
    async with conn.execute(
        """
        UPDATE records
           SET record_state = 'VALIDATING', updated_at = datetime('now')
         WHERE id IN (
             SELECT id FROM records
              WHERE record_state = 'DISCOVERED'
                AND (retry_after IS NULL OR retry_after <= datetime('now'))
              LIMIT ?
         )
        RETURNING *
        """,
        (limit,),
    ) as cursor:
        return await cursor.fetchall()  # type: ignore[return-value]


async def has_pending_validation(conn: aiosqlite.Connection) -> bool:
    """Non-claiming check: True if any DISCOVERED rows exist."""
    async with conn.execute(
        "SELECT 1 FROM records WHERE record_state = 'DISCOVERED' LIMIT 1"
    ) as cursor:
        return await cursor.fetchone() is not None


async def fetch_pending_discovery(
    conn: aiosqlite.Connection,
    limit: int = 100,
) -> list[aiosqlite.Row]:
    async with conn.execute(
        "SELECT * FROM records WHERE record_state = 'DISCOVERING' LIMIT ?",
        (limit,),
    ) as cursor:
        return await cursor.fetchall()  # type: ignore[return-value]


async def update_record_discovery(conn: aiosqlite.Connection, result: dict) -> None:
    """UPDATE discovery fields on an existing record (used by the retry loop)."""
    await conn.execute(
        """UPDATE records SET
               record_state = ?, candidate_email = ?, candidate_emails = ?,
               subdomain_emails = ?, candidate_domain = ?,
               discovery_source = ?, discovery_attempts = ?,
               mx_provider = ?, domain_confidence = ?, owner_confidence = ?,
               updated_at = datetime('now')
           WHERE unique_id = ?""",
        (
            result.get("record_state", State.DISCOVERING),
            result.get("candidate_email"),
            result.get("candidate_emails"),
            result.get("subdomain_emails"),
            result.get("candidate_domain"),
            result.get("discovery_source"),
            result.get("discovery_attempts", 1),
            result.get("mx_provider"),
            result.get("domain_confidence"),
            result.get("owner_confidence"),
            result["unique_id"],
        ),
    )
    await conn.commit()


async def requeue_record(
    conn: aiosqlite.Connection,
    unique_id: str,
    *,
    increment_attempts: bool = True,
    retry_after: str | None = None,
) -> None:
    """Return a record to DISCOVERED.

    increment_attempts=True when at least one backend returned a real verdict
    (valid/invalid/catch_all). False for infra transients (tunnel down, Zuhal
    circuit open, bbops unhealthy) that should not penalise the dispatch budget.

    requeue_count always increments regardless of increment_attempts — it is a
    safety valve that bounds records in infinite-loop infra failure scenarios.

    retry_after (ISO timestamp) sets a hold: fetch_pending_validation skips
    the record until that time passes. Use for greylisting (SMTP 4xx).
    """
    if increment_attempts:
        sql = """UPDATE records
                    SET record_state = 'DISCOVERED',
                        dispatch_attempts = dispatch_attempts + 1,
                        requeue_count = requeue_count + 1,
                        retry_after = ?,
                        updated_at = datetime('now')
                  WHERE unique_id = ?"""
    else:
        sql = """UPDATE records
                    SET record_state = 'DISCOVERED',
                        requeue_count = requeue_count + 1,
                        retry_after = ?,
                        updated_at = datetime('now')
                  WHERE unique_id = ?"""
    await conn.execute(sql, (retry_after, unique_id))
    await conn.commit()


async def update_record_status(
    conn: aiosqlite.Connection,
    unique_id: str,
    record_state: str,
    **extra_fields: object,
) -> None:
    sets = ["record_state = ?", "updated_at = datetime('now')"]
    values: list[object] = [record_state]

    for col, val in extra_fields.items():
        sets.append(f"{col} = ?")
        values.append(val)

    values.append(unique_id)

    sql = f"UPDATE records SET {', '.join(sets)} WHERE unique_id = ?"
    await conn.execute(sql, values)
    await conn.commit()


async def update_record_dual(
    conn: aiosqlite.Connection,
    unique_id: str,
    record_state: str,
    *,
    racknerd_status: str | None,
    racknerd_message: str | None,
    racknerd_verified_at: str | None,
    bbops_status: str | None,
    bbops_message: str | None,
    bbops_verified_at: str | None,
    final_verdict: str,
    candidate_email: str | None = None,
    confidence_score: float | None = None,
    dispatch_attempts_delta: int = 1,
    zuhal_status_override: str | None = None,
) -> None:
    """Write dual-backend verdicts and advance dispatch_attempts atomically."""
    sets = [
        "record_state = ?",
        "racknerd_status = ?",
        "racknerd_message = ?",
        "racknerd_verified_at = ?",
        "bbops_status = ?",
        "bbops_message = ?",
        "bbops_verified_at = ?",
        "final_verdict = ?",
        "dispatch_attempts = dispatch_attempts + ?",
        "updated_at = datetime('now')",
    ]
    values: list[object] = [
        record_state,
        racknerd_status,
        racknerd_message,
        racknerd_verified_at,
        bbops_status,
        bbops_message,
        bbops_verified_at,
        final_verdict,
        dispatch_attempts_delta,
    ]

    if candidate_email is not None:
        sets.append("candidate_email = ?")
        values.append(candidate_email)
        sets.append("zuhal_status = ?")
        values.append(zuhal_status_override if zuhal_status_override is not None else f"dual_{final_verdict}")

    if confidence_score is not None:
        sets.append("confidence_score = ?")
        values.append(confidence_score)

    # Canonical verdict — computed once here so every caller populates it. ZeroBounce
    # (ground truth) overrides this later via the ingest script; until then the source
    # is whichever backend produced this verdict.
    if zuhal_status_override is not None:
        canonical_status, canonical_source = canonical_from_zuhal(zuhal_status_override)
        recon_path = None
    else:
        ms = racknerd_status == "ms_valid"
        canonical_status, canonical_source = canonical_from_smtp(final_verdict, ms_probe=ms)
        recon_path = "ms_valid" if ms else f"dual_{final_verdict}"
    sets.append("canonical_status = ?")
    values.append(canonical_status)
    sets.append("canonical_source = ?")
    values.append(canonical_source)
    if recon_path is not None:
        sets.append("reconciliation_path = ?")
        values.append(recon_path)

    values.append(unique_id)
    sql = f"UPDATE records SET {', '.join(sets)} WHERE unique_id = ?"
    await conn.execute(sql, values)
    await conn.commit()


async def recover_stale_validating(
    conn: aiosqlite.Connection,
    timeout_minutes: int = 5,
) -> int:
    """Reset rows orphaned in VALIDATING by a crashed dispatcher back to DISCOVERED."""
    cursor = await conn.execute(
        """
        UPDATE records
           SET record_state = 'DISCOVERED',
               dispatch_attempts = dispatch_attempts + 1,
               updated_at = datetime('now')
         WHERE record_state = 'VALIDATING'
           AND updated_at < datetime('now', ?)
        """,
        (f"-{timeout_minutes} minutes",),
    )
    await conn.commit()
    return cursor.rowcount


async def flush_process_trace(
    conn: aiosqlite.Connection,
    unique_id: str,
    entries: list[dict],
) -> None:
    """Append all accumulated trace entries in a single UPDATE (one commit per record)."""
    if not entries:
        return
    await conn.execute(
        """
        UPDATE records
           SET process_trace = (
               SELECT json_group_array(value)
               FROM (
                   SELECT value FROM json_each(COALESCE(
                       (SELECT process_trace FROM records WHERE unique_id = ?), '[]'
                   ))
                   UNION ALL
                   SELECT value FROM json_each(?)
               )
           )
         WHERE unique_id = ?
        """,
        (unique_id, json.dumps(entries), unique_id),
    )
    await conn.commit()


async def append_process_trace(
    conn: aiosqlite.Connection,
    unique_id: str,
    entry: dict,
) -> None:
    """Append a stage-outcome entry to the record's process_trace JSON array."""
    await conn.execute(
        """
        UPDATE records
           SET process_trace = json_insert(COALESCE(process_trace, '[]'), '$[#]', json(?))
         WHERE unique_id = ?
        """,
        (json.dumps(entry), unique_id),
    )
    await conn.commit()
