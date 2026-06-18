from __future__ import annotations

import aiosqlite


async def mark_serper_enriched(conn: aiosqlite.Connection, unique_id: str) -> None:
    """Mark a record as having been enriched by Serper (prevents duplicate calls)."""
    await conn.execute(
        "UPDATE records SET serper_enriched = 1, updated_at = datetime('now') WHERE unique_id = ?",
        (unique_id,),
    )
    await conn.commit()


async def get_enrichment_cache(
    conn: aiosqlite.Connection,
    business_name: str,
    agent_name: str,
    state: str,
    provider: str,
    ttl_days: int = 30,
) -> str | None:
    """Return cached response JSON if within TTL, else None."""
    async with conn.execute(
        """
        SELECT response_json FROM enrichment_cache
         WHERE business_name_norm = ?
           AND agent_name_norm = ?
           AND state = ?
           AND provider = ?
           AND cached_at > datetime('now', ?)
        """,
        (
            business_name.lower().strip(),
            agent_name.lower().strip(),
            state,
            provider,
            f"-{ttl_days} days",
        ),
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else None


async def set_enrichment_cache(
    conn: aiosqlite.Connection,
    business_name: str,
    agent_name: str,
    state: str,
    provider: str,
    response_json: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO enrichment_cache
            (business_name_norm, agent_name_norm, state, provider, response_json, cached_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(business_name_norm, agent_name_norm, state, provider) DO UPDATE SET
            response_json = excluded.response_json,
            cached_at = datetime('now')
        """,
        (
            business_name.lower().strip(),
            agent_name.lower().strip(),
            state,
            provider,
            response_json,
        ),
    )
    await conn.commit()
