from __future__ import annotations

import aiosqlite


async def get_pattern_rankings(
    conn: aiosqlite.Connection,
    mx_provider: str,
) -> list[dict]:
    """Return templates ordered by success rate for this MX provider."""
    async with conn.execute(
        """
        SELECT template, success_count, total_count
          FROM pattern_stats
         WHERE mx_provider = ? AND total_count > 0
         ORDER BY CAST(success_count AS REAL) / total_count DESC
        """,
        (mx_provider,),
    ) as cursor:
        return [
            {"template": row[0], "success_count": row[1], "total_count": row[2]}
            async for row in cursor
        ]


async def record_pattern_result(
    conn: aiosqlite.Connection,
    mx_provider: str,
    template: str,
    success: bool,
) -> None:
    await conn.execute(
        """
        INSERT INTO pattern_stats (mx_provider, template, success_count, total_count)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(mx_provider, template) DO UPDATE SET
            success_count = success_count + ?,
            total_count = total_count + 1
        """,
        (mx_provider, template, 1 if success else 0, 1 if success else 0),
    )
    await conn.commit()
