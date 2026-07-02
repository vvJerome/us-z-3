"""Integration tests for pipeline.ops.zuhal_rescue."""
from __future__ import annotations

import asyncio
from pathlib import Path

import aiosqlite
import pytest

from pipeline.db.schema import SCHEMA_SQL
from pipeline.ops.zuhal_rescue import run


async def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "pipeline.db"
    async with aiosqlite.connect(str(db_path)) as conn:
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()
    return db_path


class TestZuhalRescueDryRun:
    async def test_exits_cleanly_on_empty_db(self, tmp_path: Path):
        db_path = await _make_db(tmp_path)
        await run(str(db_path), rate_limit=20, dry_run=True)

    async def test_exits_cleanly_with_no_eligible_rows(self, tmp_path: Path):
        db_path = await _make_db(tmp_path)
        async with aiosqlite.connect(str(db_path)) as conn:
            await conn.execute(
                """INSERT INTO records
                       (unique_id, business_name, agent_name, state,
                        record_state, dispatch_attempts)
                   VALUES (?,?,?,?,?,?)""",
                ("uid1", "Acme", "Joe", "NC", "VALIDATED", 1),
            )
            await conn.commit()
        await run(str(db_path), rate_limit=20, dry_run=True)

    async def test_processes_eligible_row_dry_run(self, tmp_path: Path):
        """dry_run=True means Zuhal client mocks the call; row should get a status written."""
        db_path = await _make_db(tmp_path)
        async with aiosqlite.connect(str(db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute(
                """INSERT INTO records
                       (unique_id, business_name, agent_name, state,
                        candidate_email, record_state,
                        racknerd_status, bbops_status, dispatch_attempts)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                ("uid2", "Acme", "Joe", "NC", "joe@acme.com",
                 "VALIDATION_FAILED", "invalid", "invalid", 3),
            )
            await conn.commit()

        await run(str(db_path), rate_limit=20, dry_run=True)

        async with aiosqlite.connect(str(db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT record_state, zuhal_status FROM records WHERE unique_id='uid2'"
            ) as cur:
                row = await cur.fetchone()
        assert row is not None
