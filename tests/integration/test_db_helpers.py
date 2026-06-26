"""Integration tests for db meta + zuhal_queue helpers (real SQLite)."""
from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

from pipeline import db
from pipeline.db import State

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def conn(tmp_path: Path) -> aiosqlite.Connection:
    c = await db.init_db(tmp_path / "t.db")
    yield c
    await c.close()


async def _insert(conn, uid, state):
    await conn.execute(
        "INSERT INTO records (unique_id, record_state, candidate_email) VALUES (?, ?, ?)",
        (uid, state, "a@b.com"),
    )
    await conn.commit()


class TestStatsAndSummary:
    async def test_upsert_stats_and_summary(self, conn):
        await _insert(conn, "IL-1", State.VALIDATED)
        await _insert(conn, "IL-2", State.VALIDATION_FAILED)
        await db.upsert_stats(conn, "run1", validated=1, validation_failed=1)
        await db.upsert_producer_heartbeat(conn)
        await db.upsert_dispatcher_heartbeat(conn)

        summary = await db.get_status_summary(conn)
        assert summary["total_records"] == 2
        assert summary["records_by_state"][State.VALIDATED] == 1
        assert summary["stats"]["validated"] == 1

    async def test_insert_failure(self, conn):
        await db.insert_failure(conn, "IL-1", "discovery", 1, "DNSError", "timeout")
        async with conn.execute("SELECT phase, error_type FROM failures WHERE unique_id='IL-1'") as cur:
            row = await cur.fetchone()
        assert row[0] == "discovery" and row[1] == "DNSError"


class TestResetFailedRecords:
    async def test_reset_discovery_failed_to_raw(self, conn):
        await _insert(conn, "IL-1", State.DISCOVERY_FAILED)
        n = await db.reset_failed_records(conn, State.DISCOVERY_FAILED)
        assert n == 1
        async with conn.execute("SELECT record_state FROM records WHERE unique_id='IL-1'") as cur:
            assert (await cur.fetchone())[0] == "RAW"

    async def test_reset_validation_failed_to_discovered(self, conn):
        # Regression: this branch referenced a non-existent column before.
        await _insert(conn, "IL-2", State.VALIDATION_FAILED)
        n = await db.reset_failed_records(conn, State.VALIDATION_FAILED)
        assert n == 1
        async with conn.execute("SELECT record_state FROM records WHERE unique_id='IL-2'") as cur:
            assert (await cur.fetchone())[0] == "DISCOVERED"

    async def test_reset_validation_failed_unverified_only(self, conn):
        # unverified_only re-queues only "couldn't verify" failures (no definitive
        # verdict); definitive-invalid records (final_verdict set) stay terminal.
        await _insert(conn, "IL-INCONCLUSIVE", State.VALIDATION_FAILED)  # final_verdict NULL
        await conn.execute(
            "INSERT INTO records (unique_id, record_state, candidate_email, final_verdict) "
            "VALUES (?, ?, ?, ?)",
            ("IL-INVALID", State.VALIDATION_FAILED, "a@b.com", "invalid"),
        )
        await conn.commit()

        n = await db.reset_failed_records(conn, State.VALIDATION_FAILED, unverified_only=True)

        assert n == 1
        async with conn.execute(
            "SELECT record_state FROM records WHERE unique_id='IL-INCONCLUSIVE'"
        ) as cur:
            assert (await cur.fetchone())[0] == State.DISCOVERED
        async with conn.execute(
            "SELECT record_state FROM records WHERE unique_id='IL-INVALID'"
        ) as cur:
            assert (await cur.fetchone())[0] == State.VALIDATION_FAILED


class TestZuhalQueueHelpers:
    async def test_count_and_has_pending_zuhal(self, conn):
        await _insert(conn, "IL-1", State.NEEDS_ZUHAL)
        assert await db.has_pending_zuhal(conn) is True
        assert await db.count_needs_zuhal(conn) == 1

    async def test_touch_and_recover_stale_zuhal_validating(self, conn):
        await conn.execute(
            "INSERT INTO records (unique_id, record_state, candidate_email, updated_at) "
            "VALUES ('IL-9', 'ZUHAL_VALIDATING', 'a@b.com', datetime('now', '-10 minutes'))"
        )
        await conn.commit()
        moved = await db.recover_stale_zuhal_validating(conn, timeout_minutes=5)
        assert moved == 1
        async with conn.execute("SELECT record_state FROM records WHERE unique_id='IL-9'") as cur:
            assert (await cur.fetchone())[0] == State.NEEDS_ZUHAL
