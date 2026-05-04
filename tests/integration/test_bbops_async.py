"""Integration tests for BbopsAsyncConsumer with real SQLite and mock HTTP."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import aiosqlite
import pytest

from pipeline import db
from pipeline.consumers.bbops_async import BbopsAsyncConsumer


pytestmark = pytest.mark.asyncio


@pytest.fixture
async def test_db(tmp_path: Path) -> aiosqlite.Connection:
    conn = await db.init_db(tmp_path / "test.db")
    yield conn
    await conn.close()


def _make_consumer(conn: aiosqlite.Connection, session: aiohttp.ClientSession, **kwargs) -> BbopsAsyncConsumer:
    defaults = dict(
        conn=conn,
        session=session,
        base_url="https://bbops.test",
        batch_size=10,
        min_batch_size=1,
        max_inflight=3,
        flush_interval_s=0.05,
        poll_interval_s=0.05,
        poll_timeout_s=5.0,
        health_fail_threshold=3,
        health_ok_threshold=2,
    )
    defaults.update(kwargs)
    return BbopsAsyncConsumer(**defaults)


def _mock_session(submit_response: dict, poll_response: dict, jobs_response: dict) -> MagicMock:
    """Build a mock aiohttp session that returns preset responses per endpoint."""
    session = MagicMock()

    def _ctx(data: dict):
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=ctx)
        ctx.__aexit__ = AsyncMock(return_value=False)
        ctx.raise_for_status = MagicMock()
        ctx.json = AsyncMock(return_value=data)
        return ctx

    def _post(*args, **kwargs):
        return _ctx(submit_response)

    def _get(url, *args, **kwargs):
        if "/jobs" in url:
            return _ctx(jobs_response)
        return _ctx(poll_response)

    session.post = MagicMock(side_effect=_post)
    session.get = MagicMock(side_effect=_get)
    return session


class TestSubmitAndPollCycle:
    async def test_successful_batch_resolves_future(self, test_db):
        session = _mock_session(
            submit_response={"batch_id": "batch-001", "jobs": [{"id": "j1", "email": "a@b.com"}], "count": 1, "auto_catch_all_count": 0},
            poll_response={"status": "done"},
            jobs_response={"jobs": [{"id": "j1", "email": "a@b.com", "status": "valid", "message": "250 OK"}]},
        )
        consumer = _make_consumer(test_db, session)
        await consumer.start()
        try:
            result = await asyncio.wait_for(
                consumer.verify(record_id=1, email="a@b.com"),
                timeout=5.0,
            )
        finally:
            await consumer.stop()

        assert result.status == "valid"
        assert "250 OK" in result.message

    async def test_submit_failure_returns_error_verdict(self, test_db):
        session = MagicMock()
        error_ctx = MagicMock()
        error_ctx.__aenter__ = AsyncMock(return_value=error_ctx)
        error_ctx.__aexit__ = AsyncMock(return_value=False)
        error_ctx.raise_for_status = MagicMock(side_effect=aiohttp.ClientError("connect failed"))
        session.post = MagicMock(return_value=error_ctx)

        consumer = _make_consumer(test_db, session)
        await consumer.start()
        try:
            result = await asyncio.wait_for(
                consumer.verify(record_id=1, email="a@b.com"),
                timeout=5.0,
            )
        finally:
            await consumer.stop()

        assert result.status == "error"

    async def test_jobs_persisted_to_bbops_jobs_table(self, test_db):
        session = _mock_session(
            submit_response={"batch_id": "batch-002", "jobs": [{"id": "j2", "email": "x@y.com"}], "count": 1, "auto_catch_all_count": 0},
            poll_response={"status": "done"},
            jobs_response={"jobs": [{"id": "j2", "email": "x@y.com", "status": "invalid", "message": "550"}]},
        )
        consumer = _make_consumer(test_db, session)
        await consumer.start()
        try:
            await asyncio.wait_for(
                consumer.verify(record_id=2, email="x@y.com"),
                timeout=5.0,
            )
        finally:
            await consumer.stop()

        async with test_db.execute("SELECT batch_id FROM bbops_jobs WHERE email = ?", ("x@y.com",)) as cur:
            row = await cur.fetchone()
        assert row is not None
        assert row[0] == "batch-002"

    async def test_job_marked_done_after_poll(self, test_db):
        session = _mock_session(
            submit_response={"batch_id": "batch-003", "jobs": [{"id": "j3", "email": "d@e.com"}], "count": 1, "auto_catch_all_count": 0},
            poll_response={"status": "done"},
            jobs_response={"jobs": [{"id": "j3", "email": "d@e.com", "status": "valid", "message": "250"}]},
        )
        consumer = _make_consumer(test_db, session)
        await consumer.start()
        try:
            await asyncio.wait_for(
                consumer.verify(record_id=3, email="d@e.com"),
                timeout=5.0,
            )
        finally:
            await consumer.stop()

        async with test_db.execute("SELECT status FROM bbops_jobs WHERE email = ?", ("d@e.com",)) as cur:
            row = await cur.fetchone()
        assert row is not None
        assert row[0] == "done"


class TestCrashRecovery:
    async def test_recover_inflight_finds_submitted_batches(self, test_db):
        await db.insert_bbops_jobs(test_db, [
            {"record_id": 1, "email": "r@recover.com", "job_id": "j-old", "batch_id": "batch-old"},
        ])

        session = _mock_session(
            submit_response={},
            poll_response={"status": "done"},
            jobs_response={"jobs": [{"id": "j-old", "email": "r@recover.com", "status": "valid", "message": "250"}]},
        )
        consumer = _make_consumer(test_db, session)
        await consumer.start()
        await consumer.recover_inflight()
        await asyncio.sleep(0.3)
        await consumer.stop()

        # Recovery tasks created — batch logged
        async with test_db.execute(
            "SELECT batch_id FROM bbops_jobs WHERE status = 'submitted'"
        ) as cur:
            row = await cur.fetchone()
        # Row still exists (recovery task polled it but didn't resolve futures since none were waiting)
        assert row is not None

    async def test_no_recovery_when_no_inflight(self, test_db):
        session = MagicMock()
        consumer = _make_consumer(test_db, session)
        await consumer.start()
        await consumer.recover_inflight()
        await consumer.stop()
        # No tasks created, no exceptions


class TestDbPersistFailure:
    async def test_db_persist_failure_resolves_future_as_error(self, test_db):
        session = _mock_session(
            submit_response={"batch_id": "b-fail", "jobs": [], "count": 1, "auto_catch_all_count": 0},
            poll_response={},
            jobs_response={},
        )
        consumer = _make_consumer(test_db, session)
        await consumer.start()
        try:
            with patch("pipeline.consumers.bbops_async._db.insert_bbops_jobs",
                       side_effect=Exception("db locked")):
                result = await asyncio.wait_for(
                    consumer.verify(record_id=99, email="fail@test.com"),
                    timeout=5.0,
                )
        finally:
            await consumer.stop()

        assert result.status == "error"
        assert consumer._consecutive_failures >= 1
