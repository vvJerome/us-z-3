"""Integration tests for Dispatcher with mocked backends and real SQLite."""

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import aiosqlite
import pytest

from pipeline import db
from pipeline.config import PipelineConfig
from pipeline.db import State
from pipeline.dispatcher import Dispatcher, reconcile
from pipeline.models import BackendVerdict
from pipeline.utils.cost_tracker import CostTracker

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def test_db(tmp_path: Path) -> aiosqlite.Connection:
    conn = await db.init_db(tmp_path / "test.db")
    yield conn
    await conn.close()


@pytest.fixture
def config(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        serper_api_key="test",
        zuhal_api_key="test",
        racknerd_host="localhost",
        input_path=tmp_path / "input.jsonl",
        output_dir=tmp_path,
        db_path=tmp_path / "test.db",
        log_dir=tmp_path / "logs",
        dispatch_concurrency=1,
        dispatch_backend_timeout_s=5.0,
        dispatch_poll_interval_s=0.1,
        dispatch_chunk_size=10,
    )


def _mock_racknerd(status: str, message: str = "") -> MagicMock:
    consumer = MagicMock()
    consumer.verify = AsyncMock(
        return_value=BackendVerdict(status=status, message=message, verified_at="2026-05-04T00:00:00Z")
    )
    return consumer


def _mock_bbops(status: str, message: str = "") -> MagicMock:
    consumer = MagicMock()
    consumer.verify = AsyncMock(
        return_value=BackendVerdict(status=status, message=message, verified_at="2026-05-04T00:00:01Z")
    )
    return consumer


async def _insert_discovered(conn, unique_id: str, email: str = "test@example.com") -> None:
    await conn.execute(
        """
        INSERT INTO records
            (unique_id, business_name, agent_name, record_state,
             candidate_emails, candidate_email, candidate_domain, strategy, mx_provider)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            unique_id, "Test Corp", "John Doe",
            State.DISCOVERED,
            json.dumps([email]), email, "example.com", "with", "gmail.com",
        ),
    )
    await conn.commit()


class TestDispatcherReconciliation:
    async def test_both_valid_writes_validated(self, test_db, config):
        await _insert_discovered(test_db, "rec1")
        await db.upsert_checkpoint(test_db, "producer_done", "true")

        rk = _mock_racknerd("valid", "250 OK")
        bb = _mock_bbops("valid", "250 OK")
        stop = asyncio.Event()
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None), stop)

        # Run one cycle
        rows = await db.fetch_pending_validation(test_db, limit=10)
        for row in rows:
            await dispatcher._process_record(row)

        async with test_db.execute(
            "SELECT record_state, final_verdict, racknerd_status, bbops_status "
            "FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.VALIDATED
        assert row["final_verdict"] == "valid"
        assert row["racknerd_status"] == "valid"
        assert row["bbops_status"] == "valid"

    async def test_racknerd_valid_bbops_invalid_still_valid(self, test_db, config):
        await _insert_discovered(test_db, "rec1")

        rk = _mock_racknerd("valid", "250 OK")
        bb = _mock_bbops("invalid", "550")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT final_verdict FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["final_verdict"] == "valid"

    async def test_both_invalid_writes_validation_failed(self, test_db, config):
        await _insert_discovered(test_db, "rec1")

        rk = _mock_racknerd("invalid", "550")
        bb = _mock_bbops("invalid", "550")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, final_verdict FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.VALIDATION_FAILED
        assert row["final_verdict"] == "invalid"

    async def test_both_error_requeues_without_burning_attempt(self, test_db, config):
        await _insert_discovered(test_db, "rec1")

        rk = _mock_racknerd("error", "timeout")
        bb = _mock_bbops("error", "timeout")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, dispatch_attempts FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.DISCOVERED
        assert row["dispatch_attempts"] == 0  # not burned

    async def test_tunnel_down_requeues_without_burning_attempt(self, test_db, config):
        await _insert_discovered(test_db, "rec1")

        rk = _mock_racknerd("error", "tunnel not up")
        bb = _mock_bbops("invalid", "550")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, dispatch_attempts FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.DISCOVERED
        assert row["dispatch_attempts"] == 0

    async def test_catch_all_writes_validated(self, test_db, config):
        await _insert_discovered(test_db, "rec1")

        rk = _mock_racknerd("catch_all", "accepted all")
        bb = _mock_bbops("invalid", "550")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, final_verdict FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.VALIDATED
        assert row["final_verdict"] == "catch_all"

    async def test_no_candidates_marks_failed(self, test_db, config):
        # Insert as DISCOVERED with no candidate_emails so fetch_pending_validation claims it
        await test_db.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("rec1", State.DISCOVERED),
        )
        await test_db.commit()

        rk = _mock_racknerd("valid")
        bb = _mock_bbops("valid")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        if rows:
            await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.VALIDATION_FAILED

    async def test_cost_ceiling_marks_cost_skipped(self, test_db, config):
        await _insert_discovered(test_db, "rec1")

        rk = _mock_racknerd("valid")
        bb = _mock_bbops("valid")
        cost_tracker = CostTracker(max_cost=0.0)
        # Trigger ceiling immediately
        cost_tracker.record_call("serper")

        dispatcher = Dispatcher(config, test_db, rk, bb, cost_tracker)
        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.COST_SKIPPED
