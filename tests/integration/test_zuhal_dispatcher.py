"""Integration tests for the decoupled ZuhalDispatcher worker."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import aiosqlite
import pytest

from pipeline import db
from pipeline.config import PipelineConfig
from pipeline.db import State
from pipeline.models import PipelineHaltError, ValidationResult
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.zuhal_client import (
    ZuhalCircuitOpenError,
    ZuhalClient,
    _RetryableHTTPError,
)
from pipeline.zuhal_dispatcher import ZuhalDispatcher

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
        zuhal_concurrency=2,
        zuhal_chunk_size=10,
        zuhal_poll_interval_s=0.05,
    )


async def _seed_needs_zuhal(
    conn: aiosqlite.Connection,
    unique_id: str = "rec1",
    email: str = "test@example.com",
    *,
    rk_status: str = "error",
    bb_status: str = "error",
    mx_provider: str = "gmail.com",
) -> None:
    await conn.execute(
        """
        INSERT INTO records
            (unique_id, business_name, agent_name, record_state,
             candidate_emails, candidate_email, candidate_domain,
             strategy, mx_provider,
             racknerd_status, racknerd_message, bbops_status, bbops_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            unique_id, "Test Corp", "John Doe",
            State.NEEDS_ZUHAL,
            json.dumps([email]), email, "example.com", "with", mx_provider,
            rk_status, "smtp error", bb_status, "smtp error",
        ),
    )
    await conn.commit()


def _mock_zuhal(verdict: str) -> MagicMock:
    z = MagicMock(spec=ZuhalClient)
    z.validate = AsyncMock(return_value=ValidationResult(
        email="test@example.com", verdict=verdict, score=0.0,
        is_disposable=False, raw_status="", http_status=200,
    ))
    return z


def _mock_zuhal_raises(exc: BaseException) -> MagicMock:
    z = MagicMock(spec=ZuhalClient)
    z.validate = AsyncMock(side_effect=exc)
    return z


class TestZuhalDispatcherTerminalVerdicts:
    async def test_valid_promotes_record_to_validated(self, test_db, config):
        await _seed_needs_zuhal(test_db)
        zuhal = _mock_zuhal("valid")
        cost = CostTracker(None)
        worker = ZuhalDispatcher(config, test_db, zuhal, cost)

        rows = await db.fetch_pending_zuhal(test_db, limit=10)
        await worker._process(rows[0])

        async with test_db.execute(
            "SELECT record_state, final_verdict, zuhal_status, confidence_score "
            "FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.VALIDATED
        assert row["final_verdict"] == "valid"
        assert row["zuhal_status"] == "valid"
        assert row["confidence_score"] is not None
        assert cost.counts.get("zuhal", 0) == 1
        zuhal.validate.assert_called_once_with("test@example.com")

    async def test_accept_all_normalizes_to_catch_all(self, test_db, config):
        await _seed_needs_zuhal(test_db)
        zuhal = _mock_zuhal("accept-all")
        worker = ZuhalDispatcher(config, test_db, zuhal, CostTracker(None))

        rows = await db.fetch_pending_zuhal(test_db, limit=10)
        await worker._process(rows[0])

        async with test_db.execute(
            "SELECT record_state, final_verdict, zuhal_status "
            "FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.VALIDATED
        assert row["final_verdict"] == "catch_all"
        assert row["zuhal_status"] == "catch_all"

    async def test_invalid_marks_validation_failed(self, test_db, config):
        await _seed_needs_zuhal(test_db)
        zuhal = _mock_zuhal("invalid")
        worker = ZuhalDispatcher(config, test_db, zuhal, CostTracker(None))

        rows = await db.fetch_pending_zuhal(test_db, limit=10)
        await worker._process(rows[0])

        async with test_db.execute(
            "SELECT record_state, final_verdict FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.VALIDATION_FAILED
        assert row["final_verdict"] == "invalid"


class TestZuhalUnknownCreditGuard:
    async def test_unknown_on_catchall_provider_does_not_retry(self, test_db, config):
        """Credit guard: an unknown from an accept-all/gateway provider goes straight to
        VALIDATION_FAILED — no second paid Zuhal call (it would just return unknown again)."""
        await _seed_needs_zuhal(test_db, mx_provider="aspmx.l.google.com")
        zuhal = _mock_zuhal("unknown")
        cost = CostTracker(None)
        worker = ZuhalDispatcher(config, test_db, zuhal, cost)

        rows = await db.fetch_pending_zuhal(test_db, limit=10)
        await worker._process(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATION_FAILED
        assert worker.stats["requeued"] == 0          # not re-queued for a 2nd attempt
        assert cost.counts.get("zuhal", 0) == 1        # charged exactly once

    async def test_unknown_on_normal_provider_retries_once(self, test_db, config):
        """A normal provider's unknown is retried once (could be transient)."""
        await _seed_needs_zuhal(test_db, mx_provider="mx.privatehost.example")
        zuhal = _mock_zuhal("unknown")
        worker = ZuhalDispatcher(config, test_db, zuhal, CostTracker(None))

        rows = await db.fetch_pending_zuhal(test_db, limit=10)
        await worker._process(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.NEEDS_ZUHAL  # re-queued for a 2nd attempt
        assert worker.stats["requeued"] == 1


class TestZuhalDispatcherFailureModes:
    async def test_circuit_open_requeues_to_needs_zuhal_without_cost(self, test_db, config):
        await _seed_needs_zuhal(test_db)
        zuhal = _mock_zuhal_raises(ZuhalCircuitOpenError())
        cost = CostTracker(None)

        worker = ZuhalDispatcher(config, test_db, zuhal, cost)
        rows = await db.fetch_pending_zuhal(test_db, limit=10)
        await worker._process(rows[0])

        async with test_db.execute(
            "SELECT record_state, dispatch_attempts FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.NEEDS_ZUHAL
        assert row["dispatch_attempts"] == 0  # no attempt burned on circuit-open
        assert cost.counts.get("zuhal", 0) == 0  # API was never reached
        assert worker.stats["requeued"] == 1

    async def test_cost_ceiling_marks_cost_skipped(self, test_db, config):
        await _seed_needs_zuhal(test_db)
        zuhal = _mock_zuhal("valid")

        cost = CostTracker(max_cost=0.0)
        cost.record_call("serper_producer")  # push past ceiling

        worker = ZuhalDispatcher(config, test_db, zuhal, cost)
        rows = await db.fetch_pending_zuhal(test_db, limit=10)
        await worker._process(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.COST_SKIPPED
        zuhal.validate.assert_not_called()
        assert worker.stats["cost_skipped"] == 1

    async def test_pipeline_halt_propagates(self, test_db, config):
        await _seed_needs_zuhal(test_db)
        zuhal = _mock_zuhal_raises(PipelineHaltError("auth failed"))

        worker = ZuhalDispatcher(config, test_db, zuhal, CostTracker(None))
        rows = await db.fetch_pending_zuhal(test_db, limit=10)
        with pytest.raises(PipelineHaltError):
            await worker._process(rows[0])

    async def test_unknown_exception_marks_validation_failed(self, test_db, config):
        await _seed_needs_zuhal(test_db)
        zuhal = _mock_zuhal_raises(RuntimeError("transient"))

        worker = ZuhalDispatcher(config, test_db, zuhal, CostTracker(None))
        rows = await db.fetch_pending_zuhal(test_db, limit=10)
        await worker._process(rows[0])

        async with test_db.execute(
            "SELECT record_state, zuhal_status FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.VALIDATION_FAILED
        assert row["zuhal_status"] == "error"

    async def test_retryable_429_requeues_to_needs_zuhal(self, test_db, config):
        await _seed_needs_zuhal(test_db)
        zuhal = _mock_zuhal_raises(_RetryableHTTPError(429))
        cost = CostTracker(None)

        worker = ZuhalDispatcher(config, test_db, zuhal, cost)
        rows = await db.fetch_pending_zuhal(test_db, limit=10)
        await worker._process(rows[0])

        async with test_db.execute(
            "SELECT record_state, zuhal_status, dispatch_attempts "
            "FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.NEEDS_ZUHAL
        assert row["zuhal_status"] is None
        assert row["dispatch_attempts"] == 0
        assert cost.counts.get("zuhal", 0) == 0
        assert worker.stats["requeued"] == 1

    async def test_retryable_500_marks_validation_failed(self, test_db, config):
        await _seed_needs_zuhal(test_db)
        zuhal = _mock_zuhal_raises(_RetryableHTTPError(500))

        worker = ZuhalDispatcher(config, test_db, zuhal, CostTracker(None))
        rows = await db.fetch_pending_zuhal(test_db, limit=10)
        await worker._process(rows[0])

        async with test_db.execute(
            "SELECT record_state, zuhal_status FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.VALIDATION_FAILED
        assert row["zuhal_status"] == "error"


class TestZuhalDispatcherRecovery:
    async def test_recover_stale_zuhal_validating_returns_to_queue(self, test_db):
        # Seed a stale ZUHAL_VALIDATING row by inserting with an old updated_at
        await test_db.execute(
            """
            INSERT INTO records
                (unique_id, record_state, candidate_email, updated_at)
            VALUES ('rec1', 'ZUHAL_VALIDATING', 'test@example.com',
                    datetime('now', '-10 minutes'))
            """
        )
        await test_db.commit()

        moved = await db.recover_stale_zuhal_validating(test_db, timeout_minutes=5)
        assert moved == 1

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.NEEDS_ZUHAL

    async def test_fresh_zuhal_validating_not_recovered(self, test_db):
        await test_db.execute(
            """
            INSERT INTO records
                (unique_id, record_state, candidate_email)
            VALUES ('rec1', 'ZUHAL_VALIDATING', 'test@example.com')
            """
        )
        await test_db.commit()

        moved = await db.recover_stale_zuhal_validating(test_db, timeout_minutes=5)
        assert moved == 0


class TestZuhalDispatcherRunLoop:
    async def test_exits_when_smtp_done_and_queue_empty(self, test_db, config):
        zuhal = _mock_zuhal("valid")
        smtp_done = asyncio.Event()
        smtp_done.set()

        worker = ZuhalDispatcher(
            config, test_db, zuhal, CostTracker(None),
            smtp_done_event=smtp_done,
        )

        # No NEEDS_ZUHAL rows + smtp_done set → worker should exit promptly
        await asyncio.wait_for(worker.run(), timeout=5.0)
        zuhal.validate.assert_not_called()

    async def test_processes_queued_record_in_run_loop(self, test_db, config):
        await _seed_needs_zuhal(test_db)
        zuhal = _mock_zuhal("valid")
        smtp_done = asyncio.Event()
        smtp_done.set()  # signal smtp drained — let worker exit after processing

        worker = ZuhalDispatcher(
            config, test_db, zuhal, CostTracker(None),
            smtp_done_event=smtp_done,
        )

        await asyncio.wait_for(worker.run(), timeout=5.0)

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATED
        assert worker.stats["validated"] == 1
