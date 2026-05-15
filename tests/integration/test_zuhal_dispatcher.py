"""Integration tests for ZuhalDispatcher with real SQLite and mocked Zuhal client."""
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
from pipeline.models import ValidationResult
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.zuhal_client import ZuhalCircuitOpenError
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
        zuhal_poll_interval_s=0.05,
        zuhal_chunk_size=10,
    )


def _mock_zuhal(verdict: str) -> MagicMock:
    client = MagicMock()
    client.validate = AsyncMock(
        return_value=ValidationResult(
            email="info@acme.com",
            verdict=verdict,
            score=0.9,
            is_disposable=False,
            raw_status="success",
            http_status=200,
        )
    )
    return client


async def _insert_needs_zuhal(
    conn: aiosqlite.Connection,
    unique_id: str = "MI-001",
    email: str = "info@acme.com",
    racknerd_status: str = "invalid",
    bbops_status: str = "invalid",
) -> None:
    """Insert a record directly into NEEDS_ZUHAL state."""
    await conn.execute(
        """
        INSERT INTO records (
            unique_id, business_name, agent_name, state, record_state,
            candidate_email, candidate_domain, strategy,
            racknerd_status, bbops_status,
            dispatch_attempts, created_at, updated_at
        ) VALUES (?, 'Acme Corp', 'John Doe', 'MI', 'NEEDS_ZUHAL',
                  ?, 'acme.com', 'without',
                  ?, ?,
                  1, datetime('now'), datetime('now'))
        """,
        (unique_id, email, racknerd_status, bbops_status),
    )
    await conn.commit()


class TestZuhalDispatcherValidates:
    async def test_valid_verdict_creates_validated_record(self, test_db, config):
        await _insert_needs_zuhal(test_db, "MI-001")
        zuhal = _mock_zuhal("valid")
        stop = asyncio.Event()
        smtp_done = asyncio.Event()
        smtp_done.set()

        dispatcher = ZuhalDispatcher(
            config=config, conn=test_db, zuhal=zuhal,
            cost_tracker=CostTracker(None),
            stop_event=stop, smtp_done_event=smtp_done,
        )
        await dispatcher.run()

        async with test_db.execute(
            "SELECT record_state, final_verdict FROM records WHERE unique_id = 'MI-001'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATED
        assert row["final_verdict"] == "valid"
        assert dispatcher.stats["validated"] == 1

    async def test_catch_all_verdict_creates_validated_record(self, test_db, config):
        await _insert_needs_zuhal(test_db, "MI-002")
        zuhal = _mock_zuhal("accept-all")  # Zuhal returns "accept-all", not "catch_all"
        stop = asyncio.Event()
        smtp_done = asyncio.Event()
        smtp_done.set()

        dispatcher = ZuhalDispatcher(
            config=config, conn=test_db, zuhal=zuhal,
            cost_tracker=CostTracker(None),
            stop_event=stop, smtp_done_event=smtp_done,
        )
        await dispatcher.run()

        async with test_db.execute(
            "SELECT record_state, final_verdict FROM records WHERE unique_id = 'MI-002'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATED
        assert row["final_verdict"] == "catch_all"

    async def test_invalid_verdict_creates_validation_failed(self, test_db, config):
        await _insert_needs_zuhal(test_db, "MI-003")
        zuhal = _mock_zuhal("invalid")
        stop = asyncio.Event()
        smtp_done = asyncio.Event()
        smtp_done.set()

        dispatcher = ZuhalDispatcher(
            config=config, conn=test_db, zuhal=zuhal,
            cost_tracker=CostTracker(None),
            stop_event=stop, smtp_done_event=smtp_done,
        )
        await dispatcher.run()

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'MI-003'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATION_FAILED
        assert dispatcher.stats["validation_failed"] == 1

    async def test_error_verdict_creates_validation_failed(self, test_db, config):
        await _insert_needs_zuhal(test_db, "MI-004")
        zuhal = _mock_zuhal("error")
        stop = asyncio.Event()
        smtp_done = asyncio.Event()
        smtp_done.set()

        dispatcher = ZuhalDispatcher(
            config=config, conn=test_db, zuhal=zuhal,
            cost_tracker=CostTracker(None),
            stop_event=stop, smtp_done_event=smtp_done,
        )
        await dispatcher.run()

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'MI-004'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATION_FAILED


class TestZuhalDispatcherRequeue:
    async def test_circuit_open_requeues_to_needs_zuhal(self, test_db, config):
        await _insert_needs_zuhal(test_db, "MI-005")
        zuhal = MagicMock()
        zuhal.validate = AsyncMock(side_effect=ZuhalCircuitOpenError("open"))
        stop = asyncio.Event()
        smtp_done = asyncio.Event()
        smtp_done.set()

        dispatcher = ZuhalDispatcher(
            config=config, conn=test_db, zuhal=zuhal,
            cost_tracker=CostTracker(None),
            stop_event=stop, smtp_done_event=smtp_done,
        )
        await dispatcher.run()

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'MI-005'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.NEEDS_ZUHAL
        assert dispatcher.stats["requeued"] == 1

    async def test_no_candidate_email_marks_failed(self, test_db, config):
        # Insert record with NULL candidate_email
        await conn_insert_no_email(test_db, "MI-006")
        zuhal = _mock_zuhal("valid")
        stop = asyncio.Event()
        smtp_done = asyncio.Event()
        smtp_done.set()

        dispatcher = ZuhalDispatcher(
            config=config, conn=test_db, zuhal=zuhal,
            cost_tracker=CostTracker(None),
            stop_event=stop, smtp_done_event=smtp_done,
        )
        await dispatcher.run()

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'MI-006'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATION_FAILED
        assert dispatcher.stats["validation_failed"] == 1


class TestZuhalDispatcherCostCeiling:
    async def test_cost_ceiling_skips_record(self, test_db, config):
        await _insert_needs_zuhal(test_db, "MI-007")
        zuhal = _mock_zuhal("valid")
        cost_tracker = CostTracker(max_cost=0.0)  # ceiling already reached
        cost_tracker.record_call("zuhal")  # push over

        stop = asyncio.Event()
        smtp_done = asyncio.Event()
        smtp_done.set()

        dispatcher = ZuhalDispatcher(
            config=config, conn=test_db, zuhal=zuhal,
            cost_tracker=cost_tracker,
            stop_event=stop, smtp_done_event=smtp_done,
        )
        await dispatcher.run()

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'MI-007'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.COST_SKIPPED
        assert dispatcher.stats["cost_skipped"] == 1


class TestZuhalDispatcherRecovery:
    async def test_recovers_stale_zuhal_validating_on_start(self, test_db, config):
        # Insert a record stuck in ZUHAL_VALIDATING from a previous crashed run
        await test_db.execute(
            """
            INSERT INTO records (
                unique_id, business_name, agent_name, state, record_state,
                candidate_email, candidate_domain, strategy,
                racknerd_status, bbops_status,
                dispatch_attempts, created_at, updated_at
            ) VALUES ('MI-008', 'Corp', 'Agent', 'MI', 'ZUHAL_VALIDATING',
                      'info@corp.com', 'corp.com', 'without',
                      'invalid', 'invalid',
                      1, datetime('now', '-10 minutes'), datetime('now', '-10 minutes'))
            """
        )
        await test_db.commit()

        zuhal = _mock_zuhal("valid")
        stop = asyncio.Event()
        smtp_done = asyncio.Event()
        smtp_done.set()

        dispatcher = ZuhalDispatcher(
            config=config, conn=test_db, zuhal=zuhal,
            cost_tracker=CostTracker(None),
            stop_event=stop, smtp_done_event=smtp_done,
        )
        await dispatcher.run()

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'MI-008'"
        ) as cur:
            row = await cur.fetchone()
        # Should have been recovered → NEEDS_ZUHAL → processed → VALIDATED
        assert row["record_state"] == State.VALIDATED


async def conn_insert_no_email(conn: aiosqlite.Connection, unique_id: str) -> None:
    await conn.execute(
        """
        INSERT INTO records (
            unique_id, business_name, agent_name, state, record_state,
            candidate_email, candidate_domain, strategy,
            racknerd_status, bbops_status,
            dispatch_attempts, created_at, updated_at
        ) VALUES (?, 'Corp', 'Agent', 'MI', 'NEEDS_ZUHAL',
                  NULL, 'corp.com', 'without',
                  'invalid', 'invalid',
                  1, datetime('now'), datetime('now'))
        """,
        (unique_id,),
    )
    await conn.commit()
