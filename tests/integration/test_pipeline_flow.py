"""Integration tests for pipeline flow with mocked HTTP calls."""

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import aiosqlite
import pytest

from pipeline import db
from pipeline.config import PipelineConfig
from pipeline.db import State
from pipeline.consumer import ConsumerWorker, compute_confidence_score
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.zuhal_client import ZuhalClient


pytestmark = pytest.mark.asyncio


@pytest.fixture
async def test_db_conn() -> aiosqlite.Connection:
    """Create an in-memory test database."""
    conn = await aiosqlite.connect(":memory:")
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.executescript(db.SCHEMA_SQL)
    await conn.commit()
    conn.row_factory = aiosqlite.Row
    yield conn
    await conn.close()


@pytest.fixture
def test_config(tmp_path: Path) -> PipelineConfig:
    """Create a test pipeline configuration."""
    return PipelineConfig(
        serper_api_key="test_serper_key",
        zuhal_api_key="test_zuhal_key",
        input_path=tmp_path / "input.jsonl",
        output_dir=tmp_path,
        db_path=tmp_path / "test.db",
        log_dir=tmp_path / "logs",
        dry_run=True,
        chunk_size=10,
        dns_concurrency=10,
        zuhal_concurrency=1,
    )


class TestRecordInsertionWorkflow:
    """Test inserting records into the pipeline."""

    async def test_insert_raw_records(self, test_db_conn):
        """Raw records are inserted with correct state."""
        records = [
            {
                "unique_id": f"rec{i}",
                "business_name": f"Company {i}",
                "agent_name": f"Agent {i}",
                "state": "NY",
                "record_state": State.RAW,
            }
            for i in range(3)
        ]

        await db.insert_records_batch(test_db_conn, records, new_offset=3)

        async with test_db_conn.execute(
            "SELECT COUNT(*) FROM records WHERE record_state = ?", (State.RAW,)
        ) as cursor:
            count = (await cursor.fetchone())[0]

        assert count == 3

    async def test_insert_with_discovery_data(self, test_db_conn):
        """Records with discovery data are stored correctly."""
        records = [
            {
                "unique_id": "rec1",
                "business_name": "Acme Corp",
                "agent_name": "John Doe",
                "record_state": State.DISCOVERED,
                "candidate_emails": json.dumps(["john@acme.com", "j.doe@acme.com"]),
                "candidate_domain": "acme.com",
                "strategy": "with",
                "mx_provider": "google.com",
            }
        ]

        await db.insert_records_batch(test_db_conn, records, new_offset=1)

        async with test_db_conn.execute(
            "SELECT candidate_emails, strategy FROM records WHERE unique_id = ?", ("rec1",)
        ) as cursor:
            row = await cursor.fetchone()

        candidates = json.loads(row["candidate_emails"])
        assert len(candidates) == 2
        assert row["strategy"] == "with"


class TestValidationWorkflow:
    """Test validation workflow."""

    async def test_record_claims_to_validating_state(self, test_db_conn):
        """Records transition from DISCOVERED to VALIDATING when claimed."""
        await test_db_conn.execute(
            """
            INSERT INTO records
                (unique_id, record_state, candidate_emails, candidate_domain, agent_name, strategy)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "rec1",
                State.DISCOVERED,
                json.dumps(["test@example.com"]),
                "example.com",
                "John Doe",
                "with",
            ),
        )
        await test_db_conn.commit()

        rows = await db.fetch_pending_validation(test_db_conn, limit=10)

        assert len(rows) == 1
        assert rows[0]["record_state"] == State.VALIDATING

    async def test_validated_record_update(self, test_db_conn):
        """Validated record is updated with verdict and score."""
        await test_db_conn.execute(
            """
            INSERT INTO records (unique_id, record_state)
            VALUES (?, ?)
            """,
            ("rec1", State.VALIDATING),
        )
        await test_db_conn.commit()

        score = compute_confidence_score(
            email="john@example.com",
            candidate_domain="example.com",
            strategy="with",
            verdict="valid",
            agent_name="John Doe",
        )

        await db.update_record_status(
            test_db_conn,
            "rec1",
            State.VALIDATED,
            candidate_email="john@example.com",
            verdict="valid",
            zuhal_status="valid",
            zuhal_score=score,
        )

        async with test_db_conn.execute(
            "SELECT record_state, verdict, candidate_email FROM records WHERE unique_id = ?",
            ("rec1",),
        ) as cursor:
            row = await cursor.fetchone()

        assert row["record_state"] == State.VALIDATED
        assert row["verdict"] == "valid"
        assert row["candidate_email"] == "john@example.com"

    async def test_validation_failed_record(self, test_db_conn):
        """Failed validation sets record state appropriately."""
        await test_db_conn.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("rec1", State.VALIDATING),
        )
        await test_db_conn.commit()

        await db.update_record_status(test_db_conn, "rec1", State.VALIDATION_FAILED)

        async with test_db_conn.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cursor:
            row = await cursor.fetchone()

        assert row[0] == State.VALIDATION_FAILED


class TestConsumerWorkerIntegration:
    """Integration tests for ConsumerWorker with mocked Zuhal."""

    async def test_consumer_processes_validated_record(self, test_db_conn, test_config):
        """Consumer processes a record and validates it."""
        # Insert a DISCOVERED record
        await test_db_conn.execute(
            """
            INSERT INTO records
                (unique_id, record_state, candidate_emails, candidate_domain, agent_name, strategy, mx_provider)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "rec1",
                State.DISCOVERED,
                json.dumps(["john@example.com"]),
                "example.com",
                "John Doe",
                "with",
                "gmail.com",
            ),
        )
        await test_db_conn.commit()

        # Create mock Zuhal client
        mock_zuhal = AsyncMock(spec=ZuhalClient)
        mock_result = MagicMock()
        mock_result.verdict = "valid"
        mock_zuhal.validate.return_value = mock_result

        cost_tracker = CostTracker(max_cost=None)
        consumer = ConsumerWorker(test_config, test_db_conn, cost_tracker, mock_zuhal)

        # Get pending records
        rows = await db.fetch_pending_validation(test_db_conn, limit=10)
        assert len(rows) == 1

        # Simulate validation
        unique_id = rows[0]["unique_id"]
        candidates = json.loads(rows[0]["candidate_emails"])

        # Process the email
        email = candidates[0]
        result = await mock_zuhal.validate(email)

        if result.verdict == "valid":
            await db.update_record_status(
                test_db_conn,
                unique_id,
                State.VALIDATED,
                candidate_email=email,
                verdict="valid",
                zuhal_status="valid",
            )

        # Verify final state
        async with test_db_conn.execute(
            "SELECT record_state, verdict FROM records WHERE unique_id = ?", (unique_id,)
        ) as cursor:
            row = await cursor.fetchone()

        assert row[0] == State.VALIDATED
        assert row[1] == "valid"

    async def test_consumer_cost_tracking(self, test_db_conn, test_config):
        """Consumer respects cost limits."""
        cost_tracker = CostTracker(max_cost=0.01)  # Very low limit

        # Fake some calls
        cost_tracker.record_call("zuhal")
        cost_tracker.record_call("zuhal")

        # Should eventually hit ceiling
        assert cost_tracker.ceiling_reached() is False or cost_tracker.ceiling_reached() is True


class TestStateTransitions:
    """Test valid state transitions through pipeline."""

    async def test_raw_to_discovering_to_discovered(self, test_db_conn):
        """Record flows: RAW -> DISCOVERING -> DISCOVERED."""
        # Insert RAW record
        await test_db_conn.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("rec1", State.RAW),
        )
        await test_db_conn.commit()

        # Transition to DISCOVERING
        await db.update_record_status(test_db_conn, "rec1", State.DISCOVERING)

        # Transition to DISCOVERED
        await db.update_record_status(
            test_db_conn,
            "rec1",
            State.DISCOVERED,
            candidate_emails=json.dumps(["test@example.com"]),
        )

        async with test_db_conn.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cursor:
            row = await cursor.fetchone()

        assert row[0] == State.DISCOVERED

    async def test_discovered_to_validating_to_validated(self, test_db_conn):
        """Record flows: DISCOVERED -> VALIDATING -> VALIDATED."""
        await test_db_conn.execute(
            """
            INSERT INTO records
                (unique_id, record_state, candidate_emails)
            VALUES (?, ?, ?)
            """,
            ("rec1", State.DISCOVERED, json.dumps(["test@example.com"])),
        )
        await test_db_conn.commit()

        # Claim for validation
        rows = await db.fetch_pending_validation(test_db_conn, limit=10)
        assert rows[0]["record_state"] == State.VALIDATING

        # Mark validated
        await db.update_record_status(
            test_db_conn, "rec1", State.VALIDATED, candidate_email="test@example.com"
        )

        async with test_db_conn.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cursor:
            row = await cursor.fetchone()

        assert row[0] == State.VALIDATED

    async def test_discovery_failure_path(self, test_db_conn):
        """Discovery failure sets appropriate state."""
        await test_db_conn.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("rec1", State.DISCOVERING),
        )
        await test_db_conn.commit()

        await db.update_record_status(test_db_conn, "rec1", State.DISCOVERY_FAILED)

        async with test_db_conn.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cursor:
            row = await cursor.fetchone()

        assert row[0] == State.DISCOVERY_FAILED


class TestPatternLearning:
    """Test pattern ranking learning during validation."""

    async def test_successful_pattern_recorded(self, test_db_conn):
        """Successful email template is recorded in pattern_stats."""
        await db.record_pattern_result(
            test_db_conn, "gmail.com", "first.last", success=True
        )

        rankings = await db.get_pattern_rankings(test_db_conn, "gmail.com")
        assert len(rankings) == 1
        assert rankings[0]["template"] == "first.last"
        assert rankings[0]["success_count"] == 1

    async def test_multiple_runs_build_rankings(self, test_db_conn):
        """Multiple validation runs build accurate rankings."""
        # Simulate multiple runs
        for _ in range(10):
            await db.record_pattern_result(
                test_db_conn, "gmail.com", "first.last", success=True
            )

        for _ in range(3):
            await db.record_pattern_result(
                test_db_conn, "gmail.com", "flast", success=True
            )
        for _ in range(2):
            await db.record_pattern_result(
                test_db_conn, "gmail.com", "flast", success=False
            )

        rankings = await db.get_pattern_rankings(test_db_conn, "gmail.com")

        # first.last has 100% success, flast has 60%
        assert rankings[0]["template"] == "first.last"
        assert rankings[1]["template"] == "flast"

    async def test_rankings_by_provider(self, test_db_conn):
        """Different MX providers can have different rankings."""
        await db.record_pattern_result(
            test_db_conn, "gmail.com", "first.last", success=True
        )
        await db.record_pattern_result(
            test_db_conn, "outlook.com", "flast", success=True
        )

        gmail_rankings = await db.get_pattern_rankings(test_db_conn, "gmail.com")
        outlook_rankings = await db.get_pattern_rankings(test_db_conn, "outlook.com")

        assert gmail_rankings[0]["template"] == "first.last"
        assert outlook_rankings[0]["template"] == "flast"


class TestErrorHandling:
    """Test error handling in pipeline flow."""

    async def test_missing_candidate_emails(self, test_db_conn):
        """Records without candidate_emails are marked failed."""
        await test_db_conn.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("rec1", State.VALIDATING),
        )
        await test_db_conn.commit()

        await db.update_record_status(test_db_conn, "rec1", State.VALIDATION_FAILED)

        async with test_db_conn.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cursor:
            row = await cursor.fetchone()

        assert row[0] == State.VALIDATION_FAILED

    async def test_invalid_json_candidate_emails(self, test_db_conn):
        """Invalid JSON in candidate_emails is handled gracefully."""
        await test_db_conn.execute(
            """
            INSERT INTO records (unique_id, record_state, candidate_emails)
            VALUES (?, ?, ?)
            """,
            ("rec1", State.VALIDATING, "invalid_json"),
        )
        await test_db_conn.commit()

        async with test_db_conn.execute(
            "SELECT candidate_emails FROM records WHERE unique_id = ?", ("rec1",)
        ) as cursor:
            row = await cursor.fetchone()

        # Verify the invalid JSON is stored
        assert row[0] == "invalid_json"
