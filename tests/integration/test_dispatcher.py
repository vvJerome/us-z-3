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
    async def test_racknerd_valid_skips_bbops(self, test_db, config):
        """Racknerd valid short-circuits: bbops is skipped (sequential flow)."""
        await _insert_discovered(test_db, "rec1")
        await db.upsert_checkpoint(test_db, "producer_done", "true")

        rk = _mock_racknerd("valid", "250 OK")
        bb = _mock_bbops("valid", "250 OK")
        stop = asyncio.Event()
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None), stop)

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
        assert row["bbops_status"] == "not_run"  # skipped — Racknerd hit
        bb.verify.assert_not_called()

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

    async def test_both_error_requeues_without_incrementing_attempt(self, test_db, config):
        """Both-error is a pure infra failure — re-queues without burning dispatch_attempts,
        but requeue_count always increments to bound infinite loops."""
        await _insert_discovered(test_db, "rec1")

        rk = _mock_racknerd("error", "timeout")
        bb = _mock_bbops("error", "timeout")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, dispatch_attempts, requeue_count FROM records WHERE unique_id = ?",
            ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.DISCOVERED
        assert row["dispatch_attempts"] == 0  # infra transient — budget not consumed
        assert row["requeue_count"] == 1  # safety valve always increments

    async def test_tunnel_down_requeues_without_incrementing_attempt(self, test_db, config):
        """Tunnel-down is a pure infra failure — re-queues without burning dispatch_attempts
        and bbops is never called (early return before running bbops)."""
        await _insert_discovered(test_db, "rec1")

        rk = _mock_racknerd("error", "tunnel not up")
        bb = _mock_bbops("invalid", "550")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, dispatch_attempts, requeue_count FROM records WHERE unique_id = ?",
            ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.DISCOVERED
        assert row["dispatch_attempts"] == 0  # infra transient — budget not consumed
        assert row["requeue_count"] == 1  # safety valve always increments
        bb.verify.assert_not_called()  # bbops never runs when tunnel is down

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
        cost_tracker.record_call("serper_producer")

        dispatcher = Dispatcher(config, test_db, rk, bb, cost_tracker)
        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.COST_SKIPPED

    async def test_cost_ceiling_before_zuhal_marks_cost_skipped(self, test_db, config):
        """Cost ceiling hit mid-loop before Zuhal rescue → COST_SKIPPED not VALIDATED/FAILED."""
        from unittest.mock import AsyncMock as AM
        from pipeline.utils.zuhal_client import ZuhalClient
        from pipeline.models import ValidationResult

        await _insert_discovered(test_db, "rec1")

        # Both backends error → reconcile returns unknown → Zuhal rescue path
        rk = _mock_racknerd("error", "timeout")
        bb = _mock_bbops("error", "timeout")

        zuhal = MagicMock(spec=ZuhalClient)
        zuhal.validate = AM(return_value=ValidationResult(
            email="test@example.com", verdict="valid", score=0.0,
            is_disposable=False, raw_status="", http_status=200,
        ))

        # Ceiling already reached before the loop processes anything
        cost_tracker = CostTracker(max_cost=0.0)
        cost_tracker.record_call("zuhal")

        dispatcher = Dispatcher(config, test_db, rk, bb, cost_tracker, zuhal=zuhal)
        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        # Record must not be VALIDATED — ceiling was hit before Zuhal could run
        assert row["record_state"] == State.COST_SKIPPED
        zuhal.validate.assert_not_called()

    async def test_racknerd_blocked_requeues_and_increments_attempt(self, test_db, config):
        """Racknerd blocked + bbops invalid: bbops gave a real verdict so dispatch_attempts
        is incremented. Zuhal must not be called — the block is IP-level, not an email verdict."""
        from unittest.mock import AsyncMock as AM
        from pipeline.utils.zuhal_client import ZuhalClient

        await _insert_discovered(test_db, "rec1")

        rk = _mock_racknerd("blocked", "Spamhaus block")
        bb = _mock_bbops("invalid", "550")

        zuhal = MagicMock(spec=ZuhalClient)
        zuhal.validate = AM()

        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None), zuhal=zuhal)
        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, dispatch_attempts FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.DISCOVERED
        assert row["dispatch_attempts"] == 1  # incremented to bound persistent-block loops
        zuhal.validate.assert_not_called()

    async def test_max_dispatch_attempts_terminates_loop(self, test_db, config):
        """A record that has hit max_dispatch_attempts is marked VALIDATION_FAILED
        immediately without calling any backend."""
        from pipeline.config import PipelineConfig
        from pathlib import Path

        # Insert a record that already has dispatch_attempts == max
        await _insert_discovered(test_db, "rec1")
        await test_db.execute(
            "UPDATE records SET dispatch_attempts = ? WHERE unique_id = 'rec1'",
            (config.max_dispatch_attempts,),
        )
        await test_db.commit()

        rk = _mock_racknerd("valid")  # would validate if called
        bb = _mock_bbops("valid")

        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))
        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.VALIDATION_FAILED
        rk.verify.assert_not_called()

    async def test_racknerd_blocked_bbops_valid_validates_on_bbops(self, test_db, config):
        """When Racknerd is blocked but bbops returns valid, the OR-of-valids reconciliation
        short-circuits: bbops valid wins and the record is VALIDATED. The blocked re-queue
        path only fires when no backend produced a definitive valid verdict."""
        await _insert_discovered(test_db, "rec1")

        rk = _mock_racknerd("blocked", "Spamhaus block")
        bb = _mock_bbops("valid", "250 OK")

        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))
        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, final_verdict FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.VALIDATED
        assert row["final_verdict"] == "valid"  # bbops verdict surfaces as final

    async def test_dispatcher_serper_fallback_calls_reset_and_costed(self, test_db, config):
        """When the Serper 4th fallback fires inside the dispatcher, _fallback_calls is
        reset after each record and the extra API call is charged to cost_tracker."""
        from pipeline.utils.serper_client import SerperClient
        from pipeline.models import EnrichmentResult

        await test_db.execute(
            """
            INSERT INTO records
                (unique_id, business_name, agent_name, record_state,
                 candidate_emails, candidate_email, candidate_domain,
                 strategy, mx_provider, serper_enriched)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                "rec1", "Norwood Rural Volunteer Fire Department", "John Doe",
                State.DISCOVERED,
                json.dumps(["test@nrvfd.org"]), "test@nrvfd.org",
                "nrvfd.org", "without", "gmail.com",
            ),
        )
        await test_db.commit()

        rk = _mock_racknerd("invalid", "550")
        bb = _mock_bbops("invalid", "550")

        serper = MagicMock(spec=SerperClient)
        serper.enrich = AsyncMock(return_value=EnrichmentResult(
            candidate_emails=[], candidate_domain=None,
        ))
        serper.last_was_cache_hit = False
        serper._fallback_calls = 1  # simulate 4th fallback having fired

        cost_tracker = CostTracker(max_cost=10.0)
        dispatcher = Dispatcher(config, test_db, rk, bb, cost_tracker, serper=serper)
        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        # 1 primary call + 1 fallback call = 2 serper_dispatcher charges
        assert cost_tracker.counts.get("serper_dispatcher", 0) == 2
        # _fallback_calls must be reset so subsequent records start clean
        assert serper._fallback_calls == 0

    async def test_cost_ceiling_before_serper_fallback_marks_cost_skipped(self, test_db, config):
        """Cost ceiling hit after all patterns fail but before Serper fallback → COST_SKIPPED."""
        from pipeline.utils.serper_client import SerperClient
        from pipeline.models import EnrichmentResult

        # Insert with serper_enriched=0 so dispatcher considers Serper fallback
        await test_db.execute(
            """
            INSERT INTO records
                (unique_id, business_name, agent_name, record_state,
                 candidate_emails, candidate_email, candidate_domain,
                 strategy, mx_provider, serper_enriched)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                "rec1", "Test Corp", "John Doe",
                State.DISCOVERED,
                json.dumps(["test@example.com"]), "test@example.com",
                "example.com", "without", "gmail.com",
            ),
        )
        await test_db.commit()

        rk = _mock_racknerd("invalid", "550")
        bb = _mock_bbops("invalid", "550")

        serper = MagicMock(spec=SerperClient)
        serper.enrich = AsyncMock(return_value=EnrichmentResult(
            candidate_emails=["alt@example.com"], candidate_domain="example.com",
        ))
        serper.last_was_cache_hit = False

        # Ceiling reached before Serper fallback fires
        cost_tracker = CostTracker(max_cost=0.0)
        cost_tracker.record_call("zuhal")

        dispatcher = Dispatcher(config, test_db, rk, bb, cost_tracker, serper=serper)
        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.COST_SKIPPED
        serper.enrich.assert_not_called()

    async def test_greylisting_sets_retry_after(self, test_db, config):
        """Racknerd 4xx temporary deferral sets retry_after ~30 min in the future."""
        await _insert_discovered(test_db, "rec1")

        rk = _mock_racknerd("error", "421 (4xx temporary) try again")
        bb = _mock_bbops("error", "timeout")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, retry_after FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.DISCOVERED
        assert row["retry_after"] is not None  # hold set

    async def test_non_greylist_error_no_retry_after(self, test_db, config):
        """A plain error (not 4xx temporary) does not set retry_after."""
        await _insert_discovered(test_db, "rec1")

        rk = _mock_racknerd("error", "connection refused")
        bb = _mock_bbops("error", "timeout")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, retry_after FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.DISCOVERED
        assert row["retry_after"] is None

    async def test_max_requeue_count_terminates_loop(self, test_db, config):
        """A record that has hit max_requeue_count is marked VALIDATION_FAILED
        immediately without calling any backend."""
        await _insert_discovered(test_db, "rec1")
        await test_db.execute(
            "UPDATE records SET requeue_count = ? WHERE unique_id = 'rec1'",
            (config.max_requeue_count,),
        )
        await test_db.commit()

        rk = _mock_racknerd("valid")
        bb = _mock_bbops("valid")

        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))
        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()

        assert row["record_state"] == State.VALIDATION_FAILED
        rk.verify.assert_not_called()

    async def test_fetch_pending_skips_retry_after_hold(self, test_db, config):
        """fetch_pending_validation does not return records whose retry_after is in the future."""
        import datetime

        await _insert_discovered(test_db, "rec1")
        future = (
            datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        ).strftime("%Y-%m-%d %H:%M:%S")
        await test_db.execute(
            "UPDATE records SET retry_after = ? WHERE unique_id = 'rec1'", (future,)
        )
        await test_db.commit()

        rows = await db.fetch_pending_validation(test_db, limit=10)
        assert not any(r["unique_id"] == "rec1" for r in rows)
