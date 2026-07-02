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
    async def test_harvest_fallback_validates_scraped_email(self, test_db, config):
        """With --harvest, after patterns fail SMTP a harvested email is tried and validates."""
        from pipeline.harvest import HarvestResult

        await test_db.execute(
            """
            INSERT INTO records
                (unique_id, business_name, agent_name, record_state,
                 candidate_emails, candidate_email, candidate_domain,
                 strategy, mx_provider, serper_enriched)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                "rec1", "Acme Co", "John Doe", State.DISCOVERED,
                json.dumps(["guess@acme.com"]), "guess@acme.com", "acme.com",
                "with", "gmail.com",
            ),
        )
        await test_db.commit()

        # Racknerd: the pattern guess fails, the harvested address validates.
        async def rk_verify(email, *a, **k):
            ok = email == "owner@acme.com"
            return BackendVerdict("valid" if ok else "invalid", "", "2026-05-04T00:00:00Z")
        rk = MagicMock()
        rk.verify = AsyncMock(side_effect=rk_verify)
        bb = _mock_bbops("invalid", "550")

        harvest_cfg = config.model_copy(update={"harvest_enabled": True})
        dispatcher = Dispatcher(harvest_cfg, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        with patch(
            "pipeline._dispatch_helpers.harvest",
            AsyncMock(return_value=HarvestResult(emails=["owner@acme.com"])),
        ):
            await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, candidate_email, racknerd_status "
            "FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATED
        assert row["candidate_email"] == "owner@acme.com"  # the harvested address won
        assert row["racknerd_status"] == "valid"

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

    async def test_racknerd_blocked_hands_off_to_zuhal(self, test_db, config):
        """Racknerd blocked + bbops invalid: reconcile returns unknown (blocked is inconclusive),
        so the record is handed off to the Zuhal queue (NEEDS_ZUHAL). In decoupled mode the
        ZuhalDispatcher handles it; dispatcher.validate is not called directly."""
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

        assert row["record_state"] == State.NEEDS_ZUHAL
        assert row["dispatch_attempts"] == 0  # handoff does not consume the attempt budget
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

    async def test_dispatcher_serper_fallback_charges_via_charge_costs(self, test_db, config):
        """After patterns are exhausted the dispatcher delegates cost accounting to
        SerperClient.charge_costs (which owns the cache-hit/fallback math, unit-tested
        separately) rather than reaching into its private counters."""
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

        cost_tracker = CostTracker(max_cost=10.0)
        dispatcher = Dispatcher(config, test_db, rk, bb, cost_tracker, serper=serper)
        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        # The dispatcher must hand cost accounting to charge_costs with its own service tag,
        # exactly once — never charge the tracker directly or peek at private counters.
        serper.charge_costs.assert_called_once_with(cost_tracker, "serper_dispatcher")

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
        """A plain error (not 4xx greylisting) sets an infra backoff retry_after."""
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
        # Infra backoff: retry_after is set to a future timestamp (not None)
        assert row["retry_after"] is not None

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
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
        ).strftime("%Y-%m-%d %H:%M:%S")
        await test_db.execute(
            "UPDATE records SET retry_after = ? WHERE unique_id = 'rec1'", (future,)
        )
        await test_db.commit()

        rows = await db.fetch_pending_validation(test_db, limit=10)
        assert not any(r["unique_id"] == "rec1" for r in rows)


async def _insert_discovered_with_counts(
    conn: aiosqlite.Connection,
    unique_id: str,
    email: str = "test@example.com",
    tunnel_requeue_count: int = 0,
    bbops_requeue_count: int = 0,
) -> None:
    await conn.execute(
        """
        INSERT INTO records
            (unique_id, business_name, agent_name, record_state,
             candidate_emails, candidate_email, candidate_domain, strategy, mx_provider,
             tunnel_requeue_count, bbops_requeue_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            unique_id, "Test Corp", "John Doe",
            State.DISCOVERED,
            json.dumps([email]), email, "example.com", "with", "gmail.com",
            tunnel_requeue_count, bbops_requeue_count,
        ),
    )
    await conn.commit()


def _mock_zuhal(verdict: str):
    from pipeline.models import ValidationResult
    z = MagicMock()
    z.validate = AsyncMock(return_value=ValidationResult(
        email="test@example.com", verdict=verdict, score=0.0,
        is_disposable=False, raw_status="", http_status=200,
    ))
    return z


async def _insert_with_candidates(conn, unique_id, candidates, *, discovery_source=None):
    await conn.execute(
        """
        INSERT INTO records
            (unique_id, business_name, agent_name, record_state,
             candidate_emails, candidate_email, candidate_domain, strategy,
             mx_provider, discovery_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            unique_id, "Test Corp", "John Doe", State.DISCOVERED,
            json.dumps(candidates), candidates[0], "example.com", "with",
            "gmail.com", discovery_source,
        ),
    )
    await conn.commit()


class TestInfraRequeueLimit:
    """Dispatcher enforces per-infra requeue limits."""

    async def test_tunnel_down_first_time_requeues(self, test_db, config):
        """First tunnel failure re-queues and increments tunnel_requeue_count."""
        await _insert_discovered_with_counts(test_db, "rec1", tunnel_requeue_count=0)
        rk = _mock_racknerd("error", "tunnel not up")
        bb = _mock_bbops("invalid")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, tunnel_requeue_count FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.DISCOVERED
        assert row["tunnel_requeue_count"] == 1

    async def test_tunnel_down_second_time_falls_through_to_bbops(self, test_db, config):
        """Second tunnel failure skips Racknerd and runs bbops-only (no re-queue)."""
        await _insert_discovered_with_counts(test_db, "rec1", tunnel_requeue_count=1)
        rk = _mock_racknerd("error", "tunnel not up")
        bb = _mock_bbops("invalid")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, bbops_status FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] != State.DISCOVERED
        bb.verify.assert_called_once()

    async def test_bbops_error_first_time_requeues(self, test_db, config):
        """First bbops error (unknown reconcile, no Zuhal) re-queues with bbops type."""
        cfg = PipelineConfig(
            serper_api_key="test",
            zuhal_api_key="",
            racknerd_host="localhost",
            input_path=config.input_path,
            output_dir=config.output_dir,
            db_path=config.db_path,
            log_dir=config.log_dir,
            dispatch_concurrency=1,
            dispatch_backend_timeout_s=5.0,
            dispatch_poll_interval_s=0.1,
            dispatch_chunk_size=10,
        )
        await _insert_discovered_with_counts(test_db, "rec1", bbops_requeue_count=0)
        rk = _mock_racknerd("invalid")
        bb = _mock_bbops("error")
        dispatcher = Dispatcher(cfg, test_db, rk, bb, CostTracker(None), zuhal=None)

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, bbops_requeue_count FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.DISCOVERED
        assert row["bbops_requeue_count"] == 1

    async def test_bbops_error_second_time_skips_candidate(self, test_db, config):
        """Second bbops error skips candidate and exhausts all → VALIDATION_FAILED."""
        cfg = PipelineConfig(
            serper_api_key="test",
            zuhal_api_key="",
            racknerd_host="localhost",
            input_path=config.input_path,
            output_dir=config.output_dir,
            db_path=config.db_path,
            log_dir=config.log_dir,
            dispatch_concurrency=1,
            dispatch_backend_timeout_s=5.0,
            dispatch_poll_interval_s=0.1,
            dispatch_chunk_size=10,
        )
        await _insert_discovered_with_counts(test_db, "rec1", bbops_requeue_count=1)
        rk = _mock_racknerd("invalid")
        bb = _mock_bbops("error")
        dispatcher = Dispatcher(cfg, test_db, rk, bb, CostTracker(None), zuhal=None)

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATION_FAILED


class TestFailureReason:
    """Dispatcher sets failure_reason correctly on VALIDATION_FAILED."""

    async def test_all_candidates_exhausted_no_real_test_is_infra_loop(self, test_db, config):
        """dispatch_attempts=0 after all candidates exhausted → failure_reason=infra_loop."""
        cfg = PipelineConfig(
            serper_api_key="test",
            zuhal_api_key="",
            racknerd_host="localhost",
            input_path=config.input_path,
            output_dir=config.output_dir,
            db_path=config.db_path,
            log_dir=config.log_dir,
            dispatch_concurrency=1,
            dispatch_backend_timeout_s=5.0,
            dispatch_poll_interval_s=0.1,
            dispatch_chunk_size=10,
            max_bbops_requeues=0,
        )
        await _insert_discovered_with_counts(test_db, "rec1")
        rk = _mock_racknerd("error", "some error")
        bb = _mock_bbops("error")
        dispatcher = Dispatcher(cfg, test_db, rk, bb, CostTracker(None), zuhal=None)

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, failure_reason FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATION_FAILED
        assert row["failure_reason"] == "infra_loop"

    async def test_all_candidates_invalid_is_max_attempts(self, test_db, config):
        """dispatch_attempts>0 after real invalid verdicts → failure_reason=max_attempts."""
        await _insert_discovered(test_db, "rec1")
        rk = _mock_racknerd("invalid")
        bb = _mock_bbops("invalid")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, failure_reason FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATION_FAILED
        assert row["failure_reason"] == "max_attempts"

    async def test_max_requeue_count_sets_infra_loop(self, test_db, config):
        """Record at max_requeue_count with no real tests → failure_reason=infra_loop."""
        await _insert_discovered(test_db, "rec1")
        await test_db.execute(
            "UPDATE records SET requeue_count = ?, dispatch_attempts = 0 WHERE unique_id = ?",
            (config.max_requeue_count, "rec1"),
        )
        await test_db.commit()
        rk = _mock_racknerd("invalid")
        bb = _mock_bbops("invalid")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT failure_reason FROM records WHERE unique_id = ?", ("rec1",)
        ) as cur:
            row = await cur.fetchone()
        assert row["failure_reason"] == "infra_loop"


class TestCatchAllConfidenceGate:
    async def test_catch_all_accepted_by_default(self, test_db, config):
        """catch_all_min_confidence default 0.0 → catch_all accepted (unchanged behavior)."""
        await _insert_discovered(test_db, "rec1")
        rk = _mock_racknerd("catch_all", "250 accepts all")
        bb = _mock_bbops("invalid", "550")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, final_verdict FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATED
        assert row["final_verdict"] == "catch_all"
        bb.verify.assert_not_called()

    async def test_catch_all_below_gate_not_validated(self, test_db, config):
        """With the gate raised above the candidate's pre_score, a catch_all is not accepted."""
        config = config.model_copy(update={"catch_all_min_confidence": 3.0})
        await _insert_discovered(test_db, "rec1")
        rk = _mock_racknerd("catch_all", "250 accepts all")
        bb = _mock_bbops("invalid", "550")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATION_FAILED
        bb.verify.assert_called_once()


class TestZuhalConfidenceGate:
    async def test_low_confidence_skips_decoupled_handoff(self, test_db, config):
        """zuhal_min_confidence above pre_score → unknown re-queues instead of going to Zuhal."""
        config = config.model_copy(update={"zuhal_min_confidence": 10.0})
        await _insert_discovered(test_db, "rec1")
        rk = _mock_racknerd("invalid", "550")
        bb = _mock_bbops("error", "timeout")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None), zuhal=_mock_zuhal("valid"))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.DISCOVERED
        assert dispatcher.stats["handed_off_to_zuhal"] == 0

    async def test_default_confidence_hands_off_to_zuhal(self, test_db, config):
        """Default 0.0 gate → unknown verdict still hands off to Zuhal queue."""
        await _insert_discovered(test_db, "rec1")
        rk = _mock_racknerd("invalid", "550")
        bb = _mock_bbops("error", "timeout")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None), zuhal=_mock_zuhal("valid"))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.NEEDS_ZUHAL
        assert dispatcher.stats["handed_off_to_zuhal"] == 1

    async def test_low_confidence_skips_both_invalid_rescue(self, test_db, config):
        """zuhal_on_both_invalid rescue is skipped for low-confidence candidates."""
        config = config.model_copy(update={
            "zuhal_on_both_invalid": True, "zuhal_min_confidence": 10.0,
        })
        await _insert_discovered(test_db, "rec1")
        rk = _mock_racknerd("invalid", "550")
        bb = _mock_bbops("invalid", "550")
        zuhal = _mock_zuhal("valid")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None), zuhal=zuhal)

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATION_FAILED
        zuhal.validate.assert_not_called()


class TestCandidateReorder:
    async def test_higher_confidence_candidate_tried_first(self, test_db, config):
        """Candidates are sorted by pre_score; the name-matching one is probed before the weak one."""
        await _insert_with_candidates(
            test_db, "rec1", ["zzz@example.com", "john.doe@example.com"]
        )

        async def verify(email):
            status = "valid" if email == "john.doe@example.com" else "invalid"
            return BackendVerdict(status=status, message="", verified_at="2026-05-04T00:00:00Z")

        rk = MagicMock()
        rk.verify = AsyncMock(side_effect=verify)
        bb = _mock_bbops("invalid", "550")
        dispatcher = Dispatcher(config, test_db, rk, bb, CostTracker(None))

        rows = await db.fetch_pending_validation(test_db, limit=10)
        await dispatcher._process_record(rows[0])

        async with test_db.execute(
            "SELECT record_state, candidate_email FROM records WHERE unique_id = 'rec1'"
        ) as cur:
            row = await cur.fetchone()
        assert row["record_state"] == State.VALIDATED
        assert row["candidate_email"] == "john.doe@example.com"
        assert rk.verify.call_count == 1
