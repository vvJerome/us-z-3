"""Unit tests for BbopsAsyncConsumer — mocked HTTP and DB."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.consumers.bbops_async import BbopsAsyncConsumer, BbopsUnhealthy, _normalize_bbops_status


def _make_consumer(**kwargs) -> BbopsAsyncConsumer:
    conn = MagicMock()
    session = MagicMock()
    defaults = dict(
        conn=conn,
        session=session,
        base_url="https://bbops.test",
        batch_size=10,
        min_batch_size=2,
        max_inflight=3,
        flush_interval_s=0.05,
        poll_interval_s=0.1,
        poll_timeout_s=5.0,
        health_fail_threshold=2,
        health_ok_threshold=2,
    )
    defaults.update(kwargs)
    return BbopsAsyncConsumer(**defaults)


class TestNormalizeBbopsStatus:
    def test_valid_passes_through(self):
        assert _normalize_bbops_status("valid") == "valid"

    def test_catch_all_variants_normalized(self):
        assert _normalize_bbops_status("catch_all") == "catch_all"
        assert _normalize_bbops_status("catch-all") == "catch_all"

    def test_unreachable_becomes_error(self):
        assert _normalize_bbops_status("unreachable") == "error"

    def test_unknown_status_becomes_error(self):
        assert _normalize_bbops_status("garbage") == "error"


class TestHealthStateTransitions:
    def test_mark_failure_flips_unhealthy_at_threshold(self):
        c = _make_consumer(health_fail_threshold=2)
        assert c._healthy is True
        c._mark_failure()
        assert c._healthy is True
        c._mark_failure()
        assert c._healthy is False

    def test_mark_success_restores_healthy(self):
        c = _make_consumer(health_fail_threshold=1, health_ok_threshold=2)
        c._mark_failure()
        assert c._healthy is False
        c._mark_success()
        assert c._healthy is False
        c._mark_success()
        assert c._healthy is True

    def test_mark_success_resets_failure_counter(self):
        c = _make_consumer(health_fail_threshold=3)
        c._mark_failure()
        c._mark_failure()
        c._mark_success()
        assert c._consecutive_failures == 0


class TestVerifyUnhealthy:
    async def test_verify_raises_when_unhealthy(self):
        c = _make_consumer()
        c._healthy = False
        with pytest.raises(BbopsUnhealthy):
            await c.verify(record_id=1, email="test@example.com")


class TestResolveFutures:
    def test_resolves_matched_futures(self):
        loop = asyncio.new_event_loop()
        try:
            fut = loop.create_future()
            from pipeline.consumers.bbops_async import _QueueItem
            item = _QueueItem(record_id=1, email="a@b.com", future=fut)
            future_map = {"a@b.com": [item]}
            results = [{"email": "a@b.com", "status": "valid", "message": "250 OK"}]

            c = _make_consumer()
            c._resolve_futures(future_map, results)

            assert fut.done()
            assert fut.result().status == "valid"
        finally:
            loop.close()

    def test_unmatched_futures_resolved_as_error(self):
        loop = asyncio.new_event_loop()
        try:
            fut = loop.create_future()
            from pipeline.consumers.bbops_async import _QueueItem
            item = _QueueItem(record_id=1, email="a@b.com", future=fut)
            future_map = {"a@b.com": [item]}

            c = _make_consumer()
            c._resolve_futures(future_map, [])  # no results

            assert fut.done()
            assert fut.result().status == "error"
            assert "no result from bbops" in fut.result().message
        finally:
            loop.close()

    def test_does_not_double_resolve_future(self):
        loop = asyncio.new_event_loop()
        try:
            fut = loop.create_future()
            fut.set_result(MagicMock(status="valid", message="already set"))
            from pipeline.consumers.bbops_async import _QueueItem
            item = _QueueItem(record_id=1, email="a@b.com", future=fut)
            future_map = {"a@b.com": [item]}
            results = [{"email": "a@b.com", "status": "invalid", "message": "550"}]

            c = _make_consumer()
            c._resolve_futures(future_map, results)

            # Future was already done — result should not be overwritten
            assert fut.result().status == "valid"
        finally:
            loop.close()


class TestSubmitAndPollOnSubmitFailure:
    async def test_submit_failure_resolves_all_futures_as_error(self):
        loop = asyncio.get_running_loop()
        from pipeline.consumers.bbops_async import _QueueItem
        fut = loop.create_future()
        item = _QueueItem(record_id=1, email="a@b.com", future=fut)

        c = _make_consumer()
        c._http_submit_batch = AsyncMock(side_effect=Exception("connect failed"))

        await c._submit_and_poll([item])

        assert fut.done()
        assert fut.result().status == "error"
        assert "connect failed" in fut.result().message

    async def test_db_persist_failure_resolves_futures_and_marks_failure(self):
        loop = asyncio.get_running_loop()
        from pipeline.consumers.bbops_async import _QueueItem
        fut = loop.create_future()
        item = _QueueItem(record_id=1, email="a@b.com", future=fut)

        c = _make_consumer()
        c._http_submit_batch = AsyncMock(return_value=("batch-123", []))

        with patch("pipeline.consumers.bbops_async._db.insert_bbops_jobs",
                   side_effect=Exception("db locked")):
            await c._submit_and_poll([item])

        assert fut.done()
        assert fut.result().status == "error"
        assert c._consecutive_failures >= 1


class TestLifecycle:
    async def test_start_creates_background_tasks(self):
        c = _make_consumer()
        await c.start()
        try:
            assert c._flusher_task is not None
            assert c._health_task is not None
            assert not c._flusher_task.done()
        finally:
            await c.stop()

    async def test_stop_cancels_tasks(self):
        c = _make_consumer()
        await c.start()
        await c.stop()
        assert c._stop.is_set()
