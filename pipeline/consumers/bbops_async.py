from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
import aiosqlite

from pipeline.models import BackendVerdict
from pipeline import db as _db

_log = logging.getLogger("pipeline.bbops")

_ISO_NOW = lambda: datetime.now(timezone.utc).isoformat()  # noqa: E731


class BbopsUnhealthy(Exception):
    """Raised by verify() when the bbops backend has failed health checks."""


@dataclass
class _QueueItem:
    record_id: int
    email: str
    future: asyncio.Future
    enqueued_at: float = 0.0

    def __post_init__(self) -> None:
        self.enqueued_at = time.monotonic()


class BbopsAsyncConsumer:
    """
    Async bbops.io email verification backend.

    Exposes verify(record_id, email) → Future[BackendVerdict].
    Internally batches emails, submits to bbops HTTP API, polls for results,
    and resolves the Futures. Crash recovery via bbops_jobs DB table.
    """

    def __init__(
        self,
        conn: aiosqlite.Connection,
        session: aiohttp.ClientSession,
        base_url: str = "https://email-verifier.bbops.io",
        batch_size: int = 500,
        min_batch_size: int = 8,
        max_inflight: int = 12,
        flush_interval_s: float = 2.0,
        poll_interval_s: float = 10.0,
        poll_timeout_s: float = 1800.0,
        health_fail_threshold: int = 3,
        health_ok_threshold: int = 2,
    ) -> None:
        self._conn = conn
        self._session = session
        self.base_url = base_url.rstrip("/")
        self.batch_size = batch_size
        self.min_batch_size = min_batch_size
        self.max_inflight = max_inflight
        self.flush_interval_s = flush_interval_s
        self.poll_interval_s = poll_interval_s
        self.poll_timeout_s = poll_timeout_s
        self.health_fail_threshold = health_fail_threshold
        self.health_ok_threshold = health_ok_threshold

        self._queue: asyncio.Queue[_QueueItem] = asyncio.Queue()
        self._inflight_sem = asyncio.Semaphore(max_inflight)
        self._healthy = True
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._flusher_task: asyncio.Task | None = None
        self._health_task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._stop.clear()
        self._flusher_task = asyncio.create_task(self._flusher_loop(), name="bbops-flusher")
        self._health_task = asyncio.create_task(self._health_loop(), name="bbops-health")
        _log.info("BbopsAsyncConsumer started (base_url=%s)", self.base_url)

    async def stop(self) -> None:
        self._stop.set()
        for task in (self._flusher_task, self._health_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def recover_inflight(self) -> None:
        """Re-submit polling for any batches that were in-flight during a crash."""
        batches = await _db.fetch_inflight_bbops_batches(self._conn)
        if not batches:
            return
        _log.info("BbopsAsyncConsumer: recovering %d in-flight batches", len(batches))
        for batch_id, items in batches.items():
            asyncio.create_task(
                self._poll_and_resolve_recovery(batch_id, items),
                name=f"bbops-recover-{batch_id[:8]}",
            )

    # ------------------------------------------------------------------
    # Public verify API
    # ------------------------------------------------------------------

    async def verify(self, record_id: int, email: str) -> BackendVerdict:
        """Enqueue an email for verification. Awaits the result Future."""
        if not self._healthy:
            raise BbopsUnhealthy("bbops backend is currently unhealthy")
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[BackendVerdict] = loop.create_future()
        item = _QueueItem(record_id=record_id, email=email, future=fut)
        await self._queue.put(item)
        return await fut

    # ------------------------------------------------------------------
    # Internal: batch flusher
    # ------------------------------------------------------------------

    async def _flusher_loop(self) -> None:
        while not self._stop.is_set():
            items = await self._drain_queue()
            if not items:
                continue
            asyncio.create_task(
                self._submit_and_poll(items),
                name="bbops-submit",
            )

    async def _drain_queue(self) -> list[_QueueItem]:
        """Collect items from the queue up to batch_size, respecting flush interval."""
        items: list[_QueueItem] = []
        deadline = time.monotonic() + self.flush_interval_s

        while len(items) < self.batch_size:
            timeout = max(0.0, deadline - time.monotonic())
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                items.append(item)
            except asyncio.TimeoutError:
                if not items:
                    return []
                age = time.monotonic() - items[0].enqueued_at
                if len(items) >= self.min_batch_size or age >= 2 * self.flush_interval_s:
                    break
                # Not enough items yet — extend deadline
                deadline = time.monotonic() + self.flush_interval_s

        return items

    async def _submit_and_poll(self, items: list[_QueueItem]) -> None:
        """Submit a batch to bbops and poll until done; resolve Futures."""
        async with self._inflight_sem:
            emails = [item.email for item in items]
            future_map: dict[str, list[_QueueItem]] = {}
            for item in items:
                future_map.setdefault(item.email.lower(), []).append(item)

            # Submit
            try:
                batch_id, jobs = await self._http_submit_batch(emails)
            except Exception as exc:
                _log.error("bbops submit failed: %s", exc)
                self._mark_failure()
                verdict = BackendVerdict(status="error", message=str(exc), verified_at=_ISO_NOW())
                for item in items:
                    if not item.future.done():
                        item.future.set_result(verdict)
                return

            self._mark_success()

            # Persist before polling (crash recovery)
            job_rows = [
                {
                    "record_id": item.record_id,
                    "email": item.email,
                    "job_id": next(
                        (j["id"] for j in jobs if j.get("email", "").lower() == item.email.lower()),
                        "",
                    ),
                    "batch_id": batch_id,
                }
                for item in items
            ]
            try:
                await _db.insert_bbops_jobs(self._conn, job_rows)
            except Exception as exc:
                _log.warning("Failed to persist bbops_jobs (crash recovery impaired): %s", exc)

            # Poll
            results = await self._poll_batch(batch_id)

            # Resolve futures
            self._resolve_futures(future_map, results)

            # Mark jobs done in DB
            for job_result in results:
                jid = job_result.get("job_id", "")
                if jid:
                    try:
                        await _db.mark_bbops_job_done(
                            self._conn,
                            jid,
                            job_result.get("status", "error"),
                            job_result.get("message", ""),
                        )
                    except Exception:
                        pass

    async def _poll_and_resolve_recovery(
        self,
        batch_id: str,
        items: list[dict],
    ) -> None:
        """Recovery path: poll an already-submitted batch without a future to resolve."""
        results = await self._poll_batch(batch_id)
        _log.info(
            "Recovery batch %s: got %d results for %d items",
            batch_id,
            len(results),
            len(items),
        )

    def _resolve_futures(
        self,
        future_map: dict[str, list[_QueueItem]],
        results: list[dict],
    ) -> None:
        resolved: set[str] = set()
        for job in results:
            email_key = job.get("email", "").lower()
            status = job.get("status", "error")
            message = job.get("message", "")
            verdict = BackendVerdict(status=status, message=message, verified_at=_ISO_NOW())
            for item in future_map.get(email_key, []):
                if not item.future.done():
                    item.future.set_result(verdict)
            resolved.add(email_key)

        # Resolve unmatched futures with error
        for email_key, pending_items in future_map.items():
            if email_key not in resolved:
                verdict = BackendVerdict(
                    status="error", message="no result from bbops", verified_at=_ISO_NOW()
                )
                for item in pending_items:
                    if not item.future.done():
                        item.future.set_result(verdict)

    # ------------------------------------------------------------------
    # Internal: HTTP API
    # ------------------------------------------------------------------

    async def _http_submit_batch(self, emails: list[str]) -> tuple[str, list[dict]]:
        async with self._session.post(
            f"{self.base_url}/jobs/batch",
            json={"emails": emails},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
        batch_id = data["batch_id"]
        jobs = data.get("jobs", [])
        _log.info(
            "bbops batch %s submitted: %d emails (auto catch-all: %d)",
            batch_id,
            data.get("count", len(emails)),
            data.get("auto_catch_all_count", 0),
        )
        return batch_id, jobs

    async def _poll_batch(self, batch_id: str) -> list[dict]:
        """Poll until batch is done or timeout. Returns list of job result dicts."""
        interval = self.poll_interval_s
        deadline = time.monotonic() + self.poll_timeout_s

        while time.monotonic() < deadline:
            try:
                async with self._session.get(
                    f"{self.base_url}/batches/{batch_id}",
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            except Exception as exc:
                _log.warning("bbops poll error for batch %s: %s", batch_id, exc)
                await asyncio.sleep(interval)
                continue

            if data.get("status") == "done":
                # Fetch job results
                return await self._fetch_batch_jobs(batch_id)

            _log.debug(
                "bbops batch %s: status=%s done=%s/%s",
                batch_id,
                data.get("status"),
                data.get("done", "?"),
                data.get("total", "?"),
            )
            await asyncio.sleep(interval)
            interval = min(interval * 2, 120.0)

        _log.warning(
            "bbops batch %s timed out after %.0fs", batch_id, self.poll_timeout_s
        )
        return []

    async def _fetch_batch_jobs(self, batch_id: str) -> list[dict]:
        try:
            async with self._session.get(
                f"{self.base_url}/batches/{batch_id}/jobs",
                params={"limit": 5000},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
            jobs = data.get("jobs", [])
            # Normalize field names to our model
            return [
                {
                    "email": j.get("email", ""),
                    "status": _normalize_bbops_status(j.get("status", "error")),
                    "message": j.get("message", ""),
                    "job_id": j.get("id", ""),
                }
                for j in jobs
            ]
        except Exception as exc:
            _log.error("bbops fetch jobs error for batch %s: %s", batch_id, exc)
            return []

    # ------------------------------------------------------------------
    # Internal: health monitoring
    # ------------------------------------------------------------------

    async def _health_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    asyncio.shield(self._stop.wait()), timeout=30.0
                )
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                break
            await self._check_health()

    async def _check_health(self) -> None:
        try:
            async with self._session.get(
                f"{self.base_url}/health",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                if data.get("ok"):
                    self._mark_success()
                else:
                    self._mark_failure()
        except Exception:
            self._mark_failure()

    def _mark_failure(self) -> None:
        self._consecutive_failures += 1
        self._consecutive_successes = 0
        if self._consecutive_failures >= self.health_fail_threshold:
            if self._healthy:
                _log.warning(
                    "bbops: %d consecutive failures — marking unhealthy",
                    self._consecutive_failures,
                )
                self._healthy = False

    def _mark_success(self) -> None:
        self._consecutive_successes += 1
        self._consecutive_failures = 0
        if self._consecutive_successes >= self.health_ok_threshold:
            if not self._healthy:
                _log.info("bbops: recovered — marking healthy")
                self._healthy = True


def _normalize_bbops_status(raw: str) -> str:
    """Map bbops API status strings to our BackendStatus literals."""
    mapping = {
        "valid": "valid",
        "invalid": "invalid",
        "catch_all": "catch_all",
        "catch-all": "catch_all",
        "error": "error",
        "blocked": "blocked",
        "unreachable": "error",
    }
    return mapping.get(raw.lower(), "error")
