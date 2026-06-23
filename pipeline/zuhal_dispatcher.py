from __future__ import annotations

import asyncio
import logging
import time

import aiosqlite

from pipeline.config import PipelineConfig
from pipeline.constants import (
    DISPATCH_POLL_EMPTY_BACKOFF_THRESHOLD,
    DISPATCH_POLL_MAX_INTERVAL_S,
)
from pipeline.models import PipelineHaltError
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.text import parse_name
from pipeline.utils.zuhal_client import (
    ZuhalClient,
    ZuhalCircuitOpenError,
    _RetryableHTTPError,
)
from pipeline import db
from pipeline.db import State
from pipeline._dispatch_helpers import compute_confidence_score, record_pattern

logger = logging.getLogger("pipeline.zuhal_dispatcher")

# Adaptive concurrency: scale up after this many consecutive 429-free batches.
_SCALE_UP_AFTER = 10
# Step size when scaling up (one at a time) or down (halve).
_SCALE_UP_STEP = 1


class ZuhalDispatcher:
    """Drains the NEEDS_ZUHAL queue at its own pace, decoupled from the SMTP path.

    SMTP dispatcher hands records off (state=NEEDS_ZUHAL); this worker claims them
    (state=ZUHAL_VALIDATING), runs the Zuhal probe, writes the terminal verdict.

    Two drain modes:
    - Bulk: when backlog > zuhal_bulk_threshold, uploads a CSV batch to Zuhal's
      bulk endpoint and applies results in one shot (much faster for large queues).
    - Single-verify: standard one-email-at-a-time path with adaptive concurrency.
    """

    def __init__(
        self,
        config: PipelineConfig,
        conn: aiosqlite.Connection,
        zuhal: ZuhalClient,
        cost_tracker: CostTracker,
        stop_event: asyncio.Event | None = None,
        smtp_done_event: asyncio.Event | None = None,
    ) -> None:
        self.config = config
        self.conn = conn
        self.zuhal = zuhal
        self.cost_tracker = cost_tracker
        self.stop_event = stop_event or asyncio.Event()
        self.smtp_done_event = smtp_done_event or asyncio.Event()
        self._concurrency = config.zuhal_concurrency
        self._sem = asyncio.Semaphore(self._concurrency)
        self._write_lock = asyncio.Lock()
        self._saw_429 = False
        self._ok_batches = 0
        self.stats: dict[str, int] = {
            "validated": 0,
            "validation_failed": 0,
            "requeued": 0,
            "cost_skipped": 0,
            "bulk_batches": 0,
        }

    # ── concurrency helpers ───────────────────────────────────────────────────

    def _adjust_concurrency(self) -> None:
        """Adapt concurrency target after each batch based on 429 signal."""
        cfg = self.config
        if self._saw_429:
            new = max(cfg.zuhal_concurrency_min, self._concurrency // 2)
            if new != self._concurrency:
                logger.info("Zuhal 429 detected — scaling concurrency %d → %d", self._concurrency, new)
            self._concurrency = new
            self._ok_batches = 0
        else:
            self._ok_batches += 1
            if self._ok_batches >= _SCALE_UP_AFTER:
                new = min(cfg.zuhal_concurrency_max, self._concurrency + _SCALE_UP_STEP)
                if new != self._concurrency:
                    logger.info("Scaling Zuhal concurrency %d → %d", self._concurrency, new)
                self._concurrency = new
                self._ok_batches = 0
        self._sem = asyncio.Semaphore(self._concurrency)
        self._saw_429 = False

    # ── main loop ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        base_interval = self.config.zuhal_poll_interval_s
        poll_interval = base_interval
        consecutive_empty = 0

        stale_timeout = self.config.zuhal_bulk_stale_timeout_minutes
        recovered = await db.recover_stale_zuhal_validating(self.conn, timeout_minutes=stale_timeout)
        if recovered:
            logger.warning("Recovered %d orphaned ZUHAL_VALIDATING rows → NEEDS_ZUHAL", recovered)

        logger.info(
            "Zuhal dispatcher starting (concurrency=%d, poll=%.1fs, bulk_threshold=%d)",
            self._concurrency, base_interval, self.config.zuhal_bulk_threshold,
        )

        while not self.stop_event.is_set():
            backlog = await db.count_needs_zuhal(self.conn)

            # Bulk mode: large backlog → upload N concurrent CSV batches
            if backlog >= self.config.zuhal_bulk_threshold:
                n_jobs = self.config.zuhal_bulk_concurrent_jobs
                results = await asyncio.gather(
                    *[self._drain_bulk() for _ in range(n_jobs)],
                    return_exceptions=True,
                )
                drained = sum(r for r in results if isinstance(r, int))
                for r in results:
                    if isinstance(r, PipelineHaltError):
                        raise r
                if drained > 0:
                    consecutive_empty = 0
                    poll_interval = base_interval
                    continue

            # Single-verify mode
            rows = await db.fetch_pending_zuhal(
                self.conn, limit=self.config.zuhal_chunk_size,
            )

            if not rows:
                consecutive_empty += 1
                if consecutive_empty >= DISPATCH_POLL_EMPTY_BACKOFF_THRESHOLD:
                    poll_interval = min(poll_interval * 2, DISPATCH_POLL_MAX_INTERVAL_S)

                if self.smtp_done_event.is_set():
                    if not await db.has_pending_zuhal(self.conn):
                        if consecutive_empty >= DISPATCH_POLL_EMPTY_BACKOFF_THRESHOLD:
                            logger.info("Zuhal dispatcher: queue drained and SMTP done — exiting")
                            break
                    else:
                        consecutive_empty = 0
                        poll_interval = base_interval
                        continue

                await asyncio.sleep(poll_interval)
                continue

            consecutive_empty = 0
            poll_interval = base_interval

            tasks = [self._dispatch_one(row) for row in rows]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, PipelineHaltError):
                    raise res
                if isinstance(res, BaseException):
                    logger.error("Unexpected Zuhal dispatcher error", exc_info=res)

            self._adjust_concurrency()

        logger.info(
            "Zuhal dispatcher finished — validated=%d failed=%d requeued=%d "
            "cost_skipped=%d bulk_batches=%d",
            self.stats["validated"],
            self.stats["validation_failed"],
            self.stats["requeued"],
            self.stats["cost_skipped"],
            self.stats["bulk_batches"],
        )

    # ── bulk drain ────────────────────────────────────────────────────────────

    async def _drain_bulk(self) -> int:
        """Claim a batch, upload to Zuhal bulk API, apply results. Returns rows processed."""
        rows = await db.fetch_pending_zuhal(
            self.conn, limit=self.config.zuhal_bulk_batch_size,
        )
        if not rows:
            return 0

        emails = [r["candidate_email"] for r in rows if r["candidate_email"]]
        id_by_email: dict[str, aiosqlite.Row] = {
            r["candidate_email"].lower(): r for r in rows if r["candidate_email"]
        }
        no_email_rows = [r for r in rows if not r["candidate_email"]]

        # Mark rows with no email as failed immediately
        for row in no_email_rows:
            async with self._write_lock:
                await db.update_record_status(self.conn, row["unique_id"], State.VALIDATION_FAILED)
            self.stats["validation_failed"] += 1

        if not emails:
            return len(no_email_rows)

        # Apply cached verdicts immediately — no upload needed for these
        cached_hits: dict[str, str] = {}
        for e in emails:
            verdict = await db.lookup_email_cache(self.conn, e.lower())
            if verdict is not None:
                cached_hits[e.lower()] = verdict

        for email_lower, verdict in cached_hits.items():
            row = id_by_email[email_lower]
            trace_entry = {"stage": "zuhal_fallback", "outcome": verdict, "cache_hit": True, "email": email_lower}
            await self._apply_verdict(row, verdict, bulk=True, trace_entry=trace_entry)

        emails = [e for e in emails if e.lower() not in cached_hits]
        id_by_email = {k: v for k, v in id_by_email.items() if k not in cached_hits}

        if not emails:
            return len(cached_hits) + len(no_email_rows)

        unique_ids = list(id_by_email.values())

        async def _heartbeat() -> None:
            await db.touch_zuhal_validating(self.conn, [r["unique_id"] for r in unique_ids])

        _job_id: list[str] = []

        async def _on_job_created(job_id: str) -> None:
            _job_id.append(job_id)
            async with self._write_lock:
                await db.create_zuhal_job(self.conn, job_id, len(emails))

        try:
            verdicts = await self.zuhal.bulk_validate(
                emails,
                poll_interval_s=self.config.zuhal_bulk_poll_interval_s,
                max_poll_minutes=self.config.zuhal_bulk_stale_timeout_minutes,
                on_poll=_heartbeat,
                on_job_created=_on_job_created,
            )
        except PipelineHaltError:
            raise
        except Exception as exc:
            logger.warning("Zuhal bulk failed (%s) — requeueing %d records", exc, len(rows))
            if _job_id:
                async with self._write_lock:
                    await db.update_zuhal_job_status(self.conn, _job_id[0], "failed")
            for row in rows:
                if row["candidate_email"]:
                    async with self._write_lock:
                        await db.requeue_zuhal(self.conn, row["unique_id"])
                    self.stats["requeued"] += 1
            return 0

        if not verdicts:
            # Bulk returned nothing — requeue for single-verify
            if _job_id:
                async with self._write_lock:
                    await db.update_zuhal_job_status(self.conn, _job_id[0], "failed")
            for row in rows:
                if row["candidate_email"]:
                    async with self._write_lock:
                        await db.requeue_zuhal(self.conn, row["unique_id"])
                    self.stats["requeued"] += 1
            return 0

        # Apply results
        for email_lower, row in id_by_email.items():
            status = verdicts.get(email_lower, "unknown")
            await self._apply_verdict(row, status, bulk=True)
            if status in ("valid", "catch_all", "invalid"):
                async with self._write_lock:
                    await db.write_email_cache(self.conn, email_lower, status, "zuhal")

        if _job_id:
            async with self._write_lock:
                await db.update_zuhal_job_status(self.conn, _job_id[0], "complete")

        self.stats["bulk_batches"] += 1
        processed = len(emails) + len(no_email_rows)
        logger.info("Zuhal bulk batch done — %d processed", processed)
        return processed

    async def _dispatch_one(self, row: aiosqlite.Row) -> None:
        async with self._sem:
            if self.stop_event.is_set():
                return
            await self._process(row)

    async def _process(self, row: aiosqlite.Row) -> None:
        unique_id = row["unique_id"]
        email = row["candidate_email"]
        if not email:
            logger.warning("ZUHAL_VALIDATING row %s has no candidate_email — marking failed", unique_id)
            async with self._write_lock:
                await db.update_record_status(self.conn, unique_id, State.VALIDATION_FAILED)
            self.stats["validation_failed"] += 1
            return

        if self.cost_tracker.ceiling_reached():
            logger.info("Cost ceiling reached before Zuhal — skipping %s", unique_id)
            async with self._write_lock:
                await db.update_record_status(self.conn, unique_id, State.COST_SKIPPED)
            self.stats["cost_skipped"] += 1
            return

        cached = await db.lookup_email_cache(self.conn, email)
        if cached is not None:
            logger.debug("Zuhal cache hit for %s (%s) — skipping API call", email, cached)
            trace_entry = {"stage": "zuhal_fallback", "outcome": cached, "cache_hit": True, "email": email}
            await self._apply_verdict(row, cached, bulk=False, trace_entry=trace_entry)
            return

        t0 = time.monotonic()
        status: str
        try:
            result = await self.zuhal.validate(email)
            status = result.verdict
        except PipelineHaltError:
            raise
        except ZuhalCircuitOpenError:
            async with self._write_lock:
                await db.requeue_zuhal(self.conn, unique_id)
            self.stats["requeued"] += 1
            logger.warning("Zuhal circuit open — re-queued %s to NEEDS_ZUHAL", unique_id)
            return
        except _RetryableHTTPError as exc:
            if exc.status == 429:
                self._saw_429 = True
                async with self._write_lock:
                    await db.requeue_zuhal(self.conn, unique_id)
                self.stats["requeued"] += 1
                logger.warning("Zuhal 429 — re-queued %s to NEEDS_ZUHAL", unique_id)
                return
            logger.debug("Zuhal probe HTTP %d for %s/%s", exc.status, unique_id, email)
            status = "error"
        except Exception as exc:
            logger.debug("Zuhal probe error for %s/%s: %s", unique_id, email, exc)
            status = "error"

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        trace_entry = {"stage": "zuhal_fallback", "outcome": status, "ms": elapsed_ms, "email": email}

        self.cost_tracker.record_call("zuhal")
        if status in ("valid", "catch_all", "invalid"):
            async with self._write_lock:
                await db.write_email_cache(self.conn, email, status, "zuhal")
        await self._apply_verdict(row, status, bulk=False, trace_entry=trace_entry)

    async def _apply_verdict(
        self,
        row: aiosqlite.Row,
        status: str,
        *,
        bulk: bool,
        trace_entry: dict | None = None,
    ) -> None:
        """Write terminal verdict for a single row (shared by single-verify and bulk paths)."""
        unique_id = row["unique_id"]
        email = row["candidate_email"]
        candidate_domain = row["candidate_domain"] or ""
        strategy = row["strategy"] or "without"
        agent_name = row["agent_name"] or ""
        mx_provider = row["mx_provider"]
        first, _, last = parse_name(agent_name)

        if status == "accept-all":
            status = "catch_all"

        # "unknown" means Zuhal could not determine validity — not a confirmed
        # failure. Re-queue for one more attempt; if it comes back unknown again
        # (dispatch_attempts >= 1) fall through to VALIDATION_FAILED so the
        # ZeroBounce pass can handle it.
        if status == "unknown":
            if (row["dispatch_attempts"] or 0) < 1:
                async with self._write_lock:
                    await db.requeue_zuhal(self.conn, unique_id)
                self.stats["requeued"] += 1
                logger.debug("Zuhal unknown — re-queued %s for second attempt", unique_id)
                return

        terminal = status in ("valid", "catch_all")
        record_state = State.VALIDATED if terminal else State.VALIDATION_FAILED
        score = compute_confidence_score(email, candidate_domain, strategy, status, agent_name, domain_match_score=row["domain_match_score"])

        if trace_entry is None:
            trace_entry = {"stage": "zuhal_fallback", "outcome": status, "bulk": bulk, "email": email}

        async with self._write_lock:
            await db.update_record_dual(
                self.conn,
                unique_id,
                record_state,
                racknerd_status=row["racknerd_status"],
                racknerd_message=row["racknerd_message"],
                racknerd_verified_at=row["racknerd_verified_at"],
                bbops_status=row["bbops_status"],
                bbops_message=row["bbops_message"],
                bbops_verified_at=row["bbops_verified_at"],
                final_verdict=status,
                candidate_email=email,
                confidence_score=float(score),
                zuhal_status_override=status,
                dispatch_attempts_delta=0,
            )
            await db.append_process_trace(self.conn, unique_id, trace_entry)

        if terminal:
            await record_pattern(self.conn, email, first, last, candidate_domain, mx_provider, success=True)
            self.stats["validated"] += 1
            logger.info("Zuhal-validated: %s → %s [zuhal=%s bulk=%s]", unique_id, email, status, bulk)
        else:
            self.stats["validation_failed"] += 1
            logger.debug("Zuhal terminal: %s → %s (%s)", unique_id, email, status)
