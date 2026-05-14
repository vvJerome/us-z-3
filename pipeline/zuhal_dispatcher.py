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


class ZuhalDispatcher:
    """Drains the NEEDS_ZUHAL queue at its own pace, decoupled from the SMTP path.

    SMTP dispatcher hands records off (state=NEEDS_ZUHAL); this worker claims them
    (state=ZUHAL_VALIDATING), runs the Zuhal probe, writes the terminal verdict.
    Concurrency budget is governed by `config.zuhal_concurrency`; the underlying
    ZuhalClient also has its own semaphore as a second guard.
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
        self._sem = asyncio.Semaphore(config.zuhal_concurrency)
        self._write_lock = asyncio.Lock()
        self.stats: dict[str, int] = {
            "validated": 0,
            "validation_failed": 0,
            "requeued": 0,
            "cost_skipped": 0,
        }

    async def run(self) -> None:
        base_interval = self.config.zuhal_poll_interval_s
        poll_interval = base_interval
        consecutive_empty = 0

        recovered = await db.recover_stale_zuhal_validating(self.conn)
        if recovered:
            logger.warning("Recovered %d orphaned ZUHAL_VALIDATING rows → NEEDS_ZUHAL", recovered)

        logger.info(
            "Zuhal dispatcher starting (concurrency=%d, poll=%.1fs)",
            self.config.zuhal_concurrency, base_interval,
        )

        while not self.stop_event.is_set():
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

        logger.info(
            "Zuhal dispatcher finished — validated=%d failed=%d requeued=%d cost_skipped=%d",
            self.stats["validated"],
            self.stats["validation_failed"],
            self.stats["requeued"],
            self.stats["cost_skipped"],
        )

    async def _dispatch_one(self, row: aiosqlite.Row) -> None:
        async with self._sem:
            if self.stop_event.is_set():
                # Row stays in ZUHAL_VALIDATING; recover_stale_zuhal_validating
                # will return it to NEEDS_ZUHAL on next startup.
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

        candidate_domain = row["candidate_domain"] or ""
        strategy = row["strategy"] or "without"
        agent_name = row["agent_name"] or ""
        mx_provider = row["mx_provider"]
        first, _, last = parse_name(agent_name)

        t0 = time.monotonic()
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
            # Belt-and-suspenders: a 429 must never burn a record. The client
            # converts exhausted-retry 429s into ZuhalCircuitOpenError, but if
            # any path leaks one through we still re-queue rather than mark
            # VALIDATION_FAILED.
            if exc.status == 429:
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

        trace_entry = {
            "stage": "zuhal_fallback",
            "outcome": status,
            "ms": elapsed_ms,
            "email": email,
        }

        if status == "accept-all":
            status = "catch_all"

        terminal = status in ("valid", "catch_all")
        record_state = State.VALIDATED if terminal else State.VALIDATION_FAILED
        score = compute_confidence_score(email, candidate_domain, strategy, status, agent_name)

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

        # Zuhal is the only paid backend at this stage; record cost after the call
        # succeeded (even if it returned 'invalid'/'error' — the API was hit).
        self.cost_tracker.record_call("zuhal")

        if terminal:
            await record_pattern(self.conn, email, first, last, candidate_domain, mx_provider, success=True)
            self.stats["validated"] += 1
            logger.info("Zuhal-validated: %s → %s [zuhal=%s]", unique_id, email, status)
        else:
            self.stats["validation_failed"] += 1
            logger.debug("Zuhal terminal: %s → %s (%s)", unique_id, email, status)
