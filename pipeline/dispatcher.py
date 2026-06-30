from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import cast

import aiosqlite

from pipeline.config import PipelineConfig
from pipeline.constants import (
    DISPATCH_POLL_EMPTY_BACKOFF_THRESHOLD,
    DISPATCH_POLL_MAX_INTERVAL_S,
    HEARTBEAT_INTERVAL_S,
    NOTIFY_POLL_TIMEOUT_S,
)
from pipeline.consumers.bbops_async import BbopsAsyncConsumer, BbopsUnhealthy
from pipeline.consumers.racknerd import NullRacknerd, RacknerdConsumer
from pipeline.utils.zuhal_client import ZuhalClient
from pipeline.utils.serper_client import SerperClient
from pipeline.models import BackendVerdict, PipelineHaltError
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.ms_verify import is_microsoft_mx
from pipeline.utils.notify import open_notify_reader
from pipeline.utils.rate_limiter import TokenBucket
from pipeline.utils.text import parse_name
from pipeline import db
from pipeline.db.row_types import RecordRow
from pipeline import dispatch_probes as dp
from pipeline import dispatch_verdicts as dv
from pipeline.db import State
from pipeline._dispatch_helpers import (
    catch_all_confidence_floor,
    compute_confidence_score,
    infra_retry_after,
    inject_harvest_fallback,
    inject_serper_fallback,
    pre_score,
    record_pattern,
    verifier_agreement,
)
from pipeline.reconcile import (  # noqa: F401  (reconcile re-exported for tests)
    DEFINITIVE,
    INCONCLUSIVE,
    greylisting_retry_after,
    reconcile,
    valid_email_format,
)

logger = logging.getLogger("pipeline.dispatcher")


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

class Dispatcher:
    """
    Sequential-backend dispatch coordinator.

    For each DISCOVERED record:
    1. MS probe pre-filter (free, short-circuits Microsoft domains)
    2. Racknerd SMTP probe; if valid/catch_all → done (bbops skipped)
    3. bbops probe (only when Racknerd gives error/invalid/blocked)
    4. OR-of-valids reconciliation; Zuhal rescue for inconclusive results
    5. Write dual-verdict + final_verdict to DB
    6. Pattern learning on success
    """

    def __init__(
        self,
        config: PipelineConfig,
        conn: aiosqlite.Connection,
        racknerd: RacknerdConsumer | NullRacknerd,
        bbops: BbopsAsyncConsumer,
        cost_tracker: CostTracker,
        stop_event: asyncio.Event | None = None,
        zuhal: ZuhalClient | None = None,
        serper: SerperClient | None = None,
        cache_conn: aiosqlite.Connection | None = None,
    ) -> None:
        self.config = config
        self.conn = conn
        self.cache_conn = cache_conn if cache_conn is not None else conn
        self.racknerd = racknerd
        self.bbops = bbops
        self.cost_tracker = cost_tracker
        self.stop_event = stop_event or asyncio.Event()
        self.zuhal = zuhal
        self.serper = serper
        self.harvest_enabled = config.harvest_enabled
        # ponytail: one global bucket throttles ALL harvests combined (no burst); swap for
        # per-host buckets only if a single global RPS becomes the throughput bottleneck.
        self._harvest_rl = TokenBucket(
            capacity=1, refill_rate=config.harvest_rps, initial_tokens=0,
        ) if config.harvest_enabled else None
        self._sem = asyncio.Semaphore(config.dispatch_concurrency)
        self._write_lock = asyncio.Lock()
        self._notify_reader: asyncio.StreamReader | None = None
        # Cached backpressure state — refreshed at most every 5 seconds
        self._bp_cached_count: int = 0
        self._bp_last_checked: float = 0.0
        self.stats: dict[str, int] = {
            "validated": 0,
            "validation_failed": 0,
            "disagreements": 0,
            "requeued": 0,
            "handed_off_to_zuhal": 0,
        }
        # MS probe health tracking: warn when error rate exceeds threshold
        self._ms_total: int = 0
        self._ms_errors: int = 0

    async def run(self) -> None:
        base_interval = self.config.dispatch_poll_interval_s
        poll_interval = base_interval
        consecutive_empty = 0

        recovered = await db.recover_stale_validating(self.conn)
        if recovered:
            logger.warning("Recovered %d orphaned VALIDATING rows → DISCOVERED", recovered)

        if self.config.notify_pipe:
            from pathlib import Path
            try:
                self._notify_reader = await open_notify_reader(Path(self.config.notify_pipe))
                logger.info("Dispatcher: notify pipe opened")
            except OSError as exc:
                logger.warning("Dispatcher: notify pipe unavailable (%s) — polling only", exc)

        _hb = asyncio.create_task(self._heartbeat_loop(), name="dispatcher-heartbeat")
        logger.info("Dispatcher starting (concurrency=%d)", self.config.dispatch_concurrency)

        while not self.stop_event.is_set():
            rows = await db.fetch_pending_validation(
                self.conn, limit=self.config.dispatch_chunk_size
            )

            if not rows:
                consecutive_empty += 1
                if consecutive_empty >= DISPATCH_POLL_EMPTY_BACKOFF_THRESHOLD:
                    poll_interval = min(poll_interval * 2, DISPATCH_POLL_MAX_INTERVAL_S)

                producer_done = await db.get_checkpoint(self.conn, "producer_done")
                if producer_done == "true":
                    if not await db.has_pending_validation(self.conn):
                        if consecutive_empty >= DISPATCH_POLL_EMPTY_BACKOFF_THRESHOLD:
                            logger.info("Dispatcher: queue drained and producer done — exiting")
                            break
                    else:
                        consecutive_empty = 0
                        poll_interval = base_interval
                        continue

                if self._notify_reader:
                    try:
                        await asyncio.wait_for(self._notify_reader.read(1), timeout=NOTIFY_POLL_TIMEOUT_S)
                    except (asyncio.TimeoutError, asyncio.IncompleteReadError):
                        pass
                else:
                    await asyncio.sleep(poll_interval)
                continue

            consecutive_empty = 0
            poll_interval = base_interval

            tasks = [self._dispatch_record(row) for row in rows]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, PipelineHaltError):
                    raise res
                if isinstance(res, BaseException):
                    logger.error("Unexpected dispatcher error", exc_info=res)

        _hb.cancel()
        logger.info(
            "Dispatcher finished — validated=%d failed=%d requeued=%d disagreements=%d handed_off_to_zuhal=%d",
            self.stats["validated"],
            self.stats["validation_failed"],
            self.stats["requeued"],
            self.stats["disagreements"],
            self.stats["handed_off_to_zuhal"],
        )

    async def _dispatch_record(self, row: aiosqlite.Row) -> None:
        async with self._sem:
            if self.stop_event.is_set():
                # Don't write partial verdicts on shutdown — records stay VALIDATING
                # and will be recovered as DISCOVERED on next start
                return
            await self._process_record(cast(RecordRow, row))

    async def _fail(self, unique_id: str, reason: str, *args: object, failure_reason: str | None = None) -> None:
        """Terminal VALIDATION_FAILED write + stats bump. `reason` is a %-format string."""
        logger.warning("Record %s: " + reason + " — marking VALIDATION_FAILED", unique_id, *args)
        async with self._write_lock:
            kw: dict[str, object] = {} if failure_reason is None else {"failure_reason": failure_reason}
            await db.update_record_status(self.conn, unique_id, State.VALIDATION_FAILED, **kw)
        self.stats["validation_failed"] += 1

    async def _load_candidates(self, row: RecordRow) -> list[str] | None:
        """Check dispatch budget and parse candidate_emails; mark VALIDATION_FAILED and return None if it can't proceed."""
        unique_id = row["unique_id"]
        # dispatch_attempts counts only re-queues where a real verdict was obtained.
        # requeue_count counts every re-queue including infra transients — the safety valve
        # that terminates records stuck in a permanent infra failure loop.
        attempts = row["dispatch_attempts"] or 0
        requeues = row["requeue_count"] or 0
        if attempts >= self.config.max_dispatch_attempts:
            await self._fail(unique_id, "hit max dispatch attempts (%d)", attempts)
            return None
        if requeues >= self.config.max_requeue_count:
            fr = "infra_loop" if (row["dispatch_attempts"] or 0) == 0 else "max_attempts"
            await self._fail(unique_id, "hit max requeue count (%d)", requeues, failure_reason=fr)
            return None
        raw_candidates = row["candidate_emails"]
        if not raw_candidates:
            await self._fail(unique_id, "no candidate_emails")
            return None
        try:
            parsed: list[str] = json.loads(raw_candidates)
            return parsed
        except (json.JSONDecodeError, TypeError):
            await self._fail(unique_id, "invalid candidate_emails JSON")
            return None

    async def _write_validated(
        self,
        unique_id: str,
        email: str,
        rk: BackendVerdict,
        bb: BackendVerdict,
        final_verdict: str,
        score: float,
        attempts_delta: int,
        pending_trace: list[dict],
        first: str,
        last: str,
        candidate_domain: str,
        mx_provider: str | None,
        log_msg: str,
        *log_args: object,
        verifier_agreement: str | None = None,
    ) -> None:
        """Single VALIDATED write path: dual-verdict + trace flush + pattern learning + stats + log."""
        async with self._write_lock:
            await db.update_record_dual(
                self.conn,
                unique_id,
                State.VALIDATED,
                racknerd_status=rk.status,
                racknerd_message=rk.message,
                racknerd_verified_at=rk.verified_at,
                bbops_status=bb.status,
                bbops_message=bb.message,
                bbops_verified_at=bb.verified_at,
                final_verdict=final_verdict,
                candidate_email=email,
                confidence_score=float(score),
                dispatch_attempts_delta=attempts_delta,
                verifier_agreement=verifier_agreement,
            )
            await db.flush_process_trace(self.conn, unique_id, pending_trace)
        await record_pattern(self.conn, email, first, last, candidate_domain, mx_provider, success=True)
        self.stats["validated"] += 1
        logger.info(log_msg, *log_args)

    async def _process_record(self, row: RecordRow) -> None:
        unique_id = row["unique_id"]
        candidates = await self._load_candidates(row)
        if candidates is None:
            return

        mx_provider = row["mx_provider"]
        candidate_domain = row["candidate_domain"] or ""
        strategy = row["strategy"] or "without"
        agent_name = row["agent_name"] or ""
        domain_confidence = row["domain_confidence"]
        tunnel_requeue_count = row["tunnel_requeue_count"] or 0
        bbops_requeue_count = row["bbops_requeue_count"] or 0
        _first, _, _last = parse_name(agent_name)
        use_ms_probe = is_microsoft_mx(mx_provider)
        serper_enriched = bool(row["serper_enriched"])
        # Catch-all acceptance bar, raised for providers where catch-all is the default.
        catch_all_floor = catch_all_confidence_floor(self.config.catch_all_min_confidence, mx_provider)

        # Identity-first: try the strongest candidates before paid verification.
        candidates.sort(
            key=lambda e: pre_score(e, candidate_domain, strategy, agent_name, domain_confidence),
            reverse=True,
        )

        pending_trace: list[dict] = []
        cost_skipped = False
        original_count = len(candidates)
        # fb_boundary moves out as harvest injects candidates, so they're tried before paid Serper.
        fb_boundary = original_count
        harvested = False
        i = 0
        last_rk: BackendVerdict | None = None
        last_bb: BackendVerdict | None = None
        any_real_test = False  # True when any candidate got a definitive backend verdict

        while i < len(candidates):
            email = candidates[i]
            i += 1
            if not valid_email_format(email):
                logger.debug("Skipping malformed candidate %s for %s", email, unique_id)
                pending_trace.append({"stage": "format_skip", "outcome": "invalid", "email": email})
                continue
            if self.cost_tracker.ceiling_reached():
                logger.info("Cost ceiling reached — skipping %s", unique_id)
                await db.update_record_status(self.conn, unique_id, State.COST_SKIPPED)
                cost_skipped = True
                break

            candidate_score = pre_score(
                email, candidate_domain, strategy, agent_name, domain_confidence
            )
            # Low-confidence candidates don't earn paid Zuhal rescue (flag default 0.0 = off).
            skip_paid = candidate_score < self.config.zuhal_min_confidence

            # MS probe pre-filter (free, only for Microsoft-managed domains)
            if use_ms_probe:
                ms_status, ms_trace = await dp.ms_probe(email)
                pending_trace.append(ms_trace)

                if ms_status == "valid":
                    score = compute_confidence_score(email, candidate_domain, strategy, "valid", agent_name)
                    await self._write_validated(
                        unique_id, email,
                        BackendVerdict("ms_valid", "MS GetCredentialType probe", ""),
                        BackendVerdict("not_run", "skipped — MS probe hit", ""),
                        "valid", score,
                        0,  # MS probe is free, don't count against dispatch_attempts
                        pending_trace, _first, _last, candidate_domain, mx_provider,
                        "MS-validated (no SMTP): %s → %s", unique_id, email,
                        verifier_agreement="ms_only",
                    )
                    return

                if ms_status == "invalid":
                    pending_trace.append({"stage": "ms_skip", "outcome": "invalid", "email": email})
                    await record_pattern(self.conn, email, _first, _last, candidate_domain, mx_provider, success=False)
                    continue  # try next candidate

                # unknown/error → fall through to SMTP backends

            # Sequential SMTP probe: Racknerd first, bbops only when Racknerd can't decide
            timeout = self.config.dispatch_backend_timeout_s

            t_rk = time.monotonic()
            try:
                rk_verdict = await asyncio.wait_for(
                    dp.safe_racknerd(self.racknerd, email), timeout=timeout
                )
            except asyncio.TimeoutError:
                rk_verdict = BackendVerdict(status="error", message="racknerd timeout", verified_at=None)
            elapsed_rk = int((time.monotonic() - t_rk) * 1000)
            pending_trace.append({
                "stage": "racknerd", "outcome": rk_verdict.status, "ms": elapsed_rk, "email": email,
            })
            last_rk = rk_verdict

            # Racknerd valid: accept. catch_all: accept only if identity confidence
            # clears the gate (flag default 0.0 = accept all, current behavior);
            # below the gate, fall through to bbops for a second signal.
            if rk_verdict.status == "valid" or (
                rk_verdict.status == "catch_all"
                and candidate_score >= catch_all_floor
            ):
                score = compute_confidence_score(
                    email, candidate_domain, strategy, rk_verdict.status, agent_name
                )
                await self._write_validated(
                    unique_id, email,
                    rk_verdict,
                    BackendVerdict("not_run", "skipped — Racknerd hit", ""),
                    rk_verdict.status, score,
                    1,
                    pending_trace, _first, _last, candidate_domain, mx_provider,
                    "Racknerd-validated (bbops skipped): %s → %s", unique_id, email,
                    verifier_agreement="racknerd_only",
                )
                return

            # Tunnel down: re-queue once; on second failure skip Racknerd and run bbops-only
            if rk_verdict.status == "error" and "tunnel not up" in rk_verdict.message:
                if tunnel_requeue_count < self.config.max_tunnel_requeues:
                    retry_after = infra_retry_after(tunnel_requeue_count)
                    async with self._write_lock:
                        await db.requeue_record(
                            self.conn, unique_id, increment_attempts=False,
                            retry_after=retry_after, infra_type="tunnel",
                        )
                    self.stats["requeued"] += 1
                    logger.debug("Re-queued %s (SSH tunnel not up, retry after %s)", unique_id, retry_after)
                    return
                # Tunnel limit reached — treat as not_run and proceed bbops-only
                rk_verdict = BackendVerdict(status="not_run", message="tunnel limit reached", verified_at=None)
                logger.debug("Tunnel requeue limit hit for %s — proceeding bbops-only", unique_id)

            # Racknerd gave blocked/error/invalid — run bbops
            t_bb = time.monotonic()
            try:
                bb_verdict = await asyncio.wait_for(
                    dp.safe_bbops(self.bbops, row["id"], email), timeout=timeout
                )
            except asyncio.TimeoutError:
                bb_verdict = BackendVerdict(status="error", message="bbops timeout", verified_at=None)
            except BbopsUnhealthy:
                bb_verdict = BackendVerdict(status="not_run", message="bbops unhealthy", verified_at=None)
            elapsed_bb = int((time.monotonic() - t_bb) * 1000)
            pending_trace.append({
                "stage": "bbops", "outcome": bb_verdict.status, "ms": elapsed_bb, "email": email,
            })
            last_bb = bb_verdict

            # bbops error: apply per-infra requeue budget (only when no Zuhal fallback;
            # with Zuhal configured, handle_inconclusive owns the error/unknown path)
            if bb_verdict.status == "error" and self.zuhal is None:
                if bbops_requeue_count < self.config.max_bbops_requeues:
                    retry_after = infra_retry_after(bbops_requeue_count)
                    async with self._write_lock:
                        await db.requeue_record(
                            self.conn, unique_id, increment_attempts=False,
                            retry_after=retry_after, infra_type="bbops",
                        )
                    self.stats["requeued"] += 1
                    logger.debug("Re-queued %s (bbops error, retry after %s)", unique_id, retry_after)
                    return
                # bbops budget exhausted — count rk verdict if definitive, skip to next candidate
                if rk_verdict.status in DEFINITIVE:
                    any_real_test = True
                logger.debug("bbops requeue limit hit for %s — skipping candidate %s", unique_id, email)
                continue

            result = reconcile(rk_verdict, bb_verdict)

            if rk_verdict.status in DEFINITIVE or bb_verdict.status in DEFINITIVE:
                any_real_test = True

            # Disagreement detection (only when both gave definitive verdicts)
            if (
                rk_verdict.status in DEFINITIVE
                and bb_verdict.status in DEFINITIVE
                and rk_verdict.status != bb_verdict.status
            ):
                self.stats["disagreements"] += 1
                logger.info(
                    "Backend disagreement for %s/%s: racknerd=%s bbops=%s",
                    unique_id, email, rk_verdict.status, bb_verdict.status,
                )

            if not result.should_write:
                action = await dv.handle_inconclusive(
                    self, unique_id, email, rk_verdict, bb_verdict,
                    candidate_domain, strategy, agent_name, _first, _last,
                    mx_provider, skip_paid, pending_trace,
                )
                if action == "cost_skipped":
                    cost_skipped = True
                    break
                return

            if result.final_verdict == "valid" or (
                result.final_verdict == "catch_all"
                and candidate_score >= catch_all_floor
            ):
                score = compute_confidence_score(
                    email, candidate_domain, strategy, result.final_verdict, agent_name
                )
                await self._write_validated(
                    unique_id, email, rk_verdict, bb_verdict,
                    result.final_verdict, score,
                    1,
                    pending_trace, _first, _last, candidate_domain, mx_provider,
                    "Validated %s → %s [rk=%s bb=%s]",
                    unique_id, email, rk_verdict.status, bb_verdict.status,
                    verifier_agreement=verifier_agreement(rk_verdict.status, bb_verdict.status),
                )
                return

            # Both invalid — optional Zuhal rescue when zuhal_on_both_invalid is enabled
            if self.zuhal is not None and self.config.zuhal_on_both_invalid and not skip_paid:
                rescue_action: str | None = await dv.rescue_both_invalid(
                    self, unique_id, email, rk_verdict, bb_verdict,
                    candidate_domain, strategy, agent_name, _first, _last,
                    mx_provider, pending_trace,
                )
                if rescue_action == "cost_skipped":
                    cost_skipped = True
                    break
                if rescue_action == "terminal":
                    return
                # None → Zuhal also invalid/error — fall through to try next candidate

            # invalid — record pattern miss and try next candidate
            await record_pattern(self.conn, email, _first, _last, candidate_domain, mx_provider, success=False)
            logger.debug(
                "Candidate %s for %s: %s — trying next",
                email, unique_id, result.final_verdict,
            )

            # Patterns exhausted: free harvest first, then paid Serper if harvest is empty.
            if i == fb_boundary and candidate_domain:
                if self.harvest_enabled and not harvested:
                    harvested = True
                    added = await inject_harvest_fallback(
                        unique_id, candidates, _first, _last, strategy, mx_provider, candidate_domain,
                        self._harvest_rl, self.config.harvest_timeout_s,
                    )
                    if added:
                        fb_boundary += added
                        continue  # try harvested candidates before paying for Serper
                if not serper_enriched and self.serper:
                    serper_enriched = True  # prevent re-injection on subsequent loops
                    if await inject_serper_fallback(
                        unique_id, row, candidates, self.serper, self.cache_conn, self.conn, self.cost_tracker,
                    ):
                        cost_skipped = True
                        break

        if cost_skipped:
            return

        # All candidates exhausted — write last SMTP verdicts so we know what ran
        if last_rk and last_rk.status == "blocked":
            failure_reason = "provider_blocked"
        elif not any_real_test:
            failure_reason = "infra_loop"
        else:
            failure_reason = "max_attempts"
        async with self._write_lock:
            await db.update_record_dual(
                self.conn,
                unique_id,
                State.VALIDATION_FAILED,
                racknerd_status=last_rk.status if last_rk else None,
                racknerd_message=last_rk.message if last_rk else None,
                racknerd_verified_at=last_rk.verified_at if last_rk else None,
                bbops_status=last_bb.status if last_bb else None,
                bbops_message=last_bb.message if last_bb else None,
                bbops_verified_at=last_bb.verified_at if last_bb else None,
                final_verdict="invalid",
                failure_reason=failure_reason,
            )
            await db.flush_process_trace(self.conn, unique_id, pending_trace)
        self.stats["validation_failed"] += 1
        logger.debug("All candidates failed for %s (failure_reason=%s)", unique_id, failure_reason)

    async def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await db.upsert_dispatcher_heartbeat(self.conn)
            except aiosqlite.Error as exc:
                logger.debug("Dispatcher heartbeat failed: %s", exc)
            try:
                await asyncio.wait_for(asyncio.shield(self.stop_event.wait()), timeout=HEARTBEAT_INTERVAL_S)
            except asyncio.TimeoutError:
                pass
