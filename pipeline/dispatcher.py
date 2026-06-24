from __future__ import annotations

import asyncio
import datetime
import json
import logging
import time

import aiosqlite

from pipeline.config import PipelineConfig
from pipeline.constants import (
    DISPATCH_POLL_EMPTY_BACKOFF_THRESHOLD,
    DISPATCH_POLL_MAX_INTERVAL_S,
    INFRA_RETRY_BASE_MINUTES,
    INFRA_RETRY_MULTIPLIER,
)
from pipeline.consumers.bbops_async import BbopsAsyncConsumer, BbopsUnhealthy
from pipeline.consumers.racknerd import RacknerdConsumer
from pipeline.utils.zuhal_client import ZuhalClient, ZuhalCircuitOpenError
from pipeline.utils.serper_client import SerperClient
from pipeline.models import BackendVerdict, PipelineHaltError, ReconcileResult
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.ms_verify import check_microsoft_email_async, is_microsoft_mx
from pipeline.utils.notify import open_notify_reader
from pipeline.utils.text import parse_name
from pipeline import db
from pipeline.db import State
from pipeline._dispatch_helpers import (
    compute_confidence_score,
    record_pattern,
)

logger = logging.getLogger("pipeline.dispatcher")


def _valid_email_format(email: str) -> bool:
    """Return False for emails whose local part violates RFC 5321 basics (e.g. ...@domain)."""
    parts = email.split("@")
    if len(parts) != 2:
        return False
    local = parts[0]
    return bool(local) and not local.startswith(".") and not local.endswith(".") and ".." not in local


# Statuses that indicate the backend actually ran and returned a definitive answer
_DEFINITIVE: frozenset[str] = frozenset({"valid", "invalid", "catch_all"})
# Statuses that mean "couldn't reach server" — should not count as invalid
_INCONCLUSIVE: frozenset[str] = frozenset({"error", "blocked", "not_run"})


# ---------------------------------------------------------------------------
# Reconciliation (OR-of-valids)
# ---------------------------------------------------------------------------

def reconcile(
    racknerd: BackendVerdict | None,
    bbops: BackendVerdict | None,
) -> ReconcileResult:
    """
    OR-of-valids policy:
    - Either backend valid/catch_all → accept
    - Both definitively invalid (no errors) → reject
    - Mixed error/inconclusive → unknown, re-queue without burning attempt
    - Tunnel down special-case → re-queue without burning attempt
    """
    rk = racknerd.status if racknerd else "not_run"
    bb = bbops.status if bbops else "not_run"

    # Tunnel-down special case: don't burn attempt
    if rk == "error" and (racknerd and "tunnel not up" in racknerd.message):
        return ReconcileResult(final_verdict="unknown", should_write=False, is_terminal=False)

    # OR-of-valids
    if rk == "valid" or bb == "valid":
        return ReconcileResult(final_verdict="valid", should_write=True, is_terminal=True)

    if rk == "catch_all" or bb == "catch_all":
        return ReconcileResult(final_verdict="catch_all", should_write=True, is_terminal=True)

    # Both definitively invalid (no errors mixed in)
    if rk == "invalid" and bb == "invalid":
        return ReconcileResult(final_verdict="invalid", should_write=True, is_terminal=False)

    if rk == "invalid" and bb in _INCONCLUSIVE:
        # not_run = backend intentionally disabled; treat as definitive invalid
        if bb == "not_run":
            return ReconcileResult(final_verdict="invalid", should_write=True, is_terminal=False)
        # One said invalid, one errored — can't trust the invalid verdict alone
        return ReconcileResult(final_verdict="unknown", should_write=False, is_terminal=False)

    if rk in _INCONCLUSIVE and bb == "invalid":
        # not_run = backend intentionally disabled; treat as definitive invalid
        if rk == "not_run":
            return ReconcileResult(final_verdict="invalid", should_write=True, is_terminal=False)
        return ReconcileResult(final_verdict="unknown", should_write=False, is_terminal=False)

    # Both inconclusive
    return ReconcileResult(final_verdict="unknown", should_write=False, is_terminal=False)


def _greylisting_retry_after(minutes: int = 30) -> str:
    """Return an ISO timestamp N minutes from now for a greylisting hold."""
    dt = datetime.datetime.utcnow() + datetime.timedelta(minutes=minutes)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _infra_retry_after(requeue_count: int) -> str:
    """Exponential backoff for infra re-queues: 5min → 15min → 45min."""
    minutes = INFRA_RETRY_BASE_MINUTES * (INFRA_RETRY_MULTIPLIER ** requeue_count)
    dt = datetime.datetime.utcnow() + datetime.timedelta(minutes=minutes)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _verifier_agreement(rk: str, bb: str) -> str:
    rk_ok = rk in ("valid", "catch_all")
    bb_ok = bb in ("valid", "catch_all")
    if rk_ok and bb_ok:
        return "both"
    if rk_ok:
        return "racknerd_only"
    if bb_ok:
        return "bbops_only"
    return "unknown"


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
        racknerd: RacknerdConsumer,
        bbops: BbopsAsyncConsumer,
        cost_tracker: CostTracker,
        stop_event: asyncio.Event | None = None,
        zuhal: ZuhalClient | None = None,
        serper: SerperClient | None = None,
    ) -> None:
        self.config = config
        self.conn = conn
        self.racknerd = racknerd
        self.bbops = bbops
        self.cost_tracker = cost_tracker
        self.stop_event = stop_event or asyncio.Event()
        self.zuhal = zuhal
        self.serper = serper
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
                        await asyncio.wait_for(self._notify_reader.read(1), timeout=30.0)
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
            await self._process_record(row)

    async def _process_record(self, row: aiosqlite.Row) -> None:
        unique_id = row["unique_id"]
        raw_candidates = row["candidate_emails"]

        # dispatch_attempts counts only re-queues where a real verdict was obtained.
        # requeue_count counts every re-queue including infra transients — it is the
        # safety valve that terminates records stuck in permanent infra failure loops.
        dispatch_attempts = row["dispatch_attempts"] or 0
        requeue_count = row["requeue_count"] or 0
        tunnel_requeue_count = row["tunnel_requeue_count"] or 0
        bbops_requeue_count = row["bbops_requeue_count"] or 0
        if dispatch_attempts >= self.config.max_dispatch_attempts:
            logger.warning(
                "Record %s hit max dispatch attempts (%d) — marking VALIDATION_FAILED",
                unique_id, dispatch_attempts,
            )
            async with self._write_lock:
                await db.update_record_status(
                    self.conn, unique_id, State.VALIDATION_FAILED, failure_reason="max_attempts"
                )
            self.stats["validation_failed"] += 1
            return
        if requeue_count >= self.config.max_requeue_count:
            logger.warning(
                "Record %s hit max requeue count (%d) — marking VALIDATION_FAILED",
                unique_id, requeue_count,
            )
            async with self._write_lock:
                await db.update_record_status(
                    self.conn, unique_id, State.VALIDATION_FAILED, failure_reason="infra_loop"
                )
            self.stats["validation_failed"] += 1
            return

        if not raw_candidates:
            logger.warning("No candidate_emails for %s — marking failed", unique_id)
            await db.update_record_status(
                self.conn, unique_id, State.VALIDATION_FAILED, failure_reason="no_candidates"
            )
            self.stats["validation_failed"] += 1
            return

        try:
            candidates: list[str] = json.loads(raw_candidates)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Invalid candidate_emails JSON for %s", unique_id)
            await db.update_record_status(
                self.conn, unique_id, State.VALIDATION_FAILED, failure_reason="no_candidates"
            )
            self.stats["validation_failed"] += 1
            return

        mx_provider = row["mx_provider"]
        candidate_domain = row["candidate_domain"] or ""
        strategy = row["strategy"] or "without"
        agent_name = row["agent_name"] or ""
        _first, _, _last = parse_name(agent_name)
        use_ms_probe = is_microsoft_mx(mx_provider)
        serper_enriched = bool(row["serper_enriched"])
        dms: float | None = row["domain_match_score"]

        pending_trace: list[dict] = []
        cost_skipped = False
        original_count = len(candidates)
        i = 0
        last_rk: BackendVerdict | None = None
        last_bb: BackendVerdict | None = None
        any_real_test = False  # True when any candidate got a definitive backend verdict

        while i < len(candidates):
            email = candidates[i]
            i += 1
            if not _valid_email_format(email):
                logger.debug("Skipping malformed candidate %s for %s", email, unique_id)
                pending_trace.append({"stage": "format_skip", "outcome": "invalid", "email": email})
                continue
            if self.cost_tracker.ceiling_reached():
                logger.info("Cost ceiling reached — skipping %s", unique_id)
                await db.update_record_status(self.conn, unique_id, State.COST_SKIPPED)
                cost_skipped = True
                break

            # MS probe pre-filter (free, only for Microsoft-managed domains)
            if use_ms_probe:
                ms_status, ms_trace = await self._ms_probe(email)
                pending_trace.append(ms_trace)

                if ms_status == "valid":
                    score = compute_confidence_score(email, candidate_domain, strategy, "valid", agent_name, domain_match_score=dms)
                    async with self._write_lock:
                        await db.update_record_dual(
                            self.conn,
                            unique_id,
                            State.VALIDATED,
                            racknerd_status="ms_valid",
                            racknerd_message="MS GetCredentialType probe",
                            racknerd_verified_at=None,
                            bbops_status="not_run",
                            bbops_message="skipped — MS probe hit",
                            bbops_verified_at=None,
                            final_verdict="valid",
                            candidate_email=email,
                            confidence_score=float(score),
                            dispatch_attempts_delta=0,  # MS probe is free, don't count
                            verifier_agreement="ms_only",
                        )
                        await db.flush_process_trace(self.conn, unique_id, pending_trace)
                    await record_pattern(self.conn, email, _first, _last, candidate_domain, mx_provider, success=True)
                    self.stats["validated"] += 1
                    logger.info("MS-validated (no SMTP): %s → %s", unique_id, email)
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
                    self._safe_racknerd(email), timeout=timeout
                )
            except asyncio.TimeoutError:
                rk_verdict = BackendVerdict(status="error", message="racknerd timeout", verified_at="")
            elapsed_rk = int((time.monotonic() - t_rk) * 1000)
            pending_trace.append({
                "stage": "racknerd", "outcome": rk_verdict.status, "ms": elapsed_rk, "email": email,
            })
            last_rk = rk_verdict

            # Racknerd valid/catch_all: bbops not needed
            if rk_verdict.status in ("valid", "catch_all"):
                score = compute_confidence_score(
                    email, candidate_domain, strategy, rk_verdict.status, agent_name, domain_match_score=dms
                )
                async with self._write_lock:
                    await db.update_record_dual(
                        self.conn,
                        unique_id,
                        State.VALIDATED,
                        racknerd_status=rk_verdict.status,
                        racknerd_message=rk_verdict.message,
                        racknerd_verified_at=rk_verdict.verified_at,
                        bbops_status="not_run",
                        bbops_message="skipped — Racknerd hit",
                        bbops_verified_at=None,
                        final_verdict=rk_verdict.status,
                        candidate_email=email,
                        confidence_score=float(score),
                        dispatch_attempts_delta=1,
                        verifier_agreement="racknerd_only",
                    )
                    await db.flush_process_trace(self.conn, unique_id, pending_trace)
                await record_pattern(self.conn, email, _first, _last, candidate_domain, mx_provider, success=True)
                self.stats["validated"] += 1
                logger.info("Racknerd-validated (bbops skipped): %s → %s", unique_id, email)
                return

            # Tunnel down: re-queue once; on second failure skip Racknerd and run bbops-only
            if rk_verdict.status == "error" and "tunnel not up" in rk_verdict.message:
                if tunnel_requeue_count < self.config.max_tunnel_requeues:
                    retry_after = _infra_retry_after(tunnel_requeue_count)
                    async with self._write_lock:
                        await db.requeue_record(
                            self.conn, unique_id, increment_attempts=False,
                            retry_after=retry_after, infra_type="tunnel",
                        )
                    self.stats["requeued"] += 1
                    logger.debug("Re-queued %s (SSH tunnel not up, retry after %s)", unique_id, retry_after)
                    return
                # Tunnel limit reached — treat as not_run and proceed bbops-only
                rk_verdict = BackendVerdict(status="not_run", message="tunnel limit reached", verified_at="")
                logger.debug("Tunnel requeue limit hit for %s — proceeding bbops-only", unique_id)

            # Racknerd gave blocked/error/invalid — run bbops
            t_bb = time.monotonic()
            try:
                bb_verdict = await asyncio.wait_for(
                    self._safe_bbops(row["id"], email), timeout=timeout
                )
            except asyncio.TimeoutError:
                bb_verdict = BackendVerdict(status="error", message="bbops timeout", verified_at="")
            except BbopsUnhealthy:
                bb_verdict = BackendVerdict(status="not_run", message="bbops unhealthy", verified_at="")
            elapsed_bb = int((time.monotonic() - t_bb) * 1000)
            pending_trace.append({
                "stage": "bbops", "outcome": bb_verdict.status, "ms": elapsed_bb, "email": email,
            })
            last_bb = bb_verdict

            result = reconcile(rk_verdict, bb_verdict)

            if rk_verdict.status in _DEFINITIVE or bb_verdict.status in _DEFINITIVE:
                any_real_test = True

            # Disagreement detection (only when both gave definitive verdicts)
            if (
                rk_verdict.status in _DEFINITIVE
                and bb_verdict.status in _DEFINITIVE
                and rk_verdict.status != bb_verdict.status
            ):
                self.stats["disagreements"] += 1
                logger.info(
                    "Backend disagreement for %s/%s: racknerd=%s bbops=%s",
                    unique_id, email, rk_verdict.status, bb_verdict.status,
                )

            if not result.should_write:
                # Count attempt only when at least one backend gave a definitive verdict.
                # Both-error or error+not_run are pure infra and do not consume the budget.
                any_real_verdict = (
                    rk_verdict.status in _DEFINITIVE or bb_verdict.status in _DEFINITIVE
                )

                # Greylisting: Racknerd got a 4xx temporary SMTP deferral — hold for 30 min.
                rk_is_4xx = (
                    rk_verdict.status == "error"
                    and "(4xx temporary)" in (rk_verdict.message or "")
                )
                greylist_hold = _greylisting_retry_after() if rk_is_4xx else None

                if self.zuhal is not None:
                    if self.config.zuhal_decoupled:
                        # Backpressure: pause handoffs when Zuhal backlog is too deep.
                        # Count is cached for 5 seconds to avoid per-record DB queries.
                        if self.config.zuhal_backpressure_threshold > 0:
                            now = time.monotonic()
                            if now - self._bp_last_checked >= 5.0:
                                self._bp_cached_count = await db.count_needs_zuhal(self.conn)
                                self._bp_last_checked = now
                            if self._bp_cached_count >= self.config.zuhal_backpressure_threshold:
                                logger.debug(
                                    "Zuhal backpressure: backlog=%d >= threshold=%d — pausing %.1fs",
                                    self._bp_cached_count,
                                    self.config.zuhal_backpressure_threshold,
                                    self.config.zuhal_backpressure_sleep_s,
                                )
                                await asyncio.sleep(self.config.zuhal_backpressure_sleep_s)
                        async with self._write_lock:
                            await db.handoff_to_zuhal(
                                self.conn,
                                unique_id,
                                racknerd_status=rk_verdict.status if rk_verdict else "not_run",
                                racknerd_message=rk_verdict.message if rk_verdict else "",
                                racknerd_verified_at=rk_verdict.verified_at if rk_verdict else None,
                                bbops_status=bb_verdict.status if bb_verdict else "not_run",
                                bbops_message=bb_verdict.message if bb_verdict else "",
                                bbops_verified_at=bb_verdict.verified_at if bb_verdict else None,
                                candidate_email=email,
                            )
                            await db.flush_process_trace(self.conn, unique_id, pending_trace)
                        self.stats["handed_off_to_zuhal"] += 1
                        logger.debug("Handed off to Zuhal queue: %s → %s", unique_id, email)
                        return

                    if self.cost_tracker.ceiling_reached():
                        logger.info("Cost ceiling reached before Zuhal — skipping %s", unique_id)
                        await db.update_record_status(self.conn, unique_id, State.COST_SKIPPED)
                        cost_skipped = True
                        break
                    zuhal_status, zuhal_trace = await self._zuhal_probe(email)
                    pending_trace.append(zuhal_trace)

                    if zuhal_status == "circuit_open":
                        # No re-queue — skip Zuhal for this candidate and try next
                        logger.warning("Zuhal circuit open for %s — skipping, trying next candidate", unique_id)
                        continue

                    if zuhal_status == "accept-all":
                        zuhal_status = "catch_all"
                    terminal = zuhal_status in ("valid", "catch_all")
                    state = State.VALIDATED if terminal else State.VALIDATION_FAILED
                    score = compute_confidence_score(
                        email, candidate_domain, strategy, zuhal_status, agent_name, domain_match_score=dms
                    )
                    async with self._write_lock:
                        await db.update_record_dual(
                            self.conn,
                            unique_id,
                            state,
                            racknerd_status=rk_verdict.status,
                            racknerd_message=rk_verdict.message,
                            racknerd_verified_at=rk_verdict.verified_at,
                            bbops_status=bb_verdict.status,
                            bbops_message=bb_verdict.message,
                            bbops_verified_at=bb_verdict.verified_at,
                            final_verdict=zuhal_status,
                            candidate_email=email,
                            confidence_score=float(score),
                            zuhal_status_override=zuhal_status,
                            dispatch_attempts_delta=1,
                            verifier_agreement="zuhal_only" if terminal else None,
                        )
                        await db.flush_process_trace(self.conn, unique_id, pending_trace)
                    self.cost_tracker.record_call("zuhal")
                    if terminal:
                        await record_pattern(self.conn, email, _first, _last, candidate_domain, mx_provider, success=True)
                        self.stats["validated"] += 1
                        logger.info(
                            "Zuhal-validated: %s → %s [zuhal=%s]",
                            unique_id, email, zuhal_status,
                        )
                    else:
                        self.stats["validation_failed"] += 1
                        logger.debug(
                            "Zuhal fallback terminal: %s → %s (%s)",
                            unique_id, email, zuhal_status,
                        )
                    return
                else:
                    if bbops_requeue_count < self.config.max_bbops_requeues:
                        retry_after = greylist_hold or _infra_retry_after(bbops_requeue_count)
                        async with self._write_lock:
                            await db.requeue_record(
                                self.conn, unique_id,
                                increment_attempts=any_real_verdict,
                                retry_after=retry_after,
                                infra_type="bbops",
                            )
                        self.stats["requeued"] += 1
                        if rk_is_4xx:
                            logger.debug("Re-queued %s (greylisted — 4xx hold until %s)", unique_id, greylist_hold)
                        else:
                            logger.debug("Re-queued %s (bbops inconclusive, retry after %s)", unique_id, retry_after)
                        return
                    # bbops limit reached — skip this candidate, try next
                    logger.debug("bbops requeue limit hit for %s — skipping candidate %s", unique_id, email)

            if result.final_verdict in ("valid", "catch_all"):
                score = compute_confidence_score(
                    email, candidate_domain, strategy, result.final_verdict, agent_name, domain_match_score=dms
                )
                async with self._write_lock:
                    await db.update_record_dual(
                        self.conn,
                        unique_id,
                        State.VALIDATED,
                        racknerd_status=rk_verdict.status,
                        racknerd_message=rk_verdict.message,
                        racknerd_verified_at=rk_verdict.verified_at,
                        bbops_status=bb_verdict.status,
                        bbops_message=bb_verdict.message,
                        bbops_verified_at=bb_verdict.verified_at,
                        final_verdict=result.final_verdict,
                        candidate_email=email,
                        confidence_score=float(score),
                        dispatch_attempts_delta=1,
                        verifier_agreement=_verifier_agreement(rk_verdict.status, bb_verdict.status),
                    )
                    await db.flush_process_trace(self.conn, unique_id, pending_trace)
                await record_pattern(self.conn, email, _first, _last, candidate_domain, mx_provider, success=True)
                self.stats["validated"] += 1
                logger.info(
                    "Validated %s → %s [rk=%s bb=%s]",
                    unique_id, email, rk_verdict.status, bb_verdict.status,
                )
                return

            # Both invalid — optional Zuhal rescue when zuhal_on_both_invalid is enabled
            if self.zuhal is not None and self.config.zuhal_on_both_invalid:
                if self.cost_tracker.ceiling_reached():
                    logger.info("Cost ceiling reached before Zuhal — skipping %s", unique_id)
                    await db.update_record_status(self.conn, unique_id, State.COST_SKIPPED)
                    cost_skipped = True
                    break
                zuhal_status, zuhal_trace = await self._zuhal_probe(email)
                pending_trace.append(zuhal_trace)

                if zuhal_status == "circuit_open":
                    # No re-queue — skip Zuhal for this candidate and try next
                    logger.warning("Zuhal circuit open (both-invalid rescue) for %s — skipping", unique_id)
                    continue

                if zuhal_status == "accept-all":
                    zuhal_status = "catch_all"
                if zuhal_status in ("valid", "catch_all"):
                    score = compute_confidence_score(
                        email, candidate_domain, strategy, zuhal_status, agent_name, domain_match_score=dms
                    )
                    async with self._write_lock:
                        await db.update_record_dual(
                            self.conn,
                            unique_id,
                            State.VALIDATED,
                            racknerd_status=rk_verdict.status,
                            racknerd_message=rk_verdict.message,
                            racknerd_verified_at=rk_verdict.verified_at,
                            bbops_status=bb_verdict.status,
                            bbops_message=bb_verdict.message,
                            bbops_verified_at=bb_verdict.verified_at,
                            final_verdict=zuhal_status,
                            candidate_email=email,
                            confidence_score=float(score),
                            zuhal_status_override=zuhal_status,
                            dispatch_attempts_delta=1,
                            verifier_agreement="zuhal_only",
                        )
                        await db.flush_process_trace(self.conn, unique_id, pending_trace)
                    self.cost_tracker.record_call("zuhal")
                    await record_pattern(self.conn, email, _first, _last, candidate_domain, mx_provider, success=True)
                    self.stats["validated"] += 1
                    logger.info(
                        "Zuhal-rescued both-invalid: %s → %s [zuhal=%s]",
                        unique_id, email, zuhal_status,
                    )
                    return
                # Zuhal also invalid/error — fall through to try next candidate

            # invalid — record pattern miss and try next candidate
            await record_pattern(self.conn, email, _first, _last, candidate_domain, mx_provider, success=False)
            logger.debug(
                "Candidate %s for %s: %s — trying next",
                email, unique_id, result.final_verdict,
            )

            # After exhausting original pattern candidates, inject Serper enrichment.
            # Serper was skipped in the producer (DNS hit) — call it now as a fallback
            # so we only pay $0.001 when patterns actually fail, not upfront.
            if i == original_count and not serper_enriched and self.serper and candidate_domain:
                if self.cost_tracker.ceiling_reached():
                    logger.info("Cost ceiling reached before Serper fallback — skipping %s", unique_id)
                    await db.update_record_status(self.conn, unique_id, State.COST_SKIPPED)
                    cost_skipped = True
                    break
                serper_enriched = True  # prevent re-injection on subsequent loops
                existing = set(candidates[:original_count])
                raw_emails = await self._serper_enrich(unique_id, row)
                new_emails = [e for e in raw_emails if e not in existing]
                try:
                    await db.mark_serper_enriched(self.conn, unique_id)
                except Exception as exc:
                    logger.warning("Failed to persist serper_enriched flag for %s: %s", unique_id, exc)
                if not self.serper.last_was_cache_hit:
                    self.cost_tracker.record_call("serper_dispatcher")
                for _ in range(self.serper._fallback_calls):
                    self.cost_tracker.record_call("serper_dispatcher")
                self.serper._fallback_calls = 0
                if new_emails:
                    candidates.extend(new_emails)
                    logger.info(
                        "Serper fallback for %s: %d new candidates after patterns exhausted (cache=%s)",
                        unique_id, len(new_emails), self.serper.last_was_cache_hit,
                    )
                else:
                    logger.debug("Serper fallback for %s: no candidates found", unique_id)

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

    async def _zuhal_probe(self, email: str) -> tuple[str, dict]:
        t0 = time.monotonic()
        status: str
        try:
            result = await self.zuhal.validate(email)  # type: ignore[union-attr]
            status = result.verdict
        except PipelineHaltError:
            raise
        except ZuhalCircuitOpenError:
            status = "circuit_open"
        except Exception as exc:
            logger.debug("Zuhal probe error for %s: %s", email, exc)
            status = "error"
        ms = int((time.monotonic() - t0) * 1000)
        return status, {"stage": "zuhal_fallback", "outcome": status, "ms": ms, "email": email}

    async def _serper_enrich(self, unique_id: str, row: aiosqlite.Row) -> list[str]:
        """Call Serper for a DNS-hit record whose patterns all failed. Returns snippet emails."""
        assert self.serper is not None
        try:
            result = await self.serper.enrich(
                business_name=row["business_name"] or "",
                agent_name=row["agent_name"] if (row["strategy"] or "without") == "with" else None,
                state=row["state"] or "",
                domain_hint=row["candidate_domain"] or None,
                strategy=row["strategy"] or "without",
                conn=self.conn,
            )
            return result.candidate_emails
        except Exception as exc:
            logger.warning("Serper fallback error for %s: %s", unique_id, exc)
            return []

    # Log a warning when this many MS probes have been attempted in the current window
    _MS_ALERT_WINDOW: int = 100
    # Error rate above this fraction triggers the warning
    _MS_ERROR_THRESHOLD: float = 0.5

    async def _ms_probe(self, email: str) -> tuple[str, dict]:
        t0 = time.monotonic()
        try:
            result = await check_microsoft_email_async(email)
        except Exception as exc:
            logger.debug("MS probe error for %s: %s", email, exc)
            result = {"status": "error"}
        ms = int((time.monotonic() - t0) * 1000)
        status = result.get("status", "error")

        self._ms_total += 1
        if status == "error":
            self._ms_errors += 1
        if self._ms_total >= self._MS_ALERT_WINDOW:
            rate = self._ms_errors / self._ms_total
            if rate >= self._MS_ERROR_THRESHOLD:
                logger.error(
                    "MS probe degraded: %d/%d errors (%.0f%%) in last %d probes — "
                    "Microsoft domains falling through to paid SMTP",
                    self._ms_errors, self._ms_total, rate * 100, self._ms_total,
                )
            self._ms_total = 0
            self._ms_errors = 0

        return status, {"stage": "ms_api", "outcome": status, "ms": ms, "email": email}

    async def _safe_racknerd(self, email: str) -> BackendVerdict:
        try:
            return await self.racknerd.verify(email)
        except Exception as exc:
            return BackendVerdict(status="error", message=str(exc), verified_at="")

    async def _safe_bbops(self, record_id: int, email: str) -> BackendVerdict:
        try:
            return await self.bbops.verify(record_id, email)
        except BbopsUnhealthy:
            raise
        except Exception as exc:
            return BackendVerdict(status="error", message=str(exc), verified_at="")

    async def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await db.upsert_dispatcher_heartbeat(self.conn)
            except Exception as exc:
                logger.debug("Dispatcher heartbeat failed: %s", exc)
            try:
                await asyncio.wait_for(asyncio.shield(self.stop_event.wait()), timeout=30.0)
            except asyncio.TimeoutError:
                pass
