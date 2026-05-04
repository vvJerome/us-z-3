from __future__ import annotations

import asyncio
import json
import logging
import time

import aiosqlite
from rapidfuzz import fuzz

from pipeline.config import PipelineConfig
from pipeline.constants import (
    CONSUMER_POLL_EMPTY_BACKOFF_THRESHOLD,
    CONSUMER_POLL_MAX_INTERVAL_SECONDS,
)
from pipeline.consumers.bbops_async import BbopsAsyncConsumer, BbopsUnhealthy
from pipeline.consumers.racknerd import RacknerdConsumer
from pipeline.utils.zuhal_client import ZuhalClient
from pipeline.models import BackendVerdict, FinalVerdict, PipelineHaltError, ReconcileResult
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.email_patterns import email_to_template
from pipeline.utils.ms_verify import check_microsoft_email_async, is_microsoft_mx
from pipeline.utils.notify import open_notify_reader
from pipeline.utils.text import parse_name
from pipeline import db
from pipeline.db import State

logger = logging.getLogger("pipeline.dispatcher")

_GENERIC_PREFIXES: frozenset[str] = frozenset({
    "info", "contact", "hello", "admin", "support", "sales", "help",
})

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
        # One said invalid, one errored — can't trust the invalid verdict alone
        return ReconcileResult(final_verdict="unknown", should_write=False, is_terminal=False)

    if rk in _INCONCLUSIVE and bb == "invalid":
        return ReconcileResult(final_verdict="unknown", should_write=False, is_terminal=False)

    # Both inconclusive
    return ReconcileResult(final_verdict="unknown", should_write=False, is_terminal=False)


# ---------------------------------------------------------------------------
# Confidence scoring (ported from consumer.py)
# ---------------------------------------------------------------------------

def _name_matches_email(local: str, agent_name: str) -> bool:
    parts = agent_name.strip().lower().split()
    first = parts[0] if parts else ""
    last = parts[-1] if len(parts) > 1 else ""
    variants = [v for v in [
        f"{first}{last}",
        f"{first}.{last}",
        f"{first}_{last}",
        f"{first[0]}{last}" if first else "",
        first,
        last,
    ] if v]
    return bool(variants) and max(fuzz.ratio(local.lower(), v) for v in variants) >= 75


def compute_confidence_score(
    email: str,
    candidate_domain: str | None,
    strategy: str,
    verdict: str,
    agent_name: str = "",
) -> int:
    local, _, domain = email.partition("@")
    score = 0

    if candidate_domain:
        d_norm = domain.rsplit(".", 1)[0].replace("-", "") if "." in domain else domain
        c_norm = candidate_domain.rsplit(".", 1)[0].replace("-", "") if "." in candidate_domain else candidate_domain
        if fuzz.ratio(d_norm, c_norm) >= 85:
            score += 1

    if strategy == "with":
        if agent_name and _name_matches_email(local, agent_name):
            score += 1
        if local.lower() not in _GENERIC_PREFIXES:
            score += 1
        if verdict == "valid":
            score += 1
    else:
        if local.lower() in _GENERIC_PREFIXES:
            score += 1
        if verdict == "valid":
            score += 1

    return score


def confidence_tier(score: int) -> str:
    if score >= 3:
        return "high"
    if score == 2:
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

class Dispatcher:
    """
    Dual-backend dispatch coordinator.

    For each DISCOVERED record:
    1. MS probe pre-filter (free, short-circuits Microsoft domains)
    2. Fan out to Racknerd + bbops concurrently
    3. OR-of-valids reconciliation
    4. Write dual-verdict + final_verdict to DB
    5. Pattern learning on success
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
    ) -> None:
        self.config = config
        self.conn = conn
        self.racknerd = racknerd
        self.bbops = bbops
        self.cost_tracker = cost_tracker
        self.stop_event = stop_event or asyncio.Event()
        self.zuhal = zuhal
        self._sem = asyncio.Semaphore(config.dispatch_concurrency)
        self._write_lock = asyncio.Lock()
        self._notify_reader = None
        self.stats: dict[str, int] = {
            "validated": 0,
            "validation_failed": 0,
            "disagreements": 0,
            "requeued": 0,
        }

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
                if consecutive_empty >= CONSUMER_POLL_EMPTY_BACKOFF_THRESHOLD:
                    poll_interval = min(poll_interval * 2, CONSUMER_POLL_MAX_INTERVAL_SECONDS)

                producer_done = await db.get_checkpoint(self.conn, "producer_done")
                if producer_done == "true":
                    if not await db.has_pending_validation(self.conn):
                        if consecutive_empty >= CONSUMER_POLL_EMPTY_BACKOFF_THRESHOLD:
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
            "Dispatcher finished — validated=%d failed=%d requeued=%d disagreements=%d",
            self.stats["validated"],
            self.stats["validation_failed"],
            self.stats["requeued"],
            self.stats["disagreements"],
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

        if not raw_candidates:
            logger.warning("No candidate_emails for %s — marking failed", unique_id)
            await db.update_record_status(self.conn, unique_id, State.VALIDATION_FAILED)
            self.stats["validation_failed"] += 1
            return

        try:
            candidates: list[str] = json.loads(raw_candidates)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Invalid candidate_emails JSON for %s", unique_id)
            await db.update_record_status(self.conn, unique_id, State.VALIDATION_FAILED)
            self.stats["validation_failed"] += 1
            return

        mx_provider = row["mx_provider"] if "mx_provider" in row.keys() else None
        candidate_domain = row["candidate_domain"] or ""
        strategy = row["strategy"] or "without"
        agent_name = row["agent_name"] or ""
        _first, _, _last = parse_name(agent_name)
        use_ms_probe = is_microsoft_mx(mx_provider)

        pending_trace: list[dict] = []
        cost_skipped = False

        for email in candidates:
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
                    score = compute_confidence_score(email, candidate_domain, strategy, "valid", agent_name)
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
                            zuhal_score=float(score),
                            dispatch_attempts_delta=0,  # MS probe is free, don't count
                        )
                        await db.flush_process_trace(self.conn, unique_id, pending_trace)
                    if mx_provider:
                        tmpl = email_to_template(email, _first, _last, candidate_domain)
                        if tmpl:
                            await db.record_pattern_result(self.conn, mx_provider, tmpl, success=True)
                    self.stats["validated"] += 1
                    logger.info("MS-validated (no SMTP): %s → %s", unique_id, email)
                    return

                if ms_status == "invalid":
                    pending_trace.append({"stage": "ms_skip", "outcome": "invalid", "email": email})
                    if mx_provider:
                        tmpl = email_to_template(email, _first, _last, candidate_domain)
                        if tmpl:
                            await db.record_pattern_result(self.conn, mx_provider, tmpl, success=False)
                    continue  # try next candidate

                # unknown/error → fall through to SMTP backends

            # Fan out to both SMTP backends concurrently
            rk_verdict, bb_verdict, trace_entries = await self._dual_probe(
                unique_id, email, row["id"]
            )
            pending_trace.extend(trace_entries)

            result = reconcile(rk_verdict, bb_verdict)

            # Disagreement detection
            if (
                rk_verdict and bb_verdict
                and rk_verdict.status in _DEFINITIVE
                and bb_verdict.status in _DEFINITIVE
                and rk_verdict.status != bb_verdict.status
            ):
                self.stats["disagreements"] += 1
                logger.info(
                    "Backend disagreement for %s/%s: racknerd=%s bbops=%s",
                    unique_id,
                    email,
                    rk_verdict.status,
                    bb_verdict.status,
                )

            if not result.should_write:
                # Tunnel down or both inconclusive — re-queue without burning attempt
                async with self._write_lock:
                    await db.update_record_status(self.conn, unique_id, State.DISCOVERED)
                self.stats["requeued"] += 1
                logger.debug("Re-queued %s (inconclusive verdict)", unique_id)
                return

            if result.final_verdict in ("valid", "catch_all"):
                score = compute_confidence_score(
                    email, candidate_domain, strategy, result.final_verdict, agent_name
                )
                async with self._write_lock:
                    await db.update_record_dual(
                        self.conn,
                        unique_id,
                        State.VALIDATED,
                        racknerd_status=rk_verdict.status if rk_verdict else "not_run",
                        racknerd_message=rk_verdict.message if rk_verdict else "",
                        racknerd_verified_at=rk_verdict.verified_at if rk_verdict else None,
                        bbops_status=bb_verdict.status if bb_verdict else "not_run",
                        bbops_message=bb_verdict.message if bb_verdict else "",
                        bbops_verified_at=bb_verdict.verified_at if bb_verdict else None,
                        final_verdict=result.final_verdict,
                        candidate_email=email,
                        zuhal_score=float(score),
                    )
                    await db.flush_process_trace(self.conn, unique_id, pending_trace)
                if mx_provider:
                    tmpl = email_to_template(email, _first, _last, candidate_domain)
                    if tmpl:
                        await db.record_pattern_result(self.conn, mx_provider, tmpl, success=True)
                self.stats["validated"] += 1
                logger.info(
                    "Validated %s → %s [rk=%s bb=%s]",
                    unique_id, email,
                    rk_verdict.status if rk_verdict else "n/a",
                    bb_verdict.status if bb_verdict else "n/a",
                )
                return

            # Zuhal rescue: both SMTP backends said invalid — give Zuhal a shot
            if result.final_verdict == "invalid" and self.zuhal is not None:
                if self.cost_tracker.ceiling_reached():
                    logger.info("Cost ceiling reached — skipping %s", unique_id)
                    await db.update_record_status(self.conn, unique_id, State.COST_SKIPPED)
                    cost_skipped = True
                    break

                zuhal_verdict = await self._safe_zuhal(email)
                self.cost_tracker.record_call("zuhal")

                zuhal_final: str | None = None
                if zuhal_verdict.verdict == "valid":
                    zuhal_final = "valid"
                elif zuhal_verdict.verdict == "accept-all":
                    zuhal_final = "catch_all"

                if zuhal_final is not None:
                    score = compute_confidence_score(
                        email, candidate_domain, strategy, zuhal_final, agent_name
                    )
                    async with self._write_lock:
                        await db.update_record_dual(
                            self.conn,
                            unique_id,
                            State.VALIDATED,
                            racknerd_status=rk_verdict.status if rk_verdict else "not_run",
                            racknerd_message=rk_verdict.message if rk_verdict else "",
                            racknerd_verified_at=rk_verdict.verified_at if rk_verdict else None,
                            bbops_status=bb_verdict.status if bb_verdict else "not_run",
                            bbops_message=bb_verdict.message if bb_verdict else "",
                            bbops_verified_at=bb_verdict.verified_at if bb_verdict else None,
                            final_verdict=zuhal_final,
                            candidate_email=email,
                            zuhal_status=zuhal_verdict.verdict,
                            zuhal_score=float(score),
                        )
                        await db.flush_process_trace(self.conn, unique_id, pending_trace)
                    if mx_provider:
                        tmpl = email_to_template(email, _first, _last, candidate_domain)
                        if tmpl:
                            await db.record_pattern_result(self.conn, mx_provider, tmpl, success=True)
                    self.stats["validated"] += 1
                    logger.info(
                        "Zuhal-rescued %s → %s [rk=%s bb=%s zuhal=%s]",
                        unique_id, email,
                        rk_verdict.status if rk_verdict else "n/a",
                        bb_verdict.status if bb_verdict else "n/a",
                        zuhal_verdict.verdict,
                    )
                    return
                else:
                    logger.debug(
                        "Zuhal also negative for %s/%s: %s",
                        unique_id, email, zuhal_verdict.verdict,
                    )

            # Both SMTP and Zuhal (if run) said no — record pattern miss, try next candidate
            if mx_provider:
                tmpl = email_to_template(email, _first, _last, candidate_domain)
                if tmpl:
                    await db.record_pattern_result(self.conn, mx_provider, tmpl, success=False)
            logger.debug(
                "Candidate %s for %s: %s — trying next",
                email, unique_id, result.final_verdict,
            )
            continue

        if cost_skipped:
            return

        # All candidates exhausted
        async with self._write_lock:
            await db.update_record_dual(
                self.conn,
                unique_id,
                State.VALIDATION_FAILED,
                racknerd_status=None,
                racknerd_message=None,
                racknerd_verified_at=None,
                bbops_status=None,
                bbops_message=None,
                bbops_verified_at=None,
                final_verdict="invalid",
            )
            await db.flush_process_trace(self.conn, unique_id, pending_trace)
        self.stats["validation_failed"] += 1
        logger.debug("All candidates failed for %s", unique_id)

    async def _ms_probe(self, email: str) -> tuple[str, dict]:
        t0 = time.monotonic()
        try:
            result = await check_microsoft_email_async(email)
        except Exception as exc:
            logger.debug("MS probe error for %s: %s", email, exc)
            result = {"status": "error"}
        ms = int((time.monotonic() - t0) * 1000)
        status = result.get("status", "error")
        return status, {"stage": "ms_api", "outcome": status, "ms": ms, "email": email}

    async def _dual_probe(
        self,
        unique_id: str,
        email: str,
        record_id: int,
    ) -> tuple[BackendVerdict | None, BackendVerdict | None, list[dict]]:
        """Run Racknerd + bbops concurrently. Returns (rk, bb, trace_entries)."""
        timeout = self.config.dispatch_backend_timeout_s
        trace: list[dict] = []

        rk_coro = self._safe_racknerd(email)
        bb_coro = self._safe_bbops(record_id, email)

        t0 = time.monotonic()
        rk_result, bb_result = await asyncio.gather(
            asyncio.wait_for(rk_coro, timeout=timeout),
            asyncio.wait_for(bb_coro, timeout=timeout),
            return_exceptions=True,
        )
        elapsed = int((time.monotonic() - t0) * 1000)

        rk_verdict: BackendVerdict | None = None
        bb_verdict: BackendVerdict | None = None

        if isinstance(rk_result, BackendVerdict):
            rk_verdict = rk_result
        elif isinstance(rk_result, BaseException):
            rk_verdict = BackendVerdict(status="error", message=str(rk_result), verified_at="")
        trace.append({
            "stage": "racknerd",
            "outcome": rk_verdict.status if rk_verdict else "error",
            "ms": elapsed,
            "email": email,
        })

        if isinstance(bb_result, BackendVerdict):
            bb_verdict = bb_result
        elif isinstance(bb_result, BbopsUnhealthy):
            bb_verdict = BackendVerdict(status="not_run", message="bbops unhealthy", verified_at="")
        elif isinstance(bb_result, BaseException):
            bb_verdict = BackendVerdict(status="error", message=str(bb_result), verified_at="")
        trace.append({
            "stage": "bbops",
            "outcome": bb_verdict.status if bb_verdict else "error",
            "ms": elapsed,
            "email": email,
        })

        return rk_verdict, bb_verdict, trace

    async def _safe_racknerd(self, email: str) -> BackendVerdict:
        try:
            return await self.racknerd.verify(email)
        except Exception as exc:
            return BackendVerdict(status="error", message=str(exc), verified_at="")

    async def _safe_zuhal(self, email: str):
        """Run Zuhal validation; returns ValidationResult or a stub on error."""
        from pipeline.models import ValidationResult
        try:
            return await self.zuhal.validate(email)
        except Exception as exc:
            logger.warning("Zuhal error for %s: %s", email, exc)
            return ValidationResult(
                email=email,
                verdict="error",
                score=0.0,
                is_disposable=False,
                raw_status=str(exc),
                http_status=0,
            )

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
            except Exception:
                pass
            try:
                await asyncio.wait_for(asyncio.shield(self.stop_event.wait()), timeout=30.0)
            except asyncio.TimeoutError:
                pass
