from __future__ import annotations

import asyncio
import datetime
import json
import logging
import time

import aiosqlite
from rapidfuzz import fuzz

from pipeline.config import PipelineConfig
from pipeline.constants import (
    DISPATCH_POLL_EMPTY_BACKOFF_THRESHOLD,
    DISPATCH_POLL_MAX_INTERVAL_S,
)
from pipeline.consumers.bbops_async import BbopsAsyncConsumer, BbopsUnhealthy
from pipeline.consumers.racknerd import RacknerdConsumer
from pipeline.utils.zuhal_client import ZuhalClient, ZuhalCircuitOpenError
from pipeline.utils.serper_client import SerperClient
from pipeline.models import BackendVerdict, PipelineHaltError, ReconcileResult
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.email_patterns import email_to_template
from pipeline.utils.ms_verify import check_microsoft_email_async, is_microsoft_mx
from pipeline.utils.notify import open_notify_reader
from pipeline.utils.text import parse_name
from pipeline import db
from pipeline.db import State

logger = logging.getLogger("pipeline.dispatcher")


def _valid_email_format(email: str) -> bool:
    """Return False for emails whose local part violates RFC 5321 basics (e.g. ...@domain)."""
    parts = email.split("@")
    if len(parts) != 2:
        return False
    local = parts[0]
    return bool(local) and not local.startswith(".") and not local.endswith(".") and ".." not in local


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


# ---------------------------------------------------------------------------
# Confidence scoring
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
    """Return an additive confidence score 0–4 for a validated email.

    +1 domain match (email domain fuzzy-matches candidate_domain, ≥85 ratio)
    strategy="with" (name-targeted search):
        +1 name match (local part resembles agent_name)
        +1 not a generic prefix (info/contact/admin/…)
        +1 verdict == "valid" (not catch_all)
    strategy="without" (generic/org search):
        +1 IS a generic prefix
        +1 verdict == "valid"
    High ≥ 3, medium = 2, low ≤ 1 — see confidence_tier().
    """
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


def _greylisting_retry_after(minutes: int = 30) -> str:
    """Return an ISO timestamp N minutes from now for a greylisting hold."""
    dt = datetime.datetime.utcnow() + datetime.timedelta(minutes=minutes)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


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
        self._notify_reader = None
        self.stats: dict[str, int] = {
            "validated": 0,
            "validation_failed": 0,
            "disagreements": 0,
            "requeued": 0,
        }

    async def _record_pattern(
        self,
        email: str,
        first: str,
        last: str,
        candidate_domain: str,
        mx_provider: str | None,
        success: bool,
    ) -> None:
        if not mx_provider:
            return
        tmpl = email_to_template(email, first, last, candidate_domain)
        if tmpl:
            await db.record_pattern_result(self.conn, mx_provider, tmpl, success=success)

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

        # dispatch_attempts counts only re-queues where a real verdict was obtained.
        # requeue_count counts every re-queue including infra transients — it is the
        # safety valve that terminates records stuck in permanent infra failure loops.
        dispatch_attempts = row["dispatch_attempts"] or 0
        requeue_count = row["requeue_count"] or 0
        if dispatch_attempts >= self.config.max_dispatch_attempts:
            logger.warning(
                "Record %s hit max dispatch attempts (%d) — marking VALIDATION_FAILED",
                unique_id, dispatch_attempts,
            )
            async with self._write_lock:
                await db.update_record_status(self.conn, unique_id, State.VALIDATION_FAILED)
            self.stats["validation_failed"] += 1
            return
        if requeue_count >= self.config.max_requeue_count:
            logger.warning(
                "Record %s hit max requeue count (%d) — marking VALIDATION_FAILED",
                unique_id, requeue_count,
            )
            async with self._write_lock:
                await db.update_record_status(self.conn, unique_id, State.VALIDATION_FAILED)
            self.stats["validation_failed"] += 1
            return

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

        mx_provider = row["mx_provider"]
        candidate_domain = row["candidate_domain"] or ""
        strategy = row["strategy"] or "without"
        agent_name = row["agent_name"] or ""
        _first, _, _last = parse_name(agent_name)
        use_ms_probe = is_microsoft_mx(mx_provider)
        serper_enriched = bool(row["serper_enriched"])

        pending_trace: list[dict] = []
        cost_skipped = False
        original_count = len(candidates)
        i = 0
        last_rk: BackendVerdict | None = None
        last_bb: BackendVerdict | None = None

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
                            confidence_score=float(score),
                            dispatch_attempts_delta=0,  # MS probe is free, don't count
                        )
                        await db.flush_process_trace(self.conn, unique_id, pending_trace)
                    await self._record_pattern(email, _first, _last, candidate_domain, mx_provider, success=True)
                    self.stats["validated"] += 1
                    logger.info("MS-validated (no SMTP): %s → %s", unique_id, email)
                    return

                if ms_status == "invalid":
                    pending_trace.append({"stage": "ms_skip", "outcome": "invalid", "email": email})
                    await self._record_pattern(email, _first, _last, candidate_domain, mx_provider, success=False)
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
                    email, candidate_domain, strategy, rk_verdict.status, agent_name
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
                    )
                    await db.flush_process_trace(self.conn, unique_id, pending_trace)
                await self._record_pattern(
                    email, _first, _last, candidate_domain, mx_provider, success=True
                )
                self.stats["validated"] += 1
                logger.info("Racknerd-validated (bbops skipped): %s → %s", unique_id, email)
                return

            # Tunnel down: pure infra, re-queue without burning dispatch_attempts
            if rk_verdict.status == "error" and "tunnel not up" in rk_verdict.message:
                async with self._write_lock:
                    await db.requeue_record(
                        self.conn, unique_id, increment_attempts=False, retry_after=None
                    )
                self.stats["requeued"] += 1
                logger.debug("Re-queued %s (SSH tunnel not up)", unique_id)
                return

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
                # Racknerd IP block: skip Zuhal (block is IP-level, not email verdict),
                # re-queue so the record retries after the Spamhaus cooldown clears.
                if rk_verdict.status == "blocked":
                    # Count attempt only if bbops gave a definitive verdict — a blocked
                    # Racknerd + bbops invalid is one real verdict; blocked + bbops error
                    # is purely infra and should not consume the budget.
                    any_real = bb_verdict.status in _DEFINITIVE
                    async with self._write_lock:
                        await db.requeue_record(
                            self.conn, unique_id, increment_attempts=any_real, retry_after=None
                        )
                    self.stats["requeued"] += 1
                    logger.debug("Re-queued %s (Racknerd blocked — IP-level rejection)", unique_id)
                    return

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
                    if self.cost_tracker.ceiling_reached():
                        logger.info("Cost ceiling reached before Zuhal — skipping %s", unique_id)
                        await db.update_record_status(self.conn, unique_id, State.COST_SKIPPED)
                        cost_skipped = True
                        break
                    zuhal_status, zuhal_trace = await self._zuhal_probe(email)
                    pending_trace.append(zuhal_trace)

                    if zuhal_status == "circuit_open":
                        async with self._write_lock:
                            await db.requeue_record(
                                self.conn, unique_id, increment_attempts=False, retry_after=None
                            )
                        self.stats["requeued"] += 1
                        logger.warning("Zuhal circuit open — re-queued %s for later retry", unique_id)
                        return

                    if zuhal_status == "accept-all":
                        zuhal_status = "catch_all"
                    terminal = zuhal_status in ("valid", "catch_all")
                    state = State.VALIDATED if terminal else State.VALIDATION_FAILED
                    score = compute_confidence_score(
                        email, candidate_domain, strategy, zuhal_status, agent_name
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
                        )
                        await db.flush_process_trace(self.conn, unique_id, pending_trace)
                    self.cost_tracker.record_call("zuhal")
                    if terminal:
                        await self._record_pattern(
                            email, _first, _last, candidate_domain, mx_provider, success=True
                        )
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
                    async with self._write_lock:
                        await db.requeue_record(
                            self.conn, unique_id,
                            increment_attempts=any_real_verdict,
                            retry_after=greylist_hold,
                        )
                    self.stats["requeued"] += 1
                    if rk_is_4xx:
                        logger.debug("Re-queued %s (greylisted — 4xx hold until %s)", unique_id, greylist_hold)
                    else:
                        logger.debug("Re-queued %s (inconclusive verdict, no Zuhal configured)", unique_id)
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
                        racknerd_status=rk_verdict.status,
                        racknerd_message=rk_verdict.message,
                        racknerd_verified_at=rk_verdict.verified_at,
                        bbops_status=bb_verdict.status,
                        bbops_message=bb_verdict.message,
                        bbops_verified_at=bb_verdict.verified_at,
                        final_verdict=result.final_verdict,
                        candidate_email=email,
                        confidence_score=float(score),
                    )
                    await db.flush_process_trace(self.conn, unique_id, pending_trace)
                await self._record_pattern(
                    email, _first, _last, candidate_domain, mx_provider, success=True
                )
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
                    async with self._write_lock:
                        await db.requeue_record(
                            self.conn, unique_id, increment_attempts=False, retry_after=None
                        )
                    self.stats["requeued"] += 1
                    logger.warning(
                        "Zuhal circuit open (both-invalid rescue) — re-queued %s", unique_id
                    )
                    return

                if zuhal_status == "accept-all":
                    zuhal_status = "catch_all"
                if zuhal_status in ("valid", "catch_all"):
                    score = compute_confidence_score(
                        email, candidate_domain, strategy, zuhal_status, agent_name
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
                        )
                        await db.flush_process_trace(self.conn, unique_id, pending_trace)
                    self.cost_tracker.record_call("zuhal")
                    await self._record_pattern(
                        email, _first, _last, candidate_domain, mx_provider, success=True
                    )
                    self.stats["validated"] += 1
                    logger.info(
                        "Zuhal-rescued both-invalid: %s → %s [zuhal=%s]",
                        unique_id, email, zuhal_status,
                    )
                    return
                # Zuhal also invalid/error — fall through to try next candidate

            # invalid — record pattern miss and try next candidate
            await self._record_pattern(email, _first, _last, candidate_domain, mx_provider, success=False)
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
            )
            await db.flush_process_trace(self.conn, unique_id, pending_trace)
        self.stats["validation_failed"] += 1
        logger.debug("All candidates failed for %s", unique_id)

    async def _zuhal_probe(self, email: str) -> tuple[str, dict]:
        t0 = time.monotonic()
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
