"""Zuhal-rescue verdict handling for the dispatcher's candidate loop.

These two helpers own the "what to do when SMTP couldn't confirm" decisions —
decoupled hand-off, inline paid rescue, circuit-open/greylist re-queue, cost
ceiling. They take the Dispatcher (`disp`) for its connection/config/stats and
return a control-flow signal so the candidate loop stays readable:

    "terminal"     — record fully handled; caller should return
    "cost_skipped" — cost ceiling hit; caller should break the candidate loop
    None           — (rescue only) not validated; caller tries the next candidate
"""
from __future__ import annotations

import asyncio
import logging
import time

from pipeline import db
from pipeline import dispatch_probes as dp
from pipeline.db import State
from pipeline._dispatch_helpers import compute_confidence_score, record_pattern
from pipeline.reconcile import DEFINITIVE, greylisting_retry_after

logger = logging.getLogger("pipeline.dispatcher")


async def handle_inconclusive(
    disp,
    unique_id: str,
    email: str,
    rk_verdict,
    bb_verdict,
    candidate_domain: str,
    strategy: str,
    agent_name: str,
    first: str,
    last: str,
    mx_provider: str | None,
    skip_paid: bool,
    pending_trace: list[dict],
) -> str:
    """Handle a reconcile result of `unknown` (SMTP couldn't decide). Always terminal."""
    # Count attempt only when at least one backend gave a definitive verdict.
    # Both-error or error+not_run are pure infra and do not consume the budget.
    any_real_verdict = rk_verdict.status in DEFINITIVE or bb_verdict.status in DEFINITIVE

    # Greylisting: Racknerd got a 4xx temporary SMTP deferral — hold for 30 min.
    rk_is_4xx = rk_verdict.status == "error" and "(4xx temporary)" in (rk_verdict.message or "")
    greylist_hold = greylisting_retry_after() if rk_is_4xx else None

    if disp.zuhal is not None and not skip_paid:
        if disp.config.zuhal_decoupled:
            # Backpressure: pause handoffs when Zuhal backlog is too deep.
            # Count is cached for 5 seconds to avoid per-record DB queries.
            if disp.config.zuhal_backpressure_threshold > 0:
                now = time.monotonic()
                if now - disp._bp_last_checked >= 5.0:
                    disp._bp_cached_count = await db.count_needs_zuhal(disp.conn)
                    disp._bp_last_checked = now
                if disp._bp_cached_count >= disp.config.zuhal_backpressure_threshold:
                    logger.debug(
                        "Zuhal backpressure: backlog=%d >= threshold=%d — pausing %.1fs",
                        disp._bp_cached_count,
                        disp.config.zuhal_backpressure_threshold,
                        disp.config.zuhal_backpressure_sleep_s,
                    )
                    await asyncio.sleep(disp.config.zuhal_backpressure_sleep_s)
            async with disp._write_lock:
                await db.handoff_to_zuhal(
                    disp.conn,
                    unique_id,
                    racknerd_status=rk_verdict.status if rk_verdict else "not_run",
                    racknerd_message=rk_verdict.message if rk_verdict else "",
                    racknerd_verified_at=rk_verdict.verified_at if rk_verdict else None,
                    bbops_status=bb_verdict.status if bb_verdict else "not_run",
                    bbops_message=bb_verdict.message if bb_verdict else "",
                    bbops_verified_at=bb_verdict.verified_at if bb_verdict else None,
                    candidate_email=email,
                )
                await db.flush_process_trace(disp.conn, unique_id, pending_trace)
            disp.stats["handed_off_to_zuhal"] += 1
            logger.debug("Handed off to Zuhal queue: %s → %s", unique_id, email)
            return "terminal"

        if disp.cost_tracker.ceiling_reached():
            logger.info("Cost ceiling reached before Zuhal — skipping %s", unique_id)
            await db.update_record_status(disp.conn, unique_id, State.COST_SKIPPED)
            return "cost_skipped"
        zuhal_status, zuhal_trace = await dp.zuhal_probe(disp.zuhal, email)
        pending_trace.append(zuhal_trace)

        if zuhal_status == "circuit_open":
            async with disp._write_lock:
                await db.requeue_record(disp.conn, unique_id, increment_attempts=False, retry_after=None)
            disp.stats["requeued"] += 1
            logger.warning("Zuhal circuit open — re-queued %s for later retry", unique_id)
            return "terminal"

        if zuhal_status == "accept-all":
            zuhal_status = "catch_all"
        terminal = zuhal_status in ("valid", "catch_all")
        state = State.VALIDATED if terminal else State.VALIDATION_FAILED
        score = compute_confidence_score(email, candidate_domain, strategy, zuhal_status, agent_name)
        async with disp._write_lock:
            await db.update_record_dual(
                disp.conn,
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
            )
            await db.flush_process_trace(disp.conn, unique_id, pending_trace)
        disp.cost_tracker.record_call("zuhal")
        if terminal:
            await record_pattern(disp.conn, email, first, last, candidate_domain, mx_provider, success=True)
            disp.stats["validated"] += 1
            logger.info("Zuhal-validated: %s → %s [zuhal=%s]", unique_id, email, zuhal_status)
        else:
            disp.stats["validation_failed"] += 1
            logger.debug("Zuhal fallback terminal: %s → %s (%s)", unique_id, email, zuhal_status)
        return "terminal"

    async with disp._write_lock:
        await db.requeue_record(
            disp.conn, unique_id, increment_attempts=any_real_verdict, retry_after=greylist_hold,
        )
    disp.stats["requeued"] += 1
    if rk_is_4xx:
        logger.debug("Re-queued %s (greylisted — 4xx hold until %s)", unique_id, greylist_hold)
    else:
        logger.debug("Re-queued %s (inconclusive verdict, no Zuhal configured)", unique_id)
    return "terminal"


async def rescue_both_invalid(
    disp,
    unique_id: str,
    email: str,
    rk_verdict,
    bb_verdict,
    candidate_domain: str,
    strategy: str,
    agent_name: str,
    first: str,
    last: str,
    mx_provider: str | None,
    pending_trace: list[dict],
) -> str | None:
    """Optional paid Zuhal rescue when both SMTP backends returned invalid.

    Returns "terminal"/"cost_skipped", or None to fall through to the next candidate.
    """
    if disp.cost_tracker.ceiling_reached():
        logger.info("Cost ceiling reached before Zuhal — skipping %s", unique_id)
        await db.update_record_status(disp.conn, unique_id, State.COST_SKIPPED)
        return "cost_skipped"
    zuhal_status, zuhal_trace = await dp.zuhal_probe(disp.zuhal, email)
    pending_trace.append(zuhal_trace)

    if zuhal_status == "circuit_open":
        async with disp._write_lock:
            await db.requeue_record(disp.conn, unique_id, increment_attempts=False, retry_after=None)
        disp.stats["requeued"] += 1
        logger.warning("Zuhal circuit open (both-invalid rescue) — re-queued %s", unique_id)
        return "terminal"

    if zuhal_status == "accept-all":
        zuhal_status = "catch_all"
    if zuhal_status in ("valid", "catch_all"):
        score = compute_confidence_score(email, candidate_domain, strategy, zuhal_status, agent_name)
        async with disp._write_lock:
            await db.update_record_dual(
                disp.conn,
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
            )
            await db.flush_process_trace(disp.conn, unique_id, pending_trace)
        disp.cost_tracker.record_call("zuhal")
        await record_pattern(disp.conn, email, first, last, candidate_domain, mx_provider, success=True)
        disp.stats["validated"] += 1
        logger.info("Zuhal-rescued both-invalid: %s → %s [zuhal=%s]", unique_id, email, zuhal_status)
        return "terminal"
    return None  # Zuhal also invalid/error — fall through to try next candidate
