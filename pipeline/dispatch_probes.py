"""Backend probe wrappers for the dispatcher.

Each function performs one external verification call and returns either a verdict
or a (status, trace) pair, swallowing transport errors into an "error" status so
the dispatcher's reconciliation logic stays simple. No DB writes here.
"""
from __future__ import annotations

import logging
import time

import aiosqlite

from pipeline.consumers.bbops_async import BbopsUnhealthy
from pipeline.harvest import harvest, infer_templates
from pipeline.models import BackendVerdict, PipelineHaltError
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.email_patterns import generate_ranked_candidates
from pipeline.utils.ms_verify import check_microsoft_email_async
from pipeline.utils.rate_limiter import TokenBucket
from pipeline.utils.zuhal_client import ZuhalCircuitOpenError, ZuhalCreditsExhaustedError
from pipeline import db
from pipeline.db import State

logger = logging.getLogger("pipeline.dispatcher")

# Rolling MS probe error counter — resets every _MS_ALERT_WINDOW probes.
_ms_total: int = 0
_ms_errors: int = 0
_MS_ALERT_WINDOW: int = 100
_MS_ERROR_THRESHOLD: float = 0.5


async def ms_probe(email: str) -> tuple[str, dict]:
    global _ms_total, _ms_errors
    t0 = time.monotonic()
    try:
        result = await check_microsoft_email_async(email)
    except Exception as exc:
        logger.debug("MS probe error for %s: %s", email, exc)
        result = {"status": "error"}
    ms = int((time.monotonic() - t0) * 1000)
    status = result.get("status", "error")

    _ms_total += 1
    if status == "error":
        _ms_errors += 1
    if _ms_total >= _MS_ALERT_WINDOW:
        rate = _ms_errors / _ms_total
        if rate >= _MS_ERROR_THRESHOLD:
            logger.error(
                "MS probe degraded: %d/%d errors (%.0f%%) in last %d probes — "
                "Microsoft domains falling through to paid SMTP",
                _ms_errors, _ms_total, rate * 100, _ms_total,
            )
        _ms_total = 0
        _ms_errors = 0

    return status, {"stage": "ms_api", "outcome": status, "ms": ms, "email": email}


async def zuhal_probe(zuhal, email: str) -> tuple[str, dict]:
    t0 = time.monotonic()
    status: str
    try:
        result = await zuhal.validate(email)
        status = result.verdict
    except PipelineHaltError:
        raise
    except (ZuhalCircuitOpenError, ZuhalCreditsExhaustedError):
        # circuit open OR credits out → defer (re-queue), don't burn as failed.
        status = "circuit_open"
    except Exception as exc:
        logger.debug("Zuhal probe error for %s: %s", email, exc)
        status = "error"
    ms = int((time.monotonic() - t0) * 1000)
    return status, {"stage": "zuhal_fallback", "outcome": status, "ms": ms, "email": email}


async def serper_enrich(serper, conn: aiosqlite.Connection, unique_id: str, row: aiosqlite.Row) -> list[str]:
    """Call Serper for a DNS-hit record whose patterns all failed. Returns snippet emails."""
    try:
        result = await serper.enrich(
            business_name=row["business_name"] or "",
            agent_name=row["agent_name"] if (row["strategy"] or "without") == "with" else None,
            state=row["state"] or "",
            domain_hint=row["candidate_domain"] or None,
            strategy=row["strategy"] or "without",
            conn=conn,
        )
        return result.candidate_emails
    except Exception as exc:
        logger.warning("Serper fallback error for %s: %s", unique_id, exc)
        return []


async def safe_racknerd(racknerd, email: str) -> BackendVerdict:
    try:
        return await racknerd.verify(email)
    except Exception as exc:
        return BackendVerdict(status="error", message=str(exc), verified_at="")


async def safe_bbops(bbops, record_id: int, email: str) -> BackendVerdict:
    try:
        return await bbops.verify(record_id, email)
    except BbopsUnhealthy:
        raise
    except Exception as exc:
        return BackendVerdict(status="error", message=str(exc), verified_at="")


async def inject_harvest_fallback(
    unique_id: str,
    candidates: list[str],
    first: str,
    last: str,
    strategy: str,
    mx_provider: str | None,
    domain: str,
    rate_limiter: TokenBucket | None,
    timeout_s: float,
) -> int:
    """Scrape the domain for real emails; add house-convention + direct candidates. Returns count added."""
    try:
        result = await harvest(domain, rate_limiter=rate_limiter, timeout_s=timeout_s)
    except Exception as exc:
        logger.warning("Harvest failed for %s (%s): %s", unique_id, domain, exc)
        return 0
    existing = set(candidates)
    new: list[str] = []
    templates = infer_templates(result.emails, result.officers, domain)
    if templates:
        rankings = [{"template": t, "success_count": 1, "total_count": 1} for t in templates]
        for c in generate_ranked_candidates(first, last, domain, strategy, rankings=rankings):  # type: ignore[arg-type]
            if c not in existing:
                new.append(c)
                existing.add(c)
    for e in result.emails:
        if e not in existing:
            new.append(e)
            existing.add(e)
    candidates.extend(new)
    logger.info(
        "Harvest for %s (%s): %d emails, %d officers, +%d candidates%s",
        unique_id, domain, len(result.emails), len(result.officers), len(new),
        " [BLOCKED]" if result.blocked else "",
    )
    return len(new)


async def inject_serper_fallback(
    unique_id: str,
    row: aiosqlite.Row,
    candidates: list[str],
    serper,
    cache_conn: aiosqlite.Connection,
    conn: aiosqlite.Connection,
    cost_tracker: CostTracker,
) -> bool:
    """Inject Serper enrichment after patterns exhausted; return True if cost-skipped."""
    if cost_tracker.ceiling_reached():
        logger.info("Cost ceiling reached before Serper fallback — skipping %s", unique_id)
        await db.update_record_status(conn, unique_id, State.COST_SKIPPED)
        return True
    existing = set(candidates)
    raw_emails = await serper_enrich(serper, cache_conn, unique_id, row)
    new_emails = [e for e in raw_emails if e not in existing]
    try:
        await db.mark_serper_enriched(conn, unique_id)
    except Exception as exc:
        logger.warning("Failed to persist serper_enriched flag for %s: %s", unique_id, exc)
    serper.charge_costs(cost_tracker, "serper_dispatcher")
    if new_emails:
        candidates.extend(new_emails)
        logger.info(
            "Serper fallback for %s: %d new candidates after patterns exhausted (cache=%s)",
            unique_id, len(new_emails), serper.last_was_cache_hit,
        )
    else:
        logger.debug("Serper fallback for %s: no candidates found", unique_id)
    return False
