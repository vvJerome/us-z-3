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
from pipeline.models import BackendVerdict, PipelineHaltError
from pipeline.utils.ms_verify import check_microsoft_email_async
from pipeline.utils.zuhal_client import ZuhalCircuitOpenError, ZuhalCreditsExhaustedError

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
