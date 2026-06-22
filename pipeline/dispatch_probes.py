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
from pipeline.utils.zuhal_client import ZuhalCircuitOpenError

logger = logging.getLogger("pipeline.dispatcher")


async def ms_probe(email: str) -> tuple[str, dict]:
    t0 = time.monotonic()
    try:
        result = await check_microsoft_email_async(email)
    except Exception as exc:
        logger.debug("MS probe error for %s: %s", email, exc)
        result = {"status": "error"}
    ms = int((time.monotonic() - t0) * 1000)
    status = result.get("status", "error")
    return status, {"stage": "ms_api", "outcome": status, "ms": ms, "email": email}


async def zuhal_probe(zuhal, email: str) -> tuple[str, dict]:
    t0 = time.monotonic()
    status: str
    try:
        result = await zuhal.validate(email)
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


async def safe_racknerd(racknerd, email: str, mx_provider: str | None = None) -> BackendVerdict:
    try:
        return await racknerd.verify(email, mx_provider)
    except Exception as exc:
        return BackendVerdict(status="error", message=str(exc), verified_at="")


async def safe_bbops(bbops, record_id: int, email: str) -> BackendVerdict:
    try:
        return await bbops.verify(record_id, email)
    except BbopsUnhealthy:
        raise
    except Exception as exc:
        return BackendVerdict(status="error", message=str(exc), verified_at="")
