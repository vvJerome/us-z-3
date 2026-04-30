from __future__ import annotations

import asyncio
import json
import logging

import time

import aiosqlite
from rapidfuzz import fuzz

from pipeline.config import PipelineConfig
from pipeline.constants import CONSUMER_POLL_EMPTY_BACKOFF_THRESHOLD, CONSUMER_POLL_MAX_INTERVAL_SECONDS
from pipeline.models import PipelineHaltError
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.zuhal_client import ZuhalClient
from pipeline.utils.notify import open_notify_reader
from pipeline.utils.ms_verify import check_microsoft_email_async, is_microsoft_mx
from pipeline.utils.email_patterns import email_to_template
from pipeline.utils.text import parse_name
from pipeline import db
from pipeline.db import State

logger = logging.getLogger("pipeline.consumer")


class ConsumerWorker:
    def __init__(
        self,
        config: PipelineConfig,
        conn: aiosqlite.Connection,
        cost_tracker: CostTracker,
        zuhal: ZuhalClient,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self.config = config
        self.conn = conn
        self.cost_tracker = cost_tracker
        self.zuhal = zuhal
        self.stop_event = stop_event or asyncio.Event()
        self._sem = asyncio.Semaphore(config.zuhal_concurrency)
        self._consecutive_api_errors: int = 0
        self._notify_reader: asyncio.StreamReader | None = None

    async def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await db.upsert_consumer_heartbeat(self.conn)
            except Exception:
                pass
            try:
                await asyncio.wait_for(asyncio.shield(self.stop_event.wait()), timeout=30.0)
            except asyncio.TimeoutError:
                pass

    async def run(self) -> None:
        base_interval = float(self.config.consumer_poll_interval)
        poll_interval = base_interval
        consecutive_empty = 0

        recovered = await db.recover_stale_validating(self.conn)
        if recovered:
            logger.warning("Recovered %d orphaned VALIDATING rows → DISCOVERED", recovered)

        if self.config.notify_pipe:
            from pathlib import Path
            try:
                self._notify_reader = await open_notify_reader(Path(self.config.notify_pipe))
                logger.info("Consumer: notify pipe opened at %s", self.config.notify_pipe)
            except OSError as exc:
                logger.warning("Consumer: could not open notify pipe (%s) — falling back to polling", exc)

        _hb = asyncio.create_task(self._heartbeat_loop(), name="consumer-heartbeat")
        logger.info("Consumer starting (base poll interval: %.0fs)", base_interval)

        while not self.stop_event.is_set():
            rows = await db.fetch_pending_validation(self.conn, limit=10)

            if not rows:
                # Adaptive backoff: double interval after threshold consecutive empties
                consecutive_empty += 1
                if consecutive_empty >= CONSUMER_POLL_EMPTY_BACKOFF_THRESHOLD:
                    poll_interval = min(poll_interval * 2, CONSUMER_POLL_MAX_INTERVAL_SECONDS)

                # Check if producer is done
                producer_done = await db.get_checkpoint(self.conn, "producer_done")
                if producer_done == "true":
                    # Non-claiming peek: any remaining DISCOVERED rows?
                    if not await db.has_pending_validation(self.conn):
                        if consecutive_empty >= CONSUMER_POLL_EMPTY_BACKOFF_THRESHOLD:
                            logger.info("Consumer: queue drained and producer done — exiting")
                            break
                    else:
                        # Rows appeared — reset backoff and loop to claim them
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

            # Rows found — reset adaptive backoff
            consecutive_empty = 0
            poll_interval = base_interval

            # Process batch concurrently
            tasks = [self._validate_record(row) for row in rows]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, BaseException) and not isinstance(res, PipelineHaltError):
                    logger.error("Unexpected error in validation task", exc_info=res)

        _hb.cancel()
        logger.info("Consumer finished")

    async def _validate_record(self, row: aiosqlite.Row) -> None:
        async with self._sem:
            unique_id = row["unique_id"]
            raw_candidates = row["candidate_emails"]

            if not raw_candidates:
                logger.warning("No candidate_emails for %s — marking failed", unique_id)
                await db.update_record_status(self.conn, unique_id, State.VALIDATION_FAILED)
                return

            try:
                candidates: list[str] = json.loads(raw_candidates)
            except (json.JSONDecodeError, TypeError):
                logger.warning("Invalid candidate_emails JSON for %s", unique_id)
                await db.update_record_status(self.conn, unique_id, State.VALIDATION_FAILED)
                return

            # Row already claimed as VALIDATING via atomic fetch_pending_validation
            last_verdict: str | None = None

            mx_provider = row["mx_provider"] if "mx_provider" in row.keys() else None
            use_ms_probe = is_microsoft_mx(mx_provider)
            candidate_domain = row["candidate_domain"] or ""
            _first, _, _last = parse_name(row["agent_name"] or "")

            for idx, email in enumerate(candidates):
                if self.cost_tracker.ceiling_reached():
                    await db.update_record_status(self.conn, unique_id, State.COST_SKIPPED)
                    return

                # MS GetCredentialType probe — free, no credit cost, only reliable for managed domains
                if use_ms_probe:
                    _ms_t0 = time.monotonic()
                    try:
                        ms_result = await check_microsoft_email_async(email)
                    except Exception as exc:
                        logger.debug("MS probe error for %s: %s", email, exc)
                        ms_result = {"status": "error"}
                    _ms_ms = int((time.monotonic() - _ms_t0) * 1000)
                    ms_status = ms_result.get("status", "error")

                    await db.append_process_trace(self.conn, unique_id, {
                        "stage": "ms_api", "outcome": ms_status, "ms": _ms_ms, "email": email,
                    })

                    if ms_status == "valid":
                        score = compute_confidence_score(
                            email=email,
                            candidate_domain=row["candidate_domain"],
                            strategy=row["strategy"] or "without",
                            verdict="valid",
                            agent_name=row["agent_name"] or "",
                        )
                        await db.update_record_status(
                            self.conn,
                            unique_id,
                            State.VALIDATED,
                            candidate_email=email,
                            zuhal_status="ms_valid",
                            verdict="valid",
                            zuhal_score=score,
                        )
                        logger.info("MS-validated (no Zuhal): %s -> %s", unique_id, email)
                        return

                    if ms_status == "invalid":
                        logger.debug("MS probe: %s invalid for %s — trying next", email, unique_id)
                        last_verdict = "invalid"
                        continue

                    # unknown / throttled / error → fall through to Zuhal

                try:
                    _t0 = time.monotonic()
                    result = await self.zuhal.validate(email)
                    _zuhal_ms = int((time.monotonic() - _t0) * 1000)
                    self.cost_tracker.record_call("zuhal")
                except PipelineHaltError:
                    # Restore status so it can be retried after the halt is resolved
                    await db.update_record_status(self.conn, unique_id, State.DISCOVERED)
                    raise
                except Exception as exc:
                    logger.debug(
                        "Zuhal error for %s candidate %s: %s", unique_id, email, exc,
                    )
                    await db.insert_failure(
                        self.conn,
                        unique_id,
                        "zuhal",
                        idx + 1,
                        type(exc).__name__,
                        str(exc),
                    )
                    self._consecutive_api_errors += 1
                    if self._consecutive_api_errors >= self.config.max_consecutive_errors:
                        raise PipelineHaltError(
                            f"Zuhal returning consistent errors — "
                            f"{self._consecutive_api_errors} consecutive failures. "
                            "Halting pipeline."
                        )
                    continue

                # Successful API response — reset the error streak
                self._consecutive_api_errors = 0
                last_verdict = result.verdict

                if result.verdict in ("valid", "accept-all"):
                    score = compute_confidence_score(
                        email=email,
                        candidate_domain=row["candidate_domain"],
                        strategy=row["strategy"] or "without",
                        verdict=result.verdict,
                        agent_name=row["agent_name"] or "",
                    )
                    await db.update_record_status(
                        self.conn,
                        unique_id,
                        State.VALIDATED,
                        candidate_email=email,
                        zuhal_status=result.verdict,
                        verdict=result.verdict,
                        zuhal_score=score,
                    )
                    await db.append_process_trace(self.conn, unique_id, {
                        "stage": "zuhal", "outcome": result.verdict, "ms": _zuhal_ms, "email": email,
                    })
                    if mx_provider:
                        tmpl = email_to_template(email, _first, _last, candidate_domain)
                        if tmpl:
                            await db.record_pattern_result(self.conn, mx_provider, tmpl, success=True)
                    logger.info("Validated: %s -> %s (%s)", unique_id, email, result.verdict)
                    return

                await db.append_process_trace(self.conn, unique_id, {
                    "stage": "zuhal", "outcome": result.verdict, "ms": _zuhal_ms, "email": email,
                })

                if mx_provider:
                    tmpl = email_to_template(email, _first, _last, candidate_domain)
                    if tmpl:
                        await db.record_pattern_result(self.conn, mx_provider, tmpl, success=False)

                # unknown/invalid/disposable — move to next candidate, no retry
                logger.debug(
                    "Candidate %s for %s: %s — trying next",
                    email, unique_id, result.verdict,
                )
                continue

            # All candidates exhausted without a valid verdict
            await db.update_record_status(
                self.conn,
                unique_id,
                State.VALIDATION_FAILED,
                validation_attempts=len(candidates),
                zuhal_status=last_verdict,
                verdict=last_verdict,
            )
            logger.debug("All candidates failed for %s (last verdict: %s)", unique_id, last_verdict)


_GENERIC_PREFIXES: frozenset[str] = frozenset({
    "info", "contact", "hello", "admin",
    "support", "sales", "help",
})


def _name_matches_email(local: str, agent_name: str) -> bool:
    """True if the email local part fuzzy-matches the agent name (≥75)."""
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
    """Compute additive confidence score for a validated email.

    With strategy  (max 4): domain match, name match, not generic, not catch-all
    Without strategy (max 3): domain match, IS generic, not catch-all
    """
    local, _, domain = email.partition("@")
    score = 0

    # +1 domain fuzzy-matches candidate domain (≥85)
    if candidate_domain:
        d_norm = domain.rsplit(".", 1)[0].replace("-", "") if "." in domain else domain
        c_norm = candidate_domain.rsplit(".", 1)[0].replace("-", "") if "." in candidate_domain else candidate_domain
        if fuzz.ratio(d_norm, c_norm) >= 85:
            score += 1

    if strategy == "with":
        # +1 local part fuzzy-matches agent name
        if agent_name and _name_matches_email(local, agent_name):
            score += 1
        # +1 local part is NOT a generic prefix
        if local.lower() not in _GENERIC_PREFIXES:
            score += 1
        # +1 not catch-all
        if verdict == "valid":
            score += 1
    else:
        # +1 local part IS a known generic prefix
        if local.lower() in _GENERIC_PREFIXES:
            score += 1
        # +1 not catch-all
        if verdict == "valid":
            score += 1

    return score


def confidence_tier(score: int) -> str:
    if score >= 3:
        return "high"
    if score == 2:
        return "medium"
    return "low"

