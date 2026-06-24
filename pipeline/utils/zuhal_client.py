from __future__ import annotations

import asyncio
import csv
import io
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import timedelta

import aiohttp
import aiobreaker

from pipeline.models import PipelineHaltError, ValidationResult
from pipeline.utils.backoff import SERVICE_BACKOFF, with_backoff
from pipeline.utils.rate_limiter import TokenBucket

logger = logging.getLogger("pipeline.zuhal_client")


class ZuhalClient:
    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        rate_limiter: TokenBucket,
        *,
        concurrency: int = 5,
        dry_run: bool = False,
        max_attempts: int = 3,
        jitter: float = 0.2,
    ) -> None:
        self.api_key = api_key
        self.session = session
        self.rate_limiter = rate_limiter
        self.dry_run = dry_run
        self.max_attempts = max_attempts
        self.jitter = jitter
        self._credits_exhausted = False  # set on first 402; degrades instead of halting
        self._base, self._max_delay = SERVICE_BACKOFF["zuhal"]
        self._sem = asyncio.Semaphore(concurrency)
        self._breaker = aiobreaker.CircuitBreaker(
            fail_max=5,
            timeout_duration=timedelta(seconds=600),
            exclude=[asyncio.TimeoutError],
        )

    async def check_credits(self) -> int | None:
        """Probe Zuhal at startup to confirm credits are available.

        Returns remaining credit count if the API reports it, else None.
        Raises PipelineHaltError on 401 (bad key) or 402 (no credits).
        Bypasses circuit breaker and rate limiter — one-off startup call only.
        """
        if self.dry_run:
            logger.info("Zuhal credit check skipped (dry-run)")
            return None

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        async with self.session.post(
            "https://zuhal.io/api/v1/verify",
            json={"email": "credits-probe@example.invalid"},
            headers=headers,
        ) as resp:
            if resp.status == 401:
                raise PipelineHaltError("Zuhal API key invalid or expired (401)")
            if resp.status == 402:
                raise PipelineHaltError(
                    "Zuhal credit balance exhausted — top up your account before running"
                )
            # 200: extract remaining credits; anything else (429, 5xx) means key is valid
            if resp.status == 200:
                try:
                    data = await resp.json()
                    inner = data.get("data", {})
                    return inner.get("remaining_credits")
                except Exception:
                    return None
            return None

    async def validate(self, email: str) -> ValidationResult:
        async with self._sem:
            return await self._validate_inner(email)

    async def _validate_inner(self, email: str) -> ValidationResult:
        if self._credits_exhausted:
            # Already saw a 402 — short-circuit so concurrent workers don't each
            # re-hit the dead endpoint. The dispatcher defers the record.
            raise ZuhalCreditsExhaustedError()
        if self.dry_run:
            return ValidationResult(
                email=email,
                verdict="valid",
                score=0.99,
                is_disposable=False,
                raw_status="success",
                http_status=200,
            )

        # Acquire rate limit token
        await self.rate_limiter.acquire()

        # Anti-fingerprinting random delay
        await asyncio.sleep(random.uniform(0.5, 2.5))

        try:
            return await with_backoff(
                lambda: self._breaker.call_async(self._call_api, email),
                max_attempts=self.max_attempts,
                base_delay=self._base,
                max_delay=self._max_delay,
                jitter=self.jitter,
                retryable=_is_retryable,
                on_retry=lambda attempt, exc, delay: logger.debug(
                    "Zuhal retry %d for %s: %s (wait %.1fs)", attempt, email, exc, delay,
                ),
            )
        except aiobreaker.CircuitBreakerError:
            logger.warning("Zuhal circuit breaker open — records will be re-queued for retry")
            raise ZuhalCircuitOpenError()
        except _RetryableHTTPError as exc:
            # 429 storms must always re-queue, never burn the record. Backoff
            # exhaustion before the breaker opens (fail_max=5) used to bubble
            # this exception up to the dispatcher's generic Exception handler,
            # which marked the record VALIDATION_FAILED with zuhal_status='error'.
            if exc.status == 429:
                logger.warning("Zuhal 429 after %d retries — re-queueing %s", self.max_attempts, email)
                raise ZuhalCircuitOpenError() from exc
            raise

    async def _call_api(self, email: str) -> ValidationResult:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        async with self.session.post(
            "https://zuhal.io/api/v1/verify",
            json={"email": email},
            headers=headers,
        ) as resp:
            status = resp.status

            if status == 400:
                body = await resp.text()
                raise PipelineHaltError(f"Zuhal bad request (400) — code bug: {body}")

            if status == 401:
                raise PipelineHaltError("Zuhal API key invalid or expired (401)")

            if status == 402:
                self._credits_exhausted = True
                raise ZuhalCreditsExhaustedError()

            if status == 429:
                logger.warning("Zuhal 429 — circuit breaker will count this failure")
                raise _RetryableHTTPError(429)

            if status in (500, 503):
                raise _RetryableHTTPError(status)

            resp.raise_for_status()
            data = await resp.json()

        inner = data.get("data", {})
        verdict = inner.get("email_status", "unknown")
        is_disposable = inner.get("is_disposable", False)

        if is_disposable:
            verdict = "disposable"

        remaining = inner.get("remaining_credits")
        if remaining is not None:
            if remaining < 1000:
                logger.warning("Zuhal credits low: %d remaining", remaining)
            else:
                logger.debug("Zuhal credits remaining: %d", remaining)

        return ValidationResult(
            email=email,
            verdict=verdict,
            score=0.0,
            is_disposable=is_disposable,
            raw_status=data.get("status", ""),
            http_status=status,
        )

    async def bulk_validate(
        self,
        emails: list[str],
        poll_interval_s: float = 30.0,
        max_poll_minutes: int = 120,
        on_poll: Callable[[], Awaitable[None]] | None = None,
        on_job_created: Callable[[str], Awaitable[None]] | None = None,
    ) -> dict[str, str]:
        """Upload emails as CSV, poll until complete, return {email: verdict} mapping.

        Falls back to empty dict on any non-auth error so callers can retry via
        single-verify path.
        """
        if self._credits_exhausted:
            raise ZuhalCreditsExhaustedError()
        if self.dry_run:
            return {e: "valid" for e in emails}

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["email"])
        for e in emails:
            writer.writerow([e])
        csv_bytes = buf.getvalue().encode()

        headers = {"Authorization": f"Bearer {self.api_key}"}

        # Upload
        form = aiohttp.FormData()
        form.add_field("file", csv_bytes, filename="emails.csv", content_type="text/csv")
        async with self.session.post(
            "https://zuhal.io/api/v1/bulk/upload",
            data=form,
            headers=headers,
        ) as resp:
            if resp.status == 401:
                raise PipelineHaltError("Zuhal API key invalid or expired (401)")
            if resp.status == 402:
                self._credits_exhausted = True
                raise ZuhalCreditsExhaustedError()
            resp.raise_for_status()
            data = await resp.json()

        # API wraps the payload in a "data" envelope; fall back to top-level for
        # older response shapes so both formats are handled transparently.
        _payload = data.get("data") if isinstance(data.get("data"), dict) else data
        job_id = _payload.get("job_id") or _payload.get("id") or _payload.get("file_id")
        if not job_id:
            logger.warning("Zuhal bulk upload returned no job_id: %s", data)
            return {}

        logger.info("Zuhal bulk job %s — %d emails uploaded", job_id, len(emails))

        if on_job_created:
            await on_job_created(job_id)

        # Poll
        deadline = asyncio.get_running_loop().time() + max_poll_minutes * 60
        while True:
            await asyncio.sleep(poll_interval_s)

            if on_poll:
                await on_poll()

            async with self.session.get(
                f"https://zuhal.io/api/v1/bulk/status/{job_id}",
                headers=headers,
            ) as resp:
                resp.raise_for_status()
                status_data = await resp.json()

            _status_payload = status_data.get("data") if isinstance(status_data.get("data"), dict) else status_data
            status = _status_payload.get("status", "")
            pct = _status_payload.get("percentage_complete", 0)
            logger.info("Zuhal bulk %s: %s (%s%%)", job_id, status, pct)

            _status_lower = status.lower()
            if _status_lower in ("complete", "completed"):
                break
            if _status_lower in ("error", "failed"):
                logger.warning("Zuhal bulk job %s failed: %s", job_id, status_data)
                return {}
            if asyncio.get_running_loop().time() > deadline:
                logger.warning("Zuhal bulk job %s timed out after %d min", job_id, max_poll_minutes)
                return {}

        # Download — endpoint returns JSON with a download_link, not the CSV directly
        async with self.session.get(
            f"https://zuhal.io/api/v1/bulk/download/{job_id}",
            headers=headers,
        ) as resp:
            resp.raise_for_status()
            download_data = await resp.json()

        _dl_payload = download_data.get("data") if isinstance(download_data.get("data"), dict) else download_data
        download_url = _dl_payload.get("download_link") or _dl_payload.get("url") or _dl_payload.get("link")
        if not download_url:
            logger.warning("Zuhal bulk download returned no download_link for job %s: %s", job_id, download_data)
            return {}

        async with self.session.get(download_url) as resp:
            resp.raise_for_status()
            content = await resp.text(encoding="utf-8-sig")

        results: dict[str, str] = {}
        for row in csv.DictReader(io.StringIO(content)):
            email = (row.get("email") or row.get("Email") or "").strip().lower()
            verdict = (
                row.get("email_status") or row.get("status") or
                row.get("Email Status") or "unknown"
            ).strip().lower()
            if email:
                if verdict == "accept-all":
                    verdict = "catch_all"
                results[email] = verdict

        logger.info("Zuhal bulk %s: downloaded %d results", job_id, len(results))
        return results


class ZuhalCircuitOpenError(Exception):
    """Raised when Zuhal's circuit breaker is open — service is temporarily unavailable."""


class ZuhalCreditsExhaustedError(Exception):
    """Raised on a Zuhal 402 — paid balance is out. Recoverable: the worker stops
    and leaves records in NEEDS_ZUHAL for resume after top-up, rather than halting
    the whole pipeline (which would also kill the producer + SMTP work in flight)."""


class _RetryableHTTPError(Exception):
    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"HTTP {status}")


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, _RetryableHTTPError):
        return exc.status in (429, 500, 503)
    return isinstance(exc, (aiohttp.ClientError, asyncio.TimeoutError))
