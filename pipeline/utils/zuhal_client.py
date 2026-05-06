from __future__ import annotations

import asyncio
import logging
import random
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
        self._base, self._max_delay = SERVICE_BACKOFF["zuhal"]
        self._sem = asyncio.Semaphore(concurrency)
        self._breaker = aiobreaker.CircuitBreaker(
            fail_max=5,
            timeout_duration=timedelta(seconds=600),
            exclude=[asyncio.TimeoutError],
        )

    async def validate(self, email: str) -> ValidationResult:
        async with self._sem:
            return await self._validate_inner(email)

    async def _validate_inner(self, email: str) -> ValidationResult:
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
                lambda: self._breaker.call(self._call_api, email),
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
                raise PipelineHaltError("Zuhal credit balance exhausted (402)")

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


class ZuhalCircuitOpenError(Exception):
    """Raised when Zuhal's circuit breaker is open — service is temporarily unavailable."""


class _RetryableHTTPError(Exception):
    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"HTTP {status}")


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, _RetryableHTTPError):
        return exc.status in (429, 500, 503)
    return isinstance(exc, (aiohttp.ClientError, asyncio.TimeoutError))
