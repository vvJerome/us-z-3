from __future__ import annotations

import asyncio
import time


class TokenBucket:
    """Async token bucket rate limiter.

    Enforces a hard ceiling of `capacity` calls per refill period.
    Tokens refill continuously at `refill_rate` tokens/second.
    """

    def __init__(
        self,
        capacity: int,
        refill_rate: float,
        *,
        initial_tokens: float | None = None,
    ) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate  # tokens per second
        # Default: start full (backward compatible).
        # Pass initial_tokens=0 to prevent startup burst for high-concurrency callers.
        self._tokens = float(capacity) if initial_tokens is None else initial_tokens
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    async def acquire(self) -> None:
        async with self._lock:
            self._refill()

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return

            wait = (1.0 - self._tokens) / self.refill_rate
            await asyncio.sleep(wait)
            self._refill()
            self._tokens -= 1.0


# CircuitBreaker removed — use aiobreaker.CircuitBreaker in zuhal_client.py directly.
# aiobreaker supports half-open canary probes unlike the previous hand-rolled version.
