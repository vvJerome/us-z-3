"""Unit tests for the generic exponential-backoff retry helper."""
from __future__ import annotations

import pytest

from pipeline.utils.backoff import with_backoff

pytestmark = pytest.mark.asyncio


async def test_returns_on_first_success():
    calls = 0

    async def op():
        nonlocal calls
        calls += 1
        return "ok"

    assert await with_backoff(op, jitter=0.0) == "ok"
    assert calls == 1


async def test_retries_then_succeeds():
    attempts = []
    retried = []

    async def op():
        attempts.append(1)
        if len(attempts) < 2:
            raise ValueError("transient")
        return "ok"

    # base_delay=0 so the sleep is instant; on_retry fires once.
    out = await with_backoff(op, max_attempts=3, base_delay=0.0, jitter=0.0,
                             on_retry=lambda *a: retried.append(a))
    assert out == "ok"
    assert len(attempts) == 2 and len(retried) == 1


async def test_non_retryable_raises_immediately():
    calls = 0

    async def op():
        nonlocal calls
        calls += 1
        raise KeyError("fatal")

    with pytest.raises(KeyError):
        await with_backoff(op, max_attempts=5, base_delay=0.0,
                           retryable=lambda exc: isinstance(exc, ValueError))
    assert calls == 1  # not retried


async def test_exhausts_attempts_and_reraises():
    calls = 0

    async def op():
        nonlocal calls
        calls += 1
        raise ValueError("always")

    with pytest.raises(ValueError):
        await with_backoff(op, max_attempts=3, base_delay=0.0, jitter=0.0)
    assert calls == 3
