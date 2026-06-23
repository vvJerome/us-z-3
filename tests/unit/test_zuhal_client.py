"""Unit tests for ZuhalClient retry/circuit-open conversion."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from pipeline.utils.rate_limiter import TokenBucket
from pipeline.models import PipelineHaltError
from pipeline.utils.zuhal_client import (
    ZuhalCircuitOpenError,
    ZuhalClient,
    ZuhalCreditsExhaustedError,
    _RetryableHTTPError,
)


def _client(max_attempts: int = 1) -> ZuhalClient:
    session = AsyncMock(spec=aiohttp.ClientSession)
    bucket = TokenBucket(capacity=100, refill_rate=100)
    return ZuhalClient(
        api_key="test_key",
        session=session,
        rate_limiter=bucket,
        concurrency=1,
        max_attempts=max_attempts,
    )


@patch("pipeline.utils.zuhal_client.asyncio.sleep", new=AsyncMock(return_value=None))
async def test_429_after_retries_raises_circuit_open_not_retryable():
    client = _client(max_attempts=1)
    client._call_api = AsyncMock(side_effect=_RetryableHTTPError(429))

    with pytest.raises(ZuhalCircuitOpenError):
        await client.validate("burned@example.com")


@patch("pipeline.utils.zuhal_client.asyncio.sleep", new=AsyncMock(return_value=None))
async def test_500_after_retries_raises_retryable_http_error_not_circuit_open():
    client = _client(max_attempts=1)
    client._call_api = AsyncMock(side_effect=_RetryableHTTPError(500))

    with pytest.raises(_RetryableHTTPError) as exc_info:
        await client.validate("burned@example.com")

    assert exc_info.value.status == 500


async def test_credits_exhausted_flag_short_circuits_without_calling_api():
    """Once _credits_exhausted is set, validate() raises ZuhalCreditsExhaustedError
    (recoverable, NOT PipelineHaltError) without touching the API."""
    client = _client(max_attempts=1)
    client._call_api = AsyncMock(side_effect=AssertionError("API must not be called"))
    client._credits_exhausted = True

    with pytest.raises(ZuhalCreditsExhaustedError):
        await client.validate("a@example.com")
    client._call_api.assert_not_called()


async def test_credits_exhausted_is_not_pipeline_halt():
    """A 402 degrades (recoverable) instead of halting the whole pipeline."""
    assert not issubclass(ZuhalCreditsExhaustedError, PipelineHaltError)
