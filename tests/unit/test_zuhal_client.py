"""Unit tests for ZuhalClient retry/circuit-open conversion."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

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


class TestCheckCredits:
    def _mock_post(self, client: ZuhalClient, status: int, body: dict | None = None):
        """Wire client.session.post as an async context manager returning a mock response."""
        resp = AsyncMock()
        resp.status = status
        resp.json = AsyncMock(return_value=body or {})

        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        client.session.post = MagicMock(return_value=cm)

    async def test_200_with_credits_returns_count(self):
        client = _client()
        self._mock_post(client, 200, {"data": {"remaining_credits": 1500}})

        result = await client.check_credits()
        assert result == 1500

    async def test_200_without_credits_field_returns_none(self):
        client = _client()
        self._mock_post(client, 200, {"data": {}})

        result = await client.check_credits()
        assert result is None

    async def test_402_raises_pipeline_halt(self):
        from pipeline.models import PipelineHaltError
        client = _client()
        self._mock_post(client, 402)

        with pytest.raises(PipelineHaltError, match="exhausted"):
            await client.check_credits()

    async def test_401_raises_pipeline_halt(self):
        from pipeline.models import PipelineHaltError
        client = _client()
        self._mock_post(client, 401)

        with pytest.raises(PipelineHaltError, match="invalid or expired"):
            await client.check_credits()

    async def test_dry_run_skips_call(self):
        session = AsyncMock(spec=aiohttp.ClientSession)
        bucket = TokenBucket(capacity=100, refill_rate=100)
        client = ZuhalClient("key", session, bucket, dry_run=True)

        result = await client.check_credits()
        assert result is None
        session.post.assert_not_called()


class TestBulkValidateJobCreatedCallback:
    async def test_on_job_created_called_with_job_id(self):
        session = AsyncMock(spec=aiohttp.ClientSession)
        bucket = TokenBucket(capacity=100, refill_rate=100)
        client = ZuhalClient("key", session, bucket)

        upload_resp = AsyncMock()
        upload_resp.status = 200
        upload_resp.json = AsyncMock(return_value={"data": {"job_id": "bulk_job_42"}})
        upload_resp.raise_for_status = MagicMock()

        status_resp = AsyncMock()
        status_resp.status = 200
        status_resp.json = AsyncMock(return_value={"data": {"status": "completed", "percentage_complete": 100}})
        status_resp.raise_for_status = MagicMock()

        download_resp = AsyncMock()
        download_resp.status = 200
        download_resp.json = AsyncMock(return_value={"data": {"download_link": "https://example.com/result.csv"}})
        download_resp.raise_for_status = MagicMock()

        csv_resp = AsyncMock()
        csv_resp.status = 200
        csv_resp.raise_for_status = MagicMock()
        csv_resp.text = AsyncMock(return_value="email,email_status\ntest@example.com,valid\n")

        def _cm(resp):
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=resp)
            cm.__aexit__ = AsyncMock(return_value=False)
            return cm

        session.post = MagicMock(return_value=_cm(upload_resp))
        session.get = MagicMock(side_effect=[
            _cm(status_resp),
            _cm(download_resp),
            _cm(csv_resp),
        ])

        received: list[str] = []

        async def _on_job_created(job_id: str) -> None:
            received.append(job_id)

        with patch("pipeline.utils.zuhal_client.asyncio.sleep", new=AsyncMock(return_value=None)):
            results = await client.bulk_validate(
                ["test@example.com"],
                on_job_created=_on_job_created,
            )

        assert received == ["bulk_job_42"]
        assert results == {"test@example.com": "valid"}

    async def test_on_job_created_not_called_when_dry_run(self):
        session = AsyncMock(spec=aiohttp.ClientSession)
        bucket = TokenBucket(capacity=100, refill_rate=100)
        client = ZuhalClient("key", session, bucket, dry_run=True)

        received: list[str] = []

        async def _on_job_created(job_id: str) -> None:
            received.append(job_id)

        await client.bulk_validate(["test@example.com"], on_job_created=_on_job_created)
        assert received == []


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
