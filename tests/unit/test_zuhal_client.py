"""Unit tests for ZuhalClient retry/circuit-open conversion."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aiobreaker
import aiohttp
import pytest

from pipeline.utils.rate_limiter import TokenBucket
from pipeline.models import PipelineHaltError
from pipeline.utils.zuhal_client import (
    ZuhalCircuitOpenError,
    ZuhalClient,
    ZuhalCreditsExhaustedError,
    _is_retryable,
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


def _cm(resp):
    """Wrap a mock response as an async context manager, matching aiohttp's
    `async with session.post(...) as resp:` usage."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


def _resp(status: int, json_body: dict | None = None, text_body: str | None = None):
    resp = AsyncMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_body or {})
    if text_body is not None:
        resp.text = AsyncMock(return_value=text_body)
    resp.raise_for_status = MagicMock()
    return resp


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

    async def test_200_with_malformed_json_returns_none(self):
        client = _client()
        self._mock_post(client, 200)
        client.session.post.return_value.__aenter__.return_value.json = AsyncMock(
            side_effect=ValueError("not json")
        )

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

    async def test_5xx_returns_none_key_still_considered_valid(self):
        """Anything other than 401/402/200 (e.g. a transient 500) means the key itself
        is fine — just no credit count available; must not raise."""
        client = _client()
        self._mock_post(client, 500)

        result = await client.check_credits()
        assert result is None

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


class TestValidateInner:
    async def test_dry_run_returns_synthetic_valid_result(self):
        session = AsyncMock(spec=aiohttp.ClientSession)
        bucket = TokenBucket(capacity=100, refill_rate=100)
        client = ZuhalClient("key", session, bucket, dry_run=True)

        result = await client.validate("a@example.com")

        assert result.verdict == "valid"
        session.post.assert_not_called()

    @patch("pipeline.utils.zuhal_client.asyncio.sleep", new=AsyncMock(return_value=None))
    async def test_circuit_breaker_open_raises_zuhal_circuit_open(self):
        import datetime
        client = _client(max_attempts=1)
        client._breaker.call_async = AsyncMock(
            side_effect=aiobreaker.CircuitBreakerError("open", datetime.datetime.now())
        )

        with pytest.raises(ZuhalCircuitOpenError):
            await client.validate("a@example.com")

    @patch("pipeline.utils.zuhal_client.asyncio.sleep", new=AsyncMock(return_value=None))
    async def test_successful_call_returns_validation_result(self):
        client = _client(max_attempts=1)
        client._call_api = AsyncMock(return_value=None)
        from pipeline.models import ValidationResult
        expected = ValidationResult(
            email="a@example.com", verdict="valid", score=0.0,
            is_disposable=False, raw_status="success", http_status=200,
        )
        client._call_api = AsyncMock(return_value=expected)

        result = await client.validate("a@example.com")
        assert result is expected


class TestCallApi:
    async def test_400_raises_pipeline_halt_with_body(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(400, text_body="bad email format")))

        with pytest.raises(PipelineHaltError, match="code bug"):
            await client._call_api("a@example.com")

    async def test_401_raises_pipeline_halt(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(401)))

        with pytest.raises(PipelineHaltError, match="invalid or expired"):
            await client._call_api("a@example.com")

    async def test_402_sets_credits_exhausted_and_raises(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(402)))

        with pytest.raises(ZuhalCreditsExhaustedError):
            await client._call_api("a@example.com")
        assert client._credits_exhausted is True

    async def test_429_raises_retryable_http_error(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(429)))

        with pytest.raises(_RetryableHTTPError) as exc_info:
            await client._call_api("a@example.com")
        assert exc_info.value.status == 429

    async def test_503_raises_retryable_http_error(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(503)))

        with pytest.raises(_RetryableHTTPError) as exc_info:
            await client._call_api("a@example.com")
        assert exc_info.value.status == 503

    async def test_success_maps_disposable_to_verdict(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(200, {
            "status": "success",
            "data": {"email_status": "valid", "is_disposable": True, "remaining_credits": 500},
        })))

        result = await client._call_api("a@example.com")
        assert result.verdict == "disposable"
        assert result.is_disposable is True

    async def test_success_low_credits_logs_warning(self, caplog):
        import logging
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(200, {
            "status": "success",
            "data": {"email_status": "valid", "remaining_credits": 50},
        })))

        with caplog.at_level(logging.WARNING, logger="pipeline.zuhal_client"):
            await client._call_api("a@example.com")

        assert any("credits low" in r.message for r in caplog.records)

    async def test_success_healthy_credits_logs_debug_not_warning(self, caplog):
        import logging
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(200, {
            "status": "success",
            "data": {"email_status": "valid", "remaining_credits": 50000},
        })))

        with caplog.at_level(logging.DEBUG, logger="pipeline.zuhal_client"):
            await client._call_api("a@example.com")

        assert any("credits remaining" in r.message for r in caplog.records)
        assert not any(r.levelno >= logging.WARNING for r in caplog.records)


class TestBulkValidateEdgeCases:
    async def test_credits_exhausted_short_circuits(self):
        client = _client()
        client._credits_exhausted = True
        with pytest.raises(ZuhalCreditsExhaustedError):
            await client.bulk_validate(["a@example.com"])

    async def test_dry_run_returns_all_valid(self):
        session = AsyncMock(spec=aiohttp.ClientSession)
        bucket = TokenBucket(capacity=100, refill_rate=100)
        client = ZuhalClient("key", session, bucket, dry_run=True)

        result = await client.bulk_validate(["a@example.com", "b@example.com"])
        assert result == {"a@example.com": "valid", "b@example.com": "valid"}

    async def test_upload_401_raises_pipeline_halt(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(401)))

        with pytest.raises(PipelineHaltError):
            await client.bulk_validate(["a@example.com"])

    async def test_upload_402_sets_credits_exhausted(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(402)))

        with pytest.raises(ZuhalCreditsExhaustedError):
            await client.bulk_validate(["a@example.com"])
        assert client._credits_exhausted is True

    async def test_missing_job_id_returns_empty(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(200, {"data": {}})))

        result = await client.bulk_validate(["a@example.com"])
        assert result == {}

    @patch("pipeline.utils.zuhal_client.asyncio.sleep", new=AsyncMock(return_value=None))
    async def test_job_failed_status_returns_empty(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(200, {"data": {"job_id": "j1"}})))
        client.session.get = MagicMock(return_value=_cm(
            _resp(200, {"data": {"status": "failed", "percentage_complete": 40}})
        ))

        result = await client.bulk_validate(["a@example.com"])
        assert result == {}

    @patch("pipeline.utils.zuhal_client.asyncio.sleep", new=AsyncMock(return_value=None))
    async def test_poll_deadline_exceeded_returns_empty(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(200, {"data": {"job_id": "j1"}})))
        client.session.get = MagicMock(return_value=_cm(
            _resp(200, {"data": {"status": "polling", "percentage_complete": 10}})
        ))

        with patch("pipeline.utils.zuhal_client.asyncio.get_running_loop") as mock_loop:
            loop = MagicMock()
            loop.time.side_effect = [0.0, 999999.0]  # first call sets deadline, second is "now"
            mock_loop.return_value = loop
            result = await client.bulk_validate(["a@example.com"], max_poll_minutes=1)

        assert result == {}

    @patch("pipeline.utils.zuhal_client.asyncio.sleep", new=AsyncMock(return_value=None))
    async def test_download_missing_link_returns_empty(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(200, {"data": {"job_id": "j1"}})))
        client.session.get = MagicMock(side_effect=[
            _cm(_resp(200, {"data": {"status": "complete", "percentage_complete": 100}})),
            _cm(_resp(200, {"data": {}})),
        ])

        result = await client.bulk_validate(["a@example.com"])
        assert result == {}

    @patch("pipeline.utils.zuhal_client.asyncio.sleep", new=AsyncMock(return_value=None))
    async def test_on_poll_callback_invoked(self):
        client = _client()
        client.session.post = MagicMock(return_value=_cm(_resp(200, {"data": {"job_id": "j1"}})))
        client.session.get = MagicMock(side_effect=[
            _cm(_resp(200, {"data": {"status": "completed", "percentage_complete": 100}})),
            _cm(_resp(200, {"data": {"download_link": "https://example.com/r.csv"}})),
            _cm(_resp(200, text_body="email,email_status\na@example.com,accept-all\n")),
        ])

        polled: list[bool] = []

        async def _on_poll() -> None:
            polled.append(True)

        result = await client.bulk_validate(["a@example.com"], on_poll=_on_poll)

        assert polled == [True]
        assert result == {"a@example.com": "catch_all"}  # accept-all normalized


class TestIsRetryable:
    def test_429_is_retryable(self):
        assert _is_retryable(_RetryableHTTPError(429)) is True

    def test_500_is_retryable(self):
        assert _is_retryable(_RetryableHTTPError(500)) is True

    def test_503_is_retryable(self):
        assert _is_retryable(_RetryableHTTPError(503)) is True

    def test_client_error_is_retryable(self):
        assert _is_retryable(aiohttp.ClientError()) is True

    def test_timeout_error_is_retryable(self):
        import asyncio
        assert _is_retryable(asyncio.TimeoutError()) is True

    def test_generic_exception_is_not_retryable(self):
        assert _is_retryable(ValueError("nope")) is False
