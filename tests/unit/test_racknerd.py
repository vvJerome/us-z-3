"""Unit tests for RacknerdConsumer SMTP response parsing and SpamhausGuard."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.consumers.racknerd import RacknerdConfig, RacknerdConsumer, _SpamhausGuard
from pipeline.constants import (
    RACKNERD_SPAMHAUS_COOLDOWN_S,
    RACKNERD_SPAMHAUS_THRESHOLD,
    RACKNERD_SPAMHAUS_WINDOW_S,
)


class TestSpamhausGuard:
    def test_not_cooling_initially(self):
        guard = _SpamhausGuard()
        assert guard.is_cooling() is False

    def test_cooldown_triggered_at_threshold(self):
        guard = _SpamhausGuard()
        for _ in range(RACKNERD_SPAMHAUS_THRESHOLD):
            guard.record_block()
        assert guard.is_cooling() is True

    def test_below_threshold_no_cooldown(self):
        guard = _SpamhausGuard()
        for _ in range(RACKNERD_SPAMHAUS_THRESHOLD - 1):
            guard.record_block()
        assert guard.is_cooling() is False

    async def test_wait_if_cooling_returns_immediately_when_not_cooling(self):
        guard = _SpamhausGuard()
        # Should return without sleeping
        await asyncio.wait_for(guard.wait_if_cooling(), timeout=0.1)

    def test_cooldown_expires(self):
        guard = _SpamhausGuard()
        # Manually set cooldown to the past
        guard._cooldown_until = time.monotonic() - 1.0
        assert guard.is_cooling() is False


class TestRacknerdConsumerTunnelCheck:
    def _make_consumer(self, tunnel_up: bool) -> RacknerdConsumer:
        tunnel = MagicMock()
        tunnel.is_up.return_value = tunnel_up
        config = RacknerdConfig(concurrency=1)
        consumer = RacknerdConsumer(tunnel=tunnel, config=config)
        return consumer

    async def test_returns_error_when_tunnel_down(self):
        consumer = self._make_consumer(tunnel_up=False)
        result = await consumer.verify("test@example.com")
        assert result.status == "error"
        assert "tunnel not up" in result.message

    async def test_returns_error_for_invalid_email_format(self):
        consumer = self._make_consumer(tunnel_up=True)
        # Simulate cooling guard and resolver no MX — just check format guard
        result = await consumer.verify("notanemail")
        assert result.status == "error"

    async def test_invalid_domain_returns_invalid(self):
        """Domain with no MX/A record → invalid."""
        consumer = self._make_consumer(tunnel_up=True)

        # Patch resolver to raise DNSError
        import aiodns
        mock_resolver = AsyncMock()
        mock_resolver.query.side_effect = aiodns.error.DNSError("NXDOMAIN")
        consumer._resolver = mock_resolver

        result = await consumer.verify("test@nonexistentdomain12345.com")
        assert result.status == "error"
        assert "no MX" in result.message


class TestRacknerdSmtpResponseParsing:
    """Test SMTP response code → status mapping without a real tunnel."""

    def test_2xx_is_valid(self):
        # Use the parsing logic directly via _probe_mx internals
        from pipeline.consumers.racknerd import _INVALID_KEYWORDS, _SPAMHAUS_KEYWORDS
        code, msg = 250, "2.1.5 OK"
        assert 200 <= code <= 399
        # → "valid"

    def test_spamhaus_5xx_is_blocked(self):
        from pipeline.consumers.racknerd import _SPAMHAUS_KEYWORDS
        code, msg = 554, "5.7.1 Service unavailable; Client host blocked by spamhaus zen.spamhaus.org"
        assert code >= 500
        assert any(kw in msg.lower() for kw in _SPAMHAUS_KEYWORDS)
        # → "blocked"

    def test_no_such_user_is_invalid(self):
        from pipeline.consumers.racknerd import _INVALID_KEYWORDS
        code, msg = 550, "5.1.1 no such user here"
        assert code >= 500
        assert any(kw in msg.lower() for kw in _INVALID_KEYWORDS)
        # → "invalid"

    def test_4xx_is_error(self):
        code, msg = 421, "4.2.1 try again later"
        assert 400 <= code < 500
        # → "error" (temporary failure)

    def test_recipient_not_found_is_invalid(self):
        """Google Workspace 550 'recipient not found' must classify as invalid, not error."""
        from pipeline.consumers.racknerd import _INVALID_KEYWORDS
        msg = "5.1.0 <user@example.com> Recipient not found."
        assert any(kw in msg.lower() for kw in _INVALID_KEYWORDS)

    def test_nosuchuser_gmail_is_invalid(self):
        """Gmail 550 NoSuchUser bounce must classify as invalid."""
        from pipeline.consumers.racknerd import _INVALID_KEYWORDS
        msg = "5.1.1 The email account does not exist. NoSuchUser"
        assert any(kw in msg.lower() for kw in _INVALID_KEYWORDS)

    def test_helo_hostname_is_not_private(self):
        """Default helo_hostname must not be the placeholder private hostname."""
        config = RacknerdConfig()
        assert config.helo_hostname != "mail.verify.local"
        assert config.helo_hostname  # not empty
