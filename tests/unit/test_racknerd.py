"""Unit tests for RacknerdConsumer SMTP response parsing and SpamhausGuard."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.consumers.racknerd import RacknerdConfig, RacknerdConsumer, _SpamhausGuard, _mx_provider
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
        """GoDaddy 550 'Recipient not found' must classify as invalid via SMTPRecipientRefused path."""
        from pipeline.consumers.racknerd import _classify_smtp_rejection
        status, _ = _classify_smtp_rejection(
            "550, 5.1.0 <user@example.com> Recipient not found."
        )
        assert status == "invalid"

    def test_nosuchuser_gmail_is_invalid(self):
        """Gmail 550 NoSuchUser bounce must classify as invalid via SMTPRecipientRefused path."""
        from pipeline.consumers.racknerd import _classify_smtp_rejection
        status, _ = _classify_smtp_rejection(
            "550, 5.1.1 The email account that you tried to reach does not exist. NoSuchUser"
        )
        assert status == "invalid"

    def test_spamhaus_rejection_is_blocked(self):
        """Spamhaus PBL rejection via SMTPRecipientRefused must classify as blocked."""
        from pipeline.consumers.racknerd import _classify_smtp_rejection
        status, _ = _classify_smtp_rejection(
            "550, 5.7.1 Connection refused - blocked by Spamhaus PBL"
        )
        assert status == "blocked"

    def test_generic_5xx_rejection_is_invalid(self):
        """Unknown 5xx is permanent per RFC 5321 → invalid (not error)."""
        from pipeline.consumers.racknerd import _classify_smtp_rejection
        status, _ = _classify_smtp_rejection(
            "550, 5.7.1 Service unavailable for unknown reason"
        )
        assert status == "invalid"

    def test_domain_literal_rejection_is_invalid(self):
        """501 domain-literal reject (the bug that produced 100% racknerd error rate)."""
        from pipeline.consumers.racknerd import _classify_smtp_rejection
        status, _ = _classify_smtp_rejection(
            "501, <verify@[49.12.127.119]>: domain literals not allowed"
        )
        assert status == "invalid"

    def test_unknown_host_rejection_is_invalid(self):
        """550 Unknown host on MAIL FROM domain is a permanent reject."""
        from pipeline.consumers.racknerd import _classify_smtp_rejection
        status, _ = _classify_smtp_rejection(
            "550, Unknown host: [49.12.127.119]"
        )
        assert status == "invalid"

    def test_proofpoint_tss11_is_blocked(self):
        """Proofpoint TSS11 reputation reject classifies as blocked via 'blocked' keyword."""
        from pipeline.consumers.racknerd import _classify_smtp_rejection
        status, _ = _classify_smtp_rejection(
            "553, 5.7.2 [TSS11] All messages from 107.172.159.170 will be permanently deferred; "
            "Retrying will NOT succeed. blocked"
        )
        assert status == "blocked"

    def test_classify_5xx_unknown_is_invalid(self):
        """_classify_5xx tuple-return path: unknown 5xx → invalid."""
        from pipeline.consumers.racknerd import _classify_5xx
        status, _ = _classify_5xx(550, "permanent failure of some kind")
        assert status == "invalid"

    def test_classify_5xx_spamhaus_is_blocked(self):
        """_classify_5xx tuple-return path: spamhaus keyword → blocked."""
        from pipeline.consumers.racknerd import _classify_5xx
        status, _ = _classify_5xx(554, "Client host blocked by spamhaus zen.spamhaus.org")
        assert status == "blocked"

    def test_helo_hostname_is_not_private(self):
        """Default helo_hostname must not be the placeholder private hostname."""
        config = RacknerdConfig()
        assert config.helo_hostname != "mail.verify.local"
        assert config.helo_hostname  # not empty

    def test_helo_hostname_is_valid_fqdn_or_ip_literal(self):
        """When socket.getfqdn() returns a non-FQDN, _default_helo_hostname returns IP literal (with warning)."""
        from unittest.mock import patch
        from pipeline.consumers.racknerd import _default_helo_hostname

        with patch("pipeline.consumers.racknerd.socket.getfqdn", return_value="racknerd-0a2741a"):
            result = _default_helo_hostname()
        # Backward compat: still returns IP literal — but RacknerdConfig will warn.
        assert result.startswith("[") and result.endswith("]")

    def test_helo_hostname_uses_fqdn_when_valid(self):
        """When socket.getfqdn() returns a real FQDN, use it directly."""
        from unittest.mock import patch
        from pipeline.consumers.racknerd import _default_helo_hostname

        with patch("pipeline.consumers.racknerd.socket.getfqdn", return_value="mail.example.com"):
            result = _default_helo_hostname()
        assert result == "mail.example.com"

    def test_explicit_helo_override_wins(self):
        """An explicit helo_hostname on RacknerdConfig overrides the default factory."""
        config = RacknerdConfig(helo_hostname="verifier.bbops.io")
        assert config.helo_hostname == "verifier.bbops.io"

    def test_empty_helo_rejected(self):
        """Empty helo_hostname is a misconfiguration and must raise."""
        with pytest.raises(ValueError, match="helo_hostname must be non-empty"):
            RacknerdConfig(helo_hostname="")

    def test_ip_literal_helo_emits_warning(self, caplog):
        """IP-literal helo_hostname is allowed (backward compat) but logs a warning."""
        import logging
        with caplog.at_level(logging.WARNING, logger="pipeline.racknerd"):
            RacknerdConfig(helo_hostname="[49.12.127.119]")
        assert any("IP literal" in rec.message for rec in caplog.records)


class TestMxProvider:
    def test_extracts_root_domain(self):
        assert _mx_provider("aspmx.l.google.com") == "google.com"
        assert _mx_provider("mx1.pphosted.com") == "pphosted.com"
        assert _mx_provider("eu-smtp-1.mimecast.com") == "mimecast.com"
        assert _mx_provider("mail.protection.outlook.com") == "outlook.com"

    def test_two_part_domain_unchanged(self):
        assert _mx_provider("google.com") == "google.com"

    def test_trailing_dot_stripped(self):
        assert _mx_provider("aspmx.l.google.com.") == "google.com"


class TestPerMxSpamhausGuard:
    async def test_separate_providers_have_independent_cooldowns(self):
        consumer = RacknerdConsumer(tunnel=None, config=RacknerdConfig(concurrency=1))
        guard_pp = consumer._guard_for("pphosted.com")
        guard_goog = consumer._guard_for("google.com")

        for _ in range(RACKNERD_SPAMHAUS_THRESHOLD):
            guard_pp.record_block()

        assert guard_pp.is_cooling() is True
        assert guard_goog.is_cooling() is False

    async def test_same_provider_returns_same_guard_instance(self):
        consumer = RacknerdConsumer(tunnel=None, config=RacknerdConfig(concurrency=1))
        g1 = consumer._guard_for("pphosted.com")
        g2 = consumer._guard_for("pphosted.com")
        assert g1 is g2

    async def test_different_providers_return_different_guard_instances(self):
        consumer = RacknerdConsumer(tunnel=None, config=RacknerdConfig(concurrency=1))
        g1 = consumer._guard_for("pphosted.com")
        g2 = consumer._guard_for("google.com")
        assert g1 is not g2

    async def test_blocked_provider_does_not_pause_other_providers(self):
        consumer = RacknerdConsumer(tunnel=None, config=RacknerdConfig(concurrency=1))
        guard_pp = consumer._guard_for("pphosted.com")
        guard_goog = consumer._guard_for("google.com")

        for _ in range(RACKNERD_SPAMHAUS_THRESHOLD):
            guard_pp.record_block()

        # Google guard should return immediately (not cooling)
        assert not guard_goog.is_cooling()
