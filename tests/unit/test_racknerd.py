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


def _fake_smtp(rcpt_return=(250, "OK"), rcpt_side_effect=None):
    """A mock aiosmtplib.SMTP client with the EHLO/MAIL/RCPT/RSET/QUIT sequence stubbed."""
    smtp = MagicMock()
    smtp.ehlo = AsyncMock(return_value=(250, "OK"))
    smtp.mail = AsyncMock(return_value=(250, "OK"))
    if rcpt_side_effect is not None:
        smtp.rcpt = AsyncMock(side_effect=rcpt_side_effect)
    else:
        smtp.rcpt = AsyncMock(return_value=rcpt_return)
    smtp.rset = AsyncMock(return_value=(250, "OK"))
    smtp.quit = AsyncMock(return_value=(221, "Bye"))
    return smtp


def _probe_consumer() -> RacknerdConsumer:
    """A RacknerdConsumer for calling probe internals directly — helo_hostname pinned
    so construction never touches the network via _default_helo_hostname(), and a fake
    resolver so it never constructs a real aiodns.DNSResolver() either."""
    return RacknerdConsumer(
        tunnel=None,
        config=RacknerdConfig(helo_hostname="test.verify.local"),
        resolver=AsyncMock(),
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

    async def test_wait_if_cooling_actually_sleeps_while_cooling(self):
        guard = _SpamhausGuard()
        guard._cooldown_until = time.monotonic() + 0.05
        start = time.monotonic()
        await guard.wait_if_cooling()
        assert time.monotonic() - start >= 0.04

    def test_events_outside_window_are_evicted_before_threshold_check(self):
        guard = _SpamhausGuard()
        # Backdate an old event past the sliding window so record_block()'s
        # eviction loop actually pops it instead of just appending forever.
        guard._events.append(time.monotonic() - RACKNERD_SPAMHAUS_WINDOW_S - 1)
        guard.record_block()
        assert len(guard._events) == 1  # old event evicted, only the new one remains

    def test_cooldown_expires(self):
        guard = _SpamhausGuard()
        # Manually set cooldown to the past
        guard._cooldown_until = time.monotonic() - 1.0
        assert guard.is_cooling() is False


class TestRacknerdConsumerTunnelCheck:
    def _make_consumer(self, tunnel_up: bool) -> RacknerdConsumer:
        tunnel = MagicMock()
        tunnel.is_up.return_value = tunnel_up
        config = RacknerdConfig(concurrency=1, helo_hostname="test.verify.local")
        consumer = RacknerdConsumer(tunnel=tunnel, config=config, resolver=AsyncMock())
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

    async def test_2xx_is_valid(self):
        """End-to-end through _run_smtp_probe, not just the raw classification helper."""
        smtp = _fake_smtp(rcpt_return=(250, "2.1.5 OK"))
        status, msg = await _probe_consumer()._run_smtp_probe(smtp, "a@b.com", "mx.b.com")
        assert status == "valid"
        assert "250" in msg

    async def test_spamhaus_5xx_is_blocked(self):
        smtp = _fake_smtp(rcpt_return=(
            554, "5.7.1 Service unavailable; Client host blocked by spamhaus zen.spamhaus.org",
        ))
        status, _ = await _probe_consumer()._run_smtp_probe(smtp, "a@b.com", "mx.b.com")
        assert status == "blocked"

    async def test_no_such_user_is_invalid(self):
        smtp = _fake_smtp(rcpt_return=(550, "5.1.1 no such user here"))
        status, _ = await _probe_consumer()._run_smtp_probe(smtp, "a@b.com", "mx.b.com")
        assert status == "invalid"

    async def test_4xx_is_error(self):
        smtp = _fake_smtp(rcpt_return=(421, "4.2.1 try again later"))
        status, msg = await _probe_consumer()._run_smtp_probe(smtp, "a@b.com", "mx.b.com")
        assert status == "error"
        assert "4xx temporary" in msg

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

    def test_helo_hostname_falls_back_to_gethostbyname_when_udp_trick_fails(self):
        """No FQDN and no outbound route (UDP connect fails) — fall back to gethostbyname."""
        from pipeline.consumers.racknerd import _default_helo_hostname

        with patch("pipeline.consumers.racknerd.socket.getfqdn", return_value="racknerd-0a2741a"), \
             patch("pipeline.consumers.racknerd.socket.socket", side_effect=OSError("no route")), \
             patch("pipeline.consumers.racknerd.socket.gethostbyname", return_value="10.0.0.5"):
            result = _default_helo_hostname()
        assert result == "[10.0.0.5]"

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

    def test_single_label_host_returned_unchanged(self):
        """A malformed/unqualified MX host with no dot at all — fall back to the raw value."""
        assert _mx_provider("localhost") == "localhost"

    def test_two_part_domain_unchanged(self):
        assert _mx_provider("google.com") == "google.com"

    def test_trailing_dot_stripped(self):
        assert _mx_provider("aspmx.l.google.com.") == "google.com"


class TestPerMxSpamhausGuard:
    async def test_separate_providers_have_independent_cooldowns(self):
        consumer = RacknerdConsumer(tunnel=None, config=RacknerdConfig(concurrency=1, helo_hostname="test.verify.local"), resolver=AsyncMock())
        guard_pp = consumer._guard_for("pphosted.com")
        guard_goog = consumer._guard_for("google.com")

        for _ in range(RACKNERD_SPAMHAUS_THRESHOLD):
            guard_pp.record_block()

        assert guard_pp.is_cooling() is True
        assert guard_goog.is_cooling() is False

    async def test_same_provider_returns_same_guard_instance(self):
        consumer = RacknerdConsumer(tunnel=None, config=RacknerdConfig(concurrency=1, helo_hostname="test.verify.local"), resolver=AsyncMock())
        g1 = consumer._guard_for("pphosted.com")
        g2 = consumer._guard_for("pphosted.com")
        assert g1 is g2

    async def test_different_providers_return_different_guard_instances(self):
        consumer = RacknerdConsumer(tunnel=None, config=RacknerdConfig(concurrency=1, helo_hostname="test.verify.local"), resolver=AsyncMock())
        g1 = consumer._guard_for("pphosted.com")
        g2 = consumer._guard_for("google.com")
        assert g1 is not g2

    async def test_blocked_provider_does_not_pause_other_providers(self):
        consumer = RacknerdConsumer(tunnel=None, config=RacknerdConfig(concurrency=1, helo_hostname="test.verify.local"), resolver=AsyncMock())
        guard_pp = consumer._guard_for("pphosted.com")
        guard_goog = consumer._guard_for("google.com")

        for _ in range(RACKNERD_SPAMHAUS_THRESHOLD):
            guard_pp.record_block()

        # Google guard should return immediately (not cooling)
        assert not guard_goog.is_cooling()
