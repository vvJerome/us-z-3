"""Unit tests for RacknerdConsumer's network-facing methods: MX resolution + caching,
the full verify() decision flow, direct/SOCKS5 probing, and NullRacknerd. All aiodns
and aiosmtplib calls are mocked per testing.md — no real DNS or SMTP connections."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import aiodns
import aiosmtplib

from pipeline.consumers.racknerd import NullRacknerd, RacknerdConfig, RacknerdConsumer
from pipeline.constants import RACKNERD_MX_CACHE_MAX


def _mx_record(host: str, priority: int) -> MagicMock:
    rec = MagicMock()
    rec.host = host
    rec.priority = priority
    return rec


def _consumer(**config_kwargs) -> RacknerdConsumer:
    # helo_hostname's default factory touches the network (socket.getfqdn(), a UDP
    # "connect" to pick a local IP) — pin it so these tests never depend on DNS/network.
    config_kwargs.setdefault("helo_hostname", "test.verify.local")
    resolver = AsyncMock()
    return RacknerdConsumer(tunnel=None, config=RacknerdConfig(**config_kwargs), resolver=resolver)


class TestResolveMx:
    async def test_returns_hosts_sorted_by_priority(self):
        consumer = _consumer()
        consumer._resolver.query.return_value = [
            _mx_record("mx2.example.com", 20),
            _mx_record("mx1.example.com", 10),
        ]
        hosts = await consumer._resolve_mx("example.com")
        assert hosts == ["mx1.example.com", "mx2.example.com"]

    async def test_trailing_dots_stripped(self):
        consumer = _consumer()
        consumer._resolver.query.return_value = [_mx_record("mx1.example.com.", 10)]
        hosts = await consumer._resolve_mx("example.com")
        assert hosts == ["mx1.example.com"]

    async def test_falls_back_to_a_record_when_mx_lookup_fails(self):
        consumer = _consumer()

        async def query_side_effect(domain, rtype):
            if rtype == "MX":
                raise aiodns.error.DNSError("NXDOMAIN")
            return [MagicMock()]  # A record lookup succeeds

        consumer._resolver.query.side_effect = query_side_effect
        hosts = await consumer._resolve_mx("example.com")
        assert hosts == ["example.com"]

    async def test_returns_empty_when_both_mx_and_a_fail(self):
        consumer = _consumer()
        consumer._resolver.query.side_effect = aiodns.error.DNSError("NXDOMAIN")
        hosts = await consumer._resolve_mx("example.com")
        assert hosts == []

    async def test_second_call_within_ttl_uses_cache_not_resolver(self):
        consumer = _consumer()
        consumer._resolver.query.return_value = [_mx_record("mx1.example.com", 10)]

        first = await consumer._resolve_mx("example.com")
        second = await consumer._resolve_mx("example.com")

        assert first == second
        assert consumer._resolver.query.call_count == 1

    async def test_cache_eviction_when_at_capacity(self):
        consumer = _consumer()
        # Pre-fill the cache to capacity without hitting the resolver for each entry.
        consumer._mx_cache = {f"domain{i}.com": (["mx.x.com"], 1e18) for i in range(RACKNERD_MX_CACHE_MAX)}
        consumer._resolver.query.return_value = [_mx_record("mx1.new.com", 10)]

        await consumer._resolve_mx("new-domain.com")

        # Oldest quarter evicted, then the new domain inserted.
        assert len(consumer._mx_cache) <= RACKNERD_MX_CACHE_MAX - (RACKNERD_MX_CACHE_MAX // 4) + 1


class TestVerifyInnerFlow:
    async def test_first_host_valid_returns_immediately(self):
        consumer = _consumer()
        consumer._resolve_mx = AsyncMock(return_value=["mx1.example.com", "mx2.example.com"])
        consumer._probe_mx = AsyncMock(return_value=("valid", "250 OK"))

        result = await consumer.verify("user@example.com")

        assert result.status == "valid"
        consumer._probe_mx.assert_called_once()  # never tried the second host

    async def test_invalid_stops_and_does_not_try_next_host(self):
        consumer = _consumer()
        consumer._resolve_mx = AsyncMock(return_value=["mx1.example.com", "mx2.example.com"])
        consumer._probe_mx = AsyncMock(return_value=("invalid", "550 no such user"))

        result = await consumer.verify("user@example.com")

        assert result.status == "invalid"
        consumer._probe_mx.assert_called_once()

    async def test_catch_all_returned(self):
        consumer = _consumer()
        consumer._resolve_mx = AsyncMock(return_value=["mx1.example.com"])
        consumer._probe_mx = AsyncMock(return_value=("catch_all", "250 accepts all"))

        result = await consumer.verify("user@example.com")
        assert result.status == "catch_all"

    async def test_blocked_records_a_guard_block(self):
        consumer = _consumer()
        consumer._resolve_mx = AsyncMock(return_value=["mx1.pphosted.com"])
        consumer._probe_mx = AsyncMock(return_value=("blocked", "554 blocked by spamhaus"))

        result = await consumer.verify("user@example.com")

        assert result.status == "blocked"
        assert len(consumer._guards["pphosted.com"]._events) == 1

    async def test_error_on_first_host_falls_through_to_second(self):
        consumer = _consumer()
        consumer._resolve_mx = AsyncMock(return_value=["mx1.example.com", "mx2.example.com"])
        consumer._probe_mx = AsyncMock(side_effect=[("error", "timeout"), ("valid", "250 OK")])

        result = await consumer.verify("user@example.com")

        assert result.status == "valid"
        assert consumer._probe_mx.call_count == 2

    async def test_all_hosts_error_returns_last_error(self):
        consumer = _consumer()
        consumer._resolve_mx = AsyncMock(return_value=["mx1.example.com", "mx2.example.com"])
        consumer._probe_mx = AsyncMock(side_effect=[("error", "timeout 1"), ("error", "timeout 2")])

        result = await consumer.verify("user@example.com")

        assert result.status == "error"
        assert result.message == "timeout 2"

    async def test_no_mx_hosts_returns_no_mx_error(self):
        consumer = _consumer()
        consumer._resolve_mx = AsyncMock(return_value=[])

        result = await consumer.verify("user@example.com")

        assert result.status == "error"
        assert "no MX/A record" in result.message

    async def test_only_probes_up_to_max_hosts(self):
        consumer = _consumer()
        consumer._resolve_mx = AsyncMock(
            return_value=["mx1.x.com", "mx2.x.com", "mx3.x.com", "mx4.x.com"]
        )
        consumer._probe_mx = AsyncMock(return_value=("error", "timeout"))

        await consumer.verify("user@example.com")

        from pipeline.constants import RACKNERD_MX_MAX_HOSTS
        assert consumer._probe_mx.call_count == RACKNERD_MX_MAX_HOSTS


class TestProbeMxDispatch:
    async def test_direct_mode_dispatches_to_probe_direct(self):
        consumer = _consumer(direct=True)
        consumer._probe_direct = AsyncMock(return_value=("valid", "250 OK"))
        consumer._probe_socks5 = AsyncMock(return_value=("error", "should not be called"))

        status, _ = await consumer._probe_mx("user@example.com", "mx.example.com")

        assert status == "valid"
        consumer._probe_direct.assert_called_once()
        consumer._probe_socks5.assert_not_called()

    async def test_tunnel_mode_dispatches_to_probe_socks5(self):
        consumer = _consumer(direct=False)
        consumer._probe_direct = AsyncMock(return_value=("error", "should not be called"))
        consumer._probe_socks5 = AsyncMock(return_value=("valid", "250 OK"))

        status, _ = await consumer._probe_mx("user@example.com", "mx.example.com")

        assert status == "valid"
        consumer._probe_socks5.assert_called_once()
        consumer._probe_direct.assert_not_called()


class TestProbeDirect:
    async def test_successful_probe_returns_valid(self):
        consumer = _consumer(direct=True)
        fake_smtp = AsyncMock()
        fake_smtp.connect = AsyncMock()
        consumer._run_smtp_probe = AsyncMock(return_value=("valid", "250 OK"))

        with patch("pipeline.consumers.racknerd.aiosmtplib.SMTP", return_value=fake_smtp):
            status, msg = await consumer._probe_direct("user@example.com", "mx.example.com")

        assert status == "valid"

    async def test_recipient_refused_classified_via_helper(self):
        consumer = _consumer(direct=True)
        fake_smtp = AsyncMock()
        fake_smtp.connect = AsyncMock()
        consumer._run_smtp_probe = AsyncMock(
            side_effect=aiosmtplib.SMTPRecipientRefused(550, "no such user", "user@example.com")
        )

        with patch("pipeline.consumers.racknerd.aiosmtplib.SMTP", return_value=fake_smtp):
            status, _ = await consumer._probe_direct("user@example.com", "mx.example.com")

        assert status == "invalid"

    async def test_connect_timeout_returns_error(self):
        consumer = _consumer(direct=True)
        fake_smtp = AsyncMock()
        fake_smtp.connect = AsyncMock(side_effect=asyncio.TimeoutError())

        with patch("pipeline.consumers.racknerd.aiosmtplib.SMTP", return_value=fake_smtp):
            status, msg = await consumer._probe_direct("user@example.com", "mx.example.com")

        assert status == "error"
        assert "timeout" in msg.lower()

    async def test_smtp_exception_returns_error(self):
        consumer = _consumer(direct=True)
        fake_smtp = AsyncMock()
        fake_smtp.connect = AsyncMock(side_effect=aiosmtplib.SMTPException("connection reset"))

        with patch("pipeline.consumers.racknerd.aiosmtplib.SMTP", return_value=fake_smtp):
            status, msg = await consumer._probe_direct("user@example.com", "mx.example.com")

        assert status == "error"

    async def test_generic_exception_returns_error(self):
        consumer = _consumer(direct=True)
        fake_smtp = AsyncMock()
        fake_smtp.connect = AsyncMock(side_effect=OSError("network unreachable"))

        with patch("pipeline.consumers.racknerd.aiosmtplib.SMTP", return_value=fake_smtp):
            status, msg = await consumer._probe_direct("user@example.com", "mx.example.com")

        assert status == "error"
        assert "probe error" in msg


class TestProbeSocks5:
    def _fake_sock(self, fd: int = 7) -> MagicMock:
        sock = MagicMock()
        sock.fileno.return_value = fd
        return sock

    async def test_proxy_connect_timeout_returns_error(self):
        consumer = _consumer(direct=False)
        with patch("python_socks.async_.asyncio.Proxy.from_url") as from_url:
            proxy = MagicMock()
            proxy.connect = AsyncMock(side_effect=asyncio.TimeoutError())
            from_url.return_value = proxy

            status, msg = await consumer._probe_socks5("user@example.com", "mx.example.com")

        assert status == "error"
        assert "timeout" in msg.lower()

    async def test_proxy_connect_failure_returns_error(self):
        consumer = _consumer(direct=False)
        with patch("python_socks.async_.asyncio.Proxy.from_url") as from_url:
            proxy = MagicMock()
            proxy.connect = AsyncMock(side_effect=OSError("refused"))
            from_url.return_value = proxy

            status, msg = await consumer._probe_socks5("user@example.com", "mx.example.com")

        assert status == "error"
        assert "SOCKS5 connect failed" in msg

    async def test_successful_probe_closes_smtp_and_evicts_transport(self):
        consumer = _consumer(direct=False)
        sock = self._fake_sock(fd=42)
        fake_smtp = AsyncMock()
        fake_smtp.connect = AsyncMock()
        fake_smtp.close = MagicMock()
        consumer._run_smtp_probe = AsyncMock(return_value=("valid", "250 OK"))

        loop = asyncio.get_running_loop()
        loop._transports = {42: object()}  # type: ignore[attr-defined]

        with patch("python_socks.async_.asyncio.Proxy.from_url") as from_url, \
             patch("pipeline.consumers.racknerd.aiosmtplib.SMTP", return_value=fake_smtp):
            proxy = MagicMock()
            proxy.connect = AsyncMock(return_value=sock)
            from_url.return_value = proxy

            status, _ = await consumer._probe_socks5("user@example.com", "mx.example.com")

        assert status == "valid"
        fake_smtp.close.assert_called_once()
        assert 42 not in loop._transports  # type: ignore[attr-defined]

    async def test_smtp_connect_failure_closes_raw_socket_not_smtp(self):
        consumer = _consumer(direct=False)
        sock = self._fake_sock(fd=43)
        fake_smtp = AsyncMock()
        fake_smtp.connect = AsyncMock(side_effect=aiosmtplib.SMTPException("handshake failed"))
        fake_smtp.close = MagicMock()

        with patch("python_socks.async_.asyncio.Proxy.from_url") as from_url, \
             patch("pipeline.consumers.racknerd.aiosmtplib.SMTP", return_value=fake_smtp):
            proxy = MagicMock()
            proxy.connect = AsyncMock(return_value=sock)
            from_url.return_value = proxy

            status, _ = await consumer._probe_socks5("user@example.com", "mx.example.com")

        assert status == "error"
        sock.close.assert_called_once()
        fake_smtp.close.assert_not_called()

    async def test_recipient_refused_over_tunnel_classified_via_helper(self):
        consumer = _consumer(direct=False)
        sock = self._fake_sock(fd=44)
        fake_smtp = AsyncMock()
        fake_smtp.connect = AsyncMock()
        fake_smtp.close = MagicMock()
        consumer._run_smtp_probe = AsyncMock(
            side_effect=aiosmtplib.SMTPRecipientRefused(550, "no such user", "user@example.com")
        )

        with patch("python_socks.async_.asyncio.Proxy.from_url") as from_url, \
             patch("pipeline.consumers.racknerd.aiosmtplib.SMTP", return_value=fake_smtp):
            proxy = MagicMock()
            proxy.connect = AsyncMock(return_value=sock)
            from_url.return_value = proxy

            status, _ = await consumer._probe_socks5("user@example.com", "mx.example.com")

        assert status == "invalid"

    async def test_smtp_close_raising_is_swallowed(self):
        """smtp.close() failing during cleanup must not mask the real probe result."""
        consumer = _consumer(direct=False)
        sock = self._fake_sock(fd=45)
        fake_smtp = AsyncMock()
        fake_smtp.connect = AsyncMock()
        fake_smtp.close = MagicMock(side_effect=RuntimeError("close boom"))
        consumer._run_smtp_probe = AsyncMock(return_value=("valid", "250 OK"))

        with patch("python_socks.async_.asyncio.Proxy.from_url") as from_url, \
             patch("pipeline.consumers.racknerd.aiosmtplib.SMTP", return_value=fake_smtp):
            proxy = MagicMock()
            proxy.connect = AsyncMock(return_value=sock)
            from_url.return_value = proxy

            status, _ = await consumer._probe_socks5("user@example.com", "mx.example.com")

        assert status == "valid"

    async def test_raw_socket_close_raising_is_swallowed(self):
        """sock.close() failing on the smtp.connect()-failed path must not raise out."""
        consumer = _consumer(direct=False)
        sock = self._fake_sock(fd=46)
        sock.close.side_effect = RuntimeError("close boom")
        fake_smtp = AsyncMock()
        fake_smtp.connect = AsyncMock(side_effect=aiosmtplib.SMTPException("handshake failed"))

        with patch("python_socks.async_.asyncio.Proxy.from_url") as from_url, \
             patch("pipeline.consumers.racknerd.aiosmtplib.SMTP", return_value=fake_smtp):
            proxy = MagicMock()
            proxy.connect = AsyncMock(return_value=sock)
            from_url.return_value = proxy

            status, _ = await consumer._probe_socks5("user@example.com", "mx.example.com")

        assert status == "error"

    async def test_smtp_connect_timeout_over_tunnel_returns_error(self):
        consumer = _consumer(direct=False)
        sock = self._fake_sock(fd=47)
        fake_smtp = AsyncMock()
        fake_smtp.connect = AsyncMock(side_effect=asyncio.TimeoutError())

        with patch("python_socks.async_.asyncio.Proxy.from_url") as from_url, \
             patch("pipeline.consumers.racknerd.aiosmtplib.SMTP", return_value=fake_smtp):
            proxy = MagicMock()
            proxy.connect = AsyncMock(return_value=sock)
            from_url.return_value = proxy

            status, msg = await consumer._probe_socks5("user@example.com", "mx.example.com")

        assert status == "error"
        assert "timeout" in msg.lower()

    async def test_smtp_connect_generic_exception_over_tunnel_returns_error(self):
        consumer = _consumer(direct=False)
        sock = self._fake_sock(fd=48)
        fake_smtp = AsyncMock()
        fake_smtp.connect = AsyncMock(side_effect=RuntimeError("unexpected"))

        with patch("python_socks.async_.asyncio.Proxy.from_url") as from_url, \
             patch("pipeline.consumers.racknerd.aiosmtplib.SMTP", return_value=fake_smtp):
            proxy = MagicMock()
            proxy.connect = AsyncMock(return_value=sock)
            from_url.return_value = proxy

            status, msg = await consumer._probe_socks5("user@example.com", "mx.example.com")

        assert status == "error"
        assert "probe error" in msg

    async def test_python_socks_not_installed_returns_error(self):
        consumer = _consumer(direct=False)
        with patch.dict("sys.modules", {"python_socks.async_.asyncio": None}):
            status, msg = await consumer._probe_socks5("user@example.com", "mx.example.com")

        assert status == "error"
        assert "python-socks not installed" in msg


class TestRunSmtpProbeCleanup:
    async def test_rset_failure_does_not_fail_the_probe(self):
        from tests.unit.test_racknerd import _fake_smtp, _probe_consumer
        smtp = _fake_smtp(rcpt_return=(250, "OK"))
        smtp.rset = AsyncMock(side_effect=RuntimeError("rset boom"))

        status, _ = await _probe_consumer()._run_smtp_probe(smtp, "a@b.com", "mx.b.com")
        assert status == "valid"

    async def test_quit_failure_does_not_fail_the_probe(self):
        from tests.unit.test_racknerd import _fake_smtp, _probe_consumer
        smtp = _fake_smtp(rcpt_return=(250, "OK"))
        smtp.quit = AsyncMock(side_effect=RuntimeError("quit boom"))

        status, _ = await _probe_consumer()._run_smtp_probe(smtp, "a@b.com", "mx.b.com")
        assert status == "valid"

    async def test_malformed_response_code_returns_error(self):
        from tests.unit.test_racknerd import _fake_smtp, _probe_consumer
        smtp = _fake_smtp(rcpt_return=(None, "garbage"))

        status, msg = await _probe_consumer()._run_smtp_probe(smtp, "a@b.com", "mx.b.com")
        assert status == "error"
        assert "malformed" in msg.lower()


class TestNullRacknerd:
    async def test_verify_always_not_run(self):
        result = await NullRacknerd().verify("user@example.com")
        assert result.status == "not_run"
        assert result.verified_at is None

    async def test_verify_accepts_mx_provider_kwarg_for_interface_parity(self):
        result = await NullRacknerd().verify("user@example.com", mx_provider="google.com")
        assert result.status == "not_run"

    def test_is_up_always_false(self):
        assert NullRacknerd().is_up() is False
