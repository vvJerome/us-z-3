"""Unit tests for pipeline.utils.dns_probe. aiodns.DNSResolver.query is mocked
per testing.md — no real DNS. A resolver is always passed explicitly; the
`resolver or aiodns.DNSResolver(...)` fallback branch is deliberately left
untested here rather than constructing a real DNSResolver (a C extension that
has already caused one CI incident when left unmocked — see testing.md)."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import aiodns
import pytest

from pipeline.constants import DNS_TLDS
from pipeline.utils.dns_probe import _is_transient_dns_error, _resolve_mx, probe_domains


def _mx_record(host: str, priority: int) -> MagicMock:
    rec = MagicMock()
    rec.host = host
    rec.priority = priority
    return rec


class TestProbeDomainsDryRun:
    async def test_dry_run_returns_synthetic_domain(self):
        domain, mx = await probe_domains(
            "Acme Plumbing", asyncio.Semaphore(5), dry_run=True,
        )
        assert domain == "acmeplumbing.com"
        assert mx == "mx.example.com"

    async def test_dry_run_with_no_stems_returns_none(self):
        domain, mx = await probe_domains("", asyncio.Semaphore(5), dry_run=True)
        assert domain is None and mx is None


class TestProbeDomainsNoStems:
    async def test_empty_business_name_returns_none_without_querying_resolver(self):
        resolver = AsyncMock()
        domain, mx = await probe_domains("", asyncio.Semaphore(5), resolver=resolver)
        assert domain is None and mx is None
        resolver.query.assert_not_called()


class TestProbeDomainsLiveResolution:
    async def test_first_tld_hit_is_returned(self):
        resolver = AsyncMock()

        async def query_side_effect(domain, rtype):
            if domain == f"acmeplumbing{DNS_TLDS[0]}":
                return [_mx_record("mx1.acmeplumbing.com", 10)]
            raise aiodns.error.DNSError(4)  # NOTIMP — not transient, no retry

        resolver.query.side_effect = query_side_effect
        domain, mx = await probe_domains("Acme Plumbing", asyncio.Semaphore(5), resolver=resolver, max_attempts=1)

        assert domain == f"acmeplumbing{DNS_TLDS[0]}"
        assert mx == "mx1.acmeplumbing.com"

    async def test_no_tld_has_mx_returns_none(self):
        resolver = AsyncMock()
        resolver.query.side_effect = aiodns.error.DNSError(4)  # not transient
        domain, mx = await probe_domains("Acme Plumbing", asyncio.Semaphore(5), resolver=resolver, max_attempts=1)
        assert domain is None and mx is None

    async def test_transient_error_retries_then_succeeds(self):
        resolver = AsyncMock()
        calls = {"n": 0}

        async def query_side_effect(domain, rtype):
            if domain != f"acmeplumbing{DNS_TLDS[0]}":
                raise aiodns.error.DNSError(4)  # other TLDs: permanent, no retry
            calls["n"] += 1
            if calls["n"] == 1:
                raise aiodns.error.DNSError(12)  # TIMEOUT — transient, retry
            return [_mx_record("mx1.acmeplumbing.com", 10)]

        resolver.query.side_effect = query_side_effect
        domain, mx = await probe_domains(
            "Acme Plumbing", asyncio.Semaphore(5), resolver=resolver, max_attempts=3, jitter=0,
        )

        assert domain == f"acmeplumbing{DNS_TLDS[0]}"
        assert mx == "mx1.acmeplumbing.com"
        assert calls["n"] == 2


class TestResolveMx:
    async def test_returns_lowest_priority_host(self):
        resolver = AsyncMock()
        resolver.query.return_value = [
            _mx_record("mx2.acme.com", 20),
            _mx_record("mx1.acmeplumbing.com", 10),
        ]
        result = await _resolve_mx(resolver, "acme.com")
        assert result == "mx1.acmeplumbing.com"

    async def test_empty_records_returns_none(self):
        resolver = AsyncMock()
        resolver.query.return_value = []
        result = await _resolve_mx(resolver, "acme.com")
        assert result is None

    async def test_dns_error_propagates_so_with_backoff_can_retry_it(self):
        """_resolve_mx must NOT swallow DNSError — probe_domains wraps it in with_backoff,
        which needs the real exception to decide whether the failure is worth retrying."""
        resolver = AsyncMock()
        resolver.query.side_effect = aiodns.error.DNSError(12)
        with pytest.raises(aiodns.error.DNSError):
            await _resolve_mx(resolver, "acme.com")


class TestIsTransientDnsError:
    def test_servfail_is_transient(self):
        assert _is_transient_dns_error(aiodns.error.DNSError(2)) is True

    def test_timeout_code_is_transient(self):
        assert _is_transient_dns_error(aiodns.error.DNSError(12)) is True

    def test_connrefused_is_transient(self):
        assert _is_transient_dns_error(aiodns.error.DNSError(11)) is True

    def test_formerr_is_not_transient(self):
        assert _is_transient_dns_error(aiodns.error.DNSError(1)) is False

    def test_asyncio_timeout_error_is_transient(self):
        assert _is_transient_dns_error(asyncio.TimeoutError()) is True

    def test_os_error_is_transient(self):
        assert _is_transient_dns_error(OSError("network unreachable")) is True

    def test_generic_exception_is_not_transient(self):
        assert _is_transient_dns_error(ValueError("nope")) is False
