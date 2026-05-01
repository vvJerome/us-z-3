from __future__ import annotations

import asyncio
import logging

import aiodns

from pipeline.constants import DNS_TLDS, SERVICE_BACKOFF
from pipeline.utils.backoff import with_backoff
from pipeline.utils.text import generate_domain_stems

logger = logging.getLogger("pipeline.producer")


async def probe_domains(
    business_name: str,
    semaphore: asyncio.Semaphore,
    *,
    resolver: aiodns.DNSResolver | None = None,
    max_attempts: int = 3,
    jitter: float = 0.2,
    dry_run: bool = False,
) -> tuple[str | None, str | None]:
    """Probe DNS MX records for candidate domains derived from a business name.

    Returns:
        (domain, mx_host) if a domain with MX records is found, else (None, None).

    Pass a shared resolver created at startup to avoid per-call setup overhead and
    to benefit from c-ares internal negative-TTL caching.
    """
    if dry_run:
        stems = generate_domain_stems(business_name)
        if stems:
            return (f"{stems[0]}.com", "mx.example.com")
        return (None, None)

    stems = generate_domain_stems(business_name)
    if not stems:
        return (None, None)

    # Use the shared resolver when available; create a fallback only if needed.
    _resolver = resolver or aiodns.DNSResolver(timeout=3, tries=1)
    base, max_delay = SERVICE_BACKOFF["dns"]

    async def _probe_one(domain: str) -> tuple[str, str | None]:
        try:
            mx = await with_backoff(
                lambda d=domain: _resolve_mx(_resolver, d),
                max_attempts=max_attempts,
                base_delay=base,
                max_delay=max_delay,
                jitter=jitter,
                retryable=_is_transient_dns_error,
                on_retry=lambda attempt, exc, delay: logger.debug(
                    "DNS retry %d for %s: %s (wait %.1fs)", attempt, domain, exc, delay,
                ),
            )
            return (domain, mx)
        except Exception:
            return (domain, None)

    async with semaphore:
        for stem in stems:
            # Probe all TLDs for this stem concurrently
            tld_results = await asyncio.gather(
                *[_probe_one(f"{stem}{tld}") for tld in DNS_TLDS]
            )
            for domain, mx_host in tld_results:
                if mx_host:
                    logger.debug("MX found: %s -> %s", domain, mx_host)
                    return (domain, mx_host)

    return (None, None)


async def _resolve_mx(resolver: aiodns.DNSResolver, domain: str) -> str | None:
    """Attempt MX lookup. Returns the highest-priority MX host or None."""
    try:
        records = await resolver.query(domain, "MX")
        if records:
            best = min(records, key=lambda r: r.priority)
            return best.host
        return None
    except aiodns.error.DNSError:
        return None


def _is_transient_dns_error(exc: Exception) -> bool:
    """DNS errors that warrant a retry (timeout, server failure)."""
    if isinstance(exc, aiodns.error.DNSError):
        # Error codes: 1=FORMERR, 2=SERVFAIL, 4=NOTIMP, 11=CONNREFUSED, 12=TIMEOUT
        return getattr(exc, "args", (None,))[0] in (2, 11, 12)
    return isinstance(exc, (asyncio.TimeoutError, OSError))
