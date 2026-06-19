from __future__ import annotations

import logging
from urllib.robotparser import RobotFileParser

from curl_cffi.requests import AsyncSession

from pipeline.constants import HARVEST_IMPERSONATE, HARVEST_PATHS
from pipeline.utils.rate_limiter import TokenBucket

logger = logging.getLogger("pipeline.harvest.fetch")

_BLOCKED_STATUS = (403, 429, 503)


async def _get(session: AsyncSession, url: str, timeout_s: float) -> tuple[int, str]:
    r = await session.get(url, timeout=timeout_s, allow_redirects=True)
    return r.status_code, r.text


async def _robots_allows(session: AsyncSession, base: str, timeout_s: float) -> RobotFileParser | None:
    """Fetch + parse robots.txt once. None means fetch failed → caller proceeds (fail-open)."""
    rp = RobotFileParser()
    try:
        status, body = await _get(session, base + "/robots.txt", timeout_s)
    except Exception:
        return None
    if status >= 400:
        return None  # no usable robots.txt → not disallowed
    rp.parse(body.splitlines())
    return rp


async def fetch_site(
    domain: str,
    *,
    rate_limiter: TokenBucket,
    timeout_s: float,
) -> tuple[list[tuple[str, str]], bool]:
    """Fetch the HARVEST_PATHS pages of a domain. Returns ([(url, html), ...], blocked)."""
    base = "https://" + domain
    pages: list[tuple[str, str]] = []
    blocked = False
    # HARVEST_IMPERSONATE is a valid curl_cffi browser label; mypy can't see the Literal.
    async with AsyncSession(impersonate=HARVEST_IMPERSONATE) as session:  # type: ignore[arg-type]
        robots = await _robots_allows(session, base, timeout_s)
        for path in HARVEST_PATHS:
            url = base + ("/" + path if path else "")
            if robots is not None and not robots.can_fetch("*", url):
                continue
            await rate_limiter.acquire()
            try:
                status, html = await _get(session, url, timeout_s)
            except Exception as exc:
                logger.debug("Harvest fetch failed %s: %s", url, exc)
                continue
            if status in _BLOCKED_STATUS:
                blocked = True
                continue
            if status < 400 and html:
                pages.append((url, html))
    return pages, blocked
