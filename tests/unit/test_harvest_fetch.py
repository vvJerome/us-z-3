"""Unit tests for pipeline.harvest.fetch — the only network-touching part of the
harvester. AsyncSession is mocked per testing.md; no real HTTP calls."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from pipeline.constants import HARVEST_PATHS
from pipeline.harvest.fetch import _robots_allows, fetch_site
from pipeline.utils.rate_limiter import TokenBucket

pytestmark = pytest.mark.asyncio


class _FakeResponse:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


class _FakeSession:
    """Stands in for curl_cffi's AsyncSession as both the constructor return value
    and the async context manager it's used as (`async with AsyncSession(...) as s`)."""

    def __init__(self, responses: dict[str, _FakeResponse] | None = None,
                 raise_for: frozenset[str] = frozenset()):
        self.responses = responses or {}
        self.raise_for = raise_for
        self.calls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, timeout=None, allow_redirects=True):
        self.calls.append(url)
        if url in self.raise_for:
            raise RuntimeError("network error")
        return self.responses.get(url, _FakeResponse(404, ""))


def _bucket() -> TokenBucket:
    return TokenBucket(capacity=1000, refill_rate=1000, initial_tokens=1000)


class TestRobotsAllows:
    async def test_fetch_failure_returns_none_fail_open(self):
        session = _FakeSession(raise_for=frozenset({"https://acme.com/robots.txt"}))
        result = await _robots_allows(session, "https://acme.com", 5.0)
        assert result is None

    async def test_4xx_response_returns_none_fail_open(self):
        session = _FakeSession({"https://acme.com/robots.txt": _FakeResponse(404, "")})
        result = await _robots_allows(session, "https://acme.com", 5.0)
        assert result is None

    async def test_parses_disallow_rules(self):
        body = "User-agent: *\nDisallow: /contact\n"
        session = _FakeSession({"https://acme.com/robots.txt": _FakeResponse(200, body)})
        rp = await _robots_allows(session, "https://acme.com", 5.0)
        assert rp is not None
        assert rp.can_fetch("*", "https://acme.com/contact") is False
        assert rp.can_fetch("*", "https://acme.com/about") is True


class TestFetchSite:
    async def test_collects_pages_under_200(self):
        responses = {
            f"https://acme.com{('/' + p) if p else ''}": _FakeResponse(200, f"<html>{p}</html>")
            for p in HARVEST_PATHS
        }
        responses["https://acme.com/robots.txt"] = _FakeResponse(404, "")
        session = _FakeSession(responses)

        with patch("pipeline.harvest.fetch.AsyncSession", return_value=session):
            pages, blocked = await fetch_site("acme.com", rate_limiter=_bucket(), timeout_s=5.0)

        assert blocked is False
        assert len(pages) == len(HARVEST_PATHS)
        assert all(html for _url, html in pages)

    async def test_blocked_status_sets_blocked_flag_and_skips_page(self):
        responses = {"https://acme.com/robots.txt": _FakeResponse(404, "")}
        for p in HARVEST_PATHS:
            url = f"https://acme.com{('/' + p) if p else ''}"
            responses[url] = _FakeResponse(403, "")
        session = _FakeSession(responses)

        with patch("pipeline.harvest.fetch.AsyncSession", return_value=session):
            pages, blocked = await fetch_site("acme.com", rate_limiter=_bucket(), timeout_s=5.0)

        assert blocked is True
        assert pages == []

    async def test_error_status_not_in_blocked_set_is_skipped_without_blocking(self):
        responses = {"https://acme.com/robots.txt": _FakeResponse(404, "")}
        for p in HARVEST_PATHS:
            url = f"https://acme.com{('/' + p) if p else ''}"
            responses[url] = _FakeResponse(500, "")
        session = _FakeSession(responses)

        with patch("pipeline.harvest.fetch.AsyncSession", return_value=session):
            pages, blocked = await fetch_site("acme.com", rate_limiter=_bucket(), timeout_s=5.0)

        assert blocked is False
        assert pages == []

    async def test_empty_body_on_200_is_not_collected(self):
        responses = {"https://acme.com/robots.txt": _FakeResponse(404, "")}
        for p in HARVEST_PATHS:
            url = f"https://acme.com{('/' + p) if p else ''}"
            responses[url] = _FakeResponse(200, "")
        session = _FakeSession(responses)

        with patch("pipeline.harvest.fetch.AsyncSession", return_value=session):
            pages, blocked = await fetch_site("acme.com", rate_limiter=_bucket(), timeout_s=5.0)

        assert pages == []

    async def test_fetch_exception_on_one_path_does_not_abort_the_rest(self):
        home = "https://acme.com"
        contact = "https://acme.com/contact"
        responses = {
            "https://acme.com/robots.txt": _FakeResponse(404, ""),
            contact: _FakeResponse(200, "<html>contact us</html>"),
        }
        session = _FakeSession(responses, raise_for=frozenset({home}))

        with patch("pipeline.harvest.fetch.AsyncSession", return_value=session):
            pages, blocked = await fetch_site("acme.com", rate_limiter=_bucket(), timeout_s=5.0)

        assert blocked is False
        assert (contact, "<html>contact us</html>") in pages
        assert not any(url == home for url, _html in pages)

    async def test_robots_disallowed_path_is_never_fetched(self):
        body = "User-agent: *\nDisallow: /team\n"
        responses = {"https://acme.com/robots.txt": _FakeResponse(200, body)}
        for p in HARVEST_PATHS:
            url = f"https://acme.com{('/' + p) if p else ''}"
            responses[url] = _FakeResponse(200, f"<html>{p}</html>")
        session = _FakeSession(responses)

        with patch("pipeline.harvest.fetch.AsyncSession", return_value=session):
            pages, blocked = await fetch_site("acme.com", rate_limiter=_bucket(), timeout_s=5.0)

        assert "https://acme.com/team" not in session.calls
        assert any(url == "https://acme.com/about" for url, _html in pages)

    async def test_rate_limiter_acquired_once_per_fetched_path(self):
        responses = {"https://acme.com/robots.txt": _FakeResponse(404, "")}
        for p in HARVEST_PATHS:
            url = f"https://acme.com{('/' + p) if p else ''}"
            responses[url] = _FakeResponse(200, f"<html>{p}</html>")
        session = _FakeSession(responses)
        bucket = _bucket()
        bucket.acquire = AsyncMock(wraps=bucket.acquire)

        with patch("pipeline.harvest.fetch.AsyncSession", return_value=session):
            await fetch_site("acme.com", rate_limiter=bucket, timeout_s=5.0)

        assert bucket.acquire.call_count == len(HARVEST_PATHS)
