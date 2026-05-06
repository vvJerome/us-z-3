"""Unit tests for SerperClient cache-key behaviour, query building, and fallback logic."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from pipeline import db
from pipeline.utils.serper_client import SerperClient
from pipeline.utils.rate_limiter import TokenBucket


_STUB_RESPONSE = {
    "organic": [
        {"snippet": "Contact us at info@acme.com", "link": "https://acme.com/contact"},
    ],
    "knowledgeGraph": {"website": "https://acme.com"},
}

_EMPTY_RESPONSE: dict = {"organic": [], "knowledgeGraph": {}}


@pytest.fixture
async def mem_db():
    import aiosqlite
    conn = await aiosqlite.connect(":memory:")
    conn.row_factory = aiosqlite.Row
    await conn.executescript(db.SCHEMA_SQL)
    await conn.commit()
    yield conn
    await conn.close()


def _client(ignore_cache: bool = False) -> SerperClient:
    session = AsyncMock(spec=aiohttp.ClientSession)
    rate_limiter = TokenBucket(capacity=100, refill_rate=100)
    return SerperClient("test_key", session, rate_limiter, ignore_cache=ignore_cache)


async def test_without_strategy_shares_cache_across_officers(mem_db):
    """Two officers of the same business with strategy=without share one cache entry."""
    client = _client()

    with patch.object(client, "_call_api", new_callable=AsyncMock) as mock_api:
        mock_api.return_value = _STUB_RESPONSE

        # First officer
        await client.enrich("Acme LLC", "Alice Smith", "NC", None, "without", conn=mem_db)
        # Second officer — same business, different agent
        await client.enrich("Acme LLC", "Bob Jones", "NC", None, "without", conn=mem_db)

    # Only one real API call; second hit the cache
    assert mock_api.call_count == 1


async def test_with_strategy_does_not_share_cache_across_officers(mem_db):
    """Two officers of the same business with strategy=with each get their own cache entry."""
    client = _client()

    with patch.object(client, "_call_api", new_callable=AsyncMock) as mock_api:
        mock_api.return_value = _STUB_RESPONSE

        await client.enrich("Acme LLC", "Alice Smith", "NC", None, "with", conn=mem_db)
        await client.enrich("Acme LLC", "Bob Jones", "NC", None, "with", conn=mem_db)

    # Each officer has a distinct query — two API calls
    assert mock_api.call_count == 2


async def test_without_strategy_cache_hit_returns_correct_result(mem_db):
    """Cache hit for without-strategy still returns a valid EnrichmentResult."""
    client = _client()

    with patch.object(client, "_call_api", new_callable=AsyncMock) as mock_api:
        mock_api.return_value = _STUB_RESPONSE

        first = await client.enrich("Acme LLC", "Alice Smith", "NC", None, "without", conn=mem_db)
        second = await client.enrich("Acme LLC", "Bob Jones", "NC", None, "without", conn=mem_db)

    assert second.candidate_domain == first.candidate_domain


async def test_without_strategy_with_domain_hint_shares_cache(mem_db):
    """Domain-scoped without-strategy queries also share cache across officers."""
    client = _client()

    stub = {
        "organic": [{"snippet": "Contact info@acme.com", "link": "https://acme.com/contact"}],
        "knowledgeGraph": {"website": "https://acme.com"},
    }

    with patch.object(client, "_call_api", new_callable=AsyncMock) as mock_api:
        mock_api.return_value = stub

        await client.enrich("Acme LLC", "Alice Smith", "NC", "acme.com", "without", conn=mem_db)
        await client.enrich("Acme LLC", "Bob Jones", "NC", "acme.com", "without", conn=mem_db)

    assert mock_api.call_count == 1


# ── ignore_cache ──────────────────────────────────────────────────────────────

async def test_ignore_cache_bypasses_cached_result(mem_db):
    """ignore_cache=True forces a live API call even when the cache has a hit."""
    # Populate the cache with a normal call
    normal_client = _client()
    with patch.object(normal_client, "_call_api", new_callable=AsyncMock) as m:
        m.return_value = _STUB_RESPONSE
        await normal_client.enrich("Acme LLC", None, "NC", None, "without", conn=mem_db)
    assert m.call_count == 1

    # Same business, same DB — without ignore_cache the cache would be hit
    cached_client = _client(ignore_cache=False)
    with patch.object(cached_client, "_call_api", new_callable=AsyncMock) as m2:
        m2.return_value = _STUB_RESPONSE
        await cached_client.enrich("Acme LLC", None, "NC", None, "without", conn=mem_db)
    assert m2.call_count == 0  # served from cache

    # With ignore_cache=True the API is called regardless
    bypass_client = _client(ignore_cache=True)
    with patch.object(bypass_client, "_call_api", new_callable=AsyncMock) as m3:
        m3.return_value = _STUB_RESPONSE
        await bypass_client.enrich("Acme LLC", None, "NC", None, "without", conn=mem_db)
    assert m3.call_count == 1  # bypassed cache


async def test_ignore_cache_last_was_cache_hit_is_false(mem_db):
    """ignore_cache=True means last_was_cache_hit is always False."""
    normal_client = _client()
    with patch.object(normal_client, "_call_api", new_callable=AsyncMock) as m:
        m.return_value = _STUB_RESPONSE
        await normal_client.enrich("Acme LLC", None, "NC", None, "without", conn=mem_db)

    bypass_client = _client(ignore_cache=True)
    with patch.object(bypass_client, "_call_api", new_callable=AsyncMock) as m2:
        m2.return_value = _STUB_RESPONSE
        await bypass_client.enrich("Acme LLC", None, "NC", None, "without", conn=mem_db)
    assert bypass_client.last_was_cache_hit is False


# ── _build_query ──────────────────────────────────────────────────────────────

def test_build_query_without_strategy_uses_normalized_name():
    """Without-strategy query uses normalized name without quotes around the legal name."""
    query = SerperClient._build_query(
        "NORWOOD RURAL VOLUNTEER FIRE DEPARTMENT, INCORPORATED",
        None, "NC", None, "without",
    )
    assert '"NORWOOD RURAL VOLUNTEER FIRE DEPARTMENT' not in query
    assert "norwood rural volunteer fire department" in query


def test_build_query_without_strategy_no_double_quotes():
    """Without-strategy query never wraps the business name in double quotes."""
    query = SerperClient._build_query(
        "CERAMCO, INCORPORATED", None, "NC", None, "without",
    )
    assert '"CERAMCO' not in query
    assert "ceramco" in query


def test_build_query_with_strategy_parses_agent_name_from_comma_format():
    """With-strategy query converts 'LAST, FIRST' agent name to 'first last'."""
    query = SerperClient._build_query(
        "Test Corp", "TAYLOR, TOBY", "NC", None, "with",
    )
    assert '"TAYLOR, TOBY"' not in query
    assert "toby taylor" in query


def test_build_query_with_strategy_natural_name_format():
    """With-strategy query also works when agent name is already in natural format."""
    query = SerperClient._build_query(
        "Test Corp", "Jane Smith", "NC", None, "with",
    )
    assert "jane smith" in query


# ── 4th fallback (short-name query) ──────────────────────────────────────────

async def test_short_name_fallback_fires_for_long_business_name():
    """4th fallback fires when primary misses and business name has 4+ significant words."""
    client = _client()
    call_count = 0
    hit_response = {
        "organic": [{"snippet": "contact@nrvfd.org info", "link": "https://nrvfd.org"}],
        "knowledgeGraph": {},
    }

    async def _mock_api(query: str) -> dict:
        nonlocal call_count
        call_count += 1
        return _EMPTY_RESPONSE if call_count == 1 else hit_response

    with patch.object(client, "_call_api", side_effect=_mock_api):
        result = await client.enrich(
            "Norwood Rural Volunteer Fire Department",
            None, "NC", None, "without",
        )

    assert call_count == 2  # primary + short-name fallback
    assert result.candidate_emails  # fallback found something


async def test_short_name_fallback_not_fired_for_short_name():
    """4th fallback is NOT fired when normalized business name has fewer than 4 words."""
    client = _client()
    with patch.object(client, "_call_api", new_callable=AsyncMock) as mock_api:
        mock_api.return_value = _EMPTY_RESPONSE
        # "Acme Corp" → strips "corp" → "acme" → 1 word; no fallback
        await client.enrich("Acme Corp", None, "NC", None, "without")
    assert mock_api.call_count == 1


async def test_short_name_fallback_not_fired_when_primary_finds_email():
    """4th fallback is NOT fired when the primary query already found candidate emails."""
    client = _client()
    stub_with_email = {
        "organic": [
            {"snippet": "Email info@norwood-rural-fire.org for info", "link": "https://norwood-rural-fire.org"}
        ],
        "knowledgeGraph": {},
    }
    with patch.object(client, "_call_api", new_callable=AsyncMock) as mock_api:
        mock_api.return_value = stub_with_email
        result = await client.enrich(
            "Norwood Rural Volunteer Fire Department",
            None, "NC", None, "without",
        )
    assert mock_api.call_count == 1  # no fallback needed
    assert result.candidate_emails  # primary already found one


async def test_short_name_fallback_increments_fallback_calls():
    """4th fallback increments _fallback_calls so callers can charge cost correctly."""
    client = _client()
    call_count = 0

    async def _mock_api(query: str) -> dict:
        nonlocal call_count
        call_count += 1
        return _EMPTY_RESPONSE

    with patch.object(client, "_call_api", side_effect=_mock_api):
        await client.enrich(
            "Norwood Rural Volunteer Fire Department",
            None, "NC", None, "without",
        )

    # 2 calls: primary + 4th fallback; fallback_calls should reflect the extra call
    assert client._fallback_calls == 1


# ── Serper credits exhaustion ─────────────────────────────────────────────────

async def test_credits_exhausted_returns_empty_result_not_pipeline_halt():
    """HTTP 400 'Not enough credits' returns empty EnrichmentResult — run continues."""
    from unittest.mock import MagicMock

    client = _client()

    mock_resp = MagicMock()
    mock_resp.status = 400
    mock_resp.text = AsyncMock(return_value='{"message":"Not enough credits","statusCode":400}')
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    with patch.object(client.session, "post", return_value=mock_resp):
        result = await client.enrich("Acme LLC", None, "NC", None, "without")

    assert result.candidate_domain is None
    assert result.candidate_emails == []
    assert client._credits_exhausted is True


async def test_credits_exhausted_flag_skips_subsequent_api_calls():
    """Once _credits_exhausted is set, subsequent enrich() calls skip the API entirely."""
    client = _client()
    client._credits_exhausted = True

    with patch.object(client, "_call_api", new_callable=AsyncMock) as mock_api:
        result = await client.enrich("Acme LLC", None, "NC", None, "without")

    mock_api.assert_not_called()
    assert result.candidate_domain is None


async def test_invalid_api_key_still_raises_pipeline_halt():
    """HTTP 401 (invalid key) still raises PipelineHaltError — that is always fatal."""
    from unittest.mock import MagicMock
    from pipeline.models import PipelineHaltError as PHE

    client = _client()

    mock_resp = MagicMock()
    mock_resp.status = 401
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    with patch.object(client.session, "post", return_value=mock_resp):
        with pytest.raises(PHE):
            await client.enrich("Acme LLC", None, "NC", None, "without")
