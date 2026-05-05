"""Unit tests for SerperClient cache-key behaviour."""
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


@pytest.fixture
async def mem_db():
    import aiosqlite
    conn = await aiosqlite.connect(":memory:")
    conn.row_factory = aiosqlite.Row
    await conn.executescript(db.SCHEMA_SQL)
    await conn.commit()
    yield conn
    await conn.close()


def _client() -> SerperClient:
    session = AsyncMock(spec=aiohttp.ClientSession)
    rate_limiter = TokenBucket(capacity=100, refill_rate=100)
    return SerperClient("test_key", session, rate_limiter)


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
