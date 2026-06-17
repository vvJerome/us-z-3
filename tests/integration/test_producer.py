from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.config import PipelineConfig
from pipeline.db import State
from pipeline.models import EnrichmentResult, InputRecord
from pipeline.utils.cost_tracker import CostTracker


def _make_config(**kwargs) -> PipelineConfig:
    defaults = dict(
        serper_api_key="test_key",
        zuhal_api_key="test_key",
        dns_concurrency=5,
        serper_concurrency=2,
        zuhal_concurrency=2,
        strategy="auto",
        dry_run=False,
        max_attempts=1,
        racknerd_enabled=False,
    )
    defaults.update(kwargs)
    return PipelineConfig(**defaults)


def _make_record(**kwargs) -> InputRecord:
    defaults = dict(
        unique_id="filing_001__agent_001",
        business_name="Acme Corp",
        agent_name="Jane Doe",
        state="NC",
    )
    defaults.update(kwargs)
    return InputRecord(**defaults)


async def _make_worker(db_conn, config: PipelineConfig | None = None):
    from pipeline.producer import ProducerWorker

    cfg = config or _make_config()
    session = MagicMock()
    tracker = CostTracker(max_cost=None)
    return ProducerWorker(cfg, db_conn, tracker, session)


@pytest.fixture
async def worker(db_conn):
    return await _make_worker(db_conn)


async def test_process_record_dns_hit_discovered(worker):
    """DNS hit → record reaches DISCOVERED with discovery_source=dns."""
    record = _make_record()

    with patch("pipeline.producer.probe_domains", new=AsyncMock(return_value=("acmecorp.com", "mx1.google.com"))):
        worker._serper.enrich = AsyncMock(return_value=EnrichmentResult())
        result = await worker._process_record(record)

    assert result["record_state"] == State.DISCOVERED
    assert result["discovery_source"] == "dns"
    assert result["candidate_domain"] == "acmecorp.com"
    assert json.loads(result["candidate_emails"])  # non-empty list of pattern candidates


async def test_process_record_serper_domain_hit_discovered(worker):
    """DNS miss + Serper returns domain → DISCOVERED with discovery_source=serper."""
    record = _make_record()

    with patch("pipeline.producer.probe_domains", new=AsyncMock(return_value=(None, None))):
        worker._serper.enrich = AsyncMock(return_value=EnrichmentResult(
            candidate_domain="acmecorp.com",
            candidate_emails=[],
        ))
        result = await worker._process_record(record)

    assert result["record_state"] == State.DISCOVERED
    assert result["discovery_source"] == "serper"
    assert result["candidate_domain"] == "acmecorp.com"


async def test_process_record_serper_fallback_domain_marks_source(worker):
    """DNS miss + Serper first-organic fallback domain → discovery_source=serper_fallback."""
    record = _make_record()

    with patch("pipeline.producer.probe_domains", new=AsyncMock(return_value=(None, None))):
        worker._serper.enrich = AsyncMock(return_value=EnrichmentResult(
            candidate_domain="someguess.com",
            candidate_emails=[],
            is_fallback_domain=True,
        ))
        result = await worker._process_record(record)

    assert result["record_state"] == State.DISCOVERED
    assert result["discovery_source"] == "serper_fallback"


async def test_process_record_serper_snippet_email_discovered(worker):
    """DNS miss + Serper finds email in snippet (no domain) → DISCOVERED, discovery_source=serper."""
    record = _make_record()

    with patch("pipeline.producer.probe_domains", new=AsyncMock(return_value=(None, None))):
        worker._serper.enrich = AsyncMock(return_value=EnrichmentResult(
            candidate_emails=["jane@acmecorp.com"],
            candidate_domain=None,
        ))
        result = await worker._process_record(record)

    assert result["record_state"] == State.DISCOVERED
    assert result["discovery_source"] == "serper"
    assert "jane@acmecorp.com" in json.loads(result["candidate_emails"])


async def test_process_record_both_miss_discovery_failed(worker):
    """Both DNS and Serper miss → DISCOVERY_FAILED."""
    record = _make_record()

    with patch("pipeline.producer.probe_domains", new=AsyncMock(return_value=(None, None))):
        worker._serper.enrich = AsyncMock(return_value=EnrichmentResult())
        result = await worker._process_record(record)

    assert result["record_state"] == State.DISCOVERY_FAILED
    assert result["discovery_source"] is None


async def test_serper_skipped_on_dns_hit_without_strategy(db_conn):
    """DNS hit + strategy=without → Serper not called (cost saved)."""
    record = _make_record()
    worker = await _make_worker(db_conn, _make_config(strategy="without"))

    with patch("pipeline.producer.probe_domains", new=AsyncMock(return_value=("acmecorp.com", "mx1.google.com"))):
        serper_mock = AsyncMock()
        worker._serper.enrich = serper_mock
        result = await worker._process_record(record)

    serper_mock.assert_not_called()
    assert result["record_state"] == State.DISCOVERED
    trace = json.loads(result["process_trace"])
    serper_entry = next(e for e in trace if e["stage"] == "serper")
    assert serper_entry["outcome"] == "skipped"


async def test_serper_skipped_on_dns_hit_with_strategy(db_conn):
    """DNS hit + strategy=with → Serper not called in producer (dispatcher handles fallback)."""
    record = _make_record()
    worker = await _make_worker(db_conn, _make_config(strategy="with"))

    with patch("pipeline.producer.probe_domains", new=AsyncMock(return_value=("acmecorp.com", "mx1.google.com"))):
        serper_mock = AsyncMock()
        worker._serper.enrich = serper_mock
        result = await worker._process_record(record)

    serper_mock.assert_not_called()
    assert result["record_state"] == State.DISCOVERED
    assert result.get("serper_enriched") == 0


async def test_process_record_existing_email_short_circuits(worker):
    """Record with email_biz skips DNS/Serper and is immediately DISCOVERED with source=input."""
    record = _make_record(email_biz="jane@existing.com")

    dns_mock = AsyncMock()
    serper_mock = AsyncMock()

    with patch("pipeline.producer.probe_domains", new=dns_mock):
        worker._serper.enrich = serper_mock
        result = await worker._process_record(record)

    dns_mock.assert_not_called()
    serper_mock.assert_not_called()
    assert result["record_state"] == State.DISCOVERED
    assert result["discovery_source"] == "input"
    assert result["candidate_email"] == "jane@existing.com"
