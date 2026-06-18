"""Unit tests for PipelineConfig validators."""
from __future__ import annotations

import pytest

from pipeline.config import PipelineConfig


def _cfg(**kw) -> PipelineConfig:
    base = dict(serper_api_key="x", zuhal_api_key="x", racknerd_host="localhost")
    base.update(kw)
    return PipelineConfig(**base)


class TestFlagValidation:
    def test_producer_and_consumer_only_mutually_exclusive(self):
        with pytest.raises(ValueError):
            _cfg(producer_only=True, consumer_only=True)

    def test_racknerd_requires_host_when_enabled_and_not_direct(self):
        with pytest.raises(ValueError):
            _cfg(racknerd_host="", racknerd_enabled=True, racknerd_direct=False)

    def test_racknerd_direct_needs_no_host(self):
        cfg = _cfg(racknerd_host="", racknerd_direct=True)
        assert cfg.racknerd_direct is True

    def test_producer_only_needs_no_host(self):
        cfg = _cfg(racknerd_host="", producer_only=True)
        assert cfg.producer_only is True


class TestDispatcherChunkSaturation:
    def test_chunk_bumped_below_concurrency(self):
        cfg = _cfg(dispatch_concurrency=50, dispatch_chunk_size=10)
        assert cfg.dispatch_chunk_size == 100  # auto-bumped to concurrency * 2

    def test_chunk_kept_when_adequate(self):
        cfg = _cfg(dispatch_concurrency=10, dispatch_chunk_size=50)
        assert cfg.dispatch_chunk_size == 50
