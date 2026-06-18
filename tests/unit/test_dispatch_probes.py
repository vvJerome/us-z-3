"""Unit tests for the dispatcher backend-probe wrappers."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from pipeline import dispatch_probes as dp
from pipeline.models import BackendVerdict, PipelineHaltError, ValidationResult
from pipeline.utils.zuhal_client import ZuhalCircuitOpenError

pytestmark = pytest.mark.asyncio


class TestMsProbe:
    async def test_returns_status_and_trace(self, monkeypatch):
        monkeypatch.setattr(dp, "check_microsoft_email_async", AsyncMock(return_value={"status": "valid"}))
        status, trace = await dp.ms_probe("a@b.com")
        assert status == "valid"
        assert trace["stage"] == "ms_api" and trace["email"] == "a@b.com"

    async def test_error_swallowed_to_error_status(self, monkeypatch):
        monkeypatch.setattr(dp, "check_microsoft_email_async", AsyncMock(side_effect=RuntimeError("boom")))
        status, _ = await dp.ms_probe("a@b.com")
        assert status == "error"


def _zuhal(verdict=None, exc=None):
    z = MagicMock()
    if exc is not None:
        z.validate = AsyncMock(side_effect=exc)
    else:
        z.validate = AsyncMock(return_value=ValidationResult(
            email="a@b.com", verdict=verdict, score=0.0,
            is_disposable=False, raw_status="", http_status=200,
        ))
    return z


class TestZuhalProbe:
    async def test_valid(self):
        status, trace = await dp.zuhal_probe(_zuhal(verdict="valid"), "a@b.com")
        assert status == "valid" and trace["stage"] == "zuhal_fallback"

    async def test_circuit_open(self):
        status, _ = await dp.zuhal_probe(_zuhal(exc=ZuhalCircuitOpenError()), "a@b.com")
        assert status == "circuit_open"

    async def test_generic_error(self):
        status, _ = await dp.zuhal_probe(_zuhal(exc=RuntimeError("x")), "a@b.com")
        assert status == "error"

    async def test_pipeline_halt_propagates(self):
        with pytest.raises(PipelineHaltError):
            await dp.zuhal_probe(_zuhal(exc=PipelineHaltError("auth")), "a@b.com")


class TestSafeBackends:
    async def test_safe_racknerd_passthrough(self):
        rk = MagicMock()
        rk.verify = AsyncMock(return_value=BackendVerdict(status="valid", message="", verified_at=""))
        assert (await dp.safe_racknerd(rk, "a@b.com")).status == "valid"

    async def test_safe_racknerd_error_swallowed(self):
        rk = MagicMock()
        rk.verify = AsyncMock(side_effect=RuntimeError("net"))
        v = await dp.safe_racknerd(rk, "a@b.com")
        assert v.status == "error" and "net" in v.message

    async def test_safe_bbops_unhealthy_reraised(self):
        from pipeline.consumers.bbops_async import BbopsUnhealthy
        bb = MagicMock()
        bb.verify = AsyncMock(side_effect=BbopsUnhealthy("down"))
        with pytest.raises(BbopsUnhealthy):
            await dp.safe_bbops(bb, 1, "a@b.com")


class TestSerperEnrich:
    async def test_returns_candidate_emails(self):
        serper = MagicMock()
        serper.enrich = AsyncMock(return_value=MagicMock(candidate_emails=["x@y.com"]))
        row = {"business_name": "Acme", "agent_name": "John Doe", "state": "NC",
               "candidate_domain": "acme.com", "strategy": "with"}
        out = await dp.serper_enrich(serper, MagicMock(), "id1", row)
        assert out == ["x@y.com"]

    async def test_error_returns_empty(self):
        serper = MagicMock()
        serper.enrich = AsyncMock(side_effect=RuntimeError("x"))
        row = {"business_name": "Acme", "agent_name": "", "state": "", "candidate_domain": "", "strategy": "without"}
        assert await dp.serper_enrich(serper, MagicMock(), "id1", row) == []
