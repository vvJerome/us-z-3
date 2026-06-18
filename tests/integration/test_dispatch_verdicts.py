"""Integration tests for the extracted Zuhal-rescue verdict handlers."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import aiosqlite
import pytest

from pipeline import db, dispatch_verdicts as dv
from pipeline.config import PipelineConfig
from pipeline.db import State
from pipeline.dispatcher import Dispatcher
from pipeline.models import BackendVerdict, ValidationResult
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.zuhal_client import ZuhalCircuitOpenError

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def test_db(tmp_path: Path) -> aiosqlite.Connection:
    conn = await db.init_db(tmp_path / "t.db")
    yield conn
    await conn.close()


def _config(tmp_path: Path, **kw) -> PipelineConfig:
    base = dict(
        serper_api_key="x", zuhal_api_key="x", racknerd_host="localhost",
        input_path=tmp_path / "i.jsonl", output_dir=tmp_path, db_path=tmp_path / "t.db",
        log_dir=tmp_path / "logs",
    )
    base.update(kw)
    return PipelineConfig(**base)


def _zuhal(verdict=None, exc=None):
    z = MagicMock()
    if exc is not None:
        z.validate = AsyncMock(side_effect=exc)
    else:
        z.validate = AsyncMock(return_value=ValidationResult(
            email="a@b.com", verdict=verdict, score=0.0, is_disposable=False, raw_status="", http_status=200))
    return z


async def _seed(conn, uid="IL-1"):
    await conn.execute(
        "INSERT INTO records (unique_id, candidate_emails, candidate_email, candidate_domain, "
        "strategy, mx_provider, record_state) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (uid, json.dumps(["a@b.com"]), "a@b.com", "b.com", "with", "gmail.com", State.VALIDATING),
    )
    await conn.commit()


def _disp(config, conn, zuhal, cost=None):
    return Dispatcher(config, conn, MagicMock(), MagicMock(), cost or CostTracker(None), zuhal=zuhal)


_RK = BackendVerdict(status="invalid", message="550", verified_at="")
_BB_ERR = BackendVerdict(status="error", message="timeout", verified_at="")
_BB_INV = BackendVerdict(status="invalid", message="550", verified_at="")


async def _state(conn, uid="IL-1"):
    async with conn.execute("SELECT record_state, canonical_source FROM records WHERE unique_id=?", (uid,)) as c:
        return await c.fetchone()


class TestHandleInconclusive:
    async def test_decoupled_handoff(self, test_db, tmp_path):
        await _seed(test_db)
        disp = _disp(_config(tmp_path, zuhal_decoupled=True), test_db, _zuhal(verdict="valid"))
        action = await dv.handle_inconclusive(disp, "IL-1", "a@b.com", _RK, _BB_ERR, "b.com", "with", "John Doe", "john", "doe", "gmail.com", False, [])
        assert action == "terminal"
        assert (await _state(test_db))["record_state"] == State.NEEDS_ZUHAL

    async def test_inline_zuhal_valid(self, test_db, tmp_path):
        await _seed(test_db)
        disp = _disp(_config(tmp_path, zuhal_decoupled=False), test_db, _zuhal(verdict="valid"))
        action = await dv.handle_inconclusive(disp, "IL-1", "a@b.com", _RK, _BB_ERR, "b.com", "with", "John Doe", "john", "doe", "gmail.com", False, [])
        assert action == "terminal"
        row = await _state(test_db)
        assert row["record_state"] == State.VALIDATED and row["canonical_source"] == "zuhal"

    async def test_circuit_open_requeues(self, test_db, tmp_path):
        await _seed(test_db)
        disp = _disp(_config(tmp_path, zuhal_decoupled=False), test_db, _zuhal(exc=ZuhalCircuitOpenError()))
        action = await dv.handle_inconclusive(disp, "IL-1", "a@b.com", _RK, _BB_ERR, "b.com", "with", "John Doe", "john", "doe", "gmail.com", False, [])
        assert action == "terminal"
        assert (await _state(test_db))["record_state"] == State.DISCOVERED

    async def test_no_zuhal_requeues(self, test_db, tmp_path):
        await _seed(test_db)
        disp = _disp(_config(tmp_path), test_db, None)
        action = await dv.handle_inconclusive(disp, "IL-1", "a@b.com", _RK, _BB_ERR, "b.com", "with", "John Doe", "john", "doe", "gmail.com", False, [])
        assert action == "terminal"
        assert (await _state(test_db))["record_state"] == State.DISCOVERED

    async def test_cost_ceiling(self, test_db, tmp_path):
        await _seed(test_db)
        cost = CostTracker(max_cost=0.0)
        cost.record_call("serper_producer")
        disp = _disp(_config(tmp_path, zuhal_decoupled=False), test_db, _zuhal(verdict="valid"), cost)
        action = await dv.handle_inconclusive(disp, "IL-1", "a@b.com", _RK, _BB_ERR, "b.com", "with", "John Doe", "john", "doe", "gmail.com", False, [])
        assert action == "cost_skipped"


class TestRescueBothInvalid:
    async def test_valid_terminal(self, test_db, tmp_path):
        await _seed(test_db)
        disp = _disp(_config(tmp_path), test_db, _zuhal(verdict="valid"))
        action = await dv.rescue_both_invalid(disp, "IL-1", "a@b.com", _RK, _BB_INV, "b.com", "with", "John Doe", "john", "doe", "gmail.com", [])
        assert action == "terminal"
        assert (await _state(test_db))["record_state"] == State.VALIDATED

    async def test_invalid_falls_through(self, test_db, tmp_path):
        await _seed(test_db)
        disp = _disp(_config(tmp_path), test_db, _zuhal(verdict="invalid"))
        action = await dv.rescue_both_invalid(disp, "IL-1", "a@b.com", _RK, _BB_INV, "b.com", "with", "John Doe", "john", "doe", "gmail.com", [])
        assert action is None
