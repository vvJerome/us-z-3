"""Tests for the autonomous fleet benchmark: summarize, ssh-readiness, teardown-always."""
from __future__ import annotations

import asyncio

import pytest

from pipeline.db import init_db
from pipeline.fleet import benchmark
from pipeline.fleet.benchmark import run_benchmark, summarize, wait_ssh_ready
from pipeline.fleet.provisioner import FleetHost

_COLS = "unique_id, candidate_email, racknerd_status, bbops_status, final_verdict, record_state"
_ROWS = [
    ("1", "a@x.com", "valid",     "invalid",   "valid",     "VALIDATED"),
    ("2", "b@x.com", "invalid",   "valid",     "valid",     "VALIDATED"),   # fleet wrong vs GT
    ("3", "c@x.com", "error",     "catch_all", "catch_all", "VALIDATED"),   # not definitive
    ("4", "d@x.com", "catch_all", "error",     "catch_all", "VALIDATED"),
    ("5", "e@no.com", "valid",    "valid",     "valid",     "VALIDATED"),   # absent from GT
]


async def _make_db(path: str) -> None:
    conn = await init_db(path)
    for row in _ROWS:
        await conn.execute(f"INSERT INTO records ({_COLS}) VALUES (?,?,?,?,?,?)", row)
    await conn.close()


async def test_summarize_scores_against_ground_truth(tmp_path):
    db = tmp_path / "pipeline.db"
    await _make_db(str(db))
    gt = tmp_path / "gt.csv"
    gt.write_text("email,zb_status\na@x.com,valid\nb@x.com,valid\nc@x.com,catch_all\nd@x.com,valid\n")

    rep = summarize(db, gt)

    assert rep.matched == 4                  # e@no.com excluded (not in ground truth)
    assert rep.fleet_definitive == 3         # valid, invalid, catch_all (error is not definitive)
    assert rep.fleet_attempted == 4          # error still counts as an attempt
    assert rep.fleet_correct == 2            # invalid-vs-deliverable is the one wrong call
    assert rep.decisive_accuracy_pct == round(100 * 2 / 3, 2)
    assert rep.coverage_pct == 75.0
    assert rep.validated == 4 and rep.validated_pct == 100.0


async def test_summarize_without_ground_truth_counts_all(tmp_path):
    db = tmp_path / "pipeline.db"
    await _make_db(str(db))
    rep = summarize(db)
    assert rep.has_ground_truth is False
    assert rep.matched == rep.records == 5
    assert rep.decisive_accuracy_pct is None


async def test_wait_ssh_ready_detects_open_and_closed_ports():
    server = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    hosts = [FleetHost(worker_id="w1", server_id=0, ip="127.0.0.1", region="")]
    try:
        ready = await wait_ssh_ready(hosts, port=port, timeout_s=3.0, interval_s=0.1)
        assert ready == ["127.0.0.1"]
    finally:
        server.close()
        await server.wait_closed()
    # Port now closed → not ready within the timeout.
    ready = await wait_ssh_ready(hosts, port=port, timeout_s=0.5, interval_s=0.1)
    assert ready == []


class _FakeProv:
    def __init__(self, hosts):
        self._hosts = hosts
        self.torn_down = False

    async def provision(self, count, key_ids, *, reserve_region=None):
        return self._hosts

    def write_inventory(self, hosts, *, merge=True):
        pass

    async def teardown(self):
        self.torn_down = True
        return [h.server_id for h in self._hosts]


async def test_run_benchmark_tears_down_on_validation_failure(monkeypatch):
    prov = _FakeProv([FleetHost(worker_id="w1", server_id=11, ip="1.2.3.4", region="r")])
    monkeypatch.setattr(benchmark, "wait_ssh_ready", lambda hosts, **k: _async(["1.2.3.4"]))

    async def _boom(*a, **k):
        raise RuntimeError("validation blew up")

    monkeypatch.setattr(benchmark, "_run_validation", _boom)
    with pytest.raises(RuntimeError):
        await run_benchmark(prov, count=1, key_ids=[1], input_path="x.jsonl")
    assert prov.torn_down is True   # finally always tears the fleet down


async def test_run_benchmark_raises_and_tears_down_when_no_workers(monkeypatch):
    prov = _FakeProv([])   # provisioned nothing
    with pytest.raises(RuntimeError):
        await run_benchmark(prov, count=3, key_ids=[1], input_path="x.jsonl")
    assert prov.torn_down is True


def _async(value):
    async def _coro():
        return value
    return _coro()
