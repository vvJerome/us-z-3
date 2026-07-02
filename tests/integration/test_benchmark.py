"""Tests for the autonomous fleet benchmark: summarize, ssh-readiness, teardown-always."""
from __future__ import annotations

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


async def test_wait_ssh_ready_uses_injected_auth_probe():
    # 1.1.1.1 authenticates on the 2nd poll; 2.2.2.2 on the 1st.
    calls = {"1.1.1.1": 0, "2.2.2.2": 0}

    async def probe(ip):
        calls[ip] += 1
        return calls[ip] >= (2 if ip == "1.1.1.1" else 1)

    hosts = [FleetHost("w1", 0, "1.1.1.1", ""), FleetHost("w2", 0, "2.2.2.2", "")]
    ready = await wait_ssh_ready(hosts, timeout_s=5.0, interval_s=0, probe=probe)
    assert set(ready) == {"1.1.1.1", "2.2.2.2"}


async def test_wait_ssh_ready_times_out_when_login_never_succeeds():
    async def never(ip):
        return False

    hosts = [FleetHost("w1", 0, "1.1.1.1", "")]
    ready = await wait_ssh_ready(hosts, timeout_s=0.2, interval_s=0.05, probe=never)
    assert ready == []


class _FakeClient:
    """Minimal Cherry client for the teardown sweep: no orphans in the project."""
    async def list_servers(self, project_id):
        return []

    async def delete_server(self, server_id):
        return None


class _FakeProv:
    def __init__(self, hosts):
        self._hosts = hosts
        self.torn_down = 0
        self.region = "test-region"
        self.project_id = 276330
        self.client = _FakeClient()

    async def provision(self, count, key_ids, *, reserve_region=None):
        return self._hosts

    def write_inventory(self, hosts, *, merge=True):
        pass

    def load_inventory(self):
        return self._hosts

    async def teardown(self):
        self.torn_down += 1
        return [h.server_id for h in self._hosts]


async def test_run_benchmark_tears_down_on_validation_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)   # keep output/ artifacts inside the tmp dir
    prov = _FakeProv([FleetHost(worker_id="w1", server_id=11, ip="1.2.3.4", region="r")])
    monkeypatch.setattr(benchmark, "wait_ssh_ready", lambda hosts, **k: _async(["1.2.3.4"]))

    async def _boom(*a, **k):
        raise RuntimeError("validation blew up")

    monkeypatch.setattr(benchmark, "_run_validation", _boom)
    with pytest.raises(RuntimeError):
        await run_benchmark(prov, count=1, key_ids=[1], input_path="x.jsonl", name="t_fail")
    assert prov.torn_down >= 1   # finally always tears the fleet down


async def test_run_benchmark_raises_and_tears_down_when_no_workers(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    prov = _FakeProv([])   # provisioned nothing
    with pytest.raises(RuntimeError):
        await run_benchmark(prov, count=3, key_ids=[1], input_path="x.jsonl", name="t_none",
                            provision_retries=1, provision_retry_delay_s=0)
    assert prov.torn_down >= 1


async def test_run_benchmark_keep_fleet_skips_teardown(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    prov = _FakeProv([FleetHost(worker_id="w1", server_id=11, ip="1.2.3.4", region="r")])
    monkeypatch.setattr(benchmark, "wait_ssh_ready", lambda hosts, **k: _async(["1.2.3.4"]))
    monkeypatch.setattr(benchmark, "_run_validation",
                        lambda *a, **k: _async(tmp_path / "out" / "pipeline.db"))
    monkeypatch.setattr(benchmark, "summarize",
                        lambda db, gt=None: benchmark.BenchmarkReport(records=0, matched=0,
                                                                      has_ground_truth=False))
    await run_benchmark(prov, count=1, key_ids=[1], input_path="x.jsonl", name="t_keep",
                        teardown=False)
    assert prov.torn_down == 0   # --keep-fleet must never tear down


def _async(value):
    async def _coro():
        return value
    return _coro()
