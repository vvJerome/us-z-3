"""Unit tests for FleetManager routing, reroute-on-block, and attribution."""

import asyncio

import pytest

from pipeline.fleet import manager as manager_mod
from pipeline.fleet.manager import FleetManager
from pipeline.fleet.worker import FleetWorker
from pipeline.models import BackendVerdict


class _StubVerifier:
    """Returns the configured statuses in order (last one repeats); Exceptions are raised."""

    def __init__(self, *statuses):
        self._statuses = list(statuses)
        self.calls = 0

    async def verify(self, email: str) -> BackendVerdict:
        item = self._statuses[min(self.calls, len(self._statuses) - 1)]
        self.calls += 1
        if isinstance(item, Exception):
            raise item
        return BackendVerdict(status=item, message="", verified_at="t")


def _worker(wid, *statuses, concurrency=10):
    return FleetWorker(worker_id=wid, verifier=_StubVerifier(*statuses), concurrency=concurrency)


async def test_verify_tags_probe_host():
    mgr = FleetManager([_worker("cherry-1", "valid")])
    verdict = await mgr.verify("a@b.com")
    assert verdict.status == "valid"
    assert verdict.probe_host == "cherry-1"


async def test_blocked_reroutes_to_healthy_worker():
    w1, w2 = _worker("w1", "blocked"), _worker("w2", "valid")
    mgr = FleetManager([w1, w2], max_reroutes=2)
    verdict = await mgr.verify("a@b.com")
    assert verdict.status == "valid"
    assert verdict.probe_host == "w2"


async def test_blocked_worker_is_cooled_down():
    w1, w2 = _worker("w1", "blocked"), _worker("w2", "valid")
    mgr = FleetManager([w1, w2], block_cooldown_s=300.0)
    await mgr.verify("a@b.com")
    assert w1.is_routable() is False


async def test_all_blocked_returns_blocked_verdict():
    mgr = FleetManager([_worker("w1", "blocked"), _worker("w2", "blocked")], max_reroutes=2)
    verdict = await mgr.verify("a@b.com")
    assert verdict.status == "blocked"


async def test_no_routable_worker_returns_fleet_unavailable():
    mgr = FleetManager([_worker("w1", "valid", concurrency=10)])
    mgr.workers[0].draining = True
    verdict = await mgr.verify("a@b.com")
    assert verdict.status == "error"
    assert "fleet unavailable" in verdict.message


async def test_verifier_exception_becomes_error_verdict():
    mgr = FleetManager([_worker("w1", RuntimeError("boom"))])
    verdict = await mgr.verify("a@b.com")
    assert verdict.status == "error"
    assert "w1" in verdict.message


async def test_on_outcome_receives_classified_provider():
    captured = []

    async def hook(worker_id, provider, status):
        captured.append((worker_id, provider, status))

    mgr = FleetManager([_worker("w1", "valid")], on_outcome=hook)
    await mgr.verify("a@b.com", mx_provider="aspmx.l.google.com")
    assert captured == [("w1", "google", "valid")]


async def test_least_loaded_worker_is_chosen():
    busy, idle = _worker("busy", "valid"), _worker("idle", "valid")
    busy.inflight = 5
    mgr = FleetManager([busy, idle])
    verdict = await mgr.verify("a@b.com")
    assert verdict.probe_host == "idle"


async def test_add_and_remove_worker():
    mgr = FleetManager([_worker("w1", "valid")])
    mgr.add_worker(_worker("w2", "valid"))
    assert {w.worker_id for w in mgr.workers} == {"w1", "w2"}
    mgr.remove_worker("w1")
    assert {w.worker_id for w in mgr.workers} == {"w2"}


async def test_per_domain_concurrency_is_capped():
    peak = {"now": 0, "max": 0}

    class _Slow:
        async def verify(self, email):
            peak["now"] += 1
            peak["max"] = max(peak["max"], peak["now"])
            await asyncio.sleep(0.02)
            peak["now"] -= 1
            return BackendVerdict("valid", "", "t")

    workers = [FleetWorker(worker_id=f"w{i}", verifier=_Slow(), concurrency=100) for i in range(5)]
    mgr = FleetManager(workers, domain_concurrency=2)
    await asyncio.gather(*[mgr.verify(f"u{i}@same.com") for i in range(12)])
    assert peak["max"] <= 2  # never more than 2 concurrent probes to one recipient domain


async def test_same_email_sticks_to_its_worker():
    # Greylist retry must hit the same worker (triplet) even if another is less loaded.
    mgr = FleetManager([_worker("w1", "valid", concurrency=10), _worker("w2", "valid", concurrency=10)])
    first = (await mgr.verify("a@b.com")).probe_host
    other = "w2" if first == "w1" else "w1"
    mgr._by_id[first].inflight = 8   # affined worker busier but still has capacity
    mgr._by_id[other].inflight = 0   # load-balancer alone would prefer this one
    second = (await mgr.verify("a@b.com")).probe_host
    assert second == first


async def test_probe_cancellation_propagates():
    # A CancelledError during shutdown must propagate, never become an "error" verdict.
    class _Cancelling:
        async def verify(self, email):
            raise asyncio.CancelledError()

    mgr = FleetManager([FleetWorker(worker_id="w1", verifier=_Cancelling(), concurrency=10)])
    with pytest.raises(asyncio.CancelledError):
        await mgr.verify("a@b.com")


async def test_affinity_cache_is_bounded(monkeypatch):
    monkeypatch.setattr(manager_mod, "FLEET_AFFINITY_MAX", 5)
    mgr = FleetManager([_worker("w1", "valid")])
    for i in range(50):
        await mgr.verify(f"u{i}@b.com")
    assert len(mgr._affinity) <= 5


async def test_domain_gate_map_is_bounded(monkeypatch):
    # Sequential probes to distinct domains: each is idle once done, so the gate map
    # stays within the cap instead of growing one entry per recipient domain.
    monkeypatch.setattr(manager_mod, "FLEET_DOMAIN_SEM_MAX", 5)
    mgr = FleetManager([_worker("w1", "valid")], domain_concurrency=2)
    for i in range(50):
        await mgr.verify(f"u@d{i}.com")
    assert len(mgr._domain_sems) <= 5


def test_evict_idle_domain_gates_keeps_live_gate(monkeypatch):
    # An in-flight domain (holder/waiter) must never be evicted, even under cap pressure.
    monkeypatch.setattr(manager_mod, "FLEET_DOMAIN_SEM_MAX", 3)
    mgr = FleetManager([_worker("w1", "valid")], domain_concurrency=2)
    for d in ("a.com", "b.com", "c.com", "d.com", "e.com"):
        mgr._domain_sems[d] = asyncio.Semaphore(2)
    mgr._domain_active["c.com"] = 1  # c.com has an in-flight probe
    mgr._evict_idle_domain_gates()
    assert "c.com" in mgr._domain_sems
    assert len(mgr._domain_sems) <= 3
