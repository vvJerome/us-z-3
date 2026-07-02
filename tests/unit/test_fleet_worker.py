"""Unit tests for FleetWorker load/cooldown/window state."""

from pipeline.fleet.worker import FleetWorker
from pipeline.models import BackendVerdict


class _Stub:
    async def verify(self, email: str) -> BackendVerdict:
        return BackendVerdict(status="valid", message="", verified_at="t")


class _Tunnel:
    def __init__(self, up: bool):
        self._up = up

    def is_up(self) -> bool:
        return self._up


def _worker(**kw) -> FleetWorker:
    return FleetWorker(worker_id="a", verifier=_Stub(), **kw)


def test_default_worker_is_routable():
    assert _worker().is_routable(now=0.0) is True


def test_draining_worker_not_routable():
    assert _worker(draining=True).is_routable(now=0.0) is False


def test_cooldown_blocks_then_clears():
    w = _worker()
    w.cool(100.0, now=1000.0)
    assert w.is_routable(now=1050.0) is False
    assert w.is_routable(now=1100.0) is True


def test_available_slots_reflect_inflight():
    w = _worker(concurrency=10)
    w.inflight = 4
    assert w.available_slots(now=0.0) == 6


def test_available_slots_zero_when_not_routable():
    w = _worker(concurrency=10, draining=True)
    assert w.available_slots(now=0.0) == 0


def test_down_tunnel_makes_worker_unroutable():
    assert _worker(tunnel=_Tunnel(False)).is_routable(now=0.0) is False


def test_record_tracks_consecutive_failures():
    w = _worker()
    w.record("blocked")
    w.record("error")
    assert w.health_input().consecutive_failures == 2


def test_success_resets_consecutive_failures():
    w = _worker()
    w.record("blocked")
    w.record("valid")
    assert w.health_input().consecutive_failures == 0


def test_health_input_counts_window():
    w = _worker()
    for status in ("blocked", "blocked", "error", "valid"):
        w.record(status)
    hi = w.health_input()
    assert (hi.samples, hi.blocked, hi.errors) == (4, 2, 1)
