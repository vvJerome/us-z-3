"""Unit tests for pure fleet load-balancer selection."""

from pipeline.fleet.balancer import WorkerLoad, pick_worker


def test_picks_least_inflight_among_eligible():
    loads = [
        WorkerLoad("a", routable=True, available=5, inflight=3),
        WorkerLoad("b", routable=True, available=5, inflight=1),
        WorkerLoad("c", routable=True, available=5, inflight=2),
    ]
    assert pick_worker(loads) == "b"


def test_skips_non_routable():
    loads = [
        WorkerLoad("a", routable=False, available=5, inflight=0),
        WorkerLoad("b", routable=True, available=5, inflight=9),
    ]
    assert pick_worker(loads) == "b"


def test_skips_workers_at_capacity():
    loads = [
        WorkerLoad("a", routable=True, available=0, inflight=0),
        WorkerLoad("b", routable=True, available=1, inflight=4),
    ]
    assert pick_worker(loads) == "b"


def test_returns_none_when_none_eligible():
    loads = [
        WorkerLoad("a", routable=False, available=5, inflight=0),
        WorkerLoad("b", routable=True, available=0, inflight=0),
    ]
    assert pick_worker(loads) is None


def test_tiebreak_prefers_more_available_slots():
    loads = [
        WorkerLoad("a", routable=True, available=2, inflight=1),
        WorkerLoad("b", routable=True, available=8, inflight=1),
    ]
    assert pick_worker(loads) == "b"


def test_empty_returns_none():
    assert pick_worker([]) is None
