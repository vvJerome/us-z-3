"""run_backends: OR-of-valids short-circuit on the first `valid` (throughput)."""
from __future__ import annotations

import asyncio
import time

from pipeline.dispatch_probes import run_backends
from pipeline.models import BackendVerdict


class _Now:
    """Returns a fixed verdict immediately."""
    def __init__(self, status):
        self.status = status

    async def verify(self, *a, **k):
        return BackendVerdict(self.status, "", "t")


class _Slow:
    """Returns a fixed verdict only after a long sleep (should be cancelled if peer wins)."""
    def __init__(self, status, delay=5.0):
        self.status = status
        self.delay = delay

    async def verify(self, *a, **k):
        await asyncio.sleep(self.delay)
        return BackendVerdict(self.status, "", "t")


async def test_fleet_valid_short_circuits_slow_bbops():
    t0 = time.monotonic()
    rk, bb = await run_backends(_Now("valid"), _Slow("invalid"), "a@b.com", None, 1, timeout=10)
    assert rk.status == "valid"
    assert bb.status == "not_run"                 # bbops cancelled, not awaited
    assert time.monotonic() - t0 < 1.0            # did not wait the 5s bbops sleep


async def test_bbops_valid_short_circuits_slow_fleet():
    t0 = time.monotonic()
    rk, bb = await run_backends(_Slow("error"), _Now("valid"), "a@b.com", None, 1, timeout=10)
    assert bb.status == "valid" and rk.status == "not_run"
    assert time.monotonic() - t0 < 1.0


async def test_waits_for_bbops_rescue_when_fleet_not_valid():
    # Fleet invalid → bbops must still be consulted (coverage preserved).
    rk, bb = await run_backends(_Now("invalid"), _Now("valid"), "a@b.com", None, 1, timeout=10)
    assert rk.status == "invalid" and bb.status == "valid"


async def test_returns_both_when_neither_valid():
    rk, bb = await run_backends(_Now("invalid"), _Now("invalid"), "a@b.com", None, 1, timeout=10)
    assert rk.status == "invalid" and bb.status == "invalid"
