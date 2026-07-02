"""Pure load-balancing selection for the SMTP fleet.

Per probe, pick the least-loaded eligible worker. Eligibility (healthy + not cooling
for this provider + free capacity) is computed by the manager, which passes a flat
list of WorkerLoad snapshots. Returning a worker_id (not an object) keeps this module
dependency-free and trivially testable. No static record→worker sharding, so workers
never sit idle while others are backlogged (Parallelization step 5).
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerLoad:
    worker_id: str
    routable: bool      # healthy AND not in provider-specific cooldown
    available: int      # free concurrency slots for the target provider
    inflight: int       # current in-flight probes (least-loaded tiebreak)


def pick_worker(loads: Sequence[WorkerLoad]) -> str | None:
    """Return the least-loaded eligible worker_id, or None if none can take the probe."""
    eligible = [w for w in loads if w.routable and w.available > 0]
    if not eligible:
        return None
    return min(eligible, key=lambda w: (w.inflight, -w.available)).worker_id
