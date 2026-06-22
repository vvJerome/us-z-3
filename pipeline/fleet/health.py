"""Pure worker-health classification for the SMTP fleet.

Given a snapshot of a worker's recent SMTP outcomes + tunnel state, decide whether
it is healthy, or degraded for a reason that dictates the remedy:
  - DEGRADED_TRANSIENT  → tunnel down/flapping; restart the tunnel, keep the server.
  - DEGRADED_REPUTATION → IP is getting blocked/failing; tear down and reprovision
    for a fresh IP (Improve-Existing item 5).
No I/O — the monitor loop (manager) gathers the snapshot and acts on the verdict.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Health(str, Enum):
    HEALTHY = "healthy"
    DEGRADED_TRANSIENT = "degraded_transient"
    DEGRADED_REPUTATION = "degraded_reputation"


@dataclass(frozen=True)
class HealthThresholds:
    min_samples: int = 20            # don't judge reputation until the window has this many probes
    block_rate: float = 0.50         # blocked / samples at or above this → reputation
    error_rate: float = 0.70         # error / samples at or above this → reputation
    max_consecutive_failures: int = 8


@dataclass(frozen=True)
class WorkerHealthInput:
    tunnel_up: bool
    samples: int                     # probes in the rolling window
    blocked: int
    errors: int
    consecutive_failures: int


def classify(h: WorkerHealthInput, t: HealthThresholds = HealthThresholds()) -> Health:
    """Classify one worker's health from its rolling window + tunnel state."""
    if not h.tunnel_up:
        return Health.DEGRADED_TRANSIENT
    if h.consecutive_failures >= t.max_consecutive_failures:
        return Health.DEGRADED_REPUTATION
    if h.samples >= t.min_samples:
        if h.blocked / h.samples >= t.block_rate:
            return Health.DEGRADED_REPUTATION
        if h.errors / h.samples >= t.error_rate:
            return Health.DEGRADED_REPUTATION
    return Health.HEALTHY
