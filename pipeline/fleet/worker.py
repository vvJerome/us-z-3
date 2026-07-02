"""One SMTP fleet worker: an egress IP reached via its own SSH SOCKS5 tunnel.

Holds the worker's load, cooldown, and rolling-window state. `verifier` is any object
exposing `async verify(email) -> BackendVerdict` — a RacknerdConsumer bound to this
worker's tunnel in production, or a stub in tests. Probe orchestration lives in the
manager; this type is intentionally I/O-free apart from the injected verifier.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Protocol

from pipeline.fleet.health import WorkerHealthInput
from pipeline.models import BackendVerdict


class SmtpVerifier(Protocol):
    async def verify(self, email: str) -> BackendVerdict: ...


# Probe statuses that count as a worker-level failure for the health window.
_FAILURE_STATUSES = frozenset({"blocked", "error"})
_WINDOW_SIZE = 50


@dataclass
class FleetWorker:
    worker_id: str
    verifier: SmtpVerifier
    tunnel: object | None = None      # SshSocksTunnel | None (opaque here to avoid the import)
    concurrency: int = 10
    server_id: int | None = None
    managed: bool = True              # False = pre-existing box; never deleted by auto-heal/scale-down
    is_reserve: bool = False          # cross-region failover worker (item 6); kept on scale-down
    draining: bool = False
    inflight: int = 0
    _cooldown_until: float = 0.0
    _window: deque[str] = field(default_factory=lambda: deque(maxlen=_WINDOW_SIZE))
    _consecutive_failures: int = 0

    def is_routable(self, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        return not self.draining and now >= self._cooldown_until and self.tunnel_up()

    def available_slots(self, now: float | None = None) -> int:
        if not self.is_routable(now):
            return 0
        return max(0, self.concurrency - self.inflight)

    def tunnel_up(self) -> bool:
        if self.tunnel is None:
            return True
        is_up = getattr(self.tunnel, "is_up", None)
        return bool(is_up()) if callable(is_up) else True

    def cool(self, seconds: float, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        self._cooldown_until = now + seconds

    def record(self, status: str) -> None:
        self._window.append(status)
        if status in _FAILURE_STATUSES:
            self._consecutive_failures += 1
        elif status != "not_run":
            self._consecutive_failures = 0

    def health_input(self) -> WorkerHealthInput:
        return WorkerHealthInput(
            tunnel_up=self.tunnel_up(),
            samples=len(self._window),
            blocked=sum(1 for s in self._window if s == "blocked"),
            errors=sum(1 for s in self._window if s == "error"),
            consecutive_failures=self._consecutive_failures,
        )
