"""The SMTP fleet manager — the dispatcher's verify() seam over many egress workers.

Load-balances each probe to the least-loaded healthy worker, reroutes an IP-blocked
probe to a different worker instead of trusting the block as a verdict (item 5), and
tags each verdict with the worker that ran it. Persisting per-(worker, provider)
outcomes is delegated to an injected hook so this stays DB-free and unit-testable.
Live health monitoring / auto-heal / elastic scaling layer on in C7.
"""
from __future__ import annotations

import datetime
import logging
import time
from collections.abc import Awaitable, Callable

from pipeline.fleet.balancer import WorkerLoad, pick_worker
from pipeline.fleet.worker import FleetWorker
from pipeline.models import BackendVerdict
from pipeline.utils.providers import classify_provider

logger = logging.getLogger("pipeline.fleet.manager")

# (worker_id, provider, status) -> persist the outcome (e.g. db.record_smtp_outcome).
OutcomeHook = Callable[[str, str, str], Awaitable[None]]


def _iso_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class FleetManager:
    """Routes SMTP probes across a pool of FleetWorkers behind one verify() seam."""

    def __init__(
        self,
        workers: list[FleetWorker],
        *,
        block_cooldown_s: float = 300.0,
        max_reroutes: int = 2,
        on_outcome: OutcomeHook | None = None,
    ) -> None:
        self._workers = list(workers)
        self._by_id = {w.worker_id: w for w in self._workers}
        self._block_cooldown_s = block_cooldown_s
        self._max_reroutes = max_reroutes
        self._on_outcome = on_outcome

    @property
    def workers(self) -> list[FleetWorker]:
        return list(self._workers)

    def add_worker(self, worker: FleetWorker) -> None:
        if worker.worker_id in self._by_id:
            return
        self._workers.append(worker)
        self._by_id[worker.worker_id] = worker

    def remove_worker(self, worker_id: str) -> FleetWorker | None:
        worker = self._by_id.pop(worker_id, None)
        if worker is not None:
            self._workers = [w for w in self._workers if w.worker_id != worker_id]
        return worker

    def _snapshot(self, now: float, exclude: set[str]) -> list[WorkerLoad]:
        return [
            WorkerLoad(
                worker_id=w.worker_id,
                routable=w.is_routable(now),
                available=w.available_slots(now),
                inflight=w.inflight,
            )
            for w in self._workers
            if w.worker_id not in exclude
        ]

    async def verify(self, email: str, mx_provider: str | None = None) -> BackendVerdict:
        """Probe `email` via the least-loaded healthy worker, rerouting on IP blocks."""
        provider = classify_provider(mx_provider)
        tried: set[str] = set()
        last: BackendVerdict | None = None
        for _ in range(self._max_reroutes + 1):
            now = time.monotonic()
            wid = pick_worker(self._snapshot(now, tried))
            if wid is None:
                break
            worker = self._by_id[wid]
            tried.add(wid)
            verdict = await self._probe(worker, email)
            verdict.probe_host = wid
            worker.record(verdict.status)
            if self._on_outcome is not None:
                await self._on_outcome(wid, provider, verdict.status)
            last = verdict
            if verdict.status == "blocked":
                worker.cool(self._block_cooldown_s, now)
                logger.warning("worker %s blocked on provider %s; rerouting", wid, provider)
                continue
            return verdict
        if last is not None:
            return last
        return BackendVerdict(
            status="error", message="fleet unavailable: no routable worker", verified_at=_iso_now()
        )

    async def _probe(self, worker: FleetWorker, email: str) -> BackendVerdict:
        worker.inflight += 1
        try:
            return await worker.verifier.verify(email)
        except Exception as exc:
            # Any verifier failure is a transient error verdict, never an email rejection.
            return BackendVerdict(
                status="error", message=f"worker {worker.worker_id}: {exc}", verified_at=_iso_now()
            )
        finally:
            worker.inflight -= 1
