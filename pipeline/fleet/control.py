"""Live fleet supervision: health monitoring, auto-heal, and elastic scaling.

Runs alongside the dispatcher. Each cycle classifies every worker's health and, for
an IP-reputation degradation, drains it, provisions a fresh server (new IP), swaps it
into the pool, and deletes the old one — without pausing the dispatcher (item 1/5).
Scaling is manual (a control file / scale_to) and bounded. Guards — a credit floor and
a per-run reprovision cap — stop a flapping condition from burning the account.

I/O-light and injectable: worker_factory builds a FleetWorker from a provisioned host,
so this is unit-testable with stub workers and a faked Cherry API.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path

from pipeline.fleet.cherry_client import CherryAPIError, CherryClient
from pipeline.fleet.health import Health, HealthThresholds, classify
from pipeline.fleet.manager import FleetManager
from pipeline.fleet.provisioner import FleetHost, FleetProvisioner
from pipeline.fleet.worker import FleetWorker

logger = logging.getLogger("pipeline.fleet.control")

WorkerFactory = Callable[[FleetHost], Awaitable[FleetWorker]]
DEFAULT_CONTROL = Path("output/fleet/control.json")


class FleetSupervisor:
    def __init__(
        self,
        manager: FleetManager,
        provisioner: FleetProvisioner,
        client: CherryClient,
        *,
        worker_factory: WorkerFactory,
        key_ids: list[int],
        team_id: int,
        credit_floor_eur: float = 0.10,
        max_reprovisions: int = 10,
        scale_min: int = 1,
        scale_max: int = 10,
        thresholds: HealthThresholds = HealthThresholds(),
        name_prefix: str = "cherry",
        control_path: Path = DEFAULT_CONTROL,
    ) -> None:
        self.manager = manager
        self.provisioner = provisioner
        self.client = client
        self.worker_factory = worker_factory
        self.key_ids = key_ids
        self.team_id = team_id
        self.credit_floor_eur = credit_floor_eur
        self.max_reprovisions = max_reprovisions
        self.scale_min = scale_min
        self.scale_max = scale_max
        self.thresholds = thresholds
        self.name_prefix = name_prefix
        self.control_path = Path(control_path)
        self._reprovisions = 0
        self._spawn_seq = 0

    async def can_provision(self) -> bool:
        if self._reprovisions >= self.max_reprovisions:
            logger.warning("reprovision cap %d reached — not provisioning", self.max_reprovisions)
            return False
        try:
            credit = await self.client.get_team_credit(self.team_id)
        except CherryAPIError as exc:
            logger.error("credit check failed: %s", exc)
            return False
        if credit < self.credit_floor_eur:
            logger.warning("credit %.2f below floor %.2f — not provisioning", credit, self.credit_floor_eur)
            return False
        return True

    async def monitor_once(self) -> None:
        """Classify every worker; auto-heal the IP-reputation-degraded ones."""
        for worker in self.manager.workers:
            if worker.draining:
                continue
            health = classify(worker.health_input(), self.thresholds)
            if health is Health.HEALTHY:
                continue
            if health is Health.DEGRADED_TRANSIENT:
                logger.warning("worker %s tunnel degraded; leaving it to tunnel auto-restart", worker.worker_id)
                continue
            if worker.managed and await self.can_provision():
                await self._replace(worker)

    async def _spawn(self) -> FleetWorker:
        self._spawn_seq += 1
        host = await self.provisioner.provision_one(
            self.key_ids, f"{self.name_prefix}-r{self._spawn_seq}",
        )
        worker = await self.worker_factory(host)
        self.manager.add_worker(worker)
        self.provisioner.write_inventory([host])
        return worker

    async def _replace(self, worker: FleetWorker) -> None:
        """Auto-heal: provision a fresh IP, swap it in, then delete the degraded server."""
        worker.draining = True
        try:
            new = await self._spawn()
        except CherryAPIError as exc:
            worker.draining = False  # keep using the old worker if provisioning failed
            logger.error("auto-heal of %s failed to provision: %s", worker.worker_id, exc)
            return
        self._reprovisions += 1
        self.manager.remove_worker(worker.worker_id)
        await self._stop_tunnel(worker)
        if worker.server_id is not None:
            try:
                await self.client.delete_server(worker.server_id)
            except CherryAPIError as exc:
                logger.error("could not delete old server %s: %s", worker.server_id, exc)
        logger.info("auto-healed %s -> %s (fresh IP)", worker.worker_id, new.worker_id)

    @staticmethod
    async def _stop_tunnel(worker: FleetWorker) -> None:
        if worker.tunnel is not None and hasattr(worker.tunnel, "stop"):
            await worker.tunnel.stop()

    async def scale_to(self, target: int) -> None:
        """Grow or shrink the pool toward `target`, clamped to [scale_min, scale_max]."""
        target = max(self.scale_min, min(self.scale_max, target))
        current = len(self.manager.workers)
        if target > current:
            for _ in range(target - current):
                if not await self.can_provision():
                    break
                await self._spawn()
        elif target < current:
            await self._scale_down(current - target)

    async def _scale_down(self, n: int) -> None:
        removable = sorted(
            (w for w in self.manager.workers if w.managed and not w.is_reserve),
            key=lambda w: w.inflight,
        )[:n]
        for worker in removable:
            worker.draining = True
            self.manager.remove_worker(worker.worker_id)
            await self._stop_tunnel(worker)
            logger.info("scaled down: removed worker %s", worker.worker_id)

    async def check_control_file(self) -> None:
        """Apply an out-of-band scale command: {"scale_to": N} in the control file."""
        if not self.control_path.exists():
            return
        try:
            cmd = json.loads(self.control_path.read_text())
        except ValueError:
            logger.error("invalid control file %s", self.control_path)
            return
        if "scale_to" in cmd:
            await self.scale_to(int(cmd["scale_to"]))
        self.control_path.unlink(missing_ok=True)

    async def run(self, stop_event: asyncio.Event, *, poll_interval_s: float = 15.0) -> None:
        while not stop_event.is_set():
            try:
                await self.monitor_once()
                await self.check_control_file()
            except Exception as exc:
                logger.error("supervisor cycle error: %s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_s)
            except asyncio.TimeoutError:
                pass
