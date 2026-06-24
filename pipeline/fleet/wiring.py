"""Wire the Cherry Servers SMTP fleet into the pipeline entrypoint.

Builds a FleetManager whose workers each open their own SSH SOCKS5 tunnel and run a
RacknerdConsumer over it, plus (when a Cherry token + project are configured) a
FleetSupervisor for live health/auto-heal/scaling. Returned as a FleetContext the
entrypoint passes to the dispatcher as the SMTP backend and tears down on shutdown.
"""
from __future__ import annotations

import asyncio
import logging
import os
import socket
from dataclasses import dataclass, field
from pathlib import Path

import aiodns

from pipeline import db
from pipeline.config import PipelineConfig
from pipeline.constants import FLEET_PTR_LOOKUP_TIMEOUT_S
from pipeline.consumers.racknerd import RacknerdConfig, RacknerdConsumer
from pipeline.fleet.cherry_client import CherryClient
from pipeline.fleet.control import FleetSupervisor
from pipeline.fleet.manager import FleetManager
from pipeline.fleet.provisioner import FleetHost, FleetProvisioner, read_inventory
from pipeline.fleet.worker import FleetWorker
from pipeline.tunnels.ssh_socks import SshSocksTunnel, TunnelConfig

logger = logging.getLogger("pipeline.fleet.wiring")


@dataclass
class FleetContext:
    manager: FleetManager
    supervisor: FleetSupervisor | None = None
    client: CherryClient | None = None
    tasks: list[asyncio.Task] = field(default_factory=list)

    async def aclose(self) -> None:
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        for worker in self.manager.workers:
            if worker.tunnel is not None and hasattr(worker.tunnel, "stop"):
                await worker.tunnel.stop()
        if self.client is not None:
            await self.client.__aexit__(None, None, None)


def resolve_hosts(config: PipelineConfig) -> list[FleetHost]:
    """Worker hosts from explicit --smtp-hosts, else the provisioner inventory."""
    if config.smtp_hosts:
        return [
            FleetHost(worker_id=f"smtp-{i + 1}", server_id=0, ip=ip, region="", managed=False)
            for i, ip in enumerate(config.smtp_hosts)
        ]
    return read_inventory()


async def _make_worker(
    config: PipelineConfig, host: FleetHost, socks_port: int, resolver: aiodns.DNSResolver | None
) -> FleetWorker:
    tcfg = TunnelConfig(host=host.ip, user=config.cherry_ssh_user, ssh_key=config.cherry_ssh_key,
                        socks_port=socks_port, autorestart=True)
    tunnel = SshSocksTunnel(tcfg)
    await tunnel.start(ready_timeout_s=30.0)
    # HELO/MAIL FROM with the worker's own rDNS (PTR) so it matches the connecting IP — the
    # forward-confirmed check receivers run before honoring RCPT. Explicit config wins; else
    # fall back to the RacknerdConfig default if the IP has no PTR.
    rk_kwargs: dict = {}
    if config.racknerd_helo_hostname:
        rk_kwargs["helo_hostname"] = config.racknerd_helo_hostname
    else:
        try:
            ptr = (await asyncio.wait_for(
                asyncio.to_thread(socket.gethostbyaddr, host.ip),
                timeout=FLEET_PTR_LOOKUP_TIMEOUT_S,
            ))[0]
            if ptr and "." in ptr:
                rk_kwargs["helo_hostname"] = ptr
        except (OSError, asyncio.TimeoutError):
            pass
    consumer = RacknerdConsumer(
        tunnel,
        RacknerdConfig(socks_port=socks_port, concurrency=config.racknerd_concurrency,
                       smtp_timeout_s=config.racknerd_smtp_timeout_s, **rk_kwargs),
        resolver=resolver,
    )
    return FleetWorker(worker_id=host.worker_id, verifier=consumer, tunnel=tunnel,
                       concurrency=config.racknerd_concurrency, server_id=host.server_id or None,
                       managed=host.managed, is_reserve=host.is_reserve)


async def _ensure_key(prov: FleetProvisioner, config: PipelineConfig) -> list[int]:
    pub = Path(os.path.expanduser(config.cherry_ssh_key + ".pub"))
    if not pub.exists():
        logger.warning("Cherry SSH public key %s missing — auto-heal cannot provision", pub)
        return []
    return [await prov.ensure_ssh_key("cherry_fleet", pub.read_text().strip())]


async def build_fleet(
    config: PipelineConfig,
    conn,
    stop_event: asyncio.Event,
    *,
    resolver: aiodns.DNSResolver | None = None,
) -> FleetContext:
    """Construct the SMTP fleet (and supervisor when a Cherry token is configured)."""
    hosts = resolve_hosts(config)
    if not hosts:
        raise ValueError("fleet enabled but no hosts — set --smtp-hosts or provision an inventory")

    base_port = config.racknerd_socks_port
    workers = [await _make_worker(config, host, base_port + i, resolver) for i, host in enumerate(hosts)]

    async def _on_outcome(worker_id: str, provider: str, status: str) -> None:
        await db.record_smtp_outcome(conn, worker_id, provider, status)

    manager = FleetManager(workers, block_cooldown_s=config.fleet_block_cooldown_s,
                           max_reroutes=config.fleet_max_reroutes, on_outcome=_on_outcome,
                           domain_concurrency=config.fleet_domain_concurrency)
    ctx = FleetContext(manager=manager)
    logger.info("SMTP fleet up with %d worker(s)", len(workers))

    token = os.environ.get("CHERRY_AUTH_TOKEN", "")
    if config.cherry_enabled and token and config.cherry_project_id:
        client = CherryClient(token)
        await client.__aenter__()
        ctx.client = client
        prov = FleetProvisioner(client, config.cherry_project_id, plan=config.cherry_plan,
                                region=config.cherry_region, image=config.cherry_image)
        key_ids = await _ensure_key(prov, config)
        next_port = {"v": base_port + len(hosts)}

        async def _factory(host: FleetHost) -> FleetWorker:
            port = next_port["v"]
            next_port["v"] += 1
            return await _make_worker(config, host, port, resolver)

        ctx.supervisor = FleetSupervisor(
            manager, prov, client, worker_factory=_factory, key_ids=key_ids,
            team_id=config.cherry_team_id, credit_floor_eur=config.fleet_credit_floor_eur,
            max_reprovisions=config.fleet_max_reprovisions, scale_min=config.fleet_scale_min,
            scale_max=config.fleet_scale_max,
        )
        ctx.tasks.append(asyncio.create_task(
            ctx.supervisor.run(stop_event, poll_interval_s=config.fleet_monitor_interval_s),
            name="fleet-supervisor",
        ))
        logger.info("Fleet supervisor started — monitor/auto-heal/scale")
    return ctx
