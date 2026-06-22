"""Cherry Servers fleet provisioning: create/await/teardown workers + inventory.

All API calls go through CherryClient; no DB, no SSH here. The live FleetManager
calls provision()/teardown() at runtime for auto-heal and elastic scaling (item 1).
Inventory is a JSON file the pipeline reads to build the worker pool; servers we did
not create (e.g. the pre-existing box) are marked managed=False and never torn down.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path

from pipeline.fleet.cherry_client import CherryAPIError, CherryClient, public_ip

logger = logging.getLogger("pipeline.fleet.provisioner")

DEFAULT_INVENTORY = Path("output/fleet/hosts.json")


@dataclass
class FleetHost:
    worker_id: str
    server_id: int
    ip: str
    region: str
    is_reserve: bool = False
    managed: bool = True       # False = pre-existing box; never torn down by this tool


def _key_body(public_key: str) -> str:
    """The base64 body of an SSH public key, ignoring the trailing comment."""
    parts = public_key.split()
    return parts[1] if len(parts) >= 2 else public_key


def default_cloud_init(public_key: str) -> str:
    """Belt-and-suspenders cloud-config: authorize our key for root (Cherry also installs it)."""
    return (
        "#cloud-config\n"
        "ssh_authorized_keys:\n"
        f"  - {public_key}\n"
    )


class FleetProvisioner:
    def __init__(
        self,
        client: CherryClient,
        project_id: int,
        *,
        plan: str,
        region: str,
        image: str = "ubuntu_22_04",
        poll_interval_s: float = 10.0,
        active_timeout_s: float = 600.0,
        inventory_path: Path = DEFAULT_INVENTORY,
    ) -> None:
        self.client = client
        self.project_id = project_id
        self.plan = plan
        self.region = region
        self.image = image
        self.poll_interval_s = poll_interval_s
        self.active_timeout_s = active_timeout_s
        self.inventory_path = Path(inventory_path)

    async def ensure_ssh_key(self, label: str, public_key: str) -> int:
        """Return the id of a matching registered key, creating it if absent."""
        body = _key_body(public_key)
        for key in await self.client.list_ssh_keys():
            if _key_body(key.get("key", "")) == body:
                return int(key["id"])
        created = await self.client.create_ssh_key(label, public_key)
        logger.info("registered SSH key %s (id=%s)", label, created.get("id"))
        return int(created["id"])

    async def wait_active(self, server_id: int) -> str:
        """Poll until the server is active with a public IP; return that IP."""
        attempts = max(1, int(self.active_timeout_s / self.poll_interval_s)) if self.poll_interval_s > 0 else 1
        for _ in range(attempts):
            server = await self.client.get_server(server_id)
            ip = public_ip(server)
            if server.get("state") == "active" and ip:
                return ip
            if self.poll_interval_s > 0:
                await asyncio.sleep(self.poll_interval_s)
        raise CherryAPIError(408, f"server {server_id} not active within {self.active_timeout_s}s")

    async def provision_one(
        self,
        key_ids: list[int],
        hostname: str,
        *,
        region: str | None = None,
        user_data: str | None = None,
        is_reserve: bool = False,
    ) -> FleetHost:
        server = await self.client.create_server(
            self.project_id, plan=self.plan, region=region or self.region,
            image=self.image, hostname=hostname,
            ssh_keys=[str(k) for k in key_ids], user_data=user_data,
        )
        server_id = int(server["id"])
        ip = await self.wait_active(server_id)
        logger.info("provisioned %s (id=%s) at %s", hostname, server_id, ip)
        return FleetHost(worker_id=hostname, server_id=server_id, ip=ip,
                         region=region or self.region, is_reserve=is_reserve)

    async def provision(
        self,
        count: int,
        key_ids: list[int],
        *,
        name_prefix: str = "cherry",
        reserve_region: str | None = None,
        user_data: str | None = None,
    ) -> list[FleetHost]:
        """Provision `count` servers; the last one in `reserve_region` if given (item 6)."""
        hosts: list[FleetHost] = []
        for i in range(count):
            is_reserve = reserve_region is not None and i == count - 1
            region = reserve_region if is_reserve else self.region
            hosts.append(await self.provision_one(
                key_ids, f"{name_prefix}-{i + 1}", region=region,
                user_data=user_data, is_reserve=is_reserve,
            ))
        return hosts

    def load_inventory(self) -> list[FleetHost]:
        if not self.inventory_path.exists():
            return []
        return [FleetHost(**d) for d in json.loads(self.inventory_path.read_text())]

    def write_inventory(self, hosts: list[FleetHost], *, merge: bool = True) -> None:
        by_id = {h.worker_id: h for h in (self.load_inventory() if merge else [])}
        for host in hosts:
            by_id[host.worker_id] = host
        self.inventory_path.parent.mkdir(parents=True, exist_ok=True)
        self.inventory_path.write_text(json.dumps([asdict(h) for h in by_id.values()], indent=2))

    async def teardown(self) -> list[int]:
        """Delete every managed host in the inventory; keep unmanaged (pre-existing) ones."""
        inventory = self.load_inventory()
        deleted: list[int] = []
        for host in inventory:
            if not host.managed:
                continue
            await self.client.delete_server(host.server_id)
            deleted.append(host.server_id)
            logger.info("deleted server %s (%s)", host.server_id, host.worker_id)
        self.inventory_path.parent.mkdir(parents=True, exist_ok=True)
        kept = [h for h in inventory if not h.managed]
        self.inventory_path.write_text(json.dumps([asdict(h) for h in kept], indent=2))
        return deleted
