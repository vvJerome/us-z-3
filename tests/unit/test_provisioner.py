"""Unit tests for the Cherry fleet provisioner (Cherry API faked)."""

import pytest

from pipeline.fleet.cherry_client import CherryAPIError
from pipeline.fleet.provisioner import FleetHost, FleetProvisioner, default_cloud_init


class _FakeClient:
    def __init__(self, *, ssh_keys=None, server_state="active"):
        self._ssh_keys = list(ssh_keys or [])
        self._server_state = server_state
        self._next_id = 1000
        self._servers: dict = {}
        self.deleted: list[int] = []
        self.created_keys: list[dict] = []

    async def list_ssh_keys(self):
        return list(self._ssh_keys)

    async def create_ssh_key(self, label, key):
        rec = {"id": 42, "label": label, "key": key}
        self._ssh_keys.append(rec)
        self.created_keys.append(rec)
        return rec

    async def create_server(self, project_id, *, plan, region, image, hostname, ssh_keys,
                            user_data=None, **kw):
        sid = self._next_id
        self._next_id += 1
        ips = [{"address": f"5.6.7.{sid % 256}", "type": "primary-ip"}] if self._server_state == "active" else []
        server = {"id": sid, "state": self._server_state, "region": region,
                  "hostname": hostname, "ip_addresses": ips}
        self._servers[sid] = server
        return server

    async def get_server(self, sid):
        return self._servers.get(sid, {"id": sid, "state": "pending", "ip_addresses": []})

    async def delete_server(self, sid):
        self.deleted.append(sid)


def _prov(client, tmp_path, **kw):
    opts = dict(plan="B2-1-1gb-20s-shared", region="EU-Nord-1", poll_interval_s=0.0,
                inventory_path=tmp_path / "hosts.json")
    opts.update(kw)
    return FleetProvisioner(client, 276218, **opts)


async def test_ensure_ssh_key_reuses_existing_by_body(tmp_path):
    client = _FakeClient(ssh_keys=[{"id": 7, "label": "x", "key": "ssh-ed25519 AAAABODY me@h"}])
    key_id = await _prov(client, tmp_path).ensure_ssh_key("cherry", "ssh-ed25519 AAAABODY other@host")
    assert key_id == 7
    assert client.created_keys == []


async def test_ensure_ssh_key_creates_when_absent(tmp_path):
    client = _FakeClient(ssh_keys=[])
    key_id = await _prov(client, tmp_path).ensure_ssh_key("cherry", "ssh-ed25519 NEWBODY me@h")
    assert key_id == 42
    assert len(client.created_keys) == 1


async def test_wait_active_returns_ip(tmp_path):
    client = _FakeClient(server_state="active")
    client._servers[5] = {"id": 5, "state": "active", "ip_addresses": [{"address": "9.9.9.9", "type": "primary-ip"}]}
    assert await _prov(client, tmp_path).wait_active(5) == "9.9.9.9"


async def test_wait_active_times_out(tmp_path):
    client = _FakeClient(server_state="pending")
    with pytest.raises(CherryAPIError):
        await _prov(client, tmp_path).wait_active(999)


async def test_provision_marks_last_as_reserve_in_reserve_region(tmp_path):
    client = _FakeClient(server_state="active")
    hosts = await _prov(client, tmp_path).provision(3, [42], reserve_region="US-Chicago")
    assert [h.region for h in hosts] == ["EU-Nord-1", "EU-Nord-1", "US-Chicago"]
    assert [h.is_reserve for h in hosts] == [False, False, True]


def test_inventory_round_trip_and_merge(tmp_path):
    prov = _prov(_FakeClient(), tmp_path)
    prov.write_inventory([FleetHost("cherry-1", 1, "1.1.1.1", "EU-Nord-1")])
    prov.write_inventory([FleetHost("cherry-2", 2, "2.2.2.2", "US-Chicago")])
    loaded = {h.worker_id: h for h in prov.load_inventory()}
    assert set(loaded) == {"cherry-1", "cherry-2"}
    assert loaded["cherry-2"].ip == "2.2.2.2"


async def test_teardown_deletes_managed_keeps_unmanaged(tmp_path):
    client = _FakeClient()
    prov = _prov(client, tmp_path)
    prov.write_inventory([
        FleetHost("cherry-1", 1001, "1.1.1.1", "EU-Nord-1", managed=True),
        FleetHost("existing", 911448, "88.216.208.112", "SG-Singapore", managed=False),
    ])
    deleted = await prov.teardown()
    assert deleted == [1001]
    remaining = [h.worker_id for h in prov.load_inventory()]
    assert remaining == ["existing"]


def test_default_cloud_init_embeds_key():
    assert "ssh-ed25519 AAAA" in default_cloud_init("ssh-ed25519 AAAA me@h")
