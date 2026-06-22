"""Unit tests for FleetSupervisor: auto-heal, guards, and elastic scaling."""

import json

from pipeline.fleet.control import FleetSupervisor
from pipeline.fleet.manager import FleetManager
from pipeline.fleet.provisioner import FleetProvisioner
from pipeline.fleet.worker import FleetWorker
from pipeline.models import BackendVerdict


class _Stub:
    async def verify(self, email):
        return BackendVerdict(status="valid", message="", verified_at="t")


class _FakeClient:
    def __init__(self, credit=10.0):
        self._credit = credit
        self._next = 7000
        self._servers = {}
        self.deleted = []
        self.created = 0

    async def get_team_credit(self, team_id):
        return self._credit

    async def create_server(self, project_id, *, plan, region, image, hostname, ssh_keys, user_data=None, **kw):
        sid = self._next
        self._next += 1
        self.created += 1
        self._servers[sid] = {"id": sid, "state": "active", "region": region,
                              "ip_addresses": [{"address": f"4.4.4.{sid % 256}", "type": "primary-ip"}]}
        return self._servers[sid]

    async def get_server(self, sid):
        return self._servers[sid]

    async def delete_server(self, sid):
        self.deleted.append(sid)


def _factory(host):
    return FleetWorker(worker_id=host.worker_id, verifier=_Stub(), server_id=host.server_id,
                       managed=host.managed, is_reserve=host.is_reserve)


def _degraded(wid="w1", server_id=5000):
    w = FleetWorker(worker_id=wid, verifier=_Stub(), server_id=server_id, managed=True)
    for _ in range(25):
        w.record("blocked")
    return w


def _supervisor(manager, client, tmp_path, **kw):
    prov = FleetProvisioner(client, 276218, plan="p", region="EU-Nord-1",
                            poll_interval_s=0.0, inventory_path=tmp_path / "hosts.json")
    opts = dict(worker_factory=_factory, key_ids=[42], team_id=200044,
                credit_floor_eur=0.10, control_path=tmp_path / "control.json")
    opts.update(kw)
    return FleetSupervisor(manager, prov, client, **opts)


async def test_auto_heal_replaces_reputation_degraded_worker(tmp_path):
    mgr = FleetManager([_degraded("w1", 5000)])
    client = _FakeClient(credit=10.0)
    await _supervisor(mgr, client, tmp_path).monitor_once()
    ids = {w.worker_id for w in mgr.workers}
    assert "w1" not in ids and "cherry-r1" in ids
    assert client.deleted == [5000]


async def test_auto_heal_blocked_by_credit_floor(tmp_path):
    mgr = FleetManager([_degraded("w1", 5000)])
    client = _FakeClient(credit=0.05)
    await _supervisor(mgr, client, tmp_path).monitor_once()
    assert {w.worker_id for w in mgr.workers} == {"w1"}
    assert client.created == 0


async def test_auto_heal_blocked_by_reprovision_cap(tmp_path):
    mgr = FleetManager([_degraded("w1", 5000)])
    client = _FakeClient(credit=10.0)
    await _supervisor(mgr, client, tmp_path, max_reprovisions=0).monitor_once()
    assert {w.worker_id for w in mgr.workers} == {"w1"}


async def test_healthy_worker_is_left_alone(tmp_path):
    healthy = FleetWorker(worker_id="w1", verifier=_Stub(), server_id=5000)
    mgr = FleetManager([healthy])
    client = _FakeClient(credit=10.0)
    await _supervisor(mgr, client, tmp_path).monitor_once()
    assert client.created == 0 and client.deleted == []


async def test_transient_tunnel_down_is_not_reprovisioned(tmp_path):
    class _Down:
        def is_up(self):
            return False
    mgr = FleetManager([FleetWorker(worker_id="w1", verifier=_Stub(), server_id=5000, tunnel=_Down())])
    client = _FakeClient(credit=10.0)
    await _supervisor(mgr, client, tmp_path).monitor_once()
    assert {w.worker_id for w in mgr.workers} == {"w1"}
    assert client.created == 0


async def test_can_provision_false_below_floor(tmp_path):
    sup = _supervisor(FleetManager([]), _FakeClient(credit=0.05), tmp_path)
    assert await sup.can_provision() is False


async def test_scale_up_provisions_workers(tmp_path):
    mgr = FleetManager([FleetWorker(worker_id="w1", verifier=_Stub())])
    await _supervisor(mgr, _FakeClient(credit=10.0), tmp_path).scale_to(3)
    assert len(mgr.workers) == 3


async def test_scale_up_clamped_to_max(tmp_path):
    mgr = FleetManager([FleetWorker(worker_id="w1", verifier=_Stub())])
    await _supervisor(mgr, _FakeClient(credit=10.0), tmp_path, scale_max=2).scale_to(9)
    assert len(mgr.workers) == 2


async def test_scale_down_keeps_reserve(tmp_path):
    workers = [
        FleetWorker(worker_id="w1", verifier=_Stub(), managed=True),
        FleetWorker(worker_id="w2", verifier=_Stub(), managed=True),
        FleetWorker(worker_id="reserve", verifier=_Stub(), managed=True, is_reserve=True),
    ]
    mgr = FleetManager(workers)
    await _supervisor(mgr, _FakeClient(credit=10.0), tmp_path).scale_to(1)
    assert {w.worker_id for w in mgr.workers} == {"reserve"}


async def test_control_file_triggers_scale(tmp_path):
    mgr = FleetManager([FleetWorker(worker_id="w1", verifier=_Stub())])
    sup = _supervisor(mgr, _FakeClient(credit=10.0), tmp_path)
    (tmp_path / "control.json").write_text(json.dumps({"scale_to": 2}))
    await sup.check_control_file()
    assert len(mgr.workers) == 2
    assert not (tmp_path / "control.json").exists()
