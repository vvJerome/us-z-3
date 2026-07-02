"""Unit tests for fleet wiring: host resolution, build_fleet, validator relax."""

import asyncio
from unittest.mock import patch

from pipeline.config import PipelineConfig
from pipeline.db import get_worker_provider_stats
from pipeline.fleet import wiring
from pipeline.models import BackendVerdict


class _FakeTunnel:
    def __init__(self, cfg):
        self.cfg = cfg

    async def start(self, ready_timeout_s=30.0):
        pass

    async def stop(self):
        pass

    def is_up(self):
        return True


class _FakeConsumer:
    def __init__(self, tunnel, config, resolver=None):
        self.tunnel = tunnel

    async def verify(self, email, mx_provider=None):
        return BackendVerdict("valid", "", "t")


def test_resolve_hosts_from_smtp_hosts():
    cfg = PipelineConfig(smtp_hosts=["1.1.1.1", "2.2.2.2"])
    hosts = wiring.resolve_hosts(cfg)
    assert [h.ip for h in hosts] == ["1.1.1.1", "2.2.2.2"]
    assert [h.worker_id for h in hosts] == ["smtp-1", "smtp-2"]
    assert all(h.managed is False for h in hosts)


def test_validator_relaxes_for_smtp_hosts():
    # Would raise (racknerd_host required) without smtp_hosts — must not raise here.
    PipelineConfig(smtp_hosts=["1.2.3.4"])


def test_validator_relaxes_for_cherry_enabled():
    PipelineConfig(cherry_enabled=True)


async def test_build_fleet_creates_workers(db_conn):
    cfg = PipelineConfig(smtp_hosts=["1.1.1.1", "2.2.2.2"], racknerd_socks_port=1080)
    stop = asyncio.Event()
    with patch.object(wiring, "SshSocksTunnel", _FakeTunnel), \
         patch.object(wiring, "RacknerdConsumer", _FakeConsumer):
        ctx = await wiring.build_fleet(cfg, db_conn, stop)
    assert {w.worker_id for w in ctx.manager.workers} == {"smtp-1", "smtp-2"}
    assert ctx.supervisor is None  # cherry disabled → no supervisor
    await ctx.aclose()


async def test_build_fleet_verify_records_outcome(db_conn):
    cfg = PipelineConfig(smtp_hosts=["1.1.1.1"])
    stop = asyncio.Event()
    with patch.object(wiring, "SshSocksTunnel", _FakeTunnel), \
         patch.object(wiring, "RacknerdConsumer", _FakeConsumer):
        ctx = await wiring.build_fleet(cfg, db_conn, stop)
    verdict = await ctx.manager.verify("a@b.com", mx_provider="aspmx.l.google.com")
    assert verdict.status == "valid"
    assert verdict.probe_host == "smtp-1"
    rows = await get_worker_provider_stats(db_conn, "smtp-1")
    assert rows[0]["provider"] == "google" and rows[0]["valid"] == 1
    await ctx.aclose()
