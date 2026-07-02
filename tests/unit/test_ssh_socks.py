"""Unit tests for SshSocksTunnel — mocked subprocess and socket."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.tunnels.ssh_socks import SshSocksTunnel, TunnelConfig


def _config(**kwargs) -> TunnelConfig:
    defaults = dict(host="vps.example.com", user="egress", socks_port=19999, autorestart=False)
    defaults.update(kwargs)
    return TunnelConfig(**defaults)


def _async_iter(items):
    """A real async iterator — MagicMock(return_value=iter(...)) looks right but
    async for calls __anext__(), which a plain sync iterator doesn't have; the
    AttributeError gets silently swallowed by _drain_stderr's except Exception."""
    async def gen():
        for item in items:
            yield item
    return gen()


def _mock_proc(returncode: int | None = None) -> MagicMock:
    proc = MagicMock()
    proc.returncode = returncode
    proc.stderr = MagicMock()
    proc.stderr.__aiter__ = MagicMock(return_value=_async_iter([]))
    proc.terminate = MagicMock()
    proc.kill = MagicMock()
    proc.wait = AsyncMock(return_value=returncode)
    return proc


class TestSshSocksTunnelIsUp:
    def test_is_up_false_when_proc_none(self):
        tunnel = SshSocksTunnel(_config())
        assert tunnel.is_up() is False

    def test_is_up_false_when_proc_exited(self):
        tunnel = SshSocksTunnel(_config())
        tunnel._proc = _mock_proc(returncode=1)
        with patch.object(tunnel, "_port_open", return_value=True):
            assert tunnel.is_up() is False

    def test_is_up_false_when_port_closed(self):
        tunnel = SshSocksTunnel(_config())
        tunnel._proc = _mock_proc(returncode=None)
        with patch.object(tunnel, "_port_open", return_value=False):
            assert tunnel.is_up() is False

    def test_is_up_true_when_proc_alive_and_port_open(self):
        tunnel = SshSocksTunnel(_config())
        tunnel._proc = _mock_proc(returncode=None)
        with patch.object(tunnel, "_port_open", return_value=True):
            assert tunnel.is_up() is True


class TestPortOpen:
    def test_port_open_returns_true_on_success(self):
        tunnel = SshSocksTunnel(_config())
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        with patch("socket.create_connection", return_value=mock_conn):
            assert tunnel._port_open() is True

    def test_port_open_returns_false_on_oserror(self):
        tunnel = SshSocksTunnel(_config())
        with patch("socket.create_connection", side_effect=OSError("connection refused")):
            assert tunnel._port_open() is False


class TestSshBinaryMissing:
    async def test_spawn_raises_runtime_error_when_ssh_missing(self):
        tunnel = SshSocksTunnel(_config())
        with patch(
            "asyncio.create_subprocess_exec",
            side_effect=FileNotFoundError("ssh not found"),
        ):
            with pytest.raises(RuntimeError, match="ssh binary not found in PATH"):
                await tunnel._spawn()


class TestSshCmd:
    def test_ssh_cmd_contains_socks_and_host(self):
        cfg = _config(host="myhost.com", user="myuser", socks_port=1080, port=22)
        tunnel = SshSocksTunnel(cfg)
        cmd = tunnel._ssh_cmd()
        assert "ssh" in cmd
        assert "-N" in cmd
        assert "-D" in cmd
        assert any("1080" in arg for arg in cmd)
        assert "myuser@myhost.com" in cmd

    def test_ssh_cmd_default_pins_host_keys(self):
        cmd = SshSocksTunnel(_config(host="h"))._ssh_cmd()
        assert "StrictHostKeyChecking=accept-new" in cmd
        assert not any("UserKnownHostsFile" in c for c in cmd)

    def test_ssh_cmd_ephemeral_ignores_known_hosts(self):
        cfg = _config(host="h", strict_host_key_checking="no", known_hosts_file="/dev/null")
        cmd = SshSocksTunnel(cfg)._ssh_cmd()
        assert "StrictHostKeyChecking=no" in cmd
        assert "UserKnownHostsFile=/dev/null" in cmd


class TestStop:
    async def test_stop_cancels_supervisor_task(self):
        tunnel = SshSocksTunnel(_config())
        proc = _mock_proc(returncode=None)
        tunnel._proc = proc
        task = asyncio.create_task(asyncio.sleep(100))
        tunnel._supervisor_task = task

        await tunnel.stop()

        assert tunnel._stop.is_set()
        assert task.cancelled()

    async def test_stop_terminates_alive_process(self):
        tunnel = SshSocksTunnel(_config())
        proc = _mock_proc(returncode=None)
        tunnel._proc = proc

        await tunnel.stop()

        proc.terminate.assert_called_once()
        proc.kill.assert_not_called()

    async def test_stop_kills_process_that_ignores_terminate(self):
        tunnel = SshSocksTunnel(_config())
        proc = _mock_proc(returncode=None)
        proc.wait = AsyncMock(side_effect=asyncio.TimeoutError())
        tunnel._proc = proc

        await tunnel.stop()

        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()

    async def test_stop_kill_missing_process_is_swallowed(self):
        tunnel = SshSocksTunnel(_config())
        proc = _mock_proc(returncode=None)
        proc.wait = AsyncMock(side_effect=asyncio.TimeoutError())
        proc.kill = MagicMock(side_effect=ProcessLookupError())
        tunnel._proc = proc

        await tunnel.stop()  # must not raise


class TestStart:
    async def test_start_succeeds_when_spawn_sets_ready(self):
        tunnel = SshSocksTunnel(_config())

        async def fake_spawn():
            tunnel._ready.set()

        with patch.object(tunnel, "_spawn", side_effect=fake_spawn), \
             patch.object(tunnel, "_supervisor_loop", AsyncMock(return_value=None)):
            await tunnel.start(ready_timeout_s=1.0)

        assert tunnel._ready.is_set()

    async def test_start_times_out_and_stops_then_reraises(self):
        tunnel = SshSocksTunnel(_config())

        with patch.object(tunnel, "_spawn", AsyncMock(return_value=None)), \
             patch.object(tunnel, "_supervisor_loop", AsyncMock(return_value=None)), \
             patch.object(tunnel, "stop", AsyncMock(return_value=None)) as mock_stop:
            with pytest.raises(asyncio.TimeoutError):
                await tunnel.start(ready_timeout_s=0.05)

        mock_stop.assert_called_once()


class TestSpawnPollingLoop:
    async def test_ready_once_port_opens(self):
        tunnel = SshSocksTunnel(_config())
        proc = _mock_proc(returncode=None)

        with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=proc)), \
             patch.object(tunnel, "_port_open", side_effect=[False, True]), \
             patch("pipeline.tunnels.ssh_socks.TUNNEL_READY_INTERVAL_S", 0.01):
            await tunnel._spawn()

        assert tunnel._ready.is_set()
        assert tunnel._backoff == tunnel.config.backoff_start_s

    async def test_process_exits_early_during_poll(self):
        tunnel = SshSocksTunnel(_config())
        proc = _mock_proc(returncode=None)

        def flip_returncode(*a, **kw):
            proc.returncode = 1
            return False

        with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=proc)), \
             patch.object(tunnel, "_port_open", side_effect=flip_returncode):
            await tunnel._spawn()

        assert not tunnel._ready.is_set()

    async def test_port_never_opens_exhausts_retries(self):
        tunnel = SshSocksTunnel(_config())
        proc = _mock_proc(returncode=None)

        with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=proc)), \
             patch.object(tunnel, "_port_open", return_value=False), \
             patch("pipeline.tunnels.ssh_socks.TUNNEL_READY_RETRIES", 2), \
             patch("pipeline.tunnels.ssh_socks.TUNNEL_READY_INTERVAL_S", 0.01):
            await tunnel._spawn()

        assert not tunnel._ready.is_set()


class TestDrainStderr:
    async def test_logs_nonempty_lines(self, caplog):
        import logging
        tunnel = SshSocksTunnel(_config())
        proc = _mock_proc(returncode=None)
        proc.stderr.__aiter__ = MagicMock(return_value=_async_iter([b"line one\n", b"\n", b"line two\n"]))
        tunnel._proc = proc

        with caplog.at_level(logging.DEBUG, logger="pipeline.tunnel"):
            await tunnel._drain_stderr()

        assert any("line one" in r.message for r in caplog.records)
        assert any("line two" in r.message for r in caplog.records)

    async def test_no_proc_returns_immediately(self):
        tunnel = SshSocksTunnel(_config())
        await tunnel._drain_stderr()  # must not raise

    async def test_iteration_error_is_swallowed(self):
        tunnel = SshSocksTunnel(_config())
        proc = _mock_proc(returncode=None)

        class _BoomIter:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise RuntimeError("stderr read boom")

        proc.stderr.__aiter__ = MagicMock(return_value=_BoomIter())
        tunnel._proc = proc

        await tunnel._drain_stderr()  # must not raise


class TestSupervisorLoop:
    async def test_stop_during_wait_breaks_without_restart(self):
        tunnel = SshSocksTunnel(_config(autorestart=True))
        proc = _mock_proc(returncode=None)

        async def fake_wait():
            tunnel._stop.set()

        proc.wait = AsyncMock(side_effect=fake_wait)
        tunnel._proc = proc

        await tunnel._supervisor_loop()  # must return, not hang

    async def test_no_autorestart_breaks_after_exit(self):
        tunnel = SshSocksTunnel(_config(autorestart=False))
        proc = _mock_proc(returncode=1)
        tunnel._proc = proc

        await tunnel._supervisor_loop()

        assert not tunnel._ready.is_set()

    async def test_stop_during_backoff_breaks(self):
        tunnel = SshSocksTunnel(_config(autorestart=True))
        proc = _mock_proc(returncode=1)
        tunnel._proc = proc
        tunnel._backoff = 0.05

        async def set_stop_soon():
            await asyncio.sleep(0.01)
            tunnel._stop.set()

        setter = asyncio.create_task(set_stop_soon())
        await tunnel._supervisor_loop()
        await setter

    async def test_backoff_expires_and_respawns(self):
        tunnel = SshSocksTunnel(_config(autorestart=True))
        proc = _mock_proc(returncode=1)
        tunnel._proc = proc
        tunnel._backoff = 0.01

        respawned = {"n": 0}

        async def fake_spawn():
            respawned["n"] += 1
            tunnel._stop.set()  # stop after one respawn so the loop exits

        with patch.object(tunnel, "_spawn", side_effect=fake_spawn):
            await tunnel._supervisor_loop()

        assert respawned["n"] == 1
        assert tunnel._backoff == pytest.approx(0.02)  # doubled from 0.01
