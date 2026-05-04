"""Unit tests for SshSocksTunnel — mocked subprocess and socket."""

from __future__ import annotations

import asyncio
import socket
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.tunnels.ssh_socks import SshSocksTunnel, TunnelConfig


def _config(**kwargs) -> TunnelConfig:
    defaults = dict(host="vps.example.com", user="egress", socks_port=19999, autorestart=False)
    defaults.update(kwargs)
    return TunnelConfig(**defaults)


def _mock_proc(returncode: int | None = None) -> MagicMock:
    proc = MagicMock()
    proc.returncode = returncode
    proc.stderr = MagicMock()
    proc.stderr.__aiter__ = AsyncMock(return_value=iter([]))
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
