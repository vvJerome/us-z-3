from __future__ import annotations

import asyncio
import logging
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path

from pipeline.constants import (
    TUNNEL_BACKOFF_MAX_S,
    TUNNEL_BACKOFF_START_S,
    TUNNEL_READY_INTERVAL_S,
    TUNNEL_READY_RETRIES,
    TUNNEL_STOP_TIMEOUT_S,
)

_log = logging.getLogger("pipeline.tunnel")


@dataclass
class TunnelConfig:
    host: str
    user: str = "egress"
    port: int = 22
    socks_port: int = 1080
    bind_addr: str = "127.0.0.1"
    ssh_key: str = "~/.ssh/racknerd_egress"
    server_alive_interval: int = 15
    server_alive_count_max: int = 3
    connect_timeout: int = 20
    backoff_start_s: float = field(default=TUNNEL_BACKOFF_START_S)
    backoff_max_s: float = field(default=TUNNEL_BACKOFF_MAX_S)
    autorestart: bool = True


class SshSocksTunnel:
    """Supervised SSH -N -D SOCKS5 tunnel with auto-restart and backoff."""

    def __init__(self, config: TunnelConfig) -> None:
        self.config = config
        self._proc: asyncio.subprocess.Process | None = None
        self._supervisor_task: asyncio.Task | None = None
        self._ready = asyncio.Event()
        self._stop = asyncio.Event()
        self._backoff = config.backoff_start_s

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self, ready_timeout_s: float | None = None) -> None:
        """Spawn the SSH tunnel and wait until the SOCKS5 port is connectable."""
        self._stop.clear()
        self._ready.clear()
        await self._spawn()
        self._supervisor_task = asyncio.create_task(
            self._supervisor_loop(), name="ssh-tunnel-supervisor"
        )
        timeout = ready_timeout_s or self.config.connect_timeout
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            _log.error(
                "SSH tunnel to %s did not become ready within %.0fs",
                self.config.host,
                timeout,
            )
            await self.stop()
            raise

    async def stop(self) -> None:
        """Gracefully stop the tunnel and cancel supervisor."""
        self._stop.set()
        if self._proc and self._proc.returncode is None:
            try:
                self._proc.terminate()
                await asyncio.wait_for(self._proc.wait(), timeout=TUNNEL_STOP_TIMEOUT_S)
            except asyncio.TimeoutError:
                try:
                    self._proc.kill()
                except ProcessLookupError:
                    pass
        if self._supervisor_task and not self._supervisor_task.done():
            self._supervisor_task.cancel()
            try:
                await self._supervisor_task
            except asyncio.CancelledError:
                pass

    def is_up(self) -> bool:
        """True if subprocess is alive and SOCKS5 port is connectable."""
        if self._proc is None or self._proc.returncode is not None:
            return False
        return self._port_open()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ssh_cmd(self) -> list[str]:
        cfg = self.config
        key_path = str(Path(cfg.ssh_key).expanduser())
        return [
            "ssh",
            "-N",
            "-D", f"{cfg.bind_addr}:{cfg.socks_port}",
            "-p", str(cfg.port),
            "-i", key_path,
            "-o", f"ServerAliveInterval={cfg.server_alive_interval}",
            "-o", f"ServerAliveCountMax={cfg.server_alive_count_max}",
            "-o", "ExitOnForwardFailure=yes",
            "-o", "StrictHostKeyChecking=accept-new",
            f"{cfg.user}@{cfg.host}",
        ]

    async def _spawn(self) -> None:
        cmd = self._ssh_cmd()
        _log.info("SSH tunnel spawning: %s", " ".join(cmd))
        self._proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        # Drain stderr in background so the pipe never fills and blocks ssh
        asyncio.create_task(self._drain_stderr(), name="ssh-stderr-drain")

        # Poll until SOCKS5 port responds or process exits
        for attempt in range(TUNNEL_READY_RETRIES):
            if self._proc.returncode is not None:
                _log.warning("SSH process exited early (code %d)", self._proc.returncode)
                return
            if self._port_open():
                _log.info(
                    "SSH tunnel ready (SOCKS5 on %s:%d) after %d attempts",
                    self.config.bind_addr,
                    self.config.socks_port,
                    attempt + 1,
                )
                self._ready.set()
                self._backoff = self.config.backoff_start_s  # reset on successful connect
                return
            await asyncio.sleep(TUNNEL_READY_INTERVAL_S)

        _log.warning(
            "SOCKS5 port not responding after %d retries — tunnel may still start",
            TUNNEL_READY_RETRIES,
        )

    async def _drain_stderr(self) -> None:
        if not self._proc or not self._proc.stderr:
            return
        try:
            async for line in self._proc.stderr:
                txt = line.decode(errors="replace").rstrip()
                if txt:
                    _log.debug("ssh stderr: %s", txt)
        except Exception:
            pass

    async def _supervisor_loop(self) -> None:
        """Watch the process; restart with backoff if it dies and autorestart=True."""
        while not self._stop.is_set():
            if self._proc:
                await self._proc.wait()
                if self._stop.is_set():
                    break
                code = self._proc.returncode
                _log.warning("SSH tunnel exited (code %d)", code)
                self._ready.clear()

                if not self.config.autorestart:
                    break

                _log.info("Restarting SSH tunnel in %.1fs", self._backoff)
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._stop.wait()),
                        timeout=self._backoff,
                    )
                    # stop was set during backoff sleep
                    break
                except asyncio.TimeoutError:
                    pass

                self._backoff = min(self._backoff * 2, self.config.backoff_max_s)
                await self._spawn()
            else:
                await asyncio.sleep(1.0)

    def _port_open(self) -> bool:
        """Non-blocking probe of the local SOCKS5 port."""
        try:
            with socket.create_connection(
                (self.config.bind_addr, self.config.socks_port), timeout=0.5
            ):
                return True
        except OSError:
            return False
