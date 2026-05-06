from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiodns
import aiosmtplib

from pipeline.constants import (
    RACKNERD_MX_CACHE_TTL_S,
    RACKNERD_MX_MAX_HOSTS,
    RACKNERD_SMTP_TIMEOUT_S,
    RACKNERD_SPAMHAUS_COOLDOWN_S,
    RACKNERD_SPAMHAUS_THRESHOLD,
    RACKNERD_SPAMHAUS_WINDOW_S,
)
from pipeline.models import BackendVerdict
from pipeline.tunnels.ssh_socks import SshSocksTunnel

_log = logging.getLogger("pipeline.racknerd")

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

_SPAMHAUS_KEYWORDS: tuple[str, ...] = (
    "spamhaus", "blocklist", "dnsbl", "zen.spamhaus", "xbl", "pbl", "sbl",
    "blocked", "blacklisted",
)
_INVALID_KEYWORDS: tuple[str, ...] = (
    "no such", "doesn't exist", "does not exist", "user unknown",
    "invalid mailbox", "invalid recipient", "address rejected",
)

_ISO_NOW = lambda: datetime.now(timezone.utc).isoformat()  # noqa: E731


@dataclass
class RacknerdConfig:
    socks_host: str = "127.0.0.1"
    socks_port: int = 1080
    concurrency: int = 10
    smtp_timeout_s: float = RACKNERD_SMTP_TIMEOUT_S
    helo_hostname: str = "mail.verify.local"


class _SpamhausGuard:
    """Sliding-window block counter. Triggers cooldown when threshold exceeded."""

    def __init__(self) -> None:
        self._events: deque[float] = deque()
        self._cooldown_until: float = 0.0

    def record_block(self) -> None:
        now = time.monotonic()
        self._events.append(now)
        # Evict events outside the window
        cutoff = now - RACKNERD_SPAMHAUS_WINDOW_S
        while self._events and self._events[0] < cutoff:
            self._events.popleft()
        if len(self._events) >= RACKNERD_SPAMHAUS_THRESHOLD:
            self._cooldown_until = now + RACKNERD_SPAMHAUS_COOLDOWN_S
            _log.warning(
                "SpamhausGuard: %d blocks in %ds — cooling down for %.0fs",
                len(self._events),
                RACKNERD_SPAMHAUS_WINDOW_S,
                RACKNERD_SPAMHAUS_COOLDOWN_S,
            )
            self._events.clear()

    def is_cooling(self) -> bool:
        return time.monotonic() < self._cooldown_until

    async def wait_if_cooling(self) -> None:
        remaining = self._cooldown_until - time.monotonic()
        if remaining > 0:
            _log.info("SpamhausGuard cooldown: waiting %.0fs", remaining)
            await asyncio.sleep(remaining)


class RacknerdConsumer:
    """Verify emails via SOCKS5 tunnel → MX server SMTP RCPT TO probe."""

    def __init__(
        self,
        tunnel: SshSocksTunnel,
        config: RacknerdConfig | None = None,
        resolver: aiodns.DNSResolver | None = None,
    ) -> None:
        self.tunnel = tunnel
        self.config = config or RacknerdConfig()
        self._resolver = resolver or aiodns.DNSResolver(timeout=3, tries=1)
        self._sem = asyncio.Semaphore(self.config.concurrency)
        self._guard = _SpamhausGuard()
        # MX cache: domain → (mx_hosts, expires_at)
        self._mx_cache: dict[str, tuple[list[str], float]] = {}

    async def verify(self, email: str) -> BackendVerdict:
        """Probe `email` via SOCKS5 SMTP. Returns a BackendVerdict."""
        async with self._sem:
            return await self._verify_inner(email)

    async def _verify_inner(self, email: str) -> BackendVerdict:
        if not self.tunnel.is_up():
            return BackendVerdict(status="error", message="tunnel not up", verified_at=_ISO_NOW())

        await self._guard.wait_if_cooling()

        if not _EMAIL_RE.match(email):
            return BackendVerdict(status="error", message="invalid email format", verified_at=_ISO_NOW())

        domain = email.split("@", 1)[1].lower()

        mx_hosts = await self._resolve_mx(domain)
        if not mx_hosts:
            # DNS failure is transient (SERVFAIL) — treat as error so reconciliation
            # can re-queue rather than permanently invalidating the address.
            return BackendVerdict(status="error", message="no MX/A record", verified_at=_ISO_NOW())

        last_status = "error"
        last_msg = "no hosts probed"

        for mx_host in mx_hosts[:RACKNERD_MX_MAX_HOSTS]:
            status, msg = await self._probe_mx(email, mx_host)
            last_status, last_msg = status, msg

            if status == "valid":
                return BackendVerdict(status="valid", message=msg, verified_at=_ISO_NOW())
            if status == "invalid":
                return BackendVerdict(status="invalid", message=msg, verified_at=_ISO_NOW())
            if status == "catch_all":
                return BackendVerdict(status="catch_all", message=msg, verified_at=_ISO_NOW())
            if status == "blocked":
                self._guard.record_block()
                return BackendVerdict(status="blocked", message=msg, verified_at=_ISO_NOW())
            # error → try next MX host

        return BackendVerdict(status=last_status, message=last_msg, verified_at=_ISO_NOW())

    async def _resolve_mx(self, domain: str) -> list[str]:
        """Resolve MX records with a 1-hour TTL cache."""
        now = time.monotonic()
        if domain in self._mx_cache:
            hosts, expires = self._mx_cache[domain]
            if now < expires:
                return hosts

        hosts: list[str] = []
        try:
            records = await self._resolver.query(domain, "MX")
            hosts = [r.host.rstrip(".") for r in sorted(records, key=lambda r: r.priority)]
        except aiodns.error.DNSError:
            pass

        if not hosts:
            # Fallback to A record
            try:
                await self._resolver.query(domain, "A")
                hosts = [domain]
            except aiodns.error.DNSError:
                pass

        if len(self._mx_cache) >= 10_000:
            # Drop the oldest quarter to bound memory on large runs
            evict = list(self._mx_cache)[: len(self._mx_cache) // 4]
            for k in evict:
                del self._mx_cache[k]
        self._mx_cache[domain] = (hosts, now + RACKNERD_MX_CACHE_TTL_S)
        return hosts

    async def _probe_mx(self, email: str, mx_host: str) -> tuple[str, str]:
        """Open SOCKS5 connection to mx_host:25 and run EHLO/MAIL/RCPT."""
        cfg = self.config
        try:
            from python_socks.async_.asyncio import Proxy  # type: ignore[import]
        except ImportError:
            _log.error("python-socks not installed — cannot probe via SOCKS5")
            return "error", "python-socks not installed"

        try:
            proxy = Proxy.from_url(f"socks5://{cfg.socks_host}:{cfg.socks_port}")
            sock = await asyncio.wait_for(
                proxy.connect(dest_host=mx_host, dest_port=25),
                timeout=cfg.smtp_timeout_s,
            )
        except asyncio.TimeoutError:
            return "error", f"SOCKS5 connect timeout to {mx_host}"
        except Exception as exc:
            return "error", f"SOCKS5 connect failed: {exc}"

        try:
            smtp = aiosmtplib.SMTP(
                hostname=mx_host,
                port=25,
                timeout=cfg.smtp_timeout_s,
                sock=sock,
            )
            await asyncio.wait_for(smtp.connect(), timeout=cfg.smtp_timeout_s)

            try:
                await asyncio.wait_for(smtp.ehlo(cfg.helo_hostname), timeout=cfg.smtp_timeout_s)
                await asyncio.wait_for(
                    smtp.mail(f"verify@{cfg.helo_hostname}"),
                    timeout=cfg.smtp_timeout_s,
                )
                code, msg = await asyncio.wait_for(
                    smtp.rcpt(email),
                    timeout=cfg.smtp_timeout_s,
                )
                try:
                    await asyncio.wait_for(smtp.rset(), timeout=cfg.smtp_timeout_s)
                except Exception:
                    pass

                if not isinstance(code, int):
                    return "error", "malformed SMTP response (no code)"

                msg_lower = msg.lower()
                if 200 <= code <= 399:
                    return "valid", f"{code} {msg}"
                if code >= 500:
                    if any(kw in msg_lower for kw in _SPAMHAUS_KEYWORDS):
                        return "blocked", f"{code} {msg}"
                    if any(kw in msg_lower for kw in _INVALID_KEYWORDS):
                        return "invalid", f"{code} {msg}"
                    return "error", f"{code} {msg}"
                # 4xx — temporary failure, try next MX
                return "error", f"{code} {msg} (4xx temporary)"

            finally:
                try:
                    await asyncio.wait_for(smtp.quit(), timeout=3.0)
                except Exception:
                    pass

        except aiosmtplib.SMTPException as exc:
            return "error", f"SMTP error: {exc}"
        except asyncio.TimeoutError:
            return "error", f"SMTP timeout probing {mx_host}"
        except Exception as exc:
            return "error", f"probe error: {exc}"
        finally:
            try:
                sock.close()
            except Exception:
                pass
