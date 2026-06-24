from __future__ import annotations

import asyncio
import logging
import re
import socket
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiodns
import aiosmtplib

from pipeline.constants import (
    RACKNERD_MX_CACHE_MAX,
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
    "recipient not found", "nosuchuser", "account does not exist",
    "account not found", "no mailbox",
)

_ISO_NOW = lambda: datetime.now(timezone.utc).isoformat()  # noqa: E731


def _default_helo_hostname() -> str:
    """Return a valid FQDN for SMTP EHLO/HELO, or an IP literal fallback (with warning)."""
    fqdn = socket.getfqdn()
    if "." in fqdn:
        return fqdn
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        literal = f"[{ip}]"
    except Exception:
        literal = f"[{socket.gethostbyname(socket.gethostname())}]"
    _log.warning(
        "No FQDN available — falling back to IP literal MAIL FROM (%s). "
        "Many MX servers reject `verify@%s`; set RACKNERD_HELO_HOSTNAME=<fqdn> in .env to override.",
        literal, literal,
    )
    return literal


def _classify_smtp_rejection(exc_str: str) -> tuple[str, str]:
    """Map a SMTPRecipientRefused exception string to (status, message).

    aiosmtplib only raises SMTPRecipientRefused for 5xx RCPT TO responses, which are
    permanent per RFC 5321 §4.2.1. Spamhaus-style reputation rejections route to `blocked`;
    everything else is `invalid`.
    """
    lower = exc_str.lower()
    if any(kw in lower for kw in _SPAMHAUS_KEYWORDS):
        return "blocked", f"SMTP error: {exc_str}"
    return "invalid", f"SMTP error: {exc_str}"


def _classify_5xx(code: int, msg: str) -> tuple[str, str]:
    """Map a 5xx tuple-return SMTP response to (status, message). Spamhaus → blocked; else invalid."""
    if any(kw in msg.lower() for kw in _SPAMHAUS_KEYWORDS):
        return "blocked", f"{code} {msg}"
    return "invalid", f"{code} {msg}"


@dataclass
class RacknerdConfig:
    socks_host: str = "127.0.0.1"
    socks_port: int = 1080
    concurrency: int = 10
    smtp_timeout_s: float = RACKNERD_SMTP_TIMEOUT_S
    helo_hostname: str = field(default_factory=_default_helo_hostname)
    direct: bool = False  # skip SOCKS5 tunnel, connect directly (use when running on the egress VPS)

    def __post_init__(self) -> None:
        if not self.helo_hostname or not self.helo_hostname.strip():
            raise ValueError("RacknerdConfig.helo_hostname must be non-empty")
        if self.helo_hostname.startswith("[") and self.helo_hostname.endswith("]"):
            # Domain literals as the MAIL FROM domain are widely rejected (RFC 5321 § 4.1.3 allows
            # them, but Proofpoint/Outlook/etc. respond 501/550). Warn loudly so the misconfig
            # that produced the 49.12.127.119 incident is visible at startup.
            _log.warning(
                "RacknerdConfig.helo_hostname=%s is an IP literal — MAIL FROM:<verify@%s> "
                "will be rejected by many MX servers. Set RACKNERD_HELO_HOSTNAME=<fqdn>.",
                self.helo_hostname, self.helo_hostname,
            )


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
    """Verify emails via SMTP RCPT TO probe, either through SOCKS5 tunnel or directly."""

    def __init__(
        self,
        tunnel: SshSocksTunnel | None,
        config: RacknerdConfig | None = None,
        resolver: aiodns.DNSResolver | None = None,
    ) -> None:
        self.tunnel = tunnel  # None = direct mode (no SOCKS5)
        self.config = config or RacknerdConfig()
        self._resolver = resolver or aiodns.DNSResolver(timeout=3, tries=1)
        self._sem = asyncio.Semaphore(self.config.concurrency)
        self._guard = _SpamhausGuard()
        # MX cache: domain → (mx_hosts, expires_at)
        self._mx_cache: dict[str, tuple[list[str], float]] = {}

    async def verify(self, email: str, mx_provider: str | None = None) -> BackendVerdict:
        """Probe `email` via SOCKS5 SMTP. `mx_provider` is accepted for fleet-seam parity (unused)."""
        async with self._sem:
            return await self._verify_inner(email)

    async def _verify_inner(self, email: str) -> BackendVerdict:
        if self.tunnel is not None and not self.tunnel.is_up():
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

        return BackendVerdict(status=last_status, message=last_msg, verified_at=_ISO_NOW())  # type: ignore[arg-type]

    async def _resolve_mx(self, domain: str) -> list[str]:
        """Resolve MX records with a 1-hour TTL cache."""
        now = time.monotonic()
        if domain in self._mx_cache:
            hosts, expires = self._mx_cache[domain]
            if now < expires:
                return hosts

        hosts = []
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

        if len(self._mx_cache) >= RACKNERD_MX_CACHE_MAX:
            # Drop the oldest quarter to bound memory on large runs
            evict = list(self._mx_cache)[: len(self._mx_cache) // 4]
            for k in evict:
                del self._mx_cache[k]
        self._mx_cache[domain] = (hosts, now + RACKNERD_MX_CACHE_TTL_S)
        return hosts

    async def _probe_mx(self, email: str, mx_host: str) -> tuple[str, str]:
        if self.config.direct:
            return await self._probe_direct(email, mx_host)
        return await self._probe_socks5(email, mx_host)

    async def _probe_direct(self, email: str, mx_host: str) -> tuple[str, str]:
        """Direct TCP connection to mx_host:25 — no SOCKS5 (use when already on the egress IP)."""
        cfg = self.config
        try:
            smtp = aiosmtplib.SMTP(hostname=mx_host, port=25, timeout=cfg.smtp_timeout_s)
            await asyncio.wait_for(smtp.connect(), timeout=cfg.smtp_timeout_s)
            return await self._run_smtp_probe(smtp, email, mx_host)
        except aiosmtplib.SMTPRecipientRefused as exc:
            # aiosmtplib raises SMTPRecipientRefused for 5xx RCPT TO responses instead of
            # returning (code, msg) — so keyword checks must happen here, not in _run_smtp_probe.
            return _classify_smtp_rejection(str(exc))
        except aiosmtplib.SMTPException as exc:
            return "error", f"SMTP error: {exc}"
        except asyncio.TimeoutError:
            return "error", f"SMTP timeout probing {mx_host}"
        except Exception as exc:
            return "error", f"probe error: {exc}"

    async def _probe_socks5(self, email: str, mx_host: str) -> tuple[str, str]:
        """SOCKS5-proxied connection to mx_host:25 via the SSH tunnel."""
        cfg = self.config
        try:
            from python_socks.async_.asyncio import Proxy  # type: ignore[import]
        except ImportError:
            _log.error("python-socks not installed — cannot probe via SOCKS5")
            return "error", "python-socks not installed"

        sock = None
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

        # aiosmtplib takes ownership of `sock` once smtp.connect() succeeds.
        # The transport's _call_connection_lost is deferred via call_soon and
        # only then drops the strong ref that keeps loop._transports[fd] alive
        # (it's a WeakValueDictionary). If we don't yield + force-evict, a
        # concurrent probe can grab the recycled FD before cleanup runs and
        # asyncio raises "File descriptor X is used by transport" — the error
        # then cascades into bbops/Zuhal because the selector is corrupted.
        sock_fd = sock.fileno()
        smtp = aiosmtplib.SMTP(hostname=None, port=None, timeout=cfg.smtp_timeout_s, sock=sock)
        connected = False
        try:
            await asyncio.wait_for(smtp.connect(), timeout=cfg.smtp_timeout_s)
            connected = True
            return await self._run_smtp_probe(smtp, email, mx_host)
        except aiosmtplib.SMTPRecipientRefused as exc:
            return _classify_smtp_rejection(str(exc))
        except aiosmtplib.SMTPException as exc:
            return "error", f"SMTP error: {exc}"
        except asyncio.TimeoutError:
            return "error", f"SMTP timeout probing {mx_host}"
        except Exception as exc:
            return "error", f"probe error: {exc}"
        finally:
            if connected:
                try:
                    smtp.close()
                except Exception:
                    pass
                # Yield so the loop runs the deferred _call_connection_lost
                # callback, GCs the transport, and clears loop._transports[fd].
                await asyncio.sleep(0)
            else:
                # smtp.connect() may have partially constructed the transport
                # (registering fd in loop._transports) before raising — close
                # the raw socket so the OS frees the FD.
                try:
                    sock.close()
                except Exception:
                    pass
            # Always force-evict the WeakValueDictionary entry. Handles both the
            # successful path (in case GC is delayed under load) and the partial-
            # connect failure path (where the transport was registered but never
            # got a clean close()).
            if sock_fd >= 0:
                loop = asyncio.get_running_loop()
                transports = getattr(loop, "_transports", None)
                if transports is not None:
                    transports.pop(sock_fd, None)

    async def _run_smtp_probe(
        self, smtp: aiosmtplib.SMTP, email: str, mx_host: str
    ) -> tuple[str, str]:
        """Run EHLO/MAIL/RCPT sequence on an already-connected SMTP client."""
        cfg = self.config
        try:
            await asyncio.wait_for(smtp.ehlo(), timeout=cfg.smtp_timeout_s)
            await asyncio.wait_for(
                smtp.mail(f"verify@{cfg.helo_hostname}"), timeout=cfg.smtp_timeout_s
            )
            code, msg = await asyncio.wait_for(smtp.rcpt(email), timeout=cfg.smtp_timeout_s)
            try:
                await asyncio.wait_for(smtp.rset(), timeout=cfg.smtp_timeout_s)
            except Exception:
                pass

            if not isinstance(code, int):
                return "error", "malformed SMTP response (no code)"

            if 200 <= code <= 399:
                return "valid", f"{code} {msg}"
            if code >= 500:
                return _classify_5xx(code, msg)
            # 4xx — temporary failure, try next MX
            return "error", f"{code} {msg} (4xx temporary)"
        finally:
            try:
                await asyncio.wait_for(smtp.quit(), timeout=3.0)
            except Exception:
                pass
