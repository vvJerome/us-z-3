"""Async Cherry Servers API client.

Thin aiohttp wrapper over the operations the fleet needs: team credit, SSH keys,
and server CRUD. Field names match the official cherrygo SDK. Auth is a Bearer
token (CHERRY_AUTH_TOKEN). No global state — construct one per run and use it as
an async context manager so it owns its HTTP session.
"""
from __future__ import annotations

import base64
import json as _json
import logging
from typing import Any

import aiohttp

logger = logging.getLogger("pipeline.fleet.cherry")

CHERRY_API_BASE = "https://api.cherryservers.com/v1"

# Cherry server IP types that are externally reachable (vs. private "internal-ip").
_PUBLIC_IP_TYPES = frozenset({"primary-ip", "floating-ip"})


class CherryAPIError(Exception):
    """Non-2xx response from the Cherry Servers API."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"Cherry API {status_code}: {message}")
        self.status_code = status_code
        self.message = message


class CherryClient:
    """Async Cherry Servers client. Use as `async with CherryClient(token) as c: ...`."""

    def __init__(
        self,
        token: str,
        *,
        base_url: str = CHERRY_API_BASE,
        timeout_s: float = 30.0,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self._token = token
        self._base = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._session = session
        self._owns_session = session is None

    async def __aenter__(self) -> CherryClient:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    @property
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}

    async def _request(self, method: str, path: str, *, body: Any = None) -> Any:
        if self._session is None:
            raise RuntimeError("CherryClient must be used inside an async context manager")
        url = f"{self._base}{path}"
        async with self._session.request(method, url, json=body, headers=self._headers) as resp:
            text = await resp.text()
            data: Any = None
            if text:
                try:
                    data = _json.loads(text)
                except ValueError:
                    data = None
            if resp.status >= 400:
                msg = data.get("message") if isinstance(data, dict) else text
                raise CherryAPIError(resp.status, msg or f"HTTP {resp.status}")
            return data

    # --- Account / credit ---------------------------------------------------
    async def get_user(self) -> dict:
        return await self._request("GET", "/user")

    async def list_teams(self) -> list[dict]:
        return await self._request("GET", "/teams")

    async def get_team_credit(self, team_id: int) -> float:
        """Remaining account credit (team currency, usually EUR)."""
        for team in await self.list_teams():
            if team.get("id") == team_id:
                return float(team.get("credit", {}).get("account", {}).get("remaining", 0.0))
        raise CherryAPIError(404, f"team {team_id} not found")

    async def list_projects(self, team_id: int) -> list[dict]:
        return await self._request("GET", f"/teams/{team_id}/projects")

    async def list_plans(self, team_id: int) -> list[dict]:
        return await self._request("GET", f"/teams/{team_id}/plans")

    # --- SSH keys -----------------------------------------------------------
    async def list_ssh_keys(self) -> list[dict]:
        return await self._request("GET", "/ssh-keys")

    async def create_ssh_key(self, label: str, public_key: str) -> dict:
        return await self._request("POST", "/ssh-keys", body={"label": label, "key": public_key})

    async def delete_ssh_key(self, key_id: int) -> None:
        await self._request("DELETE", f"/ssh-keys/{key_id}")

    # --- Servers ------------------------------------------------------------
    async def list_servers(self, project_id: int) -> list[dict]:
        return await self._request("GET", f"/projects/{project_id}/servers")

    async def create_server(
        self,
        project_id: int,
        *,
        plan: str,
        region: str,
        image: str,
        hostname: str,
        ssh_keys: list[str],
        user_data: str | None = None,
        spot_market: bool = False,
        cycle: str = "hourly",
        tags: dict[str, str] | None = None,
    ) -> dict:
        """Provision one server. `user_data` is raw cloud-init; Cherry wants it base64."""
        body: dict[str, Any] = {
            "plan": plan,
            "region": region,
            "image": image,
            "hostname": hostname,
            "ssh_keys": ssh_keys,
            "spot_market": spot_market,
            "cycle": cycle,
        }
        if user_data is not None:
            body["user_data"] = base64.b64encode(user_data.encode()).decode()
        if tags:
            body["tags"] = tags
        return await self._request("POST", f"/projects/{project_id}/servers", body=body)

    async def get_server(self, server_id: int) -> dict:
        return await self._request("GET", f"/servers/{server_id}")

    async def delete_server(self, server_id: int) -> None:
        await self._request("DELETE", f"/servers/{server_id}")


def public_ip(server: dict) -> str | None:
    """Return the first public IPv4 address of a server, or None if not yet assigned."""
    for ip in server.get("ip_addresses", []):
        if ip.get("type") in _PUBLIC_IP_TYPES and ip.get("address"):
            return ip["address"]
    return None
