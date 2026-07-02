"""Unit tests for the async Cherry Servers API client (HTTP via injected fake session)."""

import base64
import json as _json

import pytest

from pipeline.fleet.cherry_client import CherryAPIError, CherryClient, public_ip

BASE = "https://api.cherryservers.com/v1"


class _FakeResp:
    def __init__(self, status, text):
        self.status = status
        self._text = text

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    """Stands in for aiohttp.ClientSession; routes (method, url) -> (status, payload)."""

    def __init__(self, routes):
        self._routes = routes
        self.calls = []

    def request(self, method, url, *, json=None, headers=None):
        self.calls.append({"method": method, "url": url, "json": json})
        status, payload = self._routes[(method, url)]
        text = "" if payload is None else _json.dumps(payload)
        return _FakeResp(status, text)

    async def close(self):
        pass


def _client(routes):
    return CherryClient("tok", session=_FakeSession(routes))


async def test_list_teams_returns_parsed_list():
    c = _client({("GET", f"{BASE}/teams"): (200, [{"id": 200044, "name": "t"}])})
    assert await c.list_teams() == [{"id": 200044, "name": "t"}]


async def test_get_team_credit_extracts_remaining():
    routes = {("GET", f"{BASE}/teams"): (200, [{"id": 200044, "credit": {"account": {"remaining": 0.74}}}])}
    assert await _client(routes).get_team_credit(200044) == 0.74


async def test_get_team_credit_missing_team_raises():
    routes = {("GET", f"{BASE}/teams"): (200, [{"id": 1, "credit": {"account": {"remaining": 5.0}}}])}
    with pytest.raises(CherryAPIError):
        await _client(routes).get_team_credit(999)


async def test_create_ssh_key_posts_label_and_key():
    fake = _FakeSession({("POST", f"{BASE}/ssh-keys"): (201, {"id": 42, "label": "k"})})
    result = await CherryClient("tok", session=fake).create_ssh_key("k", "ssh-ed25519 AAAA")
    assert result["id"] == 42
    assert fake.calls[0]["json"] == {"label": "k", "key": "ssh-ed25519 AAAA"}


async def test_create_server_base64_encodes_user_data():
    fake = _FakeSession({("POST", f"{BASE}/projects/276218/servers"): (201, {"id": 911449, "state": "pending"})})
    server = await CherryClient("tok", session=fake).create_server(
        276218, plan="B2-1-1gb-20s-shared", region="EU-Nord-1", image="ubuntu_22_04",
        hostname="cherry-2", ssh_keys=["42"], user_data="#cloud-config\n",
    )
    assert server["id"] == 911449
    body = fake.calls[0]["json"]
    assert body["user_data"] == base64.b64encode(b"#cloud-config\n").decode()
    assert body["plan"] == "B2-1-1gb-20s-shared"
    assert body["ssh_keys"] == ["42"]


async def test_get_server_returns_dict():
    c = _client({("GET", f"{BASE}/servers/911448"): (200, {"id": 911448, "state": "active"})})
    assert (await c.get_server(911448))["state"] == "active"


async def test_delete_server_no_content_ok():
    c = _client({("DELETE", f"{BASE}/servers/911448"): (204, None)})
    assert await c.delete_server(911448) is None


async def test_api_error_carries_status_code():
    c = _client({("GET", f"{BASE}/user"): (402, {"message": "payment required"})})
    with pytest.raises(CherryAPIError) as exc:
        await c.get_user()
    assert exc.value.status_code == 402


async def test_request_outside_context_raises():
    client = CherryClient("tok")
    with pytest.raises(RuntimeError):
        await client.get_user()


def test_public_ip_picks_public_over_private():
    server = {"ip_addresses": [
        {"address": "10.0.0.1", "type": "private-ip"},
        {"address": "88.216.208.112", "type": "primary-ip"},
    ]}
    assert public_ip(server) == "88.216.208.112"


def test_public_ip_none_when_unassigned():
    assert public_ip({"ip_addresses": []}) is None
