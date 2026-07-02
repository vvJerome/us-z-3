"""Unit tests for pipeline.fleet.__main__ — the fleet CLI's argument parsing and
per-subcommand handlers. CherryClient/FleetProvisioner/run_benchmark are mocked;
no real Cherry API calls."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.fleet.__main__ import (
    _benchmark,
    _build_parser,
    _provision,
    _provisioner,
    _status,
    _teardown,
    main,
)


class TestBuildParser:
    def test_requires_a_subcommand(self):
        with pytest.raises(SystemExit):
            _build_parser().parse_args([])

    def test_provision_requires_count(self):
        with pytest.raises(SystemExit):
            _build_parser().parse_args(["provision"])

    def test_provision_parses_count_and_defaults(self):
        args = _build_parser().parse_args(["provision", "--count", "4"])
        assert args.count == 4
        assert args.name_prefix == "cherry"
        assert args.reserve_region is None

    def test_status_subcommand_parses(self):
        args = _build_parser().parse_args(["status"])
        assert args.command == "status"

    def test_teardown_yes_flag_defaults_false(self):
        args = _build_parser().parse_args(["teardown"])
        assert args.yes is False

    def test_benchmark_requires_input(self):
        with pytest.raises(SystemExit):
            _build_parser().parse_args(["benchmark"])

    def test_benchmark_defaults(self):
        args = _build_parser().parse_args(["benchmark", "--input", "data.jsonl"])
        assert args.count == 5
        assert args.name == "fleet_benchmark"
        assert args.with_zuhal is False
        assert args.keep_fleet is False

    def test_project_id_defaults_from_env(self, monkeypatch):
        monkeypatch.setenv("CHERRY_PROJECT_ID", "9182")
        args = _build_parser().parse_args(["status"])
        assert args.project_id == 9182


class TestProvisioner:
    def test_builds_provisioner_from_args(self):
        client = MagicMock()
        args = _build_parser().parse_args(["provision", "--count", "2"])
        prov = _provisioner(client, args)
        assert prov.client is client
        assert prov.project_id == args.project_id


class TestProvisionCommand:
    async def test_missing_key_file_returns_2(self, tmp_path):
        args = _build_parser().parse_args(["provision", "--count", "2"])
        args.key_file = str(tmp_path / "does-not-exist.pub")
        result = await _provision(MagicMock(), args)
        assert result == 2

    async def test_provisions_and_writes_inventory(self, tmp_path):
        key_file = tmp_path / "cherry_fleet.pub"
        key_file.write_text("ssh-ed25519 AAAA...")
        args = _build_parser().parse_args(["provision", "--count", "2"])
        args.key_file = str(key_file)

        fake_host = MagicMock(worker_id="w1", ip="1.2.3.4", region="EU-Nord-1", is_reserve=False)
        with patch("pipeline.fleet.__main__._provisioner") as make_prov:
            prov = MagicMock()
            prov.ensure_ssh_key = AsyncMock(return_value=42)
            prov.provision = AsyncMock(return_value=[fake_host])
            prov.write_inventory = MagicMock()
            make_prov.return_value = prov

            result = await _provision(MagicMock(), args)

        assert result == 0
        prov.ensure_ssh_key.assert_called_once()
        prov.provision.assert_called_once()
        prov.write_inventory.assert_called_once_with([fake_host])


class TestStatusCommand:
    async def test_reports_credit_when_teams_exist(self):
        client = MagicMock()
        client.list_teams = AsyncMock(return_value=[{"id": 7}])
        client.get_team_credit = AsyncMock(return_value=12.5)
        client.list_servers = AsyncMock(return_value=[])
        args = _build_parser().parse_args(["status"])

        result = await _status(client, args)

        assert result == 0
        client.get_team_credit.assert_called_once_with(7)

    async def test_skips_credit_when_no_teams(self):
        client = MagicMock()
        client.list_teams = AsyncMock(return_value=[])
        client.get_team_credit = AsyncMock()
        client.list_servers = AsyncMock(return_value=[])
        args = _build_parser().parse_args(["status"])

        result = await _status(client, args)

        assert result == 0
        client.get_team_credit.assert_not_called()

    async def test_lists_servers(self):
        client = MagicMock()
        client.list_teams = AsyncMock(return_value=[])
        client.list_servers = AsyncMock(return_value=[{"id": 1, "hostname": "w1", "state": "active",
                                                        "ip_addresses": []}])
        args = _build_parser().parse_args(["status"])

        result = await _status(client, args)

        assert result == 0
        client.list_servers.assert_called_once()


class TestTeardownCommand:
    async def test_refuses_without_yes_flag(self):
        args = _build_parser().parse_args(["teardown"])
        result = await _teardown(MagicMock(), args)
        assert result == 2

    async def test_deletes_managed_servers_with_yes(self):
        args = _build_parser().parse_args(["teardown", "--yes"])
        with patch("pipeline.fleet.__main__._provisioner") as make_prov:
            prov = MagicMock()
            prov.teardown = AsyncMock(return_value=["w1", "w2"])
            make_prov.return_value = prov

            result = await _teardown(MagicMock(), args)

        assert result == 0
        prov.teardown.assert_called_once()


class TestBenchmarkCommand:
    async def test_missing_key_file_returns_2(self, tmp_path):
        args = _build_parser().parse_args(["benchmark", "--input", "data.jsonl"])
        args.key_file = str(tmp_path / "missing.pub")
        result = await _benchmark(MagicMock(), args)
        assert result == 2

    async def test_runs_benchmark_and_renders_report(self, tmp_path):
        key_file = tmp_path / "cherry_fleet.pub"
        key_file.write_text("ssh-ed25519 AAAA...")
        args = _build_parser().parse_args(["benchmark", "--input", "data.jsonl", "--count", "3"])
        args.key_file = str(key_file)

        fake_report = MagicMock()
        fake_report.render.return_value = "report text"

        with patch("pipeline.fleet.__main__._provisioner") as make_prov, \
             patch("pipeline.fleet.benchmark.run_benchmark", new=AsyncMock(return_value=fake_report)):
            prov = MagicMock()
            prov.ensure_ssh_key = AsyncMock(return_value=1)
            make_prov.return_value = prov

            result = await _benchmark(MagicMock(), args)

        assert result == 0
        fake_report.render.assert_called_once()


class TestMain:
    def test_missing_token_returns_2(self, monkeypatch):
        monkeypatch.delenv("CHERRY_AUTH_TOKEN", raising=False)
        result = main(["status"])
        assert result == 2

    def test_dispatches_to_status_handler(self, monkeypatch):
        monkeypatch.setenv("CHERRY_AUTH_TOKEN", "fake-token")
        client = AsyncMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)

        with patch("pipeline.fleet.__main__.CherryClient", return_value=client), \
             patch("pipeline.fleet.__main__._status", new=AsyncMock(return_value=0)) as status_handler:
            result = main(["status"])

        assert result == 0
        status_handler.assert_called_once()

    def test_keyboard_interrupt_returns_130(self, monkeypatch):
        monkeypatch.setenv("CHERRY_AUTH_TOKEN", "fake-token")
        with patch("pipeline.fleet.__main__.asyncio.run", side_effect=KeyboardInterrupt()):
            result = main(["status"])
        assert result == 130
