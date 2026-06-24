"""CLI for the Cherry Servers SMTP fleet: provision / status / teardown.

    python -m pipeline.fleet provision --count 4 [--reserve-region US-Chicago]
    python -m pipeline.fleet status
    python -m pipeline.fleet teardown --yes

Credentials come from the environment: CHERRY_AUTH_TOKEN (required) and
CHERRY_PROJECT_ID (or --project-id). SSH public key from --key-file
(default ~/.ssh/cherry_fleet.pub); register it once, reuse across provisions.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

from pipeline.fleet.cherry_client import CherryClient, public_ip
from pipeline.fleet.provisioner import FleetProvisioner

logger = logging.getLogger("pipeline.fleet.cli")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m pipeline.fleet", description="Cherry SMTP fleet ops")
    p.add_argument("--project-id", type=int, default=int(os.environ.get("CHERRY_PROJECT_ID", "0")))
    p.add_argument("--plan", default=os.environ.get("CHERRY_PLAN", "B2-1-1gb-20s-shared"))
    p.add_argument("--region", default=os.environ.get("CHERRY_REGION", "EU-Nord-1"))
    p.add_argument("--image", default=os.environ.get("CHERRY_IMAGE", "ubuntu_22_04"))
    p.add_argument("--key-file", default=os.path.expanduser("~/.ssh/cherry_fleet.pub"))
    sub = p.add_subparsers(dest="command", required=True)

    prov = sub.add_parser("provision", help="create N workers and write the inventory")
    prov.add_argument("--count", type=int, required=True)
    prov.add_argument("--name-prefix", default="cherry")
    prov.add_argument("--reserve-region", default=None, help="put the last worker in this region (item 6)")
    prov.add_argument("--key-label", default="cherry_fleet")

    sub.add_parser("status", help="show fleet servers + remaining credit")

    td = sub.add_parser("teardown", help="delete all managed workers (keeps pre-existing)")
    td.add_argument("--yes", action="store_true", help="required to actually delete")

    bm = sub.add_parser("benchmark", help="provision N workers, validate a dataset, then tear down")
    bm.add_argument("--input", "-i", required=True, help="JSONL dataset to validate")
    bm.add_argument("--count", type=int, default=5, help="workers to provision (default 5)")
    bm.add_argument("--name", default="fleet_benchmark", help="run/output name (output/<name>/)")
    bm.add_argument("--ground-truth", default=None, help="optional email,zb_status CSV for an accuracy score")
    bm.add_argument("--dispatch-concurrency", type=int, default=None, help="override dispatcher concurrency")
    bm.add_argument("--with-zuhal", action="store_true", help="keep paid Zuhal rescue on (default: off)")
    return p


def _provisioner(client: CherryClient, args: argparse.Namespace) -> FleetProvisioner:
    return FleetProvisioner(client, args.project_id, plan=args.plan, region=args.region, image=args.image)


async def _provision(client: CherryClient, args: argparse.Namespace) -> int:
    pub = Path(args.key_file)
    if not pub.exists():
        logger.error("SSH public key %s not found — generate one: ssh-keygen -t ed25519 -f %s",
                     pub, pub.with_suffix(""))
        return 2
    prov = _provisioner(client, args)
    key_id = await prov.ensure_ssh_key(args.key_label, pub.read_text().strip())
    hosts = await prov.provision(args.count, [key_id], name_prefix=args.name_prefix,
                                 reserve_region=args.reserve_region)
    prov.write_inventory(hosts)
    for h in hosts:
        logger.info("worker %s -> %s (%s%s)", h.worker_id, h.ip, h.region,
                    ", reserve" if h.is_reserve else "")
    return 0


async def _status(client: CherryClient, args: argparse.Namespace) -> int:
    teams = await client.list_teams()
    if teams:
        credit = await client.get_team_credit(int(teams[0]["id"]))
        logger.info("credit remaining: %.2f", credit)
    for s in await client.list_servers(args.project_id):
        logger.info("server %s %s state=%s ip=%s", s.get("id"), s.get("hostname"),
                    s.get("state"), public_ip(s))
    return 0


async def _teardown(client: CherryClient, args: argparse.Namespace) -> int:
    if not args.yes:
        logger.error("refusing to delete without --yes")
        return 2
    deleted = await _provisioner(client, args).teardown()
    logger.info("deleted %d managed server(s): %s", len(deleted), deleted)
    return 0


async def _benchmark(client: CherryClient, args: argparse.Namespace) -> int:
    from pipeline.fleet.benchmark import run_benchmark
    pub = Path(args.key_file)
    if not pub.exists():
        logger.error("SSH public key %s not found — generate one: ssh-keygen -t ed25519 -f %s",
                     pub, pub.with_suffix(""))
        return 2
    prov = _provisioner(client, args)
    key_id = await prov.ensure_ssh_key("cherry_fleet", pub.read_text().strip())
    report = await run_benchmark(
        prov, count=args.count, key_ids=[key_id], input_path=args.input,
        name=args.name, ground_truth=args.ground_truth,
        with_zuhal=args.with_zuhal, dispatch_concurrency=args.dispatch_concurrency,
    )
    logger.info("benchmark report:\n%s", report.render())
    return 0


async def _run(args: argparse.Namespace, token: str) -> int:
    handlers = {"provision": _provision, "status": _status,
                "teardown": _teardown, "benchmark": _benchmark}
    async with CherryClient(token) as client:
        return await handlers[args.command](client, args)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # Make SIGTERM raise KeyboardInterrupt like SIGINT so a `kill` still runs the
    # benchmark's finally-block teardown instead of leaking provisioned servers.
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    args = _build_parser().parse_args(argv)
    token = os.environ.get("CHERRY_AUTH_TOKEN", "")
    if not token:
        logger.error("CHERRY_AUTH_TOKEN is not set")
        return 2
    try:
        return asyncio.run(_run(args, token))
    except KeyboardInterrupt:
        logger.warning("interrupted — any provisioned fleet was torn down")
        return 130


if __name__ == "__main__":
    sys.exit(main())
