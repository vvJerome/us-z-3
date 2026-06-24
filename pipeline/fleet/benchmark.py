"""Autonomous Cherry fleet benchmark: provision → validate a dataset → tear down.

One command, default config: point it at a JSONL dataset and it provisions a fresh
fleet, runs the validation pipeline over it, prints the SMTP verdict distribution, and
ALWAYS tears the fleet down (in a finally, so a crash or signal never leaks servers).
Optional ground truth (email,zb_status CSV) adds a deliverability-accuracy report.

The validation run is the unmodified `python -m pipeline run --smtp-hosts <ips>` path,
so the benchmark exercises the real dispatcher/fleet, not a parallel code path.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import os
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from pipeline.fleet.provisioner import FleetHost, FleetProvisioner

logger = logging.getLogger("pipeline.fleet.benchmark")

DELIVERABLE = {"valid", "catch_all"}
DEFINITIVE = {"valid", "invalid", "catch_all"}
_SKIP = {"not_run", "ms_valid", ""}


def _pct(a: int, b: int) -> float | None:
    return round(100 * a / b, 2) if b else None


async def _port_open(ip: str, port: int, timeout_s: float) -> bool:
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(ip, port), timeout_s)
    except (OSError, asyncio.TimeoutError):
        return False
    writer.close()
    try:
        await writer.wait_closed()
    except OSError:
        pass
    return True


async def wait_ssh_ready(
    hosts: list[FleetHost], *, port: int = 22, timeout_s: float = 180.0, interval_s: float = 5.0
) -> list[str]:
    """Poll each worker's sshd port until open; return the IPs that became ready in time."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    pending = {h.ip for h in hosts}
    while pending and loop.time() < deadline:
        ips = list(pending)
        oks = await asyncio.gather(*(_port_open(ip, port, 8.0) for ip in ips))
        pending -= {ip for ip, ok in zip(ips, oks) if ok}
        if pending:
            await asyncio.sleep(interval_s)
    return [h.ip for h in hosts if h.ip not in pending]


@dataclass
class BenchmarkReport:
    records: int
    matched: int
    has_ground_truth: bool
    state_dist: dict[str, int] = field(default_factory=dict)
    fleet_dist: dict[str, int] = field(default_factory=dict)
    bbops_dist: dict[str, int] = field(default_factory=dict)
    final_dist: dict[str, int] = field(default_factory=dict)
    validated: int = 0
    fleet_definitive: int = 0
    fleet_attempted: int = 0
    fleet_correct: int = 0

    @property
    def decisive_accuracy_pct(self) -> float | None:
        return _pct(self.fleet_correct, self.fleet_definitive) if self.has_ground_truth else None

    @property
    def coverage_pct(self) -> float | None:
        return _pct(self.fleet_definitive, self.fleet_attempted)

    @property
    def validated_pct(self) -> float | None:
        return _pct(self.validated, self.matched)

    def render(self) -> str:
        base = self.matched if self.has_ground_truth else self.records
        lines = [
            "CHERRY FLEET BENCHMARK",
            "=" * 50,
            f"records processed             : {self.records}",
        ]
        if self.has_ground_truth:
            lines.append(f"records matched to ground truth: {self.matched}")
        lines += [
            f"record_state distribution     : {self.state_dist}",
            "",
            "-- Cherry fleet SMTP (racknerd_status) --",
            f"  verdict distribution        : {self.fleet_dist}",
            f"  definitive decisions        : {self.fleet_definitive}",
            f"  coverage (decided/attempted): {self.coverage_pct}%",
        ]
        if self.has_ground_truth:
            lines.append(f"  decisive accuracy           : {self.decisive_accuracy_pct}%")
        lines += [
            "",
            f"-- bbops (bbops_status) --      : {self.bbops_dist}",
            f"-- final_verdict               : {self.final_dist}",
            f"system VALIDATED               : {self.validated}/{base} ({self.validated_pct}%)",
        ]
        return "\n".join(lines)


def summarize(db_path: str | Path, ground_truth_path: str | Path | None = None) -> BenchmarkReport:
    """Read a finished run's pipeline.db into a BenchmarkReport (optionally scored vs ground truth)."""
    gt: dict[str, str] = {}
    if ground_truth_path:
        with open(ground_truth_path, newline="") as f:
            for row in csv.DictReader(f):
                gt[row["email"].strip().lower()] = (row.get("zb_status") or "").strip().lower()

    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT lower(candidate_email), racknerd_status, bbops_status, final_verdict, record_state "
            "FROM records"
        ).fetchall()
    finally:
        conn.close()

    rep = BenchmarkReport(records=len(rows), matched=0, has_ground_truth=bool(gt))
    state_c: Counter = Counter()
    fleet_c: Counter = Counter()
    bbops_c: Counter = Counter()
    final_c: Counter = Counter()
    for email, rk, bb, fv, state in rows:
        if gt and email not in gt:
            continue
        rep.matched += 1
        rk = (rk or "").lower()
        bb = (bb or "").lower()
        fv = (fv or "").lower()
        state_c[state] += 1
        fleet_c[rk or "(blank)"] += 1
        bbops_c[bb or "(blank)"] += 1
        final_c[fv or "(blank)"] += 1
        if fv in DELIVERABLE:
            rep.validated += 1
        if rk not in _SKIP:
            rep.fleet_attempted += 1
        if rk in DEFINITIVE:
            rep.fleet_definitive += 1
            if gt and (rk in DELIVERABLE) == (gt.get(email, "") in DELIVERABLE):
                rep.fleet_correct += 1
    rep.state_dist = dict(state_c)
    rep.fleet_dist = dict(fleet_c)
    rep.bbops_dist = dict(bbops_c)
    rep.final_dist = dict(final_c)
    return rep


async def _run_validation(
    input_path: str, hosts: list[FleetHost], name: str, *,
    with_zuhal: bool, dispatch_concurrency: int | None,
) -> Path:
    cmd = [sys.executable, "-m", "pipeline", "run", "-i", input_path, "--name", name,
           "--smtp-hosts", *[h.ip for h in hosts]]
    if dispatch_concurrency:
        cmd += ["--dispatch-concurrency", str(dispatch_concurrency)]
    env = dict(os.environ)
    if not with_zuhal:
        env["ZUHAL_API_KEY"] = ""   # benchmark measures the SMTP fleet; paid rescue is orthogonal
    env["BACKUP_ENABLED"] = "false"
    logger.info("validating %s across %d worker(s)", input_path, len(hosts))
    proc = await asyncio.create_subprocess_exec(*cmd, env=env)
    try:
        rc = await proc.wait()
    except asyncio.CancelledError:
        proc.terminate()
        raise
    logger.info("validation run exited rc=%s", rc)
    return Path("output") / name / "pipeline.db"


async def run_benchmark(
    prov: FleetProvisioner, *, count: int, key_ids: list[int], input_path: str,
    name: str = "fleet_benchmark", ground_truth: str | None = None,
    with_zuhal: bool = False, dispatch_concurrency: int | None = None,
    reserve_region: str | None = None,
) -> BenchmarkReport:
    """Provision `count` workers, validate `input_path`, and ALWAYS tear the fleet down."""
    try:
        hosts = await prov.provision(count, key_ids, reserve_region=reserve_region)
        if not hosts:
            raise RuntimeError("provisioned 0 workers — check credit/quota/region")
        prov.write_inventory(hosts)
        logger.info("provisioned %d worker(s): %s", len(hosts), [h.ip for h in hosts])
        ready = await wait_ssh_ready(hosts)
        missing = [h.ip for h in hosts if h.ip not in ready]
        if missing:
            logger.warning("workers not ssh-ready in time (tunnel will retry): %s", missing)
        db_path = await _run_validation(
            input_path, hosts, name, with_zuhal=with_zuhal, dispatch_concurrency=dispatch_concurrency,
        )
        return summarize(db_path, ground_truth)
    finally:
        deleted = await prov.teardown()
        logger.info("teardown: deleted %d worker(s): %s", len(deleted), deleted)
