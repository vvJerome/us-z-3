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
import json
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

    def as_dict(self) -> dict:
        return {
            "records": self.records, "matched": self.matched,
            "has_ground_truth": self.has_ground_truth,
            "record_state_distribution": self.state_dist,
            "cherry_fleet_verdict_distribution": self.fleet_dist,
            "bbops_verdict_distribution": self.bbops_dist,
            "final_verdict_distribution": self.final_dist,
            "validated": self.validated, "validated_pct": self.validated_pct,
            "fleet_definitive": self.fleet_definitive, "fleet_attempted": self.fleet_attempted,
            "decisive_accuracy_pct": self.decisive_accuracy_pct, "coverage_pct": self.coverage_pct,
        }

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


def _attach_log_file(path: Path) -> logging.Handler:
    """Tee every pipeline log record to a durable, per-record-flushed file (the run trail)."""
    handler = logging.FileHandler(path)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger("pipeline")
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    return handler


def _detach_log_file(handler: logging.Handler) -> None:
    logging.getLogger("pipeline").removeHandler(handler)
    handler.close()


def _write_report(out_dir: Path, report: BenchmarkReport) -> None:
    (out_dir / "benchmark_report.txt").write_text(report.render() + "\n")
    (out_dir / "benchmark_report.json").write_text(json.dumps(report.as_dict(), indent=2))


async def _teardown_fleet(prov: FleetProvisioner, name_prefix: str) -> list[int]:
    """Inventory teardown PLUS a project sweep of any prefixed orphan (e.g. a stuck deploy)."""
    deleted = list(await prov.teardown())
    try:
        for s in await prov.client.list_servers(prov.project_id):
            sid = int(s.get("id", 0) or 0)
            host = str(s.get("hostname", ""))
            if sid and sid not in deleted and host.startswith(name_prefix):
                await prov.client.delete_server(sid)
                deleted.append(sid)
                logger.warning("swept orphan server %s (%s)", sid, host)
    except Exception:
        logger.exception("orphan sweep failed (inventory teardown already done)")
    return deleted


async def _provision_with_retry(
    prov: FleetProvisioner, count: int, key_ids: list[int], *,
    reserve_region: str | None, retries: int, retry_delay_s: float, name_prefix: str,
) -> list[FleetHost]:
    """Provision `count`; tolerate a partial fleet, retry a 0-result (quota lag) a few times."""
    for attempt in range(1, retries + 1):
        hosts = await prov.provision(count, key_ids, reserve_region=reserve_region)
        if hosts:
            if len(hosts) < count:
                logger.warning("provisioned %d/%d worker(s) — proceeding with a partial fleet",
                               len(hosts), count)
            return hosts
        logger.warning("provisioned 0/%d (attempt %d/%d) — sweeping and %s",
                       count, attempt, retries,
                       "retrying" if attempt < retries else "giving up")
        await _teardown_fleet(prov, name_prefix)
        if attempt < retries:
            await asyncio.sleep(retry_delay_s)
    return []


async def _run_validation(
    input_path: str, hosts: list[FleetHost], name: str, log_dir: Path, *,
    with_zuhal: bool, dispatch_concurrency: int | None,
) -> Path:
    cmd = [sys.executable, "-m", "pipeline", "run", "-i", input_path, "--name", name,
           "--smtp-hosts", *[h.ip for h in hosts]]
    if dispatch_concurrency:
        cmd += ["--dispatch-concurrency", str(dispatch_concurrency)]
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"        # stream the child's logs live into validation.log
    if not with_zuhal:
        env["ZUHAL_API_KEY"] = ""        # benchmark measures the SMTP fleet; paid rescue is orthogonal
    env["BACKUP_ENABLED"] = "false"
    logger.info("validating %s across %d worker(s) — child logs → %s/validation.log",
                input_path, len(hosts), log_dir)
    with open(log_dir / "validation.log", "a") as logf:
        proc = await asyncio.create_subprocess_exec(*cmd, env=env, stdout=logf, stderr=logf)
        try:
            rc = await proc.wait()
        except asyncio.CancelledError:
            proc.terminate()
            await proc.wait()
            raise
    logger.info("validation run exited rc=%s", rc)
    return Path("output") / name / "pipeline.db"


async def run_benchmark(
    prov: FleetProvisioner, *, count: int, key_ids: list[int], input_path: str,
    name: str = "fleet_benchmark", ground_truth: str | None = None,
    with_zuhal: bool = False, dispatch_concurrency: int | None = None,
    reserve_region: str | None = None, name_prefix: str = "cherry",
    provision_retries: int = 3, provision_retry_delay_s: float = 120.0,
) -> BenchmarkReport:
    """Provision `count` workers, validate `input_path`, and ALWAYS tear the fleet down.

    Self-contained: every phase is logged (and flushed) to output/<name>/benchmark.log,
    the verdict report is written to benchmark_report.{txt,json}, and teardown runs in a
    finally with a project orphan-sweep — so a crash or signal never leaks a server.
    """
    out_dir = Path("output") / name
    out_dir.mkdir(parents=True, exist_ok=True)
    handler = _attach_log_file(out_dir / "benchmark.log")
    logger.info("=== benchmark start: input=%s count=%d region=%s name=%s ===",
                input_path, count, prov.region, name)
    try:
        logger.info("[1/4] provisioning %d worker(s) in %s ...", count, prov.region)
        hosts = await _provision_with_retry(
            prov, count, key_ids, reserve_region=reserve_region,
            retries=provision_retries, retry_delay_s=provision_retry_delay_s, name_prefix=name_prefix,
        )
        if not hosts:
            raise RuntimeError(f"provisioned 0 workers after {provision_retries} attempt(s)")
        prov.write_inventory(hosts)
        logger.info("fleet up: %s", [(h.worker_id, h.ip) for h in hosts])
        logger.info("[2/4] waiting for sshd on %d worker(s) ...", len(hosts))
        ready = await wait_ssh_ready(hosts)
        missing = [h.ip for h in hosts if h.ip not in ready]
        if missing:
            logger.warning("sshd not ready on %s (tunnel autorestart will retry)", missing)
        else:
            logger.info("sshd ready on all %d worker(s)", len(hosts))
        logger.info("[3/4] validating ...")
        db_path = await _run_validation(
            input_path, hosts, name, out_dir,
            with_zuhal=with_zuhal, dispatch_concurrency=dispatch_concurrency,
        )
        report = summarize(db_path, ground_truth)
        _write_report(out_dir, report)
        logger.info("report written to %s/benchmark_report.{txt,json}\n%s", out_dir, report.render())
        return report
    finally:
        logger.info("[4/4] tearing down fleet ...")
        deleted = await _teardown_fleet(prov, name_prefix)
        logger.info("=== benchmark done: torn down %d server(s): %s ===", len(deleted), deleted)
        _detach_log_file(handler)
