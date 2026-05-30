#!/usr/bin/env python3
"""Backfill the manifest DB from existing /zuhaled, /zerobounced, and /passoff CSVs.

Idempotent: re-running will UPSERT and not duplicate rows.
"""
from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from pipeline import manifest  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("manifest_init")

US_OUT = ROOT / "output" / "us_output"

OPERATORS = ("alpha", "jerome", "sara")

EMAIL_KEYS = ("email", "Email", "candidate_email", "email_address")
EID_KEYS = ("unique_id",)
ZUHAL_VERDICT_KEYS = ("zuhal_verdict", "Status")

_ZUHAL_VERDICT_MAP = {
    "valid": "valid",
    "invalid": "invalid",
    "catch_all": "catch_all",
    "catch-all": "catch_all",
    "accept-all": "catch_all",
    "unknown": "unknown",
    "no_result": "unknown",
    "disposable account": "invalid",
    "disposable": "invalid",
}

_PART_FROM_FILENAME = re.compile(r"(part\d|w_officer|wo_officer|part1)", re.IGNORECASE)


def email_of(row: dict) -> str:
    for k in EMAIL_KEYS:
        v = row.get(k)
        if v:
            return v.strip().lower()
    return ""


def eid_of(row: dict) -> str:
    for k in EID_KEYS:
        v = row.get(k)
        if v:
            return manifest.strip_state_prefix(v.strip())
    return ""


def normalize_zuhal_verdict(v: str) -> str:
    return _ZUHAL_VERDICT_MAP.get((v or "").strip().lower(), (v or "").strip().lower())


def part_from_filename(path: Path) -> str:
    m = _PART_FROM_FILENAME.search(path.stem.lower())
    return m.group(1).lower() if m else ""


def is_zuhal_results_file(path: Path) -> bool:
    name = path.stem.lower()
    return name.endswith("_zuhaled") or name.endswith("_zuhaled_v2") or name.endswith(
        ".zuhal"
    )


def is_zb_results_file(path: Path) -> bool:
    name = path.stem.lower()
    return name.endswith("_zerobounced") or name.endswith("_unknown_for_zb") or name.endswith(
        "_valid_for_zb"
    ) or name.endswith("_valid_catchall_for_zb")


def ingest_zuhal_file(conn, path: Path, operator: str) -> int:
    part = part_from_filename(path)
    source = "standalone_zuhal"
    n = 0
    with path.open(encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = email_of(row)
            if not email:
                continue
            verdict_raw = ""
            for k in ZUHAL_VERDICT_KEYS:
                if k in row and row[k]:
                    verdict_raw = row[k]
                    break
            verdict = normalize_zuhal_verdict(verdict_raw)
            manifest.mark_zuhaled(
                conn,
                email=email,
                eid=eid_of(row),
                operator=operator,
                part=part,
                verdict=verdict,
                source=source,
            )
            n += 1
    return n


def ingest_zb_file(conn, path: Path, operator: str) -> int:
    part = part_from_filename(path)
    n = 0
    with path.open(encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = email_of(row)
            if not email:
                continue
            zb_status = (row.get("zb_status") or "").strip()
            if not zb_status:
                continue
            manifest.mark_zerobounced(
                conn,
                email=email,
                zb_status=zb_status,
                zb_sub_status=(row.get("zb_sub_status") or "").strip(),
                operator=operator,
                part=part,
                eid=eid_of(row),
            )
            n += 1
    return n


def ingest_passoff_file(conn, path: Path, operator: str) -> int:
    n = 0
    with path.open(encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = email_of(row)
            if not email:
                continue
            manifest.mark_passed_off(conn, email)
            n += 1
    return n


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=manifest.DEFAULT_DB_PATH,
        help=f"Manifest DB path (default: {manifest.DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--us-out",
        type=Path,
        default=US_OUT,
        help=f"us_output root (default: {US_OUT})",
    )
    args = parser.parse_args()

    conn = manifest.connect(args.db)
    log.info("Manifest DB at %s", args.db)

    zuhal_root = args.us_out / "zuhaled"
    zb_root = args.us_out / "zerobounced"
    passoff_root = args.us_out / "passoff"

    for op in OPERATORS:
        op_dir = zuhal_root / op
        if not op_dir.exists():
            continue
        for path in sorted(op_dir.glob("*.csv")):
            if not is_zuhal_results_file(path):
                continue
            n = ingest_zuhal_file(conn, path, op)
            log.info("zuhaled  %s/%s  +%d rows", op, path.name, n)

    for op in OPERATORS:
        op_dir = zb_root / op
        if not op_dir.exists():
            continue
        for path in sorted(op_dir.glob("*.csv")):
            if not is_zb_results_file(path):
                continue
            n = ingest_zb_file(conn, path, op)
            log.info("zb       %s/%s  +%d rows", op, path.name, n)

    for op in OPERATORS:
        op_dir = passoff_root / op
        if not op_dir.exists():
            continue
        for path in sorted(op_dir.glob("*combined.csv")):
            n = ingest_passoff_file(conn, path, op)
            log.info("passoff  %s/%s  +%d rows", op, path.name, n)

    c = manifest.counts(conn)
    log.info(
        "Totals: emails=%d  zuhaled=%d  zerobounced=%d  in_passoff=%d",
        c["total"], c["zuhaled"], c["zerobounced"], c["in_passoff"],
    )


if __name__ == "__main__":
    main()
