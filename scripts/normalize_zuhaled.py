#!/usr/bin/env python3
"""Upgrade legacy {Email,Status} zuhaled files to the canonical 7-column shape.

Reads each {Email,Status} CSV and joins unique_id (and other context columns)
back from output/us_output/collected/<operator>/*.csv on (email == lower).
Writes <stem>_normalized.csv next to the original, leaving the original intact.
"""
from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
US_OUT = ROOT / "output" / "us_output"

CANONICAL_HEADER = [
    "unique_id", "business_name", "agent_name",
    "candidate_email", "candidate_domain", "mx_provider",
    "zuhal_verdict",
]

_STATUS_MAP = {
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

_PART_RE = re.compile(r"(part\d|w_officer|wo_officer|part1)", re.IGNORECASE)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("normalize_zuhaled")


def normalize_verdict(v: str) -> str:
    return _STATUS_MAP.get((v or "").strip().lower(), (v or "").strip().lower())


def is_legacy_zuhaled(path: Path) -> bool:
    with path.open(encoding="utf-8") as f:
        header = next(csv.reader(f), [])
    return [c.strip() for c in header] == ["Email", "Status"]


def build_context_index(operator: str) -> dict[str, dict]:
    """Email -> {unique_id, business_name, agent_name, candidate_domain, mx_provider}."""
    coll_dir = US_OUT / "collected" / operator
    idx: dict[str, dict] = {}
    if not coll_dir.exists():
        log.warning("No collected dir for %s", operator)
        return idx
    for path in sorted(coll_dir.glob("*.csv")):
        with path.open(encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                email = (row.get("email") or row.get("candidate_email") or "").strip().lower()
                if not email or email in idx:
                    continue
                idx[email] = {
                    "unique_id": row.get("unique_id", ""),
                    "business_name": row.get("business_name", ""),
                    "agent_name": row.get("agent_name", ""),
                    "candidate_domain": row.get("candidate_domain", ""),
                    "mx_provider": row.get("mx_provider", ""),
                }
    log.info("Built context index for %s: %d emails", operator, len(idx))
    return idx


def normalize_file(path: Path, operator: str) -> Path:
    idx = build_context_index(operator)
    out_path = path.with_name(f"{path.stem}_normalized.csv")
    n_rows = n_with_uid = 0
    with path.open(encoding="utf-8") as fin, out_path.open(
        "w", newline="", encoding="utf-8"
    ) as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=CANONICAL_HEADER)
        writer.writeheader()
        for row in reader:
            email = (row.get("Email") or row.get("email") or "").strip().lower()
            if not email:
                continue
            ctx = idx.get(email, {})
            uid = ctx.get("unique_id", "")
            if uid:
                n_with_uid += 1
            writer.writerow({
                "unique_id": uid,
                "business_name": ctx.get("business_name", ""),
                "agent_name": ctx.get("agent_name", ""),
                "candidate_email": email,
                "candidate_domain": ctx.get("candidate_domain", ""),
                "mx_provider": ctx.get("mx_provider", ""),
                "zuhal_verdict": normalize_verdict(row.get("Status", "")),
            })
            n_rows += 1
    log.info(
        "%s/%s: %d rows, %d with unique_id -> %s",
        operator, path.name, n_rows, n_with_uid, out_path.name,
    )
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "files", nargs="*", type=Path,
        help="Specific files. Default: scan /zuhaled/<op>/*.csv for legacy shape.",
    )
    parser.add_argument(
        "--operator",
        help="Force operator for --files (alpha|jerome|sara). "
             "Default: infer from parent dir name.",
    )
    args = parser.parse_args()

    if args.files:
        targets: list[tuple[Path, str]] = []
        for f in args.files:
            op = args.operator or f.parent.name
            targets.append((f, op))
    else:
        targets = []
        for op_dir in (US_OUT / "zuhaled").glob("*"):
            if not op_dir.is_dir():
                continue
            for f in sorted(op_dir.glob("*.csv")):
                if is_legacy_zuhaled(f):
                    targets.append((f, op_dir.name))

    if not targets:
        log.info("No legacy {Email,Status} zuhaled files found.")
        return

    for path, op in targets:
        if not path.exists():
            log.error("Not found: %s", path)
            sys.exit(1)
        if not is_legacy_zuhaled(path):
            log.warning("Skipping %s: not in legacy shape", path)
            continue
        normalize_file(path, op)


if __name__ == "__main__":
    main()
