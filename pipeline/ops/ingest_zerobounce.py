"""Ingest ZeroBounce results back into a run's pipeline.db as the ground-truth verdict.

ZeroBounce runs as a separate post-pipeline step (see zb_zuhaled.py) and is the
final authority. This script joins a /zerobounced CSV back to records by unique_id
and writes zb_status/zb_sub_status/zb_verified_at, then overrides canonical_status
with the ZeroBounce verdict (canonical_source='zerobounce').

Usage:
    python -m pipeline.ops.ingest_zerobounce --db output/<run>/pipeline.db \\
        --zb output/<run>/zerobounced/<file>.csv
"""
from __future__ import annotations

import argparse
import csv
import logging
import sqlite3
import sys
from pathlib import Path

from pipeline.verdicts import normalize_verdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("ingest_zerobounce")


def _get(row: dict, *keys: str) -> str:
    for k in keys:
        if k in row and (row[k] or "").strip():
            return row[k].strip()
    return ""


def ingest(db_path: Path, zb_csv: Path) -> tuple[int, int]:
    """Returns (matched, skipped). Idempotent — re-running overwrites with the same values."""
    with zb_csv.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    conn = sqlite3.connect(str(db_path))
    try:
        matched = skipped = 0
        for r in rows:
            uid = _get(r, "unique_id")
            zb_status = _get(r, "zb_status", "ZB Status", "status")
            if not uid or not zb_status:
                skipped += 1
                continue
            zb_sub = _get(r, "zb_sub_status", "ZB Sub Status", "sub_status") or None
            zb_at = _get(r, "zb_verified_at", "zb_processed_at", "Processed At", "processed_at") or None
            cur = conn.execute(
                """UPDATE records
                      SET zb_status = ?, zb_sub_status = ?, zb_verified_at = ?,
                          canonical_status = ?, canonical_sub_status = ?,
                          canonical_source = 'zerobounce', updated_at = datetime('now')
                    WHERE unique_id = ?""",
                (zb_status, zb_sub, zb_at, normalize_verdict(zb_status), zb_sub, uid),
            )
            if cur.rowcount:
                matched += 1
            else:
                skipped += 1
        conn.commit()
        return matched, skipped
    finally:
        conn.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, required=True, help="Run pipeline.db to update")
    p.add_argument("--zb", type=Path, required=True, help="/zerobounced CSV to ingest")
    args = p.parse_args()

    for path in (args.db, args.zb):
        if not path.exists():
            log.error("Not found: %s", path)
            sys.exit(1)

    matched, skipped = ingest(args.db, args.zb)
    log.info("ZeroBounce ingest: %d records updated, %d skipped (no unique_id/status match)", matched, skipped)


if __name__ == "__main__":
    main()
