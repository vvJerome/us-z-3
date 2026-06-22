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

from pipeline.utils.email_patterns import email_to_template
from pipeline.utils.text import parse_name
from pipeline.verdicts import normalize_verdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("ingest_zerobounce")


def _get(row: dict, *keys: str) -> str:
    for k in keys:
        if k in row and (row[k] or "").strip():
            return row[k].strip()
    return ""


def _feed_pattern_stats(conn: sqlite3.Connection, uid: str, canonical: str) -> bool:
    """Reinforce pattern_stats with ZeroBounce ground truth (item 10). Returns True if recorded.

    Only the unambiguous outcomes teach the naming convention: valid → success, invalid →
    miss. catch_all/unknown/do_not_mail/abuse/disposable don't confirm the local part, so skip.
    """
    if canonical == "valid":
        success = True
    elif canonical == "invalid":
        success = False
    else:
        return False
    row = conn.execute(
        "SELECT candidate_email, mx_provider, agent_name FROM records WHERE unique_id = ?", (uid,)
    ).fetchone()
    if not row or not row[0] or not row[1]:
        return False
    email, mx_provider, agent_name = row[0], row[1], row[2] or ""
    first, _, last = parse_name(agent_name)
    template = email_to_template(email, first, last, email.rpartition("@")[2])
    if not template:
        return False
    conn.execute(
        """
        INSERT INTO pattern_stats (mx_provider, template, success_count, total_count)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(mx_provider, template) DO UPDATE SET
            success_count = success_count + ?, total_count = total_count + 1
        """,
        (mx_provider, template, 1 if success else 0, 1 if success else 0),
    )
    return True


def ingest(db_path: Path, zb_csv: Path) -> tuple[int, int, int]:
    """Returns (matched, skipped, learned). Idempotent for the verdict write; re-running also
    re-feeds pattern_stats, so ingest a given ZB CSV once."""
    with zb_csv.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    conn = sqlite3.connect(str(db_path))
    try:
        matched = skipped = learned = 0
        for r in rows:
            uid = _get(r, "unique_id")
            zb_status = _get(r, "zb_status", "ZB Status", "status")
            if not uid or not zb_status:
                skipped += 1
                continue
            zb_sub = _get(r, "zb_sub_status", "ZB Sub Status", "sub_status") or None
            zb_at = _get(r, "zb_verified_at", "zb_processed_at", "Processed At", "processed_at") or None
            canonical = normalize_verdict(zb_status)
            cur = conn.execute(
                """UPDATE records
                      SET zb_status = ?, zb_sub_status = ?, zb_verified_at = ?,
                          canonical_status = ?, canonical_sub_status = ?,
                          canonical_source = 'zerobounce', updated_at = datetime('now')
                    WHERE unique_id = ?""",
                (zb_status, zb_sub, zb_at, canonical, zb_sub, uid),
            )
            if cur.rowcount:
                matched += 1
                if _feed_pattern_stats(conn, uid, canonical):
                    learned += 1
            else:
                skipped += 1
        conn.commit()
        return matched, skipped, learned
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

    matched, skipped, learned = ingest(args.db, args.zb)
    log.info(
        "ZeroBounce ingest: %d records updated, %d skipped, %d fed back into pattern_stats",
        matched, skipped, learned,
    )


if __name__ == "__main__":
    main()
