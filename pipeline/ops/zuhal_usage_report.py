"""Report how many emails were actually checked by Zuhal for a run.

`records.zuhal_status` holds two different things: a real verdict from a live
Zuhal call (`valid`/`invalid`/`catch_all`/`catch-all`/`error`), or a `dual_*`/
`circuit_open`/`unknown` placeholder written when Zuhal was never called (see
`update_record_dual` in pipeline/db/records.py). Only the former counts toward
Zuhal credit/cost usage.

Most of a run's real Zuhal volume can also live outside the live dispatcher
entirely — backlog/error-rescue passes submitted by hand via
pipeline.ops.zuhal_bulk against exported NEEDS_ZUHAL CSVs. Pass those CSVs
with --bulk-csv to fold them into the total (one row submitted = one credit;
zuhal_bulk.py dedupes by email *within* a single file before uploading, so
that's the per-file count this script uses too).

Usage:
    python -m pipeline.ops.zuhal_usage_report --db output/<run>/pipeline.db \\
        --bulk-csv output/<run>/needs_zuhal_a.csv --bulk-csv output/<run>/needs_zuhal_b.csv
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

from pipeline.constants import API_COSTS

# ponytail: mirrors update_record_dual's placeholder convention rather than
# reading canonical_source (archived run dbs predate that column and were
# never reopened by the live app to pick up the migration). Upgrade: switch
# to `canonical_source = 'zuhal'` once every run db has been migrated.
_NO_CALL_PLACEHOLDERS = ("dual_valid", "dual_catch_all", "dual_invalid", "circuit_open", "unknown")

_EMAIL_COLUMNS = ("candidate_email", "Email", "email")

REAL_CALLS_SQL = f"""
    SELECT zuhal_status, COUNT(*) AS n
      FROM records
     WHERE zuhal_status IS NOT NULL
       AND zuhal_status NOT IN ({", ".join("?" for _ in _NO_CALL_PLACEHOLDERS)})
     GROUP BY zuhal_status
"""

REAL_CALL_IDS_SQL = f"""
    SELECT unique_id
      FROM records
     WHERE zuhal_status IS NOT NULL
       AND zuhal_status NOT IN ({", ".join("?" for _ in _NO_CALL_PLACEHOLDERS)})
"""

REAL_CALL_EMAILS_SQL = f"""
    SELECT candidate_email
      FROM records
     WHERE zuhal_status IS NOT NULL
       AND zuhal_status NOT IN ({", ".join("?" for _ in _NO_CALL_PLACEHOLDERS)})
       AND candidate_email IS NOT NULL
"""


def live_zuhal_breakdown(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(REAL_CALLS_SQL, _NO_CALL_PLACEHOLDERS).fetchall()
    return {status: n for status, n in rows}


def live_zuhal_ids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(REAL_CALL_IDS_SQL, _NO_CALL_PLACEHOLDERS).fetchall()
    return {uid for (uid,) in rows}


def live_zuhal_emails(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(REAL_CALL_EMAILS_SQL, _NO_CALL_PLACEHOLDERS).fetchall()
    return {email.strip().lower() for (email,) in rows if email and email.strip()}


def bulk_csv_emails(path: Path) -> set[str]:
    """Unique emails in one bulk-submission CSV — matches zuhal_bulk.py's own dedup."""
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        col = next((c for c in _EMAIL_COLUMNS if c in (reader.fieldnames or [])), None)
        if col is None:
            print(f"warning: {path.name} has no email column, skipping", file=sys.stderr)
            return set()
        return {row[col].strip().lower() for row in reader if row.get(col, "").strip()}


def bulk_csv_unique_ids(path: Path) -> set[str]:
    """Empty when the CSV has no unique_id column — e.g. a plain email-list
    export uploaded by hand through Zuhal's dashboard (no zuhal_bulk.py metadata).
    Callers must fall back to matching by email in that case.
    """
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "unique_id" not in (reader.fieldnames or []):
            return set()
        return {row["unique_id"] for row in reader if row.get("unique_id")}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", required=True, type=Path, help="Path to a run's pipeline.db")
    p.add_argument("--bulk-csv", action="append", default=[], type=Path,
                    help="NEEDS_ZUHAL CSV actually submitted via zuhal_bulk.py (repeatable)")
    args = p.parse_args(argv)

    if not args.db.exists():
        print(f"Database not found: {args.db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(args.db)
    breakdown = live_zuhal_breakdown(conn)
    live_total = sum(breakdown.values())
    live_ids = live_zuhal_ids(conn)
    live_emails = live_zuhal_emails(conn)
    conn.close()

    print(f"Live dispatcher Zuhal calls: {live_total:,}")
    for status, n in sorted(breakdown.items(), key=lambda kv: -kv[1]):
        print(f"  {status:12s} {n:,}")

    per_file_unique: list[set[str]] = []
    grand_unique: set[str] = set()
    overlap: set[str] = set()
    for csv_path in args.bulk_csv:
        if not csv_path.exists():
            print(f"warning: bulk csv not found: {csv_path}", file=sys.stderr)
            continue
        emails = bulk_csv_emails(csv_path)
        per_file_unique.append(emails)
        grand_unique |= emails
        file_ids = bulk_csv_unique_ids(csv_path)
        # ponytail: dashboard-upload exports carry no unique_id, only an email
        # column — fall back to matching by email so overlap detection isn't
        # silently a no-op on that format. Upgrade: drop once every bulk export
        # path is required to carry unique_id.
        overlap |= (file_ids & live_ids) if file_ids else (emails & live_emails)
        print(f"\nBulk submission {csv_path.name}: {len(emails):,} unique emails uploaded")

    bulk_raw_total = sum(len(s) for s in per_file_unique)
    cost = API_COSTS["zuhal"]

    print(f"\nBulk submissions across {len(per_file_unique)} file(s): "
          f"{bulk_raw_total:,} raw (counts re-submissions), "
          f"{len(grand_unique):,} unique emails")
    if overlap:
        print(f"warning: {len(overlap):,} record(s) have BOTH a live Zuhal verdict "
              f"and appear in a bulk CSV — verify they weren't billed twice")

    raw_total = live_total + bulk_raw_total
    unique_total = live_total + len(grand_unique)
    print(f"\nEstimated Zuhal cost — raw (incl. re-submissions): {raw_total:,} checks "
          f"x ${cost} = ${raw_total * cost:,.2f}")
    print(f"Estimated Zuhal cost — unique emails only:          {unique_total:,} checks "
          f"x ${cost} = ${unique_total * cost:,.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
