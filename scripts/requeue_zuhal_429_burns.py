"""Recover VALIDATION_FAILED records that were burned by the Zuhal 429 bug.

Background: between 2026-05-13 21:14 UTC (when the SMTP dispatcher exited) and
the deployment of the 429 → ZuhalCircuitOpenError fix, the ZuhalDispatcher's
generic `except Exception` handler marked every retry-exhausted 429 as
`VALIDATION_FAILED` with `zuhal_status='error'`. This script returns those
specific rows to `NEEDS_ZUHAL` so the fixed worker can probe them properly.

Filter: record_state='VALIDATION_FAILED' AND zuhal_status='error'
        AND updated_at > '2026-05-13 21:14:00' (the SMTP exit moment).

Defaults to dry-run; pass --apply to execute.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

CUTOFF_UTC = "2026-05-13 21:14:00"

PREVIEW_SQL = """
    SELECT COUNT(*) AS n
      FROM records
     WHERE record_state = 'VALIDATION_FAILED'
       AND zuhal_status = 'error'
       AND updated_at > ?
"""

UPDATE_SQL = """
    UPDATE records
       SET record_state = 'NEEDS_ZUHAL',
           zuhal_status = NULL,
           updated_at = datetime('now')
     WHERE record_state = 'VALIDATION_FAILED'
       AND zuhal_status = 'error'
       AND updated_at > ?
"""


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", required=True, type=Path, help="Path to pipeline.db")
    p.add_argument("--apply", action="store_true",
                   help="Actually execute the UPDATE (default: dry-run preview only)")
    p.add_argument("--cutoff", default=CUTOFF_UTC,
                   help=f"updated_at cutoff in UTC (default: {CUTOFF_UTC})")
    args = p.parse_args(argv)

    if not args.db.exists():
        print(f"Database not found: {args.db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(args.db, isolation_level=None)
    conn.row_factory = sqlite3.Row

    n = conn.execute(PREVIEW_SQL, (args.cutoff,)).fetchone()["n"]
    print(f"Matching VALIDATION_FAILED rows with zuhal_status='error' "
          f"updated_at > {args.cutoff}: {n:,}")

    if not args.apply:
        print("(dry-run — pass --apply to requeue)")
        return 0

    if n == 0:
        print("Nothing to requeue.")
        return 0

    cur = conn.execute(UPDATE_SQL, (args.cutoff,))
    print(f"Requeued {cur.rowcount:,} rows from VALIDATION_FAILED → NEEDS_ZUHAL")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
