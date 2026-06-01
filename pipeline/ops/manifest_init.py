"""Backfill the manifest DB from existing /zuhaled, /zerobounced, and /passoff CSVs.

Idempotent: re-running will UPSERT and not duplicate rows.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pipeline import manifest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("manifest_init")

ROOT = Path(__file__).resolve().parents[2]
US_OUT = ROOT / "output" / "us_output"


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

    for op in manifest.OPERATORS:
        op_dir = zuhal_root / op
        if not op_dir.exists():
            continue
        for path in sorted(op_dir.glob("*.csv")):
            if not manifest.is_zuhal_results_file(path):
                continue
            n = manifest.ingest_zuhal_file(conn, path, op)
            log.info("zuhaled  %s/%s  +%d rows", op, path.name, n)

    for op in manifest.OPERATORS:
        op_dir = zb_root / op
        if not op_dir.exists():
            continue
        for path in sorted(op_dir.glob("*.csv")):
            if not manifest.is_zb_results_file(path):
                continue
            n = manifest.ingest_zb_file(conn, path, op)
            log.info("zb       %s/%s  +%d rows", op, path.name, n)

    for op in manifest.OPERATORS:
        op_dir = passoff_root / op
        if not op_dir.exists():
            continue
        for path in sorted(op_dir.glob("*combined.csv")):
            n = manifest.ingest_passoff_file(conn, path, op)
            log.info("passoff  %s/%s  +%d rows", op, path.name, n)

    c = manifest.counts(conn)
    log.info(
        "Totals: emails=%d  zuhaled=%d  zerobounced=%d  in_passoff=%d",
        c["total"], c["zuhaled"], c["zerobounced"], c["in_passoff"],
    )


if __name__ == "__main__":
    main()
