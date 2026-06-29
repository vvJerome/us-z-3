"""Ingest completed run outputs into a persistent master verification database.

Tracks verified emails across all runs with per-status expiry windows so stale
results can be identified before re-running.

Usage:
    python -m pipeline.ops.master_db --run-dir output/run_20260625 [--db master.db]
    python -m pipeline.ops.master_db --run-dir output/run_20260625 --summary
"""
from __future__ import annotations

import argparse
import csv
import datetime
import logging
import sqlite3
import sys
from pathlib import Path

logger = logging.getLogger("pipeline.ops.master_db")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ingest_checkpoints (
    source_db  TEXT PRIMARY KEY,
    last_rowid INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS verified_emails (
    unique_id        TEXT NOT NULL,
    email            TEXT NOT NULL,
    business_name    TEXT,
    agent_name       TEXT,
    state            TEXT,
    canonical_status TEXT NOT NULL,
    canonical_source TEXT,
    confidence_score INTEGER,
    confidence_tier  TEXT,
    run_name         TEXT,
    verified_at      TEXT NOT NULL,
    expires_at       TEXT NOT NULL,
    PRIMARY KEY (unique_id, email)
);
CREATE INDEX IF NOT EXISTS idx_ve_uid     ON verified_emails (unique_id);
CREATE INDEX IF NOT EXISTS idx_ve_expires ON verified_emails (expires_at);
CREATE INDEX IF NOT EXISTS idx_ve_status  ON verified_emails (canonical_status);
"""

# How long each status result is considered trustworthy.
EXPIRY_DAYS: dict[str, int] = {
    "valid":       90,
    "catch_all":   30,
    "unknown":      7,
    "do_not_mail": 180,
    "invalid":      30,
    "abuse":       180,
    "disposable":  180,
}
_DEFAULT_EXPIRY_DAYS = 30


def _expires_at(status: str, verified_at: datetime.datetime) -> str:
    days = EXPIRY_DAYS.get(status, _DEFAULT_EXPIRY_DAYS)
    return (verified_at + datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def _safe_int(val: str | None) -> int | None:
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def open_master_db(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


def ingest_run(
    db_path: str | Path,
    run_dir: str | Path,
    run_name: str | None = None,
) -> tuple[int, int]:
    """Ingest valid_emails.csv from run_dir into the master DB.

    Returns (inserted, updated) counts.
    """
    run_dir = Path(run_dir)
    csv_path = run_dir / "valid_emails.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"No valid_emails.csv found in {run_dir}")

    run_name = run_name or run_dir.name
    now = datetime.datetime.now(datetime.timezone.utc)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    conn = open_master_db(db_path)
    inserted = updated = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            unique_id = (row.get("unique_id") or "").strip()
            email = (row.get("email") or "").strip()
            if not unique_id or not email:
                continue

            status = (
                row.get("canonical_status")
                or row.get("final_verdict")
                or "unknown"
            ).strip()

            existing = conn.execute(
                "SELECT 1 FROM verified_emails WHERE unique_id = ? AND email = ?",
                (unique_id, email),
            ).fetchone()

            conn.execute(
                """
                INSERT INTO verified_emails
                    (unique_id, email, business_name, agent_name, state,
                     canonical_status, canonical_source,
                     confidence_score, confidence_tier,
                     run_name, verified_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (unique_id, email) DO UPDATE SET
                    canonical_status = excluded.canonical_status,
                    canonical_source = excluded.canonical_source,
                    confidence_score = excluded.confidence_score,
                    confidence_tier  = excluded.confidence_tier,
                    run_name         = excluded.run_name,
                    verified_at      = excluded.verified_at,
                    expires_at       = excluded.expires_at
                """,
                (
                    unique_id,
                    email,
                    (row.get("business_name") or "").strip(),
                    (row.get("agent_name") or "").strip(),
                    (row.get("state") or "").strip(),
                    status,
                    (row.get("canonical_source") or "").strip(),
                    _safe_int(row.get("confidence_score")),
                    (row.get("confidence_tier") or "").strip(),
                    run_name,
                    now_str,
                    _expires_at(status, now),
                ),
            )
            if existing:
                updated += 1
            else:
                inserted += 1

    conn.commit()
    conn.close()
    return inserted, updated


def print_summary(db_path: str | Path) -> None:
    conn = open_master_db(db_path)
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    total = conn.execute("SELECT COUNT(*) FROM verified_emails").fetchone()[0]
    active = conn.execute(
        "SELECT COUNT(*) FROM verified_emails WHERE expires_at > ?", (now,)
    ).fetchone()[0]
    by_status = conn.execute(
        "SELECT canonical_status, COUNT(*) n"
        " FROM verified_emails GROUP BY canonical_status ORDER BY n DESC"
    ).fetchall()
    by_run = conn.execute(
        "SELECT run_name, COUNT(*) n"
        " FROM verified_emails GROUP BY run_name ORDER BY verified_at DESC LIMIT 10"
    ).fetchall()
    conn.close()

    print(f"Master DB : {db_path}")
    print(f"  Total   : {total:,}")
    print(f"  Active  : {active:,}  (not yet expired)")
    print(f"  Expired : {total - active:,}")
    print()
    print("  By status:")
    for r in by_status:
        print(f"    {r['canonical_status']:<15} {r['n']:>8,}")
    print()
    print("  By run (most recent 10):")
    for r in by_run:
        print(f"    {r['run_name']:<40} {r['n']:>8,}")


def flush_from_pipeline_db(
    master_db_path: str | Path,
    pipeline_db_path: str | Path,
    flush_every: int = 500,
) -> tuple[int, int]:
    """Read newly terminal records from pipeline.db and upsert into master.db.

    Uses a rowid watermark so repeated calls only process new records.
    Returns (inserted, updated). Returns (0, 0) if fewer than flush_every
    new terminal records have appeared since the last flush.
    """
    _TERMINAL = ("VALIDATED", "VALIDATION_FAILED", "DISCOVERY_FAILED", "COST_SKIPPED")
    source_key = str(pipeline_db_path)

    master = open_master_db(master_db_path)
    row = master.execute(
        "SELECT last_rowid FROM ingest_checkpoints WHERE source_db = ?", (source_key,)
    ).fetchone()
    last_rowid = row["last_rowid"] if row else 0

    pipeline = sqlite3.connect(str(pipeline_db_path))
    pipeline.row_factory = sqlite3.Row

    placeholders = ",".join("?" * len(_TERMINAL))
    pending_count = pipeline.execute(
        f"SELECT COUNT(*) FROM records WHERE rowid > ? AND record_state IN ({placeholders})",
        (last_rowid, *_TERMINAL),
    ).fetchone()[0]

    if pending_count < flush_every:
        pipeline.close()
        master.close()
        return 0, 0

    rows = pipeline.execute(
        f"SELECT rowid, unique_id, business_name, agent_name, state,"
        f"       candidate_email, canonical_status, canonical_source,"
        f"       confidence_score, confidence_tier"
        f"  FROM records"
        f" WHERE rowid > ? AND record_state IN ({placeholders})"
        f" ORDER BY rowid",
        (last_rowid, *_TERMINAL),
    ).fetchall()
    pipeline.close()

    now = datetime.datetime.now(datetime.timezone.utc)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    inserted = updated = 0
    new_last_rowid = last_rowid

    for r in rows:
        email = (r["candidate_email"] or "").strip()
        unique_id = (r["unique_id"] or "").strip()
        if not unique_id or not email:
            new_last_rowid = max(new_last_rowid, r["rowid"])
            continue

        status = (r["canonical_status"] or "unknown").strip()
        existing = master.execute(
            "SELECT 1 FROM verified_emails WHERE unique_id = ? AND email = ?",
            (unique_id, email),
        ).fetchone()

        master.execute(
            """
            INSERT INTO verified_emails
                (unique_id, email, business_name, agent_name, state,
                 canonical_status, canonical_source,
                 confidence_score, confidence_tier,
                 run_name, verified_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (unique_id, email) DO UPDATE SET
                canonical_status = excluded.canonical_status,
                canonical_source = excluded.canonical_source,
                confidence_score = excluded.confidence_score,
                confidence_tier  = excluded.confidence_tier,
                run_name         = excluded.run_name,
                verified_at      = excluded.verified_at,
                expires_at       = excluded.expires_at
            """,
            (
                unique_id, email,
                (r["business_name"] or "").strip(),
                (r["agent_name"] or "").strip(),
                (r["state"] or "").strip(),
                status,
                (r["canonical_source"] or "").strip(),
                _safe_int(r["confidence_score"]),
                (r["confidence_tier"] or "").strip(),
                source_key,
                now_str,
                _expires_at(status, now),
            ),
        )
        if existing:
            updated += 1
        else:
            inserted += 1
        new_last_rowid = max(new_last_rowid, r["rowid"])

    master.execute(
        """
        INSERT INTO ingest_checkpoints (source_db, last_rowid, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT (source_db) DO UPDATE SET
            last_rowid = excluded.last_rowid,
            updated_at = excluded.updated_at
        """,
        (source_key, new_last_rowid, now_str),
    )
    master.commit()
    master.close()
    return inserted, updated


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Ingest a completed pipeline run into the master verification DB"
    )
    parser.add_argument("--run-dir", required=True, help="Path to run output directory")
    parser.add_argument("--db", default="master.db", help="Master DB path (default: master.db)")
    parser.add_argument("--run-name", default=None, help="Run label (defaults to directory name)")
    parser.add_argument("--summary", action="store_true", help="Print DB summary after ingest")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    try:
        inserted, updated = ingest_run(args.db, args.run_dir, args.run_name)
        logger.info("Ingested %s — %d new, %d updated", args.run_dir, inserted, updated)
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    if args.summary:
        print()
        print_summary(args.db)


if __name__ == "__main__":
    main()
