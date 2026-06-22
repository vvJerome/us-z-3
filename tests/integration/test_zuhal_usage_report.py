"""Integration tests for the Zuhal usage/cost report (real SQLite, real CSVs)."""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from pipeline import db
from pipeline.ops.zuhal_usage_report import (
    bulk_csv_emails,
    bulk_csv_unique_ids,
    live_zuhal_breakdown,
    live_zuhal_ids,
    main,
)

pytestmark = pytest.mark.asyncio


async def _seed(db_path: Path) -> None:
    conn = await db.init_db(db_path)
    await conn.execute(
        "INSERT INTO records (unique_id, zuhal_status, candidate_email) VALUES (?, ?, ?)",
        ("r1", "valid", "x@y.com"),
    )
    await conn.execute(
        "INSERT INTO records (unique_id, zuhal_status) VALUES (?, ?)", ("r2", "dual_valid")
    )
    await conn.execute(
        "INSERT INTO records (unique_id, zuhal_status) VALUES (?, ?)", ("r3", "dual_catch_all")
    )
    await conn.execute(
        "INSERT INTO records (unique_id, zuhal_status) VALUES (?, ?)", ("r4", "circuit_open")
    )
    await conn.execute(
        "INSERT INTO records (unique_id, zuhal_status) VALUES (?, ?)", ("r5", None)
    )
    await conn.commit()
    await conn.close()


def _write_csv(path: Path, rows: list[tuple[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["unique_id", "email"])
        w.writerows(rows)


def _write_email_only_csv(path: Path, emails: list[str]) -> None:
    """Mirrors a Zuhal dashboard bulk-upload export: just an email column, no unique_id."""
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["email"])
        w.writerows([[e] for e in emails])


async def test_live_breakdown_excludes_no_call_placeholders(tmp_path: Path):
    db_path = tmp_path / "test.db"
    await _seed(db_path)

    conn = sqlite3.connect(db_path)
    breakdown = live_zuhal_breakdown(conn)
    ids = live_zuhal_ids(conn)
    conn.close()

    assert breakdown == {"valid": 1}
    assert ids == {"r1"}


async def test_bulk_csv_emails_dedupes_within_file(tmp_path: Path):
    csv_path = tmp_path / "needs_zuhal.csv"
    _write_csv(csv_path, [("a1", "x@y.com"), ("a2", "X@Y.com"), ("a3", "z@y.com")])

    assert bulk_csv_emails(csv_path) == {"x@y.com", "z@y.com"}
    assert bulk_csv_unique_ids(csv_path) == {"a1", "a2", "a3"}


async def test_main_flags_overlap_between_live_and_bulk(tmp_path: Path, capsys):
    db_path = tmp_path / "test.db"
    await _seed(db_path)

    bulk_path = tmp_path / "needs_zuhal_rescue.csv"
    _write_csv(bulk_path, [("r1", "x@y.com"), ("a2", "z@y.com")])

    rc = main(["--db", str(db_path), "--bulk-csv", str(bulk_path)])
    out = capsys.readouterr().out

    assert rc == 0
    assert "Live dispatcher Zuhal calls: 1" in out
    assert "2 unique emails uploaded" in out
    assert "billed twice" in out


async def test_main_flags_overlap_by_email_when_bulk_csv_has_no_unique_id(tmp_path: Path, capsys):
    """Dashboard bulk-upload exports carry no unique_id, only an email column —
    overlap must still be caught by matching emails directly."""
    db_path = tmp_path / "test.db"
    await _seed(db_path)

    bulk_path = tmp_path / "dashboard_export.csv"
    _write_email_only_csv(bulk_path, ["x@y.com", "other@example.com"])

    assert bulk_csv_unique_ids(bulk_path) == set()

    rc = main(["--db", str(db_path), "--bulk-csv", str(bulk_path)])
    out = capsys.readouterr().out

    assert rc == 0
    assert "billed twice" in out


async def test_main_no_overlap_warning_when_emails_dont_match(tmp_path: Path, capsys):
    db_path = tmp_path / "test.db"
    await _seed(db_path)

    bulk_path = tmp_path / "dashboard_export.csv"
    _write_email_only_csv(bulk_path, ["nomatch@example.com"])

    rc = main(["--db", str(db_path), "--bulk-csv", str(bulk_path)])
    out = capsys.readouterr().out

    assert rc == 0
    assert "billed twice" not in out
