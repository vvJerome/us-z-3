"""Unit tests for pipeline.ops.requeue_zuhal_429_burns."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pipeline.ops.requeue_zuhal_429_burns import PREVIEW_SQL, main
from pipeline.db.schema import SCHEMA_SQL


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "pipeline.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    conn.close()
    return db_path


def _insert_burn(conn: sqlite3.Connection, uid: str, updated_at: str) -> None:
    conn.execute(
        """INSERT INTO records (unique_id, business_name, agent_name, state,
               record_state, zuhal_status, updated_at, dispatch_attempts)
           VALUES (?,?,?,?,?,?,?,?)""",
        (uid, "Acme", "Joe", "NC", "VALIDATION_FAILED", "error", updated_at, 3),
    )
    conn.commit()


class TestDryRun:
    def test_missing_db_exits_2(self, tmp_path: Path, capsys):
        rc = main(["--db", str(tmp_path / "nonexistent.db")])
        assert rc == 2

    def test_zero_matches_dry_run(self, tmp_path: Path, capsys):
        db_path = _make_db(tmp_path)
        rc = main(["--db", str(db_path)])
        assert rc == 0
        out = capsys.readouterr().out
        assert "dry-run" in out.lower()

    def test_shows_match_count(self, tmp_path: Path, capsys):
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        _insert_burn(conn, "uid-1", "2026-05-14 00:00:00")
        conn.close()

        rc = main(["--db", str(db_path)])
        assert rc == 0
        out = capsys.readouterr().out
        assert "1" in out


class TestApplyMode:
    def test_requeues_matching_rows(self, tmp_path: Path, capsys):
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        _insert_burn(conn, "uid-a", "2026-05-14 00:00:00")
        _insert_burn(conn, "uid-b", "2026-05-10 00:00:00")  # before cutoff
        conn.close()

        rc = main(["--db", str(db_path), "--apply"])
        assert rc == 0

        conn2 = sqlite3.connect(str(db_path))
        conn2.row_factory = sqlite3.Row
        row = conn2.execute("SELECT record_state FROM records WHERE unique_id='uid-a'").fetchone()
        assert row["record_state"] == "NEEDS_ZUHAL"
        row_b = conn2.execute("SELECT record_state FROM records WHERE unique_id='uid-b'").fetchone()
        assert row_b["record_state"] == "VALIDATION_FAILED"  # pre-cutoff, untouched
        conn2.close()

    def test_no_rows_to_requeue(self, tmp_path: Path, capsys):
        db_path = _make_db(tmp_path)
        rc = main(["--db", str(db_path), "--apply"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "nothing" in out.lower()
