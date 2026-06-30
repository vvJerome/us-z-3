"""Integration tests for pipeline.ops.ingest_zerobounce."""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from pipeline import db
from pipeline.ops.ingest_zerobounce import _feed_pattern_stats, _get, ingest


class TestGet:
    def test_returns_first_matching_key(self):
        assert _get({"zb_status": "valid"}, "zb_status", "status") == "valid"

    def test_falls_through_empty_string(self):
        assert _get({"zb_status": ""}, "zb_status", "status") == ""

    def test_falls_through_missing_key(self):
        assert _get({}, "zb_status") == ""

    def test_strips_whitespace(self):
        assert _get({"zb_status": "  valid  "}, "zb_status") == "valid"

    def test_multiple_keys_picks_first_non_empty(self):
        assert _get({"a": "", "b": "catch_all"}, "a", "b") == "catch_all"


def _make_sync_db(tmp_path: Path) -> tuple[Path, sqlite3.Connection]:
    """Create a run pipeline.db via the schema SQL and return both path and connection."""
    from pipeline.db.schema import SCHEMA_SQL
    db_path = tmp_path / "pipeline.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return db_path, conn


def _insert_validated(conn: sqlite3.Connection, uid: str, email: str, mx: str) -> None:
    conn.execute(
        """INSERT INTO records (unique_id, business_name, agent_name, state,
               candidate_email, mx_provider, record_state, dispatch_attempts)
           VALUES (?,?,?,?,?,?,?,?)""",
        (uid, "Acme LLC", "John Smith", "NC", email, mx, "VALIDATED", 1),
    )
    conn.commit()


class TestFeedPatternStats:
    def test_valid_records_pattern(self, tmp_path: Path):
        db_path, conn = _make_sync_db(tmp_path)
        _insert_validated(conn, "uid1", "john.smith@acme.com", "google.com")
        recorded = _feed_pattern_stats(conn, "uid1", "valid")
        assert recorded is True
        row = conn.execute(
            "SELECT success_count, total_count FROM pattern_stats WHERE mx_provider='google.com'"
        ).fetchone()
        assert row is not None
        assert row["success_count"] == 1 and row["total_count"] == 1
        conn.close()

    def test_invalid_records_miss(self, tmp_path: Path):
        db_path, conn = _make_sync_db(tmp_path)
        _insert_validated(conn, "uid2", "john.smith@acme.com", "google.com")
        recorded = _feed_pattern_stats(conn, "uid2", "invalid")
        assert recorded is True
        row = conn.execute(
            "SELECT success_count, total_count FROM pattern_stats WHERE mx_provider='google.com'"
        ).fetchone()
        assert row["success_count"] == 0 and row["total_count"] == 1
        conn.close()

    def test_catch_all_skipped(self, tmp_path: Path):
        db_path, conn = _make_sync_db(tmp_path)
        _insert_validated(conn, "uid3", "john.smith@acme.com", "google.com")
        recorded = _feed_pattern_stats(conn, "uid3", "catch_all")
        assert recorded is False
        conn.close()

    def test_missing_record_returns_false(self, tmp_path: Path):
        db_path, conn = _make_sync_db(tmp_path)
        recorded = _feed_pattern_stats(conn, "no-such-id", "valid")
        assert recorded is False
        conn.close()


class TestIngest:
    def _write_zb_csv(self, path: Path, rows: list[dict]) -> None:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    def test_matched_count(self, tmp_path: Path):
        db_path, conn = _make_sync_db(tmp_path)
        _insert_validated(conn, "uid-a", "a@acme.com", "google.com")
        _insert_validated(conn, "uid-b", "b@acme.com", "google.com")
        conn.close()

        zb_csv = tmp_path / "results.csv"
        self._write_zb_csv(zb_csv, [
            {"unique_id": "uid-a", "zb_status": "valid", "zb_sub_status": ""},
            {"unique_id": "uid-b", "zb_status": "invalid", "zb_sub_status": "mailbox_not_found"},
            {"unique_id": "uid-missing", "zb_status": "valid", "zb_sub_status": ""},
        ])

        matched, skipped, learned = ingest(db_path, zb_csv)
        assert matched == 2
        assert skipped == 1  # uid-missing not in DB

    def test_canonical_status_set(self, tmp_path: Path):
        db_path, conn = _make_sync_db(tmp_path)
        _insert_validated(conn, "uid-c", "c@acme.com", "google.com")
        conn.close()

        zb_csv = tmp_path / "results.csv"
        self._write_zb_csv(zb_csv, [
            {"unique_id": "uid-c", "zb_status": "valid", "zb_sub_status": ""},
        ])
        ingest(db_path, zb_csv)

        conn2 = sqlite3.connect(str(db_path))
        row = conn2.execute(
            "SELECT canonical_status, canonical_source FROM records WHERE unique_id='uid-c'"
        ).fetchone()
        assert row[0] == "valid"
        assert row[1] == "zerobounce"
        conn2.close()

    def test_idempotent_pattern_stats_not_safe(self, tmp_path: Path):
        """Ingest twice feeds pattern_stats twice — caller's responsibility to ingest once."""
        db_path, conn = _make_sync_db(tmp_path)
        # john.smith@acme.com matches the first.last template for agent "John Smith"
        conn.execute(
            """INSERT INTO records (unique_id, business_name, agent_name, state,
                   candidate_email, mx_provider, record_state, dispatch_attempts)
               VALUES (?,?,?,?,?,?,?,?)""",
            ("uid-d", "Acme LLC", "John Smith", "NC", "john.smith@acme.com", "google.com", "VALIDATED", 1),
        )
        conn.commit()
        conn.close()

        zb_csv = tmp_path / "results.csv"
        self._write_zb_csv(zb_csv, [
            {"unique_id": "uid-d", "zb_status": "valid", "zb_sub_status": ""},
        ])
        _, _, learned1 = ingest(db_path, zb_csv)
        _, _, learned2 = ingest(db_path, zb_csv)
        assert learned1 == 1
        assert learned2 == 1  # re-feeds on second call — documented non-idempotence
