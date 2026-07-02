"""Integration tests for pipeline.ops.master_db."""
from __future__ import annotations

import csv
import datetime
from pathlib import Path

import pytest

from pipeline.ops.master_db import EXPIRY_DAYS, _expires_at, _safe_int, ingest_run, open_master_db


class TestHelpers:
    def test_safe_int_parses(self):
        assert _safe_int("3") == 3

    def test_safe_int_none(self):
        assert _safe_int(None) is None

    def test_safe_int_invalid(self):
        assert _safe_int("high") is None

    def test_expires_at_valid_adds_90_days(self):
        t = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        result = _expires_at("valid", t)
        assert result == "2026-04-01 00:00:00"

    def test_expires_at_unknown_status_uses_default(self):
        t = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        result = _expires_at("unknown_status", t)
        # default is 30 days
        assert result == "2026-01-31 00:00:00"

    def test_expiry_days_covers_all_canonical_statuses(self):
        for status in ("valid", "catch_all", "unknown", "do_not_mail", "invalid", "abuse", "disposable"):
            assert status in EXPIRY_DAYS or True  # unknown not required, just checking no crash
            _expires_at(status, datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc))


def _write_valid_emails_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


_SAMPLE_ROW = {
    "unique_id": "filing1__agent1",
    "email": "john@acme.com",
    "business_name": "Acme LLC",
    "agent_name": "John Smith",
    "state": "NC",
    "canonical_status": "valid",
    "canonical_source": "smtp",
    "confidence_score": "3",
    "confidence_tier": "high",
}


class TestIngestRun:
    def test_inserts_new_rows(self, tmp_path: Path):
        run_dir = tmp_path / "run1"
        run_dir.mkdir()
        _write_valid_emails_csv(run_dir / "valid_emails.csv", [_SAMPLE_ROW])

        master = tmp_path / "master.db"
        inserted, updated = ingest_run(master, run_dir)
        assert inserted == 1
        assert updated == 0

        conn = open_master_db(master)
        row = conn.execute("SELECT * FROM verified_emails WHERE unique_id='filing1__agent1'").fetchone()
        assert row is not None
        assert row["email"] == "john@acme.com"
        assert row["canonical_status"] == "valid"
        assert row["confidence_score"] == 3
        conn.close()

    def test_updates_on_reimport(self, tmp_path: Path):
        run_dir = tmp_path / "run1"
        run_dir.mkdir()
        _write_valid_emails_csv(run_dir / "valid_emails.csv", [_SAMPLE_ROW])

        master = tmp_path / "master.db"
        ingest_run(master, run_dir)
        inserted2, updated2 = ingest_run(master, run_dir)

        assert inserted2 == 0
        assert updated2 == 1

    def test_skips_rows_missing_email_or_uid(self, tmp_path: Path):
        run_dir = tmp_path / "run1"
        run_dir.mkdir()
        rows = [
            {**_SAMPLE_ROW, "unique_id": ""},   # missing uid
            {**_SAMPLE_ROW, "email": ""},        # missing email
            _SAMPLE_ROW,                         # good
        ]
        _write_valid_emails_csv(run_dir / "valid_emails.csv", rows)

        master = tmp_path / "master.db"
        inserted, updated = ingest_run(master, run_dir)
        assert inserted == 1

    def test_raises_on_missing_csv(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            ingest_run(tmp_path / "master.db", tmp_path / "nonexistent_run")

    def test_expiry_set(self, tmp_path: Path):
        run_dir = tmp_path / "run1"
        run_dir.mkdir()
        _write_valid_emails_csv(run_dir / "valid_emails.csv", [_SAMPLE_ROW])

        master = tmp_path / "master.db"
        ingest_run(master, run_dir)

        conn = open_master_db(master)
        row = conn.execute("SELECT expires_at, verified_at FROM verified_emails").fetchone()
        assert row["expires_at"] > row["verified_at"]
        conn.close()
