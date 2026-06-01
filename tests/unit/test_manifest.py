from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from pipeline import manifest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn(tmp_path: Path) -> sqlite3.Connection:
    return manifest.connect(tmp_path / "test.db")


@pytest.fixture
def zuhal_csv(tmp_path: Path) -> Path:
    path = tmp_path / "alpha_part2_zuhaled.csv"
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["unique_id", "candidate_email", "zuhal_verdict"])
        w.writeheader()
        w.writerow({"unique_id": "IL-001", "candidate_email": "a@example.com", "zuhal_verdict": "valid"})
        w.writerow({"unique_id": "IL-002", "candidate_email": "b@example.com", "zuhal_verdict": "catch_all"})
        w.writerow({"unique_id": "IL-003", "candidate_email": "c@example.com", "zuhal_verdict": "invalid"})
        w.writerow({"unique_id": "", "candidate_email": "", "zuhal_verdict": "valid"})  # blank email → skip
    return path


@pytest.fixture
def zb_csv(tmp_path: Path) -> Path:
    path = tmp_path / "alpha_part2_zerobounced.csv"
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["unique_id", "email", "zb_status", "zb_sub_status"])
        w.writeheader()
        w.writerow({"unique_id": "IL-001", "email": "a@example.com", "zb_status": "valid", "zb_sub_status": ""})
        w.writerow({"unique_id": "IL-004", "email": "d@example.com", "zb_status": "catch-all", "zb_sub_status": ""})
        w.writerow({"unique_id": "", "email": "", "zb_status": "valid", "zb_sub_status": ""})  # blank → skip
        w.writerow({"unique_id": "IL-005", "email": "e@example.com", "zb_status": "", "zb_sub_status": ""})  # blank status → skip
    return path


@pytest.fixture
def passoff_csv(tmp_path: Path) -> Path:
    path = tmp_path / "alpha_combined.csv"
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["email", "zb_status"])
        w.writeheader()
        w.writerow({"email": "a@example.com", "zb_status": "valid"})
        w.writerow({"email": "b@example.com", "zb_status": "catch-all"})
    return path


# ---------------------------------------------------------------------------
# connect + schema
# ---------------------------------------------------------------------------

def test_connect_creates_tables(tmp_path: Path) -> None:
    c = manifest.connect(tmp_path / "m.db")
    tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "emails" in tables
    assert "batches" in tables


def test_connect_idempotent(tmp_path: Path) -> None:
    p = tmp_path / "m.db"
    manifest.connect(p)
    manifest.connect(p)  # second call must not fail


# ---------------------------------------------------------------------------
# strip_state_prefix
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("uid,expected", [
    ("IL-001234", "001234"),
    ("NC-999", "999"),
    ("001234", "001234"),
    ("", ""),
    (None, ""),
])
def test_strip_state_prefix(uid, expected):
    assert manifest.strip_state_prefix(uid) == expected


# ---------------------------------------------------------------------------
# email_of / eid_of
# ---------------------------------------------------------------------------

def test_email_of_primary_keys():
    assert manifest.email_of({"email": "A@Example.COM"}) == "a@example.com"
    assert manifest.email_of({"Email": "B@X.com"}) == "b@x.com"
    assert manifest.email_of({"candidate_email": "C@X.com"}) == "c@x.com"
    assert manifest.email_of({"email_address": "D@X.com"}) == "d@x.com"


def test_email_of_missing():
    assert manifest.email_of({}) == ""
    assert manifest.email_of({"email": ""}) == ""
    assert manifest.email_of({"email": None}) == ""


def test_eid_of_strips_prefix():
    assert manifest.eid_of({"unique_id": "IL-00123"}) == "00123"


def test_eid_of_missing():
    assert manifest.eid_of({}) == ""


# ---------------------------------------------------------------------------
# normalize_zuhal_verdict
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("valid", "valid"),
    ("VALID", "valid"),
    ("catch_all", "catch_all"),
    ("catch-all", "catch_all"),
    ("accept-all", "catch_all"),
    ("unknown", "unknown"),
    ("no_result", "unknown"),
    ("disposable account", "invalid"),
    ("disposable", "invalid"),
    ("invalid", "invalid"),
    ("", ""),
])
def test_normalize_zuhal_verdict(raw, expected):
    assert manifest.normalize_zuhal_verdict(raw) == expected


# ---------------------------------------------------------------------------
# part_from_filename
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stem,expected", [
    ("alpha_part2_zuhaled", "part2"),
    ("sara_w_officer_zuhaled", "w_officer"),
    ("jerome_part4_unknown_for_zb", "part4"),
    ("no_part_here", ""),
])
def test_part_from_filename(tmp_path, stem, expected):
    p = tmp_path / f"{stem}.csv"
    p.touch()
    assert manifest.part_from_filename(p) == expected


# ---------------------------------------------------------------------------
# is_zuhal_results_file / is_zb_results_file
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name,expected", [
    ("alpha_part2_zuhaled.csv", True),
    ("alpha_part2_zuhaled_v2.csv", True),
    ("alpha_part2.zuhal.csv", True),   # stem is alpha_part2.zuhal → endswith(".zuhal")
    ("alpha_part2_zerobounced.csv", False),
    ("random.csv", False),
])
def test_is_zuhal_results_file(tmp_path, name, expected):
    p = tmp_path / name
    p.touch()
    assert manifest.is_zuhal_results_file(p) == expected


@pytest.mark.parametrize("name,expected", [
    ("alpha_part2_zerobounced.csv", True),
    ("alpha_unknown_for_zb.csv", True),
    ("alpha_valid_for_zb.csv", True),
    ("alpha_valid_catchall_for_zb.csv", True),
    ("alpha_zuhaled.csv", False),
    ("random.csv", False),
])
def test_is_zb_results_file(tmp_path, name, expected):
    p = tmp_path / name
    p.touch()
    assert manifest.is_zb_results_file(p) == expected


# ---------------------------------------------------------------------------
# mark_zuhaled / mark_zerobounced / mark_passed_off
# ---------------------------------------------------------------------------

def test_mark_zuhaled_inserts(conn):
    manifest.mark_zuhaled(conn, "a@x.com", "001", "alpha", "part2", "valid", "standalone_zuhal")
    row = conn.execute("SELECT * FROM emails WHERE email='a@x.com'").fetchone()
    assert row is not None
    d = dict(zip([c[0] for c in conn.execute("SELECT * FROM emails LIMIT 0").description or
                  conn.execute("PRAGMA table_info(emails)").fetchall()], row))
    assert row[0] == "a@x.com"


def test_mark_zuhaled_upsert(conn):
    manifest.mark_zuhaled(conn, "a@x.com", "001", "alpha", "part2", "unknown", "standalone_zuhal")
    manifest.mark_zuhaled(conn, "a@x.com", "001", "alpha", "part2", "valid", "standalone_zuhal")
    rows = conn.execute("SELECT zuhal_verdict FROM emails WHERE email='a@x.com'").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "valid"


def test_mark_zerobounced_inserts(conn):
    manifest.mark_zerobounced(conn, "b@x.com", "valid", "", "alpha", "part2", "002")
    row = conn.execute("SELECT zerobounced, zb_status FROM emails WHERE email='b@x.com'").fetchone()
    assert row[0] == 1
    assert row[1] == "valid"


def test_mark_zerobounced_upsert(conn):
    manifest.mark_zerobounced(conn, "b@x.com", "unknown", "")
    manifest.mark_zerobounced(conn, "b@x.com", "valid", "alias")
    row = conn.execute("SELECT zb_status, zb_sub_status FROM emails WHERE email='b@x.com'").fetchone()
    assert row[0] == "valid"
    assert row[1] == "alias"


def test_mark_passed_off(conn):
    manifest.mark_zuhaled(conn, "c@x.com", "", "jerome", "part4", "valid", "standalone_zuhal")
    assert not manifest.is_passed_off(conn, "c@x.com")
    manifest.mark_passed_off(conn, "c@x.com")
    assert manifest.is_passed_off(conn, "c@x.com")


def test_is_passed_off_missing_email(conn):
    assert not manifest.is_passed_off(conn, "nobody@x.com")


# ---------------------------------------------------------------------------
# seen_by_zb / seen_by_zuhal
# ---------------------------------------------------------------------------

def test_seen_by_zb(conn):
    manifest.mark_zerobounced(conn, "z1@x.com", "valid", "")
    manifest.mark_zerobounced(conn, "z2@x.com", "invalid", "")
    manifest.mark_zuhaled(conn, "z3@x.com", "", "alpha", "part2", "valid", "standalone_zuhal")
    seen = manifest.seen_by_zb(conn)
    assert "z1@x.com" in seen
    assert "z2@x.com" in seen
    assert "z3@x.com" not in seen


def test_seen_by_zuhal(conn):
    manifest.mark_zuhaled(conn, "z1@x.com", "", "alpha", "part2", "valid", "standalone_zuhal")
    manifest.mark_zerobounced(conn, "z2@x.com", "valid", "")
    seen = manifest.seen_by_zuhal(conn)
    assert "z1@x.com" in seen
    assert "z2@x.com" not in seen


# ---------------------------------------------------------------------------
# get_email
# ---------------------------------------------------------------------------

def test_get_email_returns_dict(conn):
    manifest.mark_zuhaled(conn, "a@x.com", "001", "sara", "w_officer", "valid", "standalone_zuhal")
    result = manifest.get_email(conn, "a@x.com")
    assert result is not None
    assert result["email"] == "a@x.com"
    assert result["operator"] == "sara"
    assert result["zuhaled"] == 1


def test_get_email_missing(conn):
    assert manifest.get_email(conn, "nobody@x.com") is None


# ---------------------------------------------------------------------------
# batch lifecycle
# ---------------------------------------------------------------------------

def test_batch_lifecycle(conn):
    manifest.start_batch(conn, "batch-1", "alpha", "part2", "zb", "/tmp/input.csv")
    row = conn.execute("SELECT status FROM batches WHERE batch_id='batch-1'").fetchone()
    assert row[0] == "uploading"

    manifest.record_file_id(conn, "batch-1", "file-abc", 500)
    row = conn.execute("SELECT status, zb_file_id, row_count FROM batches WHERE batch_id='batch-1'").fetchone()
    assert row[0] == "polling"
    assert row[1] == "file-abc"
    assert row[2] == 500

    manifest.finish_batch(conn, "batch-1", row_count=498)
    row = conn.execute("SELECT status, row_count FROM batches WHERE batch_id='batch-1'").fetchone()
    assert row[0] == "complete"
    assert row[1] == 498


def test_batch_fail(conn):
    manifest.start_batch(conn, "batch-2", "jerome", "part4", "zb", "/tmp/x.csv")
    manifest.fail_batch(conn, "batch-2")
    row = conn.execute("SELECT status FROM batches WHERE batch_id='batch-2'").fetchone()
    assert row[0] == "failed"


def test_get_unfinished_batches(conn):
    manifest.start_batch(conn, "b1", "alpha", "part2", "zb", "/tmp/a.csv")
    manifest.record_file_id(conn, "b1", "f1", 100)
    manifest.start_batch(conn, "b2", "jerome", "part4", "zb", "/tmp/b.csv")
    manifest.finish_batch(conn, "b2")

    unfinished = manifest.get_unfinished_batches(conn)
    ids = [b["batch_id"] for b in unfinished]
    assert "b1" in ids
    assert "b2" not in ids


def test_start_batch_upsert(conn):
    manifest.start_batch(conn, "b1", "alpha", "part2", "zb", "/old.csv")
    manifest.start_batch(conn, "b1", "alpha", "part2", "zb", "/new.csv")
    row = conn.execute("SELECT input_path FROM batches WHERE batch_id='b1'").fetchone()
    assert row[0] == "/new.csv"


# ---------------------------------------------------------------------------
# counts
# ---------------------------------------------------------------------------

def test_counts_empty(conn):
    c = manifest.counts(conn)
    assert c == {"total": 0, "zuhaled": 0, "zerobounced": 0, "in_passoff": 0}


def test_counts_populated(conn):
    manifest.mark_zuhaled(conn, "a@x.com", "", "alpha", "part2", "valid", "standalone_zuhal")
    manifest.mark_zerobounced(conn, "b@x.com", "valid", "")
    manifest.mark_zuhaled(conn, "c@x.com", "", "alpha", "part2", "valid", "standalone_zuhal")
    manifest.mark_passed_off(conn, "c@x.com")
    c = manifest.counts(conn)
    assert c["total"] == 3
    assert c["zuhaled"] == 2
    assert c["zerobounced"] == 1
    assert c["in_passoff"] == 1


# ---------------------------------------------------------------------------
# ingest_zuhal_file
# ---------------------------------------------------------------------------

def test_ingest_zuhal_file(conn, zuhal_csv):
    n = manifest.ingest_zuhal_file(conn, zuhal_csv, "alpha")
    assert n == 3  # 4 rows but 1 has blank email
    rows = conn.execute("SELECT email, zuhal_verdict FROM emails ORDER BY email").fetchall()
    assert ("a@example.com", "valid") in rows
    assert ("b@example.com", "catch_all") in rows
    assert ("c@example.com", "invalid") in rows


def test_ingest_zuhal_file_idempotent(conn, zuhal_csv):
    manifest.ingest_zuhal_file(conn, zuhal_csv, "alpha")
    manifest.ingest_zuhal_file(conn, zuhal_csv, "alpha")
    count = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    assert count == 3


# ---------------------------------------------------------------------------
# ingest_zb_file
# ---------------------------------------------------------------------------

def test_ingest_zb_file(conn, zb_csv):
    n = manifest.ingest_zb_file(conn, zb_csv, "alpha")
    assert n == 2  # blank email and blank status rows skipped
    rows = conn.execute("SELECT email, zb_status FROM emails ORDER BY email").fetchall()
    assert ("a@example.com", "valid") in rows
    assert ("d@example.com", "catch-all") in rows


# ---------------------------------------------------------------------------
# ingest_passoff_file
# ---------------------------------------------------------------------------

def test_ingest_passoff_file(conn, passoff_csv):
    manifest.mark_zuhaled(conn, "a@example.com", "", "alpha", "part2", "valid", "standalone_zuhal")
    manifest.mark_zuhaled(conn, "b@example.com", "", "alpha", "part2", "catch_all", "standalone_zuhal")
    n = manifest.ingest_passoff_file(conn, passoff_csv, "alpha")
    assert n == 2
    assert manifest.is_passed_off(conn, "a@example.com")
    assert manifest.is_passed_off(conn, "b@example.com")


def test_ingest_passoff_file_skips_blank_email(conn, tmp_path):
    path = tmp_path / "combined.csv"
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["email"])
        w.writeheader()
        w.writerow({"email": ""})
        w.writerow({"email": "x@y.com"})
    manifest.mark_zuhaled(conn, "x@y.com", "", "alpha", "part2", "valid", "standalone_zuhal")
    n = manifest.ingest_passoff_file(conn, path, "alpha")
    assert n == 1


# ---------------------------------------------------------------------------
# OPERATORS constant
# ---------------------------------------------------------------------------

def test_operators_constant():
    assert "alpha" in manifest.OPERATORS
    assert "jerome" in manifest.OPERATORS
    assert "sara" in manifest.OPERATORS
