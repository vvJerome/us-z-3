"""Integration: canonical verdict is written by update_record_dual and overridden by ZB ingest."""
from __future__ import annotations

import csv
from pathlib import Path

import aiosqlite
import pytest

from pipeline import db
from pipeline.db import State
from pipeline.ops.ingest_zerobounce import ingest

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def test_db(tmp_path: Path) -> aiosqlite.Connection:
    conn = await db.init_db(tmp_path / "test.db")
    yield conn
    await conn.close()


async def _seed(conn, uid="IL-000123", email="john@acme.com"):
    await conn.execute(
        "INSERT INTO records (unique_id, candidate_email, candidate_domain, strategy, "
        "record_state) VALUES (?, ?, ?, ?, ?)",
        (uid, email, "acme.com", "with", State.DISCOVERED),
    )
    await conn.commit()


async def _canonical(conn, uid):
    async with conn.execute(
        "SELECT canonical_status, canonical_source, reconciliation_path, zb_status "
        "FROM records WHERE unique_id = ?", (uid,)
    ) as cur:
        return await cur.fetchone()


class TestCanonicalOnVerdictWrite:
    async def test_schema_has_canonical_columns(self, test_db):
        async with test_db.execute("PRAGMA table_info(records)") as cur:
            cols = {r[1] for r in await cur.fetchall()}
        assert {"canonical_status", "canonical_source", "zb_status", "reconciliation_path"} <= cols

    async def test_smtp_valid_sets_canonical_smtp(self, test_db):
        await _seed(test_db)
        await db.update_record_dual(
            test_db, "IL-000123", State.VALIDATED,
            racknerd_status="valid", racknerd_message="", racknerd_verified_at="",
            bbops_status="not_run", bbops_message="", bbops_verified_at="",
            final_verdict="valid", candidate_email="john@acme.com",
        )
        row = await _canonical(test_db, "IL-000123")
        assert row["canonical_status"] == "valid"
        assert row["canonical_source"] == "smtp"
        assert row["reconciliation_path"] == "dual_valid"

    async def test_ms_valid_sets_ms_probe_source(self, test_db):
        await _seed(test_db)
        await db.update_record_dual(
            test_db, "IL-000123", State.VALIDATED,
            racknerd_status="ms_valid", racknerd_message="", racknerd_verified_at=None,
            bbops_status="not_run", bbops_message="", bbops_verified_at=None,
            final_verdict="valid", candidate_email="john@acme.com",
        )
        row = await _canonical(test_db, "IL-000123")
        assert row["canonical_source"] == "ms_probe"
        assert row["reconciliation_path"] == "ms_valid"

    async def test_zuhal_accept_all_normalizes_to_catch_all(self, test_db):
        await _seed(test_db)
        await db.update_record_dual(
            test_db, "IL-000123", State.VALIDATED,
            racknerd_status="invalid", racknerd_message="", racknerd_verified_at="",
            bbops_status="invalid", bbops_message="", bbops_verified_at="",
            final_verdict="catch_all", candidate_email="john@acme.com",
            zuhal_status_override="accept-all",
        )
        row = await _canonical(test_db, "IL-000123")
        assert row["canonical_status"] == "catch_all"
        assert row["canonical_source"] == "zuhal"


class TestZeroBounceIngest:
    async def test_ingest_overrides_canonical_as_ground_truth(self, test_db, tmp_path):
        await _seed(test_db, uid="IL-000123", email="john@acme.com")
        # SMTP first called it valid...
        await db.update_record_dual(
            test_db, "IL-000123", State.VALIDATED,
            racknerd_status="valid", racknerd_message="", racknerd_verified_at="",
            bbops_status="not_run", bbops_message="", bbops_verified_at="",
            final_verdict="valid", candidate_email="john@acme.com",
        )
        await test_db.commit()
        # ...ZeroBounce (ground truth) says do_not_mail.
        zb_csv = tmp_path / "zerobounced.csv"
        with zb_csv.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["email", "unique_id", "zb_status", "zb_sub_status", "zb_processed_at"])
            w.writerow(["john@acme.com", "IL-000123", "do_not_mail", "role_based", "2026-06-18"])

        matched, skipped, learned = ingest(tmp_path / "test.db", zb_csv)

        row = await _canonical(test_db, "IL-000123")
        assert matched == 1
        assert learned == 0  # do_not_mail is inconclusive for pattern learning
        assert row["zb_status"] == "do_not_mail"
        assert row["canonical_status"] == "do_not_mail"   # ZB overrides the SMTP 'valid'
        assert row["canonical_source"] == "zerobounce"

    async def test_ingest_skips_unmatched_unique_id(self, test_db, tmp_path):
        await _seed(test_db, uid="IL-000123")
        zb_csv = tmp_path / "zb.csv"
        with zb_csv.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["email", "unique_id", "zb_status"])
            w.writerow(["x@y.com", "IL-999999", "valid"])
        matched, skipped, learned = ingest(tmp_path / "test.db", zb_csv)
        assert matched == 0 and skipped == 1 and learned == 0

    async def test_ingest_feeds_valid_verdict_into_pattern_stats(self, test_db, tmp_path):
        # Record with a parseable name + mx_provider so a template can be derived.
        await test_db.execute(
            "INSERT INTO records (unique_id, candidate_email, candidate_domain, agent_name, "
            "mx_provider, strategy, record_state) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("IL-1", "john.smith@acme.com", "acme.com", "John Smith", "google.com", "with", State.VALIDATED),
        )
        await test_db.commit()
        zb_csv = tmp_path / "zb.csv"
        with zb_csv.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["email", "unique_id", "zb_status"])
            w.writerow(["john.smith@acme.com", "IL-1", "valid"])

        matched, skipped, learned = ingest(tmp_path / "test.db", zb_csv)
        assert matched == 1 and learned == 1

        async with test_db.execute(
            "SELECT template, success_count, total_count FROM pattern_stats WHERE mx_provider = ?",
            ("google.com",),
        ) as cur:
            row = await cur.fetchone()
        assert row["template"] == "first.last"
        assert row["success_count"] == 1 and row["total_count"] == 1
