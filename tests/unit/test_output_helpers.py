"""Unit tests for CSV output helper functions: _validation_method, _zuhal_verdict."""
from __future__ import annotations

import pytest

from pipeline.__main__ import _validation_method, _zuhal_verdict
from pipeline.constants import DNS_TLDS


# ── _validation_method ────────────────────────────────────────────────────────

class TestValidationMethod:
    def test_ms_probe_when_zuhal_status_is_ms_valid(self):
        assert _validation_method(None, None, "ms_valid") == "ms_probe"

    def test_ms_probe_takes_priority_over_any_smtp_status(self):
        assert _validation_method("valid", "valid", "ms_valid") == "ms_probe"

    def test_smtp_both_when_both_backends_valid(self):
        assert _validation_method("valid", "valid", "dual_valid") == "smtp_both"

    def test_smtp_both_when_both_backends_catch_all(self):
        assert _validation_method("catch_all", "catch_all", "dual_catch_all") == "smtp_both"

    def test_smtp_racknerd_when_only_racknerd_valid(self):
        assert _validation_method("valid", "invalid", "dual_invalid") == "smtp_racknerd"

    def test_smtp_racknerd_when_racknerd_catch_all_bbops_invalid(self):
        assert _validation_method("catch_all", "invalid", "dual_catch_all") == "smtp_racknerd"

    def test_smtp_bbops_when_only_bbops_valid(self):
        assert _validation_method("invalid", "valid", "dual_invalid") == "smtp_bbops"

    def test_smtp_bbops_when_bbops_catch_all_racknerd_invalid(self):
        assert _validation_method("invalid", "catch_all", "dual_catch_all") == "smtp_bbops"

    def test_zuhal_rescue_when_zuhal_status_valid(self):
        assert _validation_method("invalid", "invalid", "valid") == "zuhal_rescue"

    def test_zuhal_rescue_when_zuhal_status_catch_all(self):
        assert _validation_method("invalid", "invalid", "catch_all") == "zuhal_rescue"

    def test_zuhal_rescue_when_zuhal_status_accept_all(self):
        assert _validation_method("invalid", "invalid", "accept-all") == "zuhal_rescue"

    def test_unknown_when_no_status_set(self):
        assert _validation_method(None, None, None) == "unknown"

    def test_unknown_when_zuhal_status_invalid(self):
        assert _validation_method("invalid", "invalid", "invalid") == "unknown"

    def test_unknown_when_zuhal_status_error(self):
        assert _validation_method("invalid", "invalid", "error") == "unknown"


# ── _zuhal_verdict ────────────────────────────────────────────────────────────

class TestZuhalVerdict:
    def test_not_run_when_status_is_none(self):
        assert _zuhal_verdict(None) == "not_run"

    def test_not_run_when_status_is_ms_valid(self):
        assert _zuhal_verdict("ms_valid") == "not_run"

    def test_not_run_for_dual_valid(self):
        assert _zuhal_verdict("dual_valid") == "not_run"

    def test_not_run_for_dual_catch_all(self):
        assert _zuhal_verdict("dual_catch_all") == "not_run"

    def test_not_run_for_dual_invalid(self):
        assert _zuhal_verdict("dual_invalid") == "not_run"

    def test_passthrough_for_valid(self):
        assert _zuhal_verdict("valid") == "valid"

    def test_passthrough_for_invalid(self):
        assert _zuhal_verdict("invalid") == "invalid"

    def test_passthrough_for_accept_all(self):
        assert _zuhal_verdict("accept-all") == "accept-all"

    def test_passthrough_for_error(self):
        assert _zuhal_verdict("error") == "error"


# ── DNS_TLDS ─────────────────────────────────────────────────────────────────

class TestDnsTlds:
    def test_includes_standard_tlds(self):
        assert ".com" in DNS_TLDS
        assert ".net" in DNS_TLDS
        assert ".org" in DNS_TLDS

    def test_includes_us_for_government_and_nonprofit_entities(self):
        assert ".us" in DNS_TLDS

    def test_includes_info(self):
        assert ".info" in DNS_TLDS


# ── DB migration — no warnings on fresh installs ──────────────────────────────

class TestMigrationWarnings:
    async def test_fresh_db_emits_no_warning_for_zuhal_score_backfill(
        self, tmp_path, caplog
    ):
        """A fresh DB install must not emit a WARNING for the zuhal_score backfill
        migration — that column never existed on new installs and the failure is expected."""
        import logging
        from pipeline import db

        with caplog.at_level(logging.WARNING, logger="pipeline.db"):
            conn = await db.init_db(tmp_path / "fresh.db")
            await conn.close()

        warning_msgs = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert not any("zuhal_score" in m for m in warning_msgs)

    async def test_existing_column_migration_no_warning(self, tmp_path, caplog):
        """Running init_db twice (simulating an upgrade) must not emit spurious warnings
        for the duplicate-column-add attempts."""
        import logging
        from pipeline import db

        conn = await db.init_db(tmp_path / "existing.db")
        await conn.close()

        with caplog.at_level(logging.WARNING, logger="pipeline.db"):
            conn2 = await db.init_db(tmp_path / "existing.db")
            await conn2.close()

        warning_msgs = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert not any("migration statement skipped" in m.lower() for m in warning_msgs)
