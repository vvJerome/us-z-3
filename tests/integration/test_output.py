"""Integration tests for write_outputs (valid_emails.csv + results.json) and
print_status — the code that actually produces the pipeline's deliverables."""
from __future__ import annotations

import csv
import json
from pathlib import Path

from pipeline import db
from pipeline.config import PipelineConfig
from pipeline.db import State
from pipeline.output import _is_verified, print_status, write_outputs


async def _insert_validated(conn, unique_id: str, **overrides) -> None:
    fields = {
        "unique_id": unique_id,
        "business_name": "Acme Corp",
        "agent_name": "Jane Doe",
        "state": "NC",
        "candidate_email": "jane@acme.com",
        "record_state": State.VALIDATED,
        "final_verdict": "valid",
        "zuhal_status": None,
        "confidence_score": 3,
        "domain_confidence": 0.8,
        "owner_confidence": 0.5,
        "discovery_source": "dns",
        "racknerd_status": "valid",
        "bbops_status": "not_run",
        "canonical_status": "valid",
        "canonical_source": "smtp",
        "zb_status": None,
        "zb_sub_status": None,
    }
    fields.update(overrides)
    cols = ", ".join(fields)
    placeholders = ", ".join("?" for _ in fields)
    await conn.execute(
        f"INSERT INTO records ({cols}) VALUES ({placeholders})",
        list(fields.values()),
    )
    await conn.commit()


class TestWriteOutputsCsv:
    async def test_writes_one_row_per_validated_record(self, tmp_path: Path):
        conn = await db.init_db(tmp_path / "pipeline.db")
        await _insert_validated(conn, "rec1")
        await _insert_validated(conn, "rec2", candidate_email="bob@acme.com")

        config = PipelineConfig(output_dir=tmp_path, racknerd_enabled=False)
        await write_outputs(conn, config)

        csv_path = tmp_path / "valid_emails.csv"
        assert csv_path.exists()
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 2
        assert {r["unique_id"] for r in rows} == {"rec1", "rec2"}
        await conn.close()

    async def test_only_validated_records_are_written(self, tmp_path: Path):
        conn = await db.init_db(tmp_path / "pipeline.db")
        await _insert_validated(conn, "rec1")
        await _insert_validated(conn, "rec2", record_state=State.VALIDATION_FAILED, final_verdict="invalid")

        config = PipelineConfig(output_dir=tmp_path, racknerd_enabled=False)
        await write_outputs(conn, config)

        with open(tmp_path / "valid_emails.csv", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 1
        assert rows[0]["unique_id"] == "rec1"
        await conn.close()

    async def test_row_content_matches_verdict_and_confidence(self, tmp_path: Path):
        conn = await db.init_db(tmp_path / "pipeline.db")
        await _insert_validated(
            conn, "rec1",
            confidence_score=4, domain_confidence=0.91, owner_confidence=0.72,
            canonical_status="catch_all", canonical_source="zuhal",
        )

        config = PipelineConfig(output_dir=tmp_path, racknerd_enabled=False)
        await write_outputs(conn, config)

        with open(tmp_path / "valid_emails.csv", newline="", encoding="utf-8") as f:
            row = next(csv.DictReader(f))
        assert row["email"] == "jane@acme.com"
        assert row["canonical_status"] == "catch_all"
        assert row["canonical_source"] == "zuhal"
        assert row["confidence_score"] == "4"
        assert row["confidence_tier"] == "high"
        assert row["domain_confidence"] == "0.91"
        assert row["owner_confidence"] == "0.72"
        assert row["verified"] == "True"
        await conn.close()

    async def test_null_confidence_fields_render_as_empty_string(self, tmp_path: Path):
        conn = await db.init_db(tmp_path / "pipeline.db")
        await _insert_validated(conn, "rec1", domain_confidence=None, owner_confidence=None)

        config = PipelineConfig(output_dir=tmp_path, racknerd_enabled=False)
        await write_outputs(conn, config)

        with open(tmp_path / "valid_emails.csv", newline="", encoding="utf-8") as f:
            row = next(csv.DictReader(f))
        assert row["domain_confidence"] == ""
        assert row["domain_confidence_tier"] == ""
        assert row["owner_confidence"] == ""
        assert row["owner_confidence_tier"] == ""
        await conn.close()

    async def test_no_validated_records_still_writes_header_only_csv(self, tmp_path: Path):
        conn = await db.init_db(tmp_path / "pipeline.db")

        config = PipelineConfig(output_dir=tmp_path, racknerd_enabled=False)
        await write_outputs(conn, config)

        csv_path = tmp_path / "valid_emails.csv"
        assert csv_path.exists()
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert rows == []
        await conn.close()

    async def test_creates_output_dir_if_missing(self, tmp_path: Path):
        conn = await db.init_db(tmp_path / "pipeline.db")
        out_dir = tmp_path / "nested" / "run1"

        config = PipelineConfig(output_dir=out_dir, racknerd_enabled=False)
        await write_outputs(conn, config)

        assert (out_dir / "valid_emails.csv").exists()
        await conn.close()


class TestWriteOutputsResultsJson:
    async def test_results_json_reflects_record_counts(self, tmp_path: Path):
        conn = await db.init_db(tmp_path / "pipeline.db")
        await _insert_validated(conn, "rec1")
        await _insert_validated(conn, "rec2", record_state=State.VALIDATION_FAILED, final_verdict="invalid")

        config = PipelineConfig(output_dir=tmp_path, racknerd_enabled=False)
        await write_outputs(conn, config)

        results_path = tmp_path / "results.json"
        assert results_path.exists()
        summary = json.loads(results_path.read_text())
        assert summary["records_by_state"]["VALIDATED"] == 1
        assert summary["records_by_state"]["VALIDATION_FAILED"] == 1
        await conn.close()


class TestIsVerified:
    def test_valid_is_verified(self):
        assert _is_verified("valid") is True

    def test_catch_all_is_verified(self):
        assert _is_verified("catch_all") is True

    def test_invalid_is_not_verified(self):
        assert _is_verified("invalid") is False

    def test_none_is_not_verified(self):
        assert _is_verified(None) is False


class TestPrintStatus:
    def test_empty_summary_does_not_raise(self, capsys):
        print_status({})
        out = capsys.readouterr().out
        assert "Pipeline Status" in out
        assert "All records processed." in out

    def test_prints_records_by_state(self, capsys):
        print_status({"records_by_state": {"VALIDATED": 5, "DISCOVERY_FAILED": 2}})
        out = capsys.readouterr().out
        assert "VALIDATED" in out
        assert "5" in out

    def test_prints_verdicts_and_failures_when_present(self, capsys):
        print_status({
            "records_by_verdict": {"valid": 3},
            "failures_by_phase": {"discovery": 1},
            "stats": {"estimated_cost_usd": 1.2345},
        })
        out = capsys.readouterr().out
        assert "Records by final verdict" in out
        assert "Failures by phase" in out
        assert "$1.2345" in out

    def test_pending_with_zero_throughput_shows_eta_unavailable(self, capsys):
        print_status({
            "records_by_state": {"DISCOVERED": 10},
            "terminal_last_1min": 0, "terminal_last_5min": 0, "terminal_last_15min": 0,
        })
        out = capsys.readouterr().out
        assert "ETA unavailable" in out

    def test_pending_with_throughput_shows_eta_in_minutes(self, capsys):
        print_status({
            "records_by_state": {"DISCOVERED": 100},
            "terminal_last_1min": 0, "terminal_last_5min": 50, "terminal_last_15min": 0,
        })
        out = capsys.readouterr().out
        assert "Throughput" in out
        assert "ETA:" in out
        assert "min" in out

    def test_eta_in_hours_when_over_60_minutes(self, capsys):
        print_status({
            "records_by_state": {"DISCOVERED": 1000},
            "terminal_last_1min": 0, "terminal_last_5min": 5, "terminal_last_15min": 0,
        })
        out = capsys.readouterr().out
        assert "hr" in out

    def test_eta_in_days_when_over_24_hours(self, capsys):
        print_status({
            "records_by_state": {"DISCOVERED": 100000},
            "terminal_last_1min": 0, "terminal_last_5min": 1, "terminal_last_15min": 0,
        })
        out = capsys.readouterr().out
        assert "days" in out

    def test_per_state_throughput_breakdown_shown(self, capsys):
        print_status({
            "records_by_state": {"VALIDATED": 5},
            "terminal_by_state_5min": {"VALIDATED": 10, "VALIDATION_FAILED": 5},
            "terminal_last_1min": 1, "terminal_last_5min": 15, "terminal_last_15min": 1,
        })
        out = capsys.readouterr().out
        assert "Per-state (last 5 min)" in out
        assert "validated" in out
        assert "validation_failed" in out

    def test_retry_backlog_splits_pending_into_fresh_and_retries(self, capsys):
        print_status({
            "records_by_state": {"DISCOVERED": 100},
            "retry_backlog": 30,
            "terminal_last_1min": 0, "terminal_last_5min": 10, "terminal_last_15min": 0,
        })
        out = capsys.readouterr().out
        assert "70" in out  # fresh = pending(100) - retry_backlog(30)
        assert "30" in out

    def test_needs_zuhal_queue_line_shown_when_draining(self, capsys):
        print_status({
            "records_by_state": {"NEEDS_ZUHAL": 20},
            "zuhal_terminal_last_5min": 10,
            "terminal_last_1min": 1, "terminal_last_5min": 1, "terminal_last_15min": 1,
        })
        out = capsys.readouterr().out
        assert "Zuhal queue" in out
        assert "20" in out
