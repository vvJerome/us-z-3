"""Unit tests for pipeline.ops.normalize_zuhaled pure functions."""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

from pipeline.ops.normalize_zuhaled import (
    CANONICAL_HEADER,
    is_legacy_zuhaled,
    normalize_file,
    normalize_verdict,
)


class TestNormalizeVerdict:
    def test_catch_all_aliases(self):
        assert normalize_verdict("catch-all") == "catch_all"
        assert normalize_verdict("accept-all") == "catch_all"
        assert normalize_verdict("catch_all") == "catch_all"

    def test_invalid_aliases(self):
        assert normalize_verdict("disposable account") == "invalid"
        assert normalize_verdict("disposable") == "invalid"

    def test_known_pass_through(self):
        assert normalize_verdict("valid") == "valid"
        assert normalize_verdict("invalid") == "invalid"
        assert normalize_verdict("unknown") == "unknown"

    def test_empty_string(self):
        assert normalize_verdict("") == ""

    def test_strips_whitespace(self):
        assert normalize_verdict("  valid  ") == "valid"

    def test_no_result_maps_to_unknown(self):
        assert normalize_verdict("no_result") == "unknown"


class TestIsLegacyZuhaled:
    def test_detects_legacy(self, tmp_path: Path):
        p = tmp_path / "legacy.csv"
        p.write_text("Email,Status\na@b.com,valid\n")
        assert is_legacy_zuhaled(p) is True

    def test_rejects_canonical(self, tmp_path: Path):
        p = tmp_path / "canonical.csv"
        p.write_text(",".join(CANONICAL_HEADER) + "\n")
        assert is_legacy_zuhaled(p) is False

    def test_rejects_other_header(self, tmp_path: Path):
        p = tmp_path / "other.csv"
        p.write_text("email,status\na@b.com,valid\n")
        assert is_legacy_zuhaled(p) is False


class TestNormalizeFile:
    def _write_legacy(self, path: Path, rows: list[tuple[str, str]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Email", "Status"])
            writer.writerows(rows)

    def _write_collected(self, path: Path, rows: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    def test_produces_canonical_output(self, tmp_path: Path):
        legacy = tmp_path / "file.csv"
        self._write_legacy(legacy, [("a@acme.com", "valid")])
        out = normalize_file(legacy, "test_op")
        assert out.name == "file_normalized.csv"
        with out.open() as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 1
        assert rows[0]["candidate_email"] == "a@acme.com"
        assert rows[0]["zuhal_verdict"] == "valid"

    def test_normalizes_catch_all(self, tmp_path: Path):
        legacy = tmp_path / "file2.csv"
        self._write_legacy(legacy, [("b@acme.com", "catch-all")])
        out = normalize_file(legacy, "op")
        with out.open() as f:
            rows = list(csv.DictReader(f))
        assert rows[0]["zuhal_verdict"] == "catch_all"

    def test_skips_empty_email(self, tmp_path: Path):
        legacy = tmp_path / "file3.csv"
        self._write_legacy(legacy, [("", "valid"), ("c@acme.com", "valid")])
        out = normalize_file(legacy, "op")
        with out.open() as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 1
