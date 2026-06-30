"""Integration tests for pipeline.ops.passoff_watcher."""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from pipeline import manifest
from pipeline.ops.passoff_watcher import (
    PASSOFF_COLS,
    append_confirmed_from_zb,
    append_confirmed_from_zuhal,
    ensure_passoff_header,
    passoff_path,
)


def _make_manifest(tmp_path: Path) -> sqlite3.Connection:
    return manifest.connect(tmp_path / "manifest.db")


def _write_zb_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def _write_zuhal_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


class TestEnsurePassoffHeader:
    def test_creates_header_on_missing_file(self, tmp_path: Path, monkeypatch):
        target = tmp_path / "op" / "op_combined.csv"
        ensure_passoff_header(target)
        assert target.exists()
        with target.open() as f:
            header = next(csv.reader(f))
        assert header == PASSOFF_COLS

    def test_does_not_overwrite_existing(self, tmp_path: Path):
        target = tmp_path / "op" / "op_combined.csv"
        target.parent.mkdir(parents=True)
        target.write_text("existing content\n")
        ensure_passoff_header(target)
        assert target.read_text() == "existing content\n"


class TestAppendConfirmedFromZb:
    def test_appends_valid_rows(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "pipeline.ops.passoff_watcher.passoff_path",
            lambda op: tmp_path / "passoff" / op / f"{op}_combined.csv",
        )

        conn = _make_manifest(tmp_path)
        zb_src = tmp_path / "zb.csv"
        _write_zb_csv(zb_src, [
            {"email": "a@acme.com", "zb_status": "valid", "zb_sub_status": "",
             "zb_free_email": "", "zb_did_you_mean": "", "zb_account": "",
             "zb_domain": "", "zb_mx_found": "", "zb_mx_record": "", "zb_processed_at": "",
             "unique_id": "uid1", "eid": ""},
            {"email": "b@acme.com", "zb_status": "invalid", "zb_sub_status": "",
             "zb_free_email": "", "zb_did_you_mean": "", "zb_account": "",
             "zb_domain": "", "zb_mx_found": "", "zb_mx_record": "", "zb_processed_at": "",
             "unique_id": "uid2", "eid": ""},
        ])
        n = append_confirmed_from_zb(conn, zb_src, "op")
        assert n == 1  # only "valid" row appended

    def test_idempotent_on_rerun(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "pipeline.ops.passoff_watcher.passoff_path",
            lambda op: tmp_path / "passoff" / op / f"{op}_combined.csv",
        )
        conn = _make_manifest(tmp_path)
        # Email must exist in manifest first for mark_passed_off to take effect
        manifest.mark_zuhaled(conn, "c@acme.com", eid="", operator="op", part="", verdict="valid", source="test")
        zb_src = tmp_path / "zb2.csv"
        _write_zb_csv(zb_src, [
            {"email": "c@acme.com", "zb_status": "valid", "zb_sub_status": "",
             "zb_free_email": "", "zb_did_you_mean": "", "zb_account": "",
             "zb_domain": "", "zb_mx_found": "", "zb_mx_record": "", "zb_processed_at": "",
             "unique_id": "uid3", "eid": ""},
        ])
        n1 = append_confirmed_from_zb(conn, zb_src, "op")
        n2 = append_confirmed_from_zb(conn, zb_src, "op")
        assert n1 == 1
        assert n2 == 0  # already marked in_passoff


class TestAppendConfirmedFromZuhal:
    def test_appends_valid_zuhal_rows(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "pipeline.ops.passoff_watcher.passoff_path",
            lambda op: tmp_path / "passoff" / op / f"{op}_combined.csv",
        )
        conn = _make_manifest(tmp_path)
        zuhal_src = tmp_path / "zuhal.csv"
        _write_zuhal_csv(zuhal_src, [
            {"unique_id": "uid4", "candidate_email": "d@acme.com",
             "zuhal_verdict": "valid", "business_name": "Acme"},
            {"unique_id": "uid5", "candidate_email": "e@acme.com",
             "zuhal_verdict": "invalid", "business_name": "Acme"},
        ])
        n = append_confirmed_from_zuhal(conn, zuhal_src, "op")
        assert n == 1
