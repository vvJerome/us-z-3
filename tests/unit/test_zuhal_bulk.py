"""Unit tests for pipeline.ops.zuhal_bulk pure helpers."""
from __future__ import annotations

from pipeline.ops.zuhal_bulk import _input_has_required_columns


class TestInputHasRequiredColumns:
    def test_accepts_valid_header(self):
        assert _input_has_required_columns(["unique_id", "candidate_email"]) is True

    def test_rejects_missing_unique_id(self):
        assert _input_has_required_columns(["candidate_email"]) is False

    def test_rejects_missing_email(self):
        assert _input_has_required_columns(["unique_id"]) is False

    def test_accepts_extra_columns(self):
        assert _input_has_required_columns(["unique_id", "candidate_email", "extra"]) is True

    def test_rejects_empty(self):
        assert _input_has_required_columns([]) is False
