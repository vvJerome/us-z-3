"""Unit tests for the ZeroBounce confidence gate in pipeline.ops.zb_zuhaled."""

from pipeline.ops.zb_zuhaled import build_upload_csv, confidence_of


class TestConfidenceOf:
    def test_reads_confidence_score(self):
        assert confidence_of({"confidence_score": "3"}) == 3.0

    def test_reads_domain_confidence(self):
        assert confidence_of({"domain_confidence": "0.8"}) == 0.8

    def test_missing_returns_none(self):
        assert confidence_of({"email": "a@b.com"}) is None

    def test_non_numeric_returns_none(self):
        assert confidence_of({"confidence_score": "high"}) is None


class TestBuildUploadCsvGate:
    def _rows(self):
        return [
            {"email": "weak@a.com", "confidence_score": "1"},
            {"email": "strong@b.com", "confidence_score": "4"},
        ]

    def test_no_gate_submits_all(self):
        _, unique = build_upload_csv(self._rows(), skip=set(), min_confidence=0.0)
        assert set(unique) == {"weak@a.com", "strong@b.com"}

    def test_gate_drops_low_confidence(self):
        _, unique = build_upload_csv(self._rows(), skip=set(), min_confidence=3.0)
        assert unique == ["strong@b.com"]

    def test_rows_without_confidence_always_submitted(self):
        rows = [{"email": "nopconf@a.com"}]
        _, unique = build_upload_csv(rows, skip=set(), min_confidence=3.0)
        assert unique == ["nopconf@a.com"]

    def test_skip_set_respected(self):
        _, unique = build_upload_csv(self._rows(), skip={"strong@b.com"}, min_confidence=0.0)
        assert unique == ["weak@a.com"]
