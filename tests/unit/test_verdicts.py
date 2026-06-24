"""Unit tests for the canonical verdict normalizer."""

from pipeline.verdicts import (
    CANONICAL_STATUSES,
    normalize_verdict,
    canonical_from_smtp,
    canonical_from_zuhal,
)


class TestNormalizeVerdict:
    def test_catch_all_spellings_collapse(self):
        for raw in ("accept-all", "accept_all", "catch-all", "catchall"):
            assert normalize_verdict(raw) == "catch_all"

    def test_valid_variants(self):
        assert normalize_verdict("ms_valid") == "valid"
        assert normalize_verdict("bbops_valid") == "valid"
        assert normalize_verdict("VALID") == "valid"

    def test_dual_encodings(self):
        assert normalize_verdict("dual_invalid") == "invalid"
        assert normalize_verdict("dual_catch_all") == "catch_all"

    def test_inconclusive_maps_to_unknown(self):
        for raw in ("error", "not_run", "blocked", "circuit_open"):
            assert normalize_verdict(raw) == "unknown"

    def test_zerobounce_extras(self):
        assert normalize_verdict("spamtrap") == "do_not_mail"
        assert normalize_verdict("do_not_mail") == "do_not_mail"
        assert normalize_verdict("abuse") == "abuse"

    def test_blank_and_unknown_to_unknown(self):
        assert normalize_verdict(None) == "unknown"
        assert normalize_verdict("") == "unknown"
        assert normalize_verdict("gibberish") == "unknown"

    def test_output_always_canonical(self):
        for raw in ("valid", "accept-all", "spamtrap", "weird", "", "error"):
            assert normalize_verdict(raw) in CANONICAL_STATUSES


class TestCanonicalSources:
    def test_smtp_source(self):
        assert canonical_from_smtp("valid") == ("valid", "smtp")

    def test_ms_probe_source(self):
        assert canonical_from_smtp("valid", ms_probe=True) == ("valid", "ms_probe")

    def test_zuhal_source_normalizes(self):
        assert canonical_from_zuhal("accept-all") == ("catch_all", "zuhal")
