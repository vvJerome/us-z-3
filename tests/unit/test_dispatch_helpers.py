"""Unit tests for pipeline._dispatch_helpers."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from pipeline._dispatch_helpers import (
    compute_confidence_score,
    confidence_tier,
    name_matches_email,
    pre_score,
    record_pattern,
    GENERIC_PREFIXES,
)


class TestNameMatchesEmail:
    def test_exact_firstlast_match(self):
        assert name_matches_email("johndoe", "John Doe") is True

    def test_dot_separated_match(self):
        assert name_matches_email("john.doe", "John Doe") is True

    def test_underscore_separated_match(self):
        assert name_matches_email("john_doe", "John Doe") is True

    def test_initial_last_match(self):
        assert name_matches_email("jdoe", "John Doe") is True

    def test_first_name_only_match(self):
        assert name_matches_email("john", "John Doe") is True

    def test_last_name_only_match(self):
        assert name_matches_email("doe", "John Doe") is True

    def test_no_match_unrelated(self):
        assert name_matches_email("info", "John Doe") is False

    def test_empty_agent_name(self):
        assert name_matches_email("john", "") is False

    def test_single_word_agent_name(self):
        assert name_matches_email("acme", "acme") is True

    def test_case_insensitive(self):
        assert name_matches_email("JOHNDOE", "john doe") is True

    def test_fuzzy_close_match(self):
        # slight misspelling still matches (fuzz ratio ≥ 75)
        assert name_matches_email("johndoe", "Jonh Doe") is True

    def test_completely_different(self):
        assert name_matches_email("xyzwvq", "John Doe") is False


class TestConfidenceScoreWithStrategy:
    def test_full_score_strategy_with(self):
        score = compute_confidence_score(
            email="john.doe@acme.com",
            candidate_domain="acme.com",
            strategy="with",
            verdict="valid",
            agent_name="John Doe",
        )
        assert score == 4

    def test_catch_all_loses_valid_point(self):
        score = compute_confidence_score(
            email="john.doe@acme.com",
            candidate_domain="acme.com",
            strategy="with",
            verdict="catch_all",
            agent_name="John Doe",
        )
        assert score == 3

    def test_generic_prefix_loses_not_generic_point(self):
        score = compute_confidence_score(
            email="info@acme.com",
            candidate_domain="acme.com",
            strategy="with",
            verdict="valid",
            agent_name="John Doe",
        )
        assert score == 2

    def test_no_name_match_loses_point(self):
        score = compute_confidence_score(
            email="zzz@acme.com",
            candidate_domain="acme.com",
            strategy="with",
            verdict="valid",
            agent_name="John Doe",
        )
        assert score == 3  # domain + not generic + valid; no name match


class TestConfidenceScoreWithoutStrategy:
    def test_full_score_without(self):
        score = compute_confidence_score(
            email="info@acme.com",
            candidate_domain="acme.com",
            strategy="without",
            verdict="valid",
        )
        assert score == 3  # domain match + IS generic + valid

    def test_non_generic_loses_generic_point(self):
        score = compute_confidence_score(
            email="john.doe@acme.com",
            candidate_domain="acme.com",
            strategy="without",
            verdict="valid",
        )
        assert score == 2  # domain match + not generic (no point) + valid

    def test_domain_mismatch_loses_point(self):
        score = compute_confidence_score(
            email="info@othercorp.com",
            candidate_domain="acme.com",
            strategy="without",
            verdict="valid",
        )
        assert score == 2  # no domain match + IS generic + valid

    def test_no_candidate_domain(self):
        score = compute_confidence_score(
            email="info@acme.com",
            candidate_domain=None,
            strategy="without",
            verdict="valid",
        )
        assert score == 2  # no domain check + IS generic + valid

    def test_catch_all_loses_valid_point(self):
        score = compute_confidence_score(
            email="info@acme.com",
            candidate_domain="acme.com",
            strategy="without",
            verdict="catch_all",
        )
        assert score == 2  # domain match + IS generic; no valid point


class TestPreScore:
    def test_drops_verdict_term_relative_to_full_score(self):
        # pre_score == confidence score with no "valid" verdict point.
        pre = pre_score("john.doe@acme.com", "acme.com", "with", "John Doe")
        full = compute_confidence_score("john.doe@acme.com", "acme.com", "with", "valid", "John Doe")
        assert pre == float(full) - 1.0

    def test_dns_source_adds_confidence(self):
        base = pre_score("john.doe@acme.com", "acme.com", "with", "John Doe")
        dns = pre_score("john.doe@acme.com", "acme.com", "with", "John Doe", "dns")
        assert dns == base + 1.0

    def test_serper_fallback_source_penalized(self):
        base = pre_score("john.doe@acme.com", "acme.com", "with", "John Doe")
        fallback = pre_score("john.doe@acme.com", "acme.com", "with", "John Doe", "serper_fallback")
        assert fallback == base - 1.0

    def test_strong_candidate_outranks_weak(self):
        strong = pre_score("john.doe@acme.com", "acme.com", "with", "John Doe", "dns")
        weak = pre_score("zzz@other.com", "acme.com", "with", "John Doe", "serper_fallback")
        assert strong > weak


class TestConfidenceTier:
    def test_high(self):
        assert confidence_tier(4) == "high"
        assert confidence_tier(3) == "high"

    def test_medium(self):
        assert confidence_tier(2) == "medium"

    def test_low(self):
        assert confidence_tier(1) == "low"
        assert confidence_tier(0) == "low"


class TestGenericPrefixes:
    def test_known_generics_present(self):
        for prefix in ("info", "contact", "hello", "admin", "support", "sales", "help"):
            assert prefix in GENERIC_PREFIXES

    def test_non_generic_absent(self):
        assert "john" not in GENERIC_PREFIXES
        assert "doe" not in GENERIC_PREFIXES


class TestRecordPattern:
    async def test_skips_when_no_mx_provider(self):
        conn = MagicMock()
        # Should return without calling db.record_pattern_result
        with patch("pipeline._dispatch_helpers.db.record_pattern_result") as mock_db:
            await record_pattern(conn, "info@acme.com", "john", "doe", "acme.com", None, success=True)
            mock_db.assert_not_called()

    async def test_skips_when_no_template(self):
        conn = MagicMock()
        with patch("pipeline._dispatch_helpers.email_to_template", return_value=None), \
             patch("pipeline._dispatch_helpers.db.record_pattern_result") as mock_db:
            await record_pattern(conn, "info@acme.com", "john", "doe", "acme.com", "google", success=True)
            mock_db.assert_not_called()

    async def test_records_when_template_found(self):
        conn = MagicMock()
        with patch("pipeline._dispatch_helpers.email_to_template", return_value="{first}.{last}"), \
             patch("pipeline._dispatch_helpers.db.record_pattern_result", new_callable=AsyncMock) as mock_db:
            await record_pattern(conn, "john.doe@acme.com", "john", "doe", "acme.com", "google", success=True)
            mock_db.assert_called_once_with(conn, "google", "{first}.{last}", success=True)
