"""Unit tests for confidence scoring and tier calculation."""

import pytest
from pipeline.dispatcher import compute_confidence_score, confidence_tier


class TestComputeConfidenceScore:
    def test_with_strategy_full_score(self):
        score = compute_confidence_score(
            email="john.doe@acme.com",
            candidate_domain="acme.com",
            strategy="with",
            verdict="valid",
            agent_name="John Doe",
        )
        assert score == 4  # domain match + name match + not generic + not catch-all

    def test_with_strategy_catch_all_loses_point(self):
        score = compute_confidence_score(
            email="john.doe@acme.com",
            candidate_domain="acme.com",
            strategy="with",
            verdict="accept-all",
            agent_name="John Doe",
        )
        assert score == 3  # no "not catch-all" point

    def test_with_strategy_generic_loses_point(self):
        score = compute_confidence_score(
            email="info@acme.com",
            candidate_domain="acme.com",
            strategy="with",
            verdict="valid",
            agent_name="John Doe",
        )
        # domain match + name mismatch(0) + IS generic(-1) + not catch-all
        assert score == 2

    def test_without_strategy_generic_email_valid(self):
        score = compute_confidence_score(
            email="info@acme.com",
            candidate_domain="acme.com",
            strategy="without",
            verdict="valid",
        )
        assert score == 3  # domain match + IS generic + not catch-all

    def test_without_strategy_non_generic_email(self):
        score = compute_confidence_score(
            email="john.doe@acme.com",
            candidate_domain="acme.com",
            strategy="without",
            verdict="valid",
        )
        assert score == 2  # domain match + not generic (0 points) + not catch-all

    def test_domain_mismatch_loses_point(self):
        score = compute_confidence_score(
            email="info@othercorp.com",
            candidate_domain="acme.com",
            strategy="without",
            verdict="valid",
        )
        assert score == 2  # no domain match + IS generic + not catch-all

    def test_no_candidate_domain_skips_domain_check(self):
        score = compute_confidence_score(
            email="info@acme.com",
            candidate_domain=None,
            strategy="without",
            verdict="valid",
        )
        assert score == 2  # no domain match (0) + IS generic + not catch-all


class TestConfidenceTier:
    def test_high_tier(self):
        assert confidence_tier(4) == "high"
        assert confidence_tier(3) == "high"

    def test_medium_tier(self):
        assert confidence_tier(2) == "medium"

    def test_low_tier(self):
        assert confidence_tier(1) == "low"
        assert confidence_tier(0) == "low"
