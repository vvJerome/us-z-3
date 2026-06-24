"""Unit tests for pipeline.utils.text — pure name/domain/strategy logic."""

from pipeline.models import InputRecord
from pipeline.utils.text import (
    parse_name,
    normalize_business_name,
    generate_domain_stems,
    assign_email_strategy,
    is_org_agent,
    score_domain_confidence,
    domain_confidence_tier,
)


class TestScoreDomainConfidence:
    def test_no_domain_is_zero(self):
        assert score_domain_confidence("Acme Corp", None) == 0.0

    def test_dns_hit_with_name_match_is_high(self):
        assert score_domain_confidence("Acme Widgets", "acmewidgets.com", "dns") >= 0.7

    def test_fallback_with_unrelated_domain_is_low(self):
        assert score_domain_confidence("Acme Widgets", "randomguess.com", "serper_fallback") < 0.4

    def test_better_source_scores_higher(self):
        dns = score_domain_confidence("Acme Widgets", "acmewidgets.com", "dns")
        fb = score_domain_confidence("Acme Widgets", "acmewidgets.com", "serper_fallback")
        assert dns > fb

    def test_clamped_to_one(self):
        assert score_domain_confidence("Acme Widgets", "acmewidgets.com", "input") <= 1.0


class TestDomainConfidenceTier:
    def test_tiers(self):
        assert domain_confidence_tier(0.8) == "high"
        assert domain_confidence_tier(0.5) == "medium"
        assert domain_confidence_tier(0.2) == "low"


def _rec(**kwargs) -> InputRecord:
    defaults = dict(unique_id="id1", business_name="Acme Corp", agent_name="John Doe", state="NC")
    defaults.update(kwargs)
    return InputRecord(**defaults)


class TestParseName:
    def test_standard_first_last(self):
        assert parse_name("John Doe") == ("john", "", "doe")

    def test_first_middle_last(self):
        assert parse_name("John Quincy Adams") == ("john", "quincy", "adams")

    def test_comma_last_first(self):
        assert parse_name("Doe, John") == ("john", "", "doe")

    def test_comma_last_first_middle(self):
        assert parse_name("Doe, John Quincy") == ("john", "quincy", "doe")

    def test_single_word_is_last(self):
        assert parse_name("Madonna") == ("", "", "madonna")

    def test_strips_suffix(self):
        assert parse_name("John Doe Jr") == ("john", "", "doe")

    def test_empty_returns_blanks(self):
        assert parse_name("") == ("", "", "")

    def test_whitespace_collapsed(self):
        assert parse_name("  John   Doe  ") == ("john", "", "doe")


class TestNormalizeBusinessName:
    def test_strips_legal_suffix(self):
        assert "llc" not in normalize_business_name("Acme LLC")

    def test_lowercases_and_collapses(self):
        assert normalize_business_name("  ACME   Corp  ") == "acme"

    def test_strips_leading_article(self):
        assert normalize_business_name("The Acme Company") == "acme"

    def test_strips_punctuation(self):
        assert normalize_business_name("Acme, Inc.") == "acme"


class TestGenerateDomainStems:
    def test_joined_and_hyphenated(self):
        # "Corp" is a legal suffix (stripped), so use plain words.
        stems = generate_domain_stems("Blue Widget")
        assert "bluewidget" in stems
        assert "blue-widget" in stems

    def test_initials_survive_when_long_enough(self):
        # Initials must reach DOMAIN_STEM_MIN_LENGTH (5) to survive the filter.
        stems = generate_domain_stems("Alpha Beta Gamma Delta Echo")
        assert "abgde" in stems

    def test_empty_returns_empty(self):
        assert generate_domain_stems("") == []

    def test_all_geographic_falls_back_to_raw_words(self):
        # Every word is geographic → falls back to using them rather than empty.
        stems = generate_domain_stems("Texas California")
        assert stems  # non-empty

    def test_deduplicated(self):
        stems = generate_domain_stems("Acme Acme")
        assert len(stems) == len(set(stems))


class TestAssignEmailStrategy:
    def test_registered_agent_is_without(self):
        rec = _rec(position_type="Registered Agent")
        assert assign_email_strategy(rec) == "without"

    def test_org_agent_is_without(self):
        rec = _rec(agent_name="Acme Corp", name_entity_type="Organization")
        assert assign_email_strategy(rec) == "without"

    def test_person_is_with(self):
        rec = _rec(agent_name="John Doe")
        assert assign_email_strategy(rec) == "with"

    def test_person_named_like_business_is_without(self):
        rec = _rec(agent_name="John Doe LLC")
        assert assign_email_strategy(rec) == "without"

    def test_single_name_is_without(self):
        rec = _rec(agent_name="Madonna")
        assert assign_email_strategy(rec) == "without"


class TestIsOrgAgent:
    def test_org_matching_business_is_org(self):
        rec = _rec(business_name="Acme Corp", agent_name="Acme Corp", name_entity_type="Organization")
        assert is_org_agent(rec) is True

    def test_empty_agent_norm_is_org(self):
        rec = _rec(agent_name="LLC", name_entity_type="Organization")  # normalizes to empty
        assert is_org_agent(rec) is True

    def test_individual_is_not_org(self):
        rec = _rec(agent_name="John Doe", name_entity_type="Individual")
        assert is_org_agent(rec) is False

    def test_org_with_different_name_is_not_org(self):
        rec = _rec(business_name="Acme Corp", agent_name="Beta Holdings", name_entity_type="Organization")
        assert is_org_agent(rec) is False
