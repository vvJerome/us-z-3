"""Unit tests for email pattern generation and reverse-mapping."""

import pytest
from pipeline.utils.email_patterns import (
    generate_ranked_candidates,
    email_to_template,
    _PERSONAL_TEMPLATES,
    _GENERIC_TEMPLATES,
    _expand_personal,
)


class TestGenerateRankedCandidatesWithStrategy:
    def test_returns_top_n_personal_patterns(self):
        result = generate_ranked_candidates("john", "doe", "acme.com", "with", max_candidates=5)
        assert len(result) == 5
        assert result[0] == "john.doe@acme.com"  # first.last is highest ranked

    def test_all_personal_patterns_contain_domain(self):
        result = generate_ranked_candidates("alice", "smith", "example.com", "with", max_candidates=13)
        for email in result:
            assert email.endswith("@example.com")

    def test_missing_first_name_returns_empty(self):
        result = generate_ranked_candidates("", "smith", "example.com", "with")
        assert result == []

    def test_missing_domain_returns_empty(self):
        result = generate_ranked_candidates("john", "doe", "", "with")
        assert result == []


class TestGenerateRankedCandidatesWithoutStrategy:
    def test_returns_generic_patterns(self):
        result = generate_ranked_candidates("", "", "acme.com", "without")
        assert "info@acme.com" in result
        assert "contact@acme.com" in result

    def test_capped_at_max_without_candidates(self):
        from pipeline.constants import MAX_WITHOUT_CANDIDATES
        result = generate_ranked_candidates("", "", "acme.com", "without")
        assert len(result) <= MAX_WITHOUT_CANDIDATES


class TestPatternRankingReorder:
    def test_high_success_rate_template_moves_first(self):
        rankings = [
            {"template": "firstlast", "success_count": 9, "total_count": 10},  # 90%
            {"template": "first.last", "success_count": 1, "total_count": 10},  # 10%
        ]
        result = generate_ranked_candidates("john", "doe", "acme.com", "with", max_candidates=5, rankings=rankings)
        # firstlast should rank before first.last
        firstlast_idx = next(i for i, e in enumerate(result) if e == "johndoe@acme.com")
        first_last_idx = next(i for i, e in enumerate(result) if e == "john.doe@acme.com")
        assert firstlast_idx < first_last_idx

    def test_empty_rankings_uses_default_order(self):
        default = generate_ranked_candidates("john", "doe", "acme.com", "with", max_candidates=5)
        ranked = generate_ranked_candidates("john", "doe", "acme.com", "with", max_candidates=5, rankings=[])
        assert default == ranked

    def test_unseen_templates_appended_after_seen(self):
        rankings = [
            {"template": "last", "success_count": 5, "total_count": 5},
        ]
        result = generate_ranked_candidates("john", "doe", "acme.com", "with", max_candidates=13, rankings=rankings)
        last_idx = next(i for i, e in enumerate(result) if e == "doe@acme.com")
        # "last" template should be first because it has 100% success rate
        assert last_idx == 0


class TestEmailToTemplate:
    def test_roundtrip_all_personal_templates(self):
        first, last, domain = "alice", "jones", "corp.com"
        for template in _PERSONAL_TEMPLATES:
            email = _expand_personal(template, first, last, domain)
            if email:
                result = email_to_template(email, first, last, domain)
                assert result == template, f"roundtrip failed for {template}: got {result}"

    def test_generic_template_roundtrip(self):
        domain = "corp.com"
        for template in _GENERIC_TEMPLATES:
            email = f"{template}@{domain}"
            result = email_to_template(email, "alice", "jones", domain)
            assert result == template

    def test_unknown_email_returns_none(self):
        result = email_to_template("weird@corp.com", "john", "doe", "corp.com")
        assert result is None
