from __future__ import annotations

from pipeline.models import InputRecord
from pipeline.utils.owner_inference import (
    is_commercial_agent,
    owner_confidence_tier,
    score_owner_confidence,
)


def _rec(agent: str, biz: str, pos: str = "", ent: str = "") -> InputRecord:
    return InputRecord(
        unique_id="x", business_name=biz, agent_name=agent, state="NC",
        position_type=pos, name_entity_type=ent,
    )


# ── is_commercial_agent ───────────────────────────────────────────────────────

def test_commercial_agent_detected_despite_corporation_suffix():
    # normalize_business_name would strip "Corporation" — light normalize must not.
    assert is_commercial_agent("CT Corporation System") is True
    assert is_commercial_agent("Northwest Registered Agent LLC") is True


def test_real_person_is_not_a_commercial_agent():
    assert is_commercial_agent("John Smith") is False


# ── score_owner_confidence ────────────────────────────────────────────────────

def test_commercial_agent_scores_zero():
    assert score_owner_confidence(_rec("CT Corporation System", "Acme LLC"), True) == 0.0


def test_org_agent_scores_low():
    rec = _rec("Acme LLC", "Acme LLC", ent="Organization")
    assert score_owner_confidence(rec, True) == 0.1


def test_named_owner_matching_business_with_role_and_site_scores_high():
    rec = _rec("John Smith", "Smith Plumbing LLC", pos="Member", ent="Individual")
    score = score_owner_confidence(rec, has_website=True)
    assert score == 1.0  # base .2 + overlap .4 + role .3 + website .1
    assert owner_confidence_tier(score) == "high"


def test_bare_individual_agent_scores_base_only():
    rec = _rec("Jane Doe", "Acme Industries", pos="Registered Agent", ent="Individual")
    assert score_owner_confidence(rec, has_website=False) == 0.2  # no overlap/role/site


def test_website_alone_nudges_an_unrelated_individual():
    rec = _rec("Jane Doe", "Acme Industries", pos="Registered Agent", ent="Individual")
    assert score_owner_confidence(rec, has_website=True) == 0.3


# ── owner_confidence_tier ─────────────────────────────────────────────────────

def test_tier_thresholds():
    assert owner_confidence_tier(0.6) == "high"
    assert owner_confidence_tier(0.3) == "medium"
    assert owner_confidence_tier(0.29) == "low"
