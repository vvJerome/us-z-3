from __future__ import annotations

import re

from rapidfuzz import fuzz

from pipeline.constants import COMMERCIAL_AGENT_NAMES, OWNER_ROLE_KEYWORDS
from pipeline.models import InputRecord
from pipeline.utils.text import is_org_agent, normalize_business_name, parse_name

_NON_ALNUM = re.compile(r"[^a-z0-9 ]")

# Additive component weights (sum capped at 1.0). The signals our input actually carries —
# the spec's principal-address match isn't in the NC data, so it's omitted, not faked.
_W_NAME_OVERLAP = 0.4   # agent surname appears in the business name
_W_OWNER_ROLE = 0.3     # filing role implies a principal, not just agent-of-record
_W_WEBSITE = 0.1        # business has a real operating website
_BASE_INDIVIDUAL = 0.2  # a parseable individual person, before corroboration


def is_commercial_agent(agent_name: str) -> bool:
    """True when the agent name is a known commercial registered-agent service."""
    # Light normalize only — normalize_business_name strips "corporation"/"company",
    # which would erase the very service names we're matching against.
    norm = _NON_ALNUM.sub(" ", agent_name.lower())
    norm = " ".join(norm.split())
    return any(svc in norm for svc in COMMERCIAL_AGENT_NAMES)


def _is_owner_role(position_type: str) -> bool:
    return any(kw in position_type.lower() for kw in OWNER_ROLE_KEYWORDS)


def _name_overlaps_business(last: str, business_name: str) -> bool:
    biz = normalize_business_name(business_name)
    if not last or not biz:
        return False
    if last.lower() in biz.split():
        return True
    return fuzz.partial_ratio(last.lower(), biz) >= 85


def score_owner_confidence(record: InputRecord, has_website: bool) -> float:
    """Likelihood the registered agent is the business owner/principal, in [0, 1].

    Heuristic, not a model: deterministic signals (commercial-agent detection, entity type,
    name↔business overlap, filing role, website presence). Baseline to measure before any ML.
    """
    if is_commercial_agent(record.agent_name):
        return 0.0  # a paid service is never the owner
    if is_org_agent(record):
        return 0.1  # an organization agent, not an individual principal

    first, _, last = parse_name(record.agent_name)
    if not (first and last):
        return 0.1  # unparseable / not a person

    score = _BASE_INDIVIDUAL
    if _name_overlaps_business(last, record.business_name):
        score += _W_NAME_OVERLAP
    if _is_owner_role(record.position_type):
        score += _W_OWNER_ROLE
    if has_website:
        score += _W_WEBSITE
    return round(min(1.0, score), 3)


def owner_confidence_tier(score: float) -> str:
    """Tier an owner-confidence score (mirrors domain_confidence_tier thresholds)."""
    if score >= 0.6:
        return "high"
    if score >= 0.3:
        return "medium"
    return "low"
