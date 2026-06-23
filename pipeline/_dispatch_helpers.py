from __future__ import annotations

import aiosqlite
from rapidfuzz import fuzz

from pipeline import db
from pipeline.utils.email_patterns import email_to_template

GENERIC_PREFIXES: frozenset[str] = frozenset({
    "info", "contact", "hello", "admin", "support", "sales", "help",
})


def name_matches_email(local: str, agent_name: str) -> bool:
    parts = agent_name.strip().lower().split()
    first = parts[0] if parts else ""
    last = parts[-1] if len(parts) > 1 else ""
    variants = [v for v in [
        f"{first}{last}",
        f"{first}.{last}",
        f"{first}_{last}",
        f"{first[0]}{last}" if first else "",
        first,
        last,
    ] if v]
    return bool(variants) and max(fuzz.ratio(local.lower(), v) for v in variants) >= 75


def compute_confidence_score(
    email: str,
    candidate_domain: str | None,
    strategy: str,
    verdict: str,
    agent_name: str = "",
    domain_match_score: float | None = None,
) -> int:
    """Return an additive confidence score 0–4 for a validated email.

    +1 domain match (email domain fuzzy-matches candidate_domain, ≥85 ratio)
    strategy="with" (name-targeted search):
        +1 name match (local part resembles agent_name)
        +1 not a generic prefix (info/contact/admin/…)
        +1 verdict == "valid" (not catch_all)
    strategy="without" (generic/org search):
        +1 IS a generic prefix
        +1 verdict == "valid"
    High ≥ 3, medium = 2, low ≤ 1 — see confidence_tier().
    """
    local, _, domain = email.partition("@")
    score = 0

    if candidate_domain:
        d_norm = domain.rsplit(".", 1)[0].replace("-", "") if "." in domain else domain
        c_norm = candidate_domain.rsplit(".", 1)[0].replace("-", "") if "." in candidate_domain else candidate_domain
        if fuzz.ratio(d_norm, c_norm) >= 85:
            score += 1

    if strategy == "with":
        if agent_name and name_matches_email(local, agent_name):
            score += 1
        if local.lower() not in GENERIC_PREFIXES:
            score += 1
        if verdict == "valid":
            score += 1
    else:
        if local.lower() in GENERIC_PREFIXES:
            score += 1
        if verdict == "valid":
            score += 1

    # Cap score when the discovered domain is a weak match for the business name.
    # Thresholds are intentionally loose (<0.2 / <0.5) because abbreviation domains
    # (ncrg.com for "NC Restaurant Group") score ~0.25–0.38 and should not be hard-penalized.
    if domain_match_score is not None:
        if domain_match_score < 0.2:
            score = min(score, 1)   # truly unrelated domain → force low
        elif domain_match_score < 0.5:
            score = min(score, 2)   # weak match → cap at medium

    return score


def confidence_tier(score: int) -> str:
    if score >= 3:
        return "high"
    if score == 2:
        return "medium"
    return "low"


async def record_pattern(
    conn: aiosqlite.Connection,
    email: str,
    first: str,
    last: str,
    candidate_domain: str,
    mx_provider: str | None,
    success: bool,
) -> None:
    if not mx_provider:
        return
    tmpl = email_to_template(email, first, last, candidate_domain)
    if tmpl:
        await db.record_pattern_result(conn, mx_provider, tmpl, success=success)
