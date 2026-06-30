from __future__ import annotations

import datetime

import aiosqlite
from rapidfuzz import fuzz

from pipeline.constants import INFRA_RETRY_BASE_MINUTES, INFRA_RETRY_MULTIPLIER

from pipeline import db
from pipeline.constants import is_untrustworthy_catchall_mx
from pipeline.utils.email_patterns import email_to_template

def infra_retry_after(requeue_count: int) -> str:
    """Exponential backoff for infra re-queues: 5min → 15min → 45min."""
    minutes = INFRA_RETRY_BASE_MINUTES * (INFRA_RETRY_MULTIPLIER ** requeue_count)
    dt = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def verifier_agreement(rk: str, bb: str) -> str:
    rk_ok = rk in ("valid", "catch_all")
    bb_ok = bb in ("valid", "catch_all")
    if rk_ok and bb_ok:
        return "both"
    if rk_ok:
        return "racknerd_only"
    if bb_ok:
        return "bbops_only"
    return "unknown"


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


def pre_score(
    email: str,
    candidate_domain: str | None,
    strategy: str,
    agent_name: str = "",
    domain_confidence: float | None = None,
) -> float:
    """Identity/deliverability confidence available BEFORE any verdict.

    Same components as compute_confidence_score minus the verdict term (verdict is
    unknown pre-validation), plus the business-to-domain confidence (0–1, scaled to
    0–2 points) computed at discovery. Used to rank candidates and gate paid
    verification.
    """
    score = float(compute_confidence_score(email, candidate_domain, strategy, "pending", agent_name))
    return score + 2.0 * (domain_confidence or 0.0)


def catch_all_confidence_floor(base: float, mx_provider: str | None) -> float:
    """Confidence a catch-all must clear to be accepted, raised for untrustworthy providers.

    When the gate is disabled (base <= 0) nothing changes — every catch-all is accepted,
    today's behavior. When enabled, providers that accept-all by default or sit behind a
    security gateway (always 250) must clear a higher bar before a catch-all counts.
    """
    if base <= 0.0:
        return 0.0
    return base + 1.0 if is_untrustworthy_catchall_mx(mx_provider) else base


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
