"""Canonical verdict vocabulary — the single place provider statuses are normalized.

Every service (Racknerd/bbops SMTP, Zuhal, ZeroBounce) speaks its own dialect:
`accept-all` vs `catch_all` vs `catch-all`, `ms_valid`, `circuit_open`, `dual_*`,
ZeroBounce's 30+ statuses. Downstream code should read `canonical_status` (one of
CANONICAL_STATUSES) and never branch on raw per-service values. Raw values stay in
their own columns; canonical is derived through normalize_verdict().
"""
from __future__ import annotations

# Core canonical set, mapped from every provider. ZeroBounce sub-statuses
# (role/toxic/etc.) are kept separately in canonical_sub_status, not here.
CANONICAL_STATUSES: frozenset[str] = frozenset({
    "valid", "invalid", "catch_all", "unknown", "do_not_mail", "abuse", "disposable",
})

# Raw provider value -> canonical. Anything unmapped and not already canonical
# falls through to "unknown" (never trust a status we don't recognize).
_ALIASES: dict[str, str] = {
    # catch-all spellings
    "accept_all": "catch_all", "acceptall": "catch_all", "catchall": "catch_all",
    # confirmed-valid variants
    "ms_valid": "valid", "bbops_valid": "valid", "deliverable": "valid",
    # inconclusive / infra -> unknown (not a verdict)
    "error": "unknown", "not_run": "unknown", "blocked": "unknown",
    "circuit_open": "unknown", "greylisted": "unknown", "unverifiable": "unknown",
    # SMTP reconciliation encodings (the dual_* carried in zuhal_status historically)
    "dual_valid": "valid", "dual_catch_all": "catch_all", "dual_invalid": "invalid",
    # ZeroBounce extras
    "spamtrap": "do_not_mail", "toxic": "do_not_mail",
}


def normalize_verdict(raw: str | None) -> str:
    """Map any provider status to a canonical one. Unknown/blank -> 'unknown'.

    Service-agnostic: the alias table covers every provider's dialect, so callers
    don't pass the service. Hyphens/spaces are folded to underscores first.
    """
    if not raw:
        return "unknown"
    v = raw.strip().lower().replace("-", "_").replace(" ", "_")
    v = _ALIASES.get(v, v)
    return v if v in CANONICAL_STATUSES else "unknown"


def canonical_from_smtp(final_verdict: str, *, ms_probe: bool = False) -> tuple[str, str]:
    """(canonical_status, canonical_source) for an SMTP-reconciled verdict."""
    return normalize_verdict(final_verdict), ("ms_probe" if ms_probe else "smtp")


def canonical_from_zuhal(zuhal_status: str) -> tuple[str, str]:
    """(canonical_status, canonical_source) for a Zuhal verdict."""
    return normalize_verdict(zuhal_status), "zuhal"


if __name__ == "__main__":
    # ponytail self-check: the money path is value-vocab drift collapsing to one set.
    assert normalize_verdict("accept-all") == "catch_all"
    assert normalize_verdict("catch-all") == "catch_all"
    assert normalize_verdict("ms_valid") == "valid"
    assert normalize_verdict("dual_invalid") == "invalid"
    assert normalize_verdict("circuit_open") == "unknown"
    assert normalize_verdict("spamtrap") == "do_not_mail"
    assert normalize_verdict("DoNotMail".replace("DoNotMail", "do_not_mail")) == "do_not_mail"
    assert normalize_verdict(None) == "unknown"
    assert normalize_verdict("nonsense") == "unknown"
    assert canonical_from_zuhal("accept-all") == ("catch_all", "zuhal")
    assert canonical_from_smtp("valid", ms_probe=True) == ("valid", "ms_probe")
    print("verdicts self-check OK")
