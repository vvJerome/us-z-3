from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


class PipelineHaltError(Exception):
    """Raised when a non-recoverable error requires stopping the entire pipeline.

    Used for 401/402 API responses where retrying individual records is pointless.
    """


@dataclass
class InputRecord:
    unique_id: str
    business_name: str
    agent_name: str
    state: str
    jurisdiction: str = ""
    position_type: str = ""
    name_entity_type: str = ""
    email_biz: str = ""
    email_agent: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> InputRecord:
        uid = str(d.get("unique_id", "") or "").strip()
        biz = str(d.get("business_name", "") or "").strip()
        if not uid:
            raise ValueError("unique_id is required and must be non-empty")
        if not biz:
            raise ValueError("business_name is required and must be non-empty")
        return cls(
            unique_id=uid,
            business_name=biz,
            agent_name=str(d.get("agent_name", "") or ""),
            state=str(d.get("state", "") or ""),
            jurisdiction=str(d.get("jurisdiction", "") or ""),
            position_type=str(d.get("position_type", "") or ""),
            name_entity_type=str(d.get("name_entity_type", "") or ""),
            email_biz=str(d.get("email_biz", "") or ""),
            email_agent=str(d.get("email_agent", "") or ""),
        )


@dataclass
class EnrichmentResult:
    candidate_emails: list[str] = field(default_factory=list)
    subdomain_emails: list[str] = field(default_factory=list)
    candidate_domain: str | None = None
    is_fallback_domain: bool = False  # True when domain came from first-organic fallback, not fuzzy match
    source: Literal["serper"] = "serper"
    query_used: str = ""
    raw_snippets: list[str] = field(default_factory=list)


@dataclass
class ValidationResult:
    email: str
    verdict: Literal["valid", "invalid", "unknown", "disposable", "accept-all", "ms_valid", "bbops_valid", "catch_all"]
    score: float
    is_disposable: bool
    raw_status: str
    http_status: int


# ---------------------------------------------------------------------------
# Dual-backend verdict types
# ---------------------------------------------------------------------------

# "ms_valid" is written into racknerd_status when the MS HTTP probe short-circuits SMTP.
BackendStatus = Literal["valid", "invalid", "catch_all", "blocked", "error", "not_run", "ms_valid"]


@dataclass
class BackendVerdict:
    """Result from a single SMTP backend for one email probe."""
    status: BackendStatus
    message: str
    verified_at: str | None  # ISO timestamp; None for synthetic/stub verdicts
    probe_host: str | None = None  # fleet worker/IP that ran this probe (None = single-host/bbops)


FinalVerdict = Literal["valid", "invalid", "catch_all", "unknown"]


@dataclass
class ReconcileResult:
    """Output of OR-of-valids reconciliation across both backends."""
    final_verdict: FinalVerdict
    # True → write verdict and advance state; False → re-queue without burning attempt
    should_write: bool
    # True → mark as validated terminal state; False → mark as validation_failed or pending
    is_terminal: bool
