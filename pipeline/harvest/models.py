from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class HarvestResult:
    """Emails and officer names scraped from a single business domain."""

    emails: list[str] = field(default_factory=list)            # real addresses @domain
    officers: list[tuple[str, str]] = field(default_factory=list)  # (first, last)
    blocked: bool = False                                       # any 403/429/503 — logged for manual proxy decision
