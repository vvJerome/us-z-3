"""Mail-provider classification from an MX host string.

Generalizes the Microsoft-only `is_microsoft_mx` check into a canonical provider
label so the SMTP fleet can apply provider-specific concurrency/retry limits and
track outcomes per (worker, provider) — Improve-Existing item 5.
"""
from __future__ import annotations

from pipeline.constants import PROVIDER_MX_PATTERNS, PROVIDER_OTHER


def classify_provider(mx_provider: str | None) -> str:
    """Map an MX host to a canonical provider label; `PROVIDER_OTHER` when unknown."""
    if not mx_provider:
        return PROVIDER_OTHER
    lp = mx_provider.lower()
    for provider, patterns in PROVIDER_MX_PATTERNS:
        if any(p in lp for p in patterns):
            return provider
    return PROVIDER_OTHER
