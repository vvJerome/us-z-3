from __future__ import annotations

from pipeline.harvest.extract import extract_emails, extract_officers, infer_templates
from pipeline.harvest.fetch import fetch_site
from pipeline.harvest.models import HarvestResult
from pipeline.utils.rate_limiter import TokenBucket

__all__ = ["harvest", "HarvestResult", "infer_templates"]


async def harvest(
    domain: str,
    *,
    rate_limiter: TokenBucket,
    timeout_s: float,
) -> HarvestResult:
    """Scrape a business domain's own pages for real emails and officer names."""
    pages, blocked = await fetch_site(domain, rate_limiter=rate_limiter, timeout_s=timeout_s)
    result = HarvestResult(blocked=blocked)
    for _url, html in pages:
        for e in extract_emails(html, domain):
            if e not in result.emails:
                result.emails.append(e)
        for officer in extract_officers(html):
            if officer not in result.officers:
                result.officers.append(officer)
    return result
