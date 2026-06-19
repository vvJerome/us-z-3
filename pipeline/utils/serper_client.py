from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Literal
from urllib.parse import urlparse

import aiohttp
import aiosqlite
from rapidfuzz import fuzz

from pipeline.constants import SERVICE_BACKOFF
from pipeline.models import EnrichmentResult, PipelineHaltError
from pipeline.utils.backoff import with_backoff
from pipeline.utils.rate_limiter import TokenBucket
from pipeline.utils.text import normalize_business_name, parse_name
from pipeline import db

logger = logging.getLogger("pipeline.producer")

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

_STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}


class SerperClient:
    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        rate_limiter: TokenBucket,
        *,
        dry_run: bool = False,
        max_attempts: int = 3,
        jitter: float = 0.2,
        ignore_cache: bool = False,
    ) -> None:
        self.api_key = api_key
        self.session = session
        self.rate_limiter = rate_limiter
        self.dry_run = dry_run
        self.max_attempts = max_attempts
        self.jitter = jitter
        self.ignore_cache = ignore_cache
        self._base, self._max_delay = SERVICE_BACKOFF["serper"]
        self._fallback_calls = 0   # extra API calls made by site: fallback retries
        self.last_was_cache_hit = False  # set after each enrich() call
        self._credits_exhausted = False  # set on first 400 "not enough credits"

    def charge_costs(self, cost_tracker, service: str) -> None:
        """Charge cost_tracker for the last enrich() call plus its site: fallback retries, then reset.

        Callers must not touch last_was_cache_hit / _fallback_calls directly — this keeps the
        cost-accounting rule (cache hit = free; each live call + each fallback retry = paid) in one place.
        """
        if not self.last_was_cache_hit:
            cost_tracker.record_call(service)
        for _ in range(self._fallback_calls):
            cost_tracker.record_call(service)
        self._fallback_calls = 0

    async def enrich(
        self,
        business_name: str,
        agent_name: str | None,
        state: str,
        domain_hint: str | None,
        strategy: Literal["with", "without"],
        fallback_blocklist: set[str] | None = None,
        conn: aiosqlite.Connection | None = None,
    ) -> EnrichmentResult:
        query = self._build_query(business_name, agent_name, state, domain_hint, strategy)

        if self._credits_exhausted:
            self.last_was_cache_hit = False
            return EnrichmentResult(source="serper", query_used=query)

        if self.dry_run:
            return EnrichmentResult(
                candidate_emails=["dryrun@example-business.com"],
                candidate_domain="example-business.com",
                source="serper",
                query_used=f"[dry-run] {query}",
                raw_snippets=["[dry-run stub snippet]"],
            )

        biz_norm = business_name.lower().strip()
        agent_norm = (agent_name or "").lower().strip()
        # Cache key includes strategy and domain_hint so different query types
        # for the same business don't return each other's cached results.
        # For "without" strategy the query never includes agent_name, so all
        # officers of the same business share one cache entry (agent_norm="").
        cache_provider = f"serper:{strategy}:{(domain_hint or '').lower()}"
        cache_agent_norm = agent_norm if strategy == "with" else ""

        if conn is not None and not self.ignore_cache:
            cached = await db.get_enrichment_cache(conn, biz_norm, cache_agent_norm, state, cache_provider)
            if cached is not None:
                logger.debug("Serper cache hit for %s/%s/%s", biz_norm, cache_agent_norm, state)
                self.last_was_cache_hit = True
                return self._extract(json.loads(cached), business_name, query, domain_hint=domain_hint, strategy=strategy, agent_name=agent_name, fallback_blocklist=fallback_blocklist)

        self.last_was_cache_hit = False
        await self.rate_limiter.acquire()

        # Single credits-exhaustion guard for the primary call and all fallbacks.
        # Any _SerperCreditsError from any with_backoff call sets the flag and
        # returns empty so future enrich() calls short-circuit at the top.
        try:
            data = await with_backoff(
                lambda: self._call_api(query),
                max_attempts=self.max_attempts,
                base_delay=self._base,
                max_delay=self._max_delay,
                jitter=self.jitter,
                retryable=_is_retryable,
                on_retry=lambda attempt, exc, delay: logger.debug(
                    "Serper retry %d: %s (wait %.1fs)", attempt, exc, delay,
                ),
            )

            if conn is not None:
                await db.set_enrichment_cache(conn, biz_norm, cache_agent_norm, state, cache_provider, json.dumps(data))

            result = self._extract(data, business_name, query, domain_hint=domain_hint, strategy=strategy, agent_name=agent_name, fallback_blocklist=fallback_blocklist)

            # Fallback: if site:-scoped query returned no emails, retry without site: filter
            if domain_hint and not result.candidate_emails and "site:" in query:
                fallback_query = self._build_query(
                    business_name, agent_name, state, None, strategy
                )
                logger.debug("Serper site: miss for %s — retrying without site: filter", domain_hint)
                await self.rate_limiter.acquire()
                data2 = await with_backoff(
                    lambda: self._call_api(fallback_query),
                    max_attempts=self.max_attempts,
                    base_delay=self._base,
                    max_delay=self._max_delay,
                    jitter=self.jitter,
                    retryable=_is_retryable,
                    on_retry=lambda attempt, exc, delay: logger.debug(
                        "Serper fallback retry %d: %s (wait %.1fs)", attempt, exc, delay,
                    ),
                )
                self._fallback_calls += 1
                fallback = self._extract(data2, business_name, fallback_query, domain_hint=domain_hint, strategy=strategy, agent_name=agent_name, fallback_blocklist=fallback_blocklist)
                if fallback.candidate_emails:
                    result = EnrichmentResult(
                        candidate_emails=fallback.candidate_emails,
                        candidate_domain=result.candidate_domain or fallback.candidate_domain,
                        source="serper",
                        query_used=fallback_query,
                        raw_snippets=fallback.raw_snippets,
                    )

            # Third fallback: agent-name-focused query when primary found neither emails nor domain.
            # Registered agents are often individual lawyers whose contact appears under their name,
            # not the filing entity — a simpler personal query can surface what the business query misses.
            if (
                strategy == "with"
                and agent_name
                and not result.candidate_emails
                and not result.candidate_domain
            ):
                state_name = _STATE_NAMES.get((state or "").upper(), state or "")
                first, _, last = parse_name(agent_name)
                agent_q = f"{first} {last}".strip() if first and last else normalize_business_name(agent_name)
                agent_query = f"{agent_q} {state_name} email"
                logger.debug("Serper agent-name fallback for %s → %s", agent_name, agent_query)
                await self.rate_limiter.acquire()
                data3 = await with_backoff(
                    lambda: self._call_api(agent_query),
                    max_attempts=self.max_attempts,
                    base_delay=self._base,
                    max_delay=self._max_delay,
                    jitter=self.jitter,
                    retryable=_is_retryable,
                    on_retry=lambda attempt, exc, delay: logger.debug(
                        "Serper agent-name retry %d: %s (wait %.1fs)", attempt, exc, delay,
                    ),
                )
                agent_result = self._extract(
                    data3, business_name, agent_query,
                    domain_hint=None, strategy="with",
                    agent_name=agent_name, fallback_blocklist=fallback_blocklist,
                )
                if agent_result.candidate_emails or agent_result.candidate_domain:
                    result = EnrichmentResult(
                        candidate_emails=agent_result.candidate_emails,
                        candidate_domain=agent_result.candidate_domain,
                        source="serper",
                        query_used=agent_query,
                        raw_snippets=agent_result.raw_snippets,
                    )

            # 4th fallback: for long business names (4+ significant words), retry with
            # first 3 words only. Full legal names like "BREWER-LOWDER-MCCUISTON POST 9010
            # VETERANS OF FOREIGN WARS" rarely appear verbatim; a shorter prefix often works.
            words = normalize_business_name(business_name).split()
            if (
                len(words) >= 3
                and not result.candidate_domain
                and not result.candidate_emails
            ):
                state_name = _STATE_NAMES.get((state or "").upper(), state or "")
                short_query = f"{' '.join(words[:3])} {state_name} email contact"
                logger.debug("Serper short-name fallback for %s → %s", business_name, short_query)
                await self.rate_limiter.acquire()
                data4 = await with_backoff(
                    lambda: self._call_api(short_query),
                    max_attempts=self.max_attempts,
                    base_delay=self._base,
                    max_delay=self._max_delay,
                    jitter=self.jitter,
                    retryable=_is_retryable,
                    on_retry=lambda attempt, exc, delay: logger.debug(
                        "Serper short-name retry %d: %s (wait %.1fs)", attempt, exc, delay,
                    ),
                )
                self._fallback_calls += 1
                short_result = self._extract(
                    data4, business_name, short_query,
                    domain_hint=None, strategy=strategy,
                    agent_name=agent_name, fallback_blocklist=fallback_blocklist,
                )
                if short_result.candidate_emails or short_result.candidate_domain:
                    result = EnrichmentResult(
                        candidate_emails=short_result.candidate_emails,
                        candidate_domain=short_result.candidate_domain,
                        source="serper",
                        query_used=short_query,
                        raw_snippets=short_result.raw_snippets,
                    )

            return result

        except _SerperCreditsError as exc:
            logger.error("Serper credits exhausted — enrichment disabled for remaining records: %s", exc)
            self._credits_exhausted = True
            return EnrichmentResult(source="serper", query_used=query)

    async def _call_api(self, query: str) -> dict:
        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
        }
        payload = {"q": query, "num": 10, "gl": "us", "hl": "en"}

        async with self.session.post(
            "https://google.serper.dev/search",
            json=payload,
            headers=headers,
        ) as resp:
            if resp.status == 401:
                raise PipelineHaltError("Serper API key invalid or missing (401)")
            if resp.status == 400:
                body = await resp.text()
                if "not enough credits" in body.lower():
                    raise _SerperCreditsError(body)
                raise PipelineHaltError(f"Serper bad request (400): {body}")
            if resp.status in (429, 500, 503):
                raise _RetryableHTTPError(resp.status)
            resp.raise_for_status()
            return await resp.json()

    def _extract(
        self,
        data: dict,
        business_name: str,
        query: str,
        domain_hint: str | None = None,
        strategy: str = "without",
        agent_name: str | None = None,
        fallback_blocklist: set[str] | None = None,
    ) -> EnrichmentResult:
        emails: list[str] = []
        snippets: list[str] = []
        domain: str | None = None

        for result in data.get("organic", []):
            snippet = result.get("snippet", "")
            if snippet:
                snippets.append(snippet)
                emails.extend(EMAIL_RE.findall(snippet))

        seen: set[str] = set()
        unique_emails: list[str] = []
        for e in emails:
            lower = e.lower()
            if lower not in seen:
                seen.add(lower)
                unique_emails.append(lower)

        kg = data.get("knowledgeGraph", {})
        if kg and kg.get("website"):
            parsed = urlparse(kg["website"])
            domain = parsed.netloc.lower().lstrip("www.")

        is_fallback_domain = False
        if not domain:
            # Use normalize_business_name so legal suffixes (INC., CORP.) don't
            # corrupt the fuzzy match against short brand domains.
            norm_biz_joined = normalize_business_name(business_name).replace(" ", "")
            blocked = fallback_blocklist or set()
            first_organic_domain: str | None = None
            for result in data.get("organic", []):
                link = result.get("link", "")
                if not link:
                    continue
                netloc = urlparse(link).netloc.lower().lstrip("www.")
                if not netloc:
                    continue
                netloc_base = netloc.rsplit(".", 1)[0] if "." in netloc else netloc
                netloc_norm = netloc_base.replace("-", "")
                # partial_ratio catches short brand domains ("anixter") inside longer
                # normalized names ("anixerpowersolutions") that ratio() would miss.
                if fuzz.partial_ratio(netloc_norm, norm_biz_joined) >= 75:
                    domain = netloc
                    break
                # Track first non-blocked organic domain for with-strategy fallback
                if first_organic_domain is None and netloc not in blocked:
                    first_organic_domain = netloc
            # For with-strategy, fall back to first non-blocked organic domain
            if not domain and strategy == "with" and first_organic_domain:
                domain = first_organic_domain
                is_fallback_domain = True
                logger.debug("Serper using first organic domain as fallback: %s", domain)

        # Split emails into confirmed-domain and subdomain buckets.
        # Unrelated domains are discarded entirely.
        # Also strip any email whose domain is in the blocklist regardless of known_domain.
        blocked = fallback_blocklist or set()
        known_domain = domain or domain_hint
        subdomain_emails: list[str] = []
        if known_domain:
            filtered: list[str] = []
            for e in unique_emails:
                host = e.split("@")[1] if "@" in e else ""
                if e.endswith(f"@{known_domain}"):
                    filtered.append(e)
                elif host.endswith(f".{known_domain}"):
                    subdomain_emails.append(e)
            unique_emails = filtered
        else:
            # No domain context — discard emails from known directory/aggregator domains
            unique_emails = [
                e for e in unique_emails
                if (e.split("@")[1] if "@" in e else "") not in blocked
            ]

        # For "with" strategy, only keep snippet emails whose local part
        # fuzzy-matches the agent name. Unmatched emails are discarded —
        # the producer will generate personal patterns from the domain instead.
        if strategy == "with" and agent_name:
            parts = agent_name.strip().lower().split()
            first = parts[0] if parts else ""
            last = parts[-1] if len(parts) > 1 else ""
            name_variants = [v for v in [
                f"{first}{last}",
                f"{first}.{last}",
                f"{first}_{last}",
                f"{first[0]}{last}" if first else "",
                first,
                last,
            ] if v]
            matched: list[str] = []
            for e in unique_emails:
                local = e.split("@")[0]
                score = max(fuzz.ratio(local, v) for v in name_variants)
                if score >= 75:
                    matched.append(e)
                else:
                    logger.debug(
                        "Serper snippet email %s discarded for with-strategy (best score %d)",
                        e, score,
                    )
            unique_emails = matched

        return EnrichmentResult(
            candidate_emails=unique_emails,
            subdomain_emails=subdomain_emails,
            candidate_domain=domain,
            is_fallback_domain=is_fallback_domain,
            source="serper",
            query_used=query,
            raw_snippets=snippets,
        )

    @staticmethod
    def _build_query(
        business_name: str,
        agent_name: str | None,
        state: str,
        domain_hint: str | None,
        strategy: Literal["with", "without"],
    ) -> str:
        norm_biz = normalize_business_name(business_name)
        loc = _STATE_NAMES.get((state or "").upper(), state or "")
        if strategy == "with" and agent_name:
            first, _, last = parse_name(agent_name)
            agent_q = f"{first} {last}".strip() if first and last else normalize_business_name(agent_name)
            if domain_hint:
                return f"{agent_q} {norm_biz} email site:{domain_hint}"
            return f"{agent_q} {norm_biz} {loc} email contact".strip()
        else:
            if domain_hint:
                return f"site:{domain_hint} contact email"
            return f"{norm_biz} {loc} contact email".strip()


class _RetryableHTTPError(Exception):
    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"HTTP {status}")


class _SerperCreditsError(Exception):
    """Raised when Serper returns 400 'Not enough credits'. Not a pipeline halt."""


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, _RetryableHTTPError):
        return True  # only 429, 500, 503 ever raise this
    if isinstance(exc, aiohttp.ClientResponseError):
        return False  # explicit HTTP errors are not transient
    return isinstance(exc, (aiohttp.ClientConnectionError, asyncio.TimeoutError))
