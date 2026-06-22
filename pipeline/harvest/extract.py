from __future__ import annotations

import re
from html.parser import HTMLParser

from pipeline.constants import HARVEST_ROLE_KEYWORDS
from pipeline.utils.email_patterns import email_to_template

# RFC-pragmatic, not RFC-complete: good enough to lift addresses out of page text/mailto.
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
# A capitalized two-token "First Last" (allows an internal hyphen in the surname).
_NAME_RE = re.compile(r"\b([A-Z][a-z]+)\s+([A-Z][a-z]+(?:-[A-Z][a-z]+)?)\b")
# Image/asset false positives that match the email regex (e.g. "logo@2x.png").
_ASSET_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".css", ".js")


def extract_emails(html: str, domain: str) -> list[str]:
    """Lower-cased, de-duped emails whose host is `domain` (or a subdomain of it)."""
    domain = domain.lower()
    out: list[str] = []
    for raw in _EMAIL_RE.findall(html):
        e = raw.lower()
        if e.endswith(_ASSET_SUFFIXES):
            continue
        host = e.rpartition("@")[2]
        if host == domain or host.endswith("." + domain):
            if e not in out:
                out.append(e)
    return out


class _TextExtractor(HTMLParser):
    """Collect visible text, dropping <script>/<style> bodies."""

    def __init__(self) -> None:
        super().__init__()
        self._skip = False
        self.chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: object) -> None:
        if tag in ("script", "style"):
            self._skip = True

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style"):
            self._skip = False

    def handle_data(self, data: str) -> None:
        if not self._skip and data.strip():
            self.chunks.append(data.strip())


def extract_officers(html: str) -> list[tuple[str, str]]:
    """Names appearing in a text chunk that also mentions a role keyword.

    ponytail: naive keyword-proximity name grab, not NER. Low recall, acceptable
    precision — upgrade to a real model only if officer quality measurably matters.
    """
    parser = _TextExtractor()
    parser.feed(html)
    out: list[tuple[str, str]] = []
    for chunk in parser.chunks:
        lowered = chunk.lower()
        if not any(kw in lowered for kw in HARVEST_ROLE_KEYWORDS):
            continue
        for first, last in _NAME_RE.findall(chunk):
            pair = (first, last)
            if pair not in out:
                out.append(pair)
    return out


def infer_templates(emails: list[str], officers: list[tuple[str, str]], domain: str) -> list[str]:
    """House email-convention template names, inferred by pairing scraped names to harvested emails.

    e.g. officer ("John","Smith") + email john.smith@domain → template "{first}.{last}".
    Returned in discovery order so the dispatcher can lead candidate generation with them.
    """
    out: list[str] = []
    for email in emails:
        for first, last in officers:
            # parse_name lowercases; scraped names are capitalized — normalize to match.
            t = email_to_template(email, first.lower(), last.lower(), domain)
            if t and t not in out:
                out.append(t)
    return out
