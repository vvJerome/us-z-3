from __future__ import annotations

import pipeline.harvest as harvest_pkg
from pipeline.harvest import harvest, infer_templates
from pipeline.harvest.extract import extract_emails, extract_officers


# ── extract_emails ────────────────────────────────────────────────────────────

def test_extract_emails_keeps_only_domain_and_subdomains():
    html = """
        <a href="mailto:John.Smith@Acme.com">email</a>
        info@acme.com  hr@careers.acme.com  someone@gmail.com
    """
    assert extract_emails(html, "acme.com") == [
        "john.smith@acme.com", "info@acme.com", "hr@careers.acme.com",
    ]


def test_extract_emails_drops_asset_false_positives_and_dedupes():
    html = "logo@2x.png sprite@3x.jpg info@acme.com info@acme.com"
    assert extract_emails(html, "acme.com") == ["info@acme.com"]


# ── extract_officers ──────────────────────────────────────────────────────────

def test_extract_officers_requires_role_keyword_in_chunk():
    html = """
        <p>Jane Doe, Owner and Founder</p>
        <p>Some Visitor wrote a review</p>
        <script>var Hidden Name = 1;</script>
    """
    assert extract_officers(html) == [("Jane", "Doe")]


# ── infer_templates ───────────────────────────────────────────────────────────

def test_infer_templates_pairs_name_to_harvested_email():
    # john.smith@acme.com + "John Smith" → the first.last house convention.
    assert infer_templates(
        ["john.smith@acme.com"], [("John", "Smith")], "acme.com"
    ) == ["first.last"]


def test_infer_templates_empty_when_no_pairing():
    # An address that matches neither a personal nor a generic template → no convention.
    assert infer_templates(["xq7z@acme.com"], [("John", "Smith")], "acme.com") == []


# ── harvest orchestration (fetch patched — no network) ────────────────────────

async def test_harvest_collects_emails_and_officers(monkeypatch):
    async def fake_fetch_site(domain, **kw):
        return [("https://acme.com/about", "<p>Jane Doe, CEO</p> jane.doe@acme.com")], False

    monkeypatch.setattr(harvest_pkg, "fetch_site", fake_fetch_site)
    result = await harvest("acme.com", rate_limiter=None, timeout_s=1)

    assert result.emails == ["jane.doe@acme.com"]
    assert result.officers == [("Jane", "Doe")]
    assert result.blocked is False


async def test_harvest_propagates_blocked_flag(monkeypatch):
    async def fake_fetch_site(domain, **kw):
        return [], True

    monkeypatch.setattr(harvest_pkg, "fetch_site", fake_fetch_site)
    result = await harvest("acme.com", rate_limiter=None, timeout_s=1)
    assert result.blocked is True
    assert result.emails == []
