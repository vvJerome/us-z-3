from __future__ import annotations

import re
from typing import Literal

from pipeline.constants import MAX_WITHOUT_CANDIDATES

# Template names used as keys in pattern_stats.
# Personal templates: sorted by empirical likelihood (most common first).
_PERSONAL_TEMPLATES: list[str] = [
    "first.last",
    "flast",
    "firstlast",
    "first",
    "f.last",
    "first_last",
    "firstl",
    "last.first",
    "lastfirst",
    "first.l",
    "lfirst",
    "last",
    "lastf",
]

_GENERIC_TEMPLATES: list[str] = [
    "info",
    "contact",
    "hello",
    "office",
    "admin",
    "support",
    "sales",
    "hi",
]


def _expand_personal(template: str, first: str, last: str, domain: str) -> str | None:
    if not first or not last or not domain:
        return None
    f = first[0]
    li = last[0]
    mapping = {
        "first.last": f"{first}.{last}@{domain}",
        "flast":      f"{f}{last}@{domain}",
        "firstlast":  f"{first}{last}@{domain}",
        "first":      f"{first}@{domain}",
        "f.last":     f"{f}.{last}@{domain}",
        "first_last": f"{first}_{last}@{domain}",
        "firstl":     f"{first}{li}@{domain}",
        "last.first": f"{last}.{first}@{domain}",
        "lastfirst":  f"{last}{first}@{domain}",
        "first.l":    f"{first}.{li}@{domain}",
        "lfirst":     f"{li}{first}@{domain}",
        "last":       f"{last}@{domain}",
        "lastf":      f"{last}{f}@{domain}",
    }
    return mapping.get(template)


# Common given-name <-> diminutive groups. Bidirectional: any member maps to the
# others. ponytail: a static map of the frequent cases, not a name-science library —
# research shows nickname expansion has unproven yield, so we keep it small and rank
# these variants last. Grow the list if misses warrant it.
_NICKNAME_GROUPS: list[set[str]] = [
    {"robert", "bob", "rob", "bobby"}, {"william", "will", "bill", "billy"},
    {"richard", "rick", "rich", "dick"}, {"james", "jim", "jimmy", "jamie"},
    {"john", "jack", "johnny"}, {"michael", "mike", "mick"},
    {"charles", "charlie", "chuck"}, {"thomas", "tom", "tommy"},
    {"joseph", "joe", "joey"}, {"edward", "ed", "eddie", "ted"},
    {"daniel", "dan", "danny"}, {"matthew", "matt"}, {"anthony", "tony"},
    {"christopher", "chris"}, {"nicholas", "nick"}, {"benjamin", "ben"},
    {"elizabeth", "liz", "beth", "betty"}, {"margaret", "maggie", "meg", "peggy"},
    {"katherine", "kate", "kathy", "katie"}, {"jennifer", "jen", "jenny"},
    {"patricia", "pat", "patty", "tricia"}, {"deborah", "deb", "debbie"},
    {"susan", "sue", "suzie"}, {"rebecca", "becca", "becky"},
]
_NICKNAME_MAP: dict[str, list[str]] = {
    name: sorted(group - {name})
    for group in _NICKNAME_GROUPS
    for name in group
}


def _nickname_variants(first: str) -> list[str]:
    """Known diminutives/given-name forms for a first name, or [] if none."""
    return _NICKNAME_MAP.get(first.lower(), []) if first else []


def _surname_variants(last: str) -> list[str]:
    """Single-part surnames from a compound/hyphenated last name, or [] if simple.

    "smith-jones" -> ["smith", "jones"]; the raw form is handled by the caller.
    """
    if not last:
        return []
    parts = [p for p in re.split(r"[-\s]+", last) if p]
    return parts if len(parts) > 1 else []


def generate_personal_patterns(first: str, last: str, domain: str) -> list[str]:
    return [
        email
        for t in _PERSONAL_TEMPLATES
        if (email := _expand_personal(t, first, last, domain)) is not None
    ]


def generate_generic_patterns(domain: str) -> list[str]:
    if not domain:
        return []
    return [f"{t}@{domain}" for t in _GENERIC_TEMPLATES]


def generate_ranked_candidates(
    first: str,
    last: str,
    domain: str,
    strategy: Literal["with", "without"],
    max_candidates: int = 5,
    rankings: list[dict] | None = None,
) -> list[str]:
    """Generate top-N ranked email candidates based on strategy.

    rankings: list of dicts with 'template', 'success_count', 'total_count' from
    pattern_stats for the current mx_provider. When provided, templates are reordered
    by descending success rate before the top-N slice.
    """
    if strategy == "with":
        templates = _reorder_personal(rankings)
        # Compound surname ("smith-jones", "de la cruz"): companies usually pick one
        # part or drop the separator. Lead with the top-ranked template for the raw
        # surname AND each part, so every part surfaces within the cap before the
        # lower-ranked templates of the raw surname fill the remaining slots.
        surnames = [last, *_surname_variants(last)]
        lead = [
            email
            for sv in surnames
            if (email := _expand_personal(templates[0], first, sv, domain)) is not None
        ]
        # Nickname/given-name forms (Bob<->Robert) for the raw surname only — ranked
        # after the primary leads, before lower-ranked templates, so they make the cap.
        nick = [
            email
            for fn in _nickname_variants(first)
            if (email := _expand_personal(templates[0], fn, last, domain)) is not None
        ]
        rest = [
            email
            for t in templates[1:]
            if (email := _expand_personal(t, first, last, domain)) is not None
        ]
        return list(dict.fromkeys(lead + nick + rest))[:max_candidates]
    else:
        templates = _reorder_generic(rankings)
        if not domain:
            return []
        return [f"{t}@{domain}" for t in templates][:MAX_WITHOUT_CANDIDATES]


def _reorder_personal(rankings: list[dict] | None) -> list[str]:
    if not rankings:
        return _PERSONAL_TEMPLATES
    return _apply_rankings(_PERSONAL_TEMPLATES, rankings)


def _reorder_generic(rankings: list[dict] | None) -> list[str]:
    if not rankings:
        return _GENERIC_TEMPLATES
    return _apply_rankings(_GENERIC_TEMPLATES, rankings)


def email_to_template(email: str, first: str, last: str, domain: str) -> str | None:
    """Reverse-map a generated email back to its template name, or None if unknown."""
    local, _, _ = email.partition("@")
    for t in _PERSONAL_TEMPLATES + _GENERIC_TEMPLATES:
        if t in _GENERIC_TEMPLATES:
            if local == t:
                return t
        else:
            candidate = _expand_personal(t, first, last, domain)
            if candidate and candidate.split("@")[0] == local:
                return t
    return None


def _apply_rankings(base: list[str], rankings: list[dict]) -> list[str]:
    rate: dict[str, float] = {}
    for r in rankings:
        total = r.get("total_count") or 0
        if total > 0:
            rate[r["template"]] = r.get("success_count", 0) / total

    def sort_key(t: str) -> float:
        if t in rate:
            return -rate[t]  # higher rate = ranked first (negate for ascending sort)
        return 0.0  # unseen templates stay at their original position (0 = neutral)

    # Stable sort: unseen templates keep their original relative order.
    seen = sorted([t for t in base if t in rate], key=lambda t: -rate[t])
    unseen = [t for t in base if t not in rate]
    return seen + unseen
