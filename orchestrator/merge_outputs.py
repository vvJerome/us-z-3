from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

MERGED_COLUMNS = [
    "filing_id",
    "agent_id",
    "business_name",
    "agent_name",
    "state",
    "email",
    "domain",
    "source",
    "validation_status",
    "confidence_tier",
]


def merge(v2_db: Path, out_csv: Path) -> dict[str, int]:
    """Write validated V2 records to a single merged CSV.

    Dedupe key: (filing_id, agent_id, email.lower()). First occurrence wins.
    """
    rows_by_key: dict[tuple[str, str, str], dict[str, str]] = {}
    counts = {"total": 0, "duplicates": 0}

    for row in _read_v2(v2_db):
        key = (row["filing_id"], row["agent_id"], row["email"].lower())
        if key in rows_by_key:
            counts["duplicates"] += 1
            continue
        rows_by_key[key] = row
        counts["total"] += 1

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MERGED_COLUMNS)
        writer.writeheader()
        for row in rows_by_key.values():
            writer.writerow({col: row.get(col, "") for col in MERGED_COLUMNS})

    return counts


def _read_v2(db_path: Path) -> list[dict[str, str]]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            """
            SELECT unique_id, business_name, agent_name, state,
                   candidate_email, candidate_domain, zuhal_status, zuhal_score
              FROM records
             WHERE record_state = 'VALIDATED'
               AND verdict IN ('valid', 'catch_all', 'accept-all', 'ms_valid')
               AND candidate_email IS NOT NULL
               AND candidate_email <> ''
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    out: list[dict[str, str]] = []
    for r in rows:
        filing_id, agent_id = _split_composite(r["unique_id"] or "")
        out.append({
            "filing_id": filing_id,
            "agent_id": agent_id,
            "business_name": r["business_name"] or "",
            "agent_name": r["agent_name"] or "",
            "state": r["state"] or "",
            "email": (r["candidate_email"] or "").strip(),
            "domain": r["candidate_domain"] or "",
            "source": "v2",
            "validation_status": r["zuhal_status"] or "",
            "confidence_tier": _tier(r["zuhal_score"]),
        })
    return out


def _split_composite(composite: str) -> tuple[str, str]:
    if "__" in composite:
        filing, _, agent = composite.partition("__")
        return filing, agent
    return composite, ""


def _tier(score) -> str:
    try:
        n = int(score or 0)
    except (TypeError, ValueError):
        return ""
    if n >= 3:
        return "high"
    if n == 2:
        return "medium"
    return "low"
