"""TypedDict definitions for aiosqlite/sqlite3 row shapes.

Usage:
    from pipeline.db.row_types import RecordRow, as_record_row

    row = cast(RecordRow, raw_row)
    email = row["candidate_email"]   # typed str | None, not Any
"""
from __future__ import annotations

from typing import TypedDict


class RecordRow(TypedDict):
    unique_id: str
    business_name: str | None
    agent_name: str | None
    state: str | None
    record_state: str
    candidate_email: str | None
    candidate_emails: str | None
    candidate_domain: str | None
    mx_provider: str | None
    strategy: str | None
    discovery_source: str | None
    domain_confidence: float | None
    owner_confidence: float | None
    confidence_score: int | None
    racknerd_status: str | None
    bbops_status: str | None
    final_verdict: str | None
    zuhal_status: str | None
    canonical_status: str | None
    canonical_source: str | None
    canonical_sub_status: str | None
    reconciliation_path: str | None
    zb_status: str | None
    zb_sub_status: str | None
    dispatch_attempts: int | None
    requeue_count: int | None
    tunnel_requeue_count: int | None
    bbops_requeue_count: int | None
    failure_reason: str | None
    process_trace: str | None
    serper_enriched: int | None
    id: int
