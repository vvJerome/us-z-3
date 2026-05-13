#!/usr/bin/env python3
"""
zerobounce_validate.py — Run VALIDATED emails through ZeroBounce bulk API.

Reads all records with final_verdict IN ('valid', 'catch_all') from one or more
pipeline.db files, submits them to ZeroBounce's batch file API, polls until
complete, then writes a merged CSV alongside each database.

Usage:
    # Validate emails from a specific run DB
    python scripts/zerobounce_validate.py --db output/run_20260501/pipeline.db

    # Auto-discover all DBs under output/
    python scripts/zerobounce_validate.py --output-dir output/

    # Override API key at runtime
    python scripts/zerobounce_validate.py --db output/.../pipeline.db --api-key <key>

Output:
    output/<run>/zerobounce_results_<timestamp>.csv
    Columns: all original pipeline columns + zb_status, zb_sub_status,
             zb_free_email, zb_did_you_mean, zb_account, zb_domain,
             zb_mx_found, zb_mx_record, zb_processed_at
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import io
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import aiosqlite

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("zerobounce")

ZB_SEND_URL   = "https://bulkapi.zerobounce.net/v2/sendfile"
ZB_STATUS_URL = "https://bulkapi.zerobounce.net/v2/filestatus"
ZB_GET_URL    = "https://bulkapi.zerobounce.net/v2/getfile"

POLL_INTERVAL_S = 15
MAX_POLL_MINUTES = 120


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def fetch_validated_emails(db_path: Path) -> list[dict]:
    """Return all VALIDATED rows with final_verdict valid or catch_all."""
    async with aiosqlite.connect(str(db_path)) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            """
            SELECT unique_id, business_name, agent_name, state,
                   candidate_email, candidate_domain, mx_provider,
                   final_verdict, racknerd_status, bbops_status,
                   zuhal_status, confidence_score, discovery_source,
                   strategy, created_at, updated_at
              FROM records
             WHERE record_state = 'VALIDATED'
               AND final_verdict IN ('valid', 'catch_all')
               AND candidate_email IS NOT NULL
               AND candidate_email != ''
            """
        ) as cursor:
            return [dict(row) async for row in cursor]


# ---------------------------------------------------------------------------
# ZeroBounce API
# ---------------------------------------------------------------------------

def _build_upload_csv(rows: list[dict]) -> bytes:
    """Build a minimal CSV for ZeroBounce: email_address column only."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["email_address"])
    for row in rows:
        writer.writerow([row["candidate_email"]])
    return buf.getvalue().encode()


async def upload_file(session: aiohttp.ClientSession, api_key: str, csv_bytes: bytes) -> str:
    """Upload CSV to ZeroBounce, return file_id."""
    form = aiohttp.FormData()
    form.add_field("api_key", api_key)
    form.add_field("has_header_row", "true")
    form.add_field("email_address_column", "1")
    form.add_field(
        "file",
        csv_bytes,
        filename="emails.csv",
        content_type="text/csv",
    )
    async with session.post(ZB_SEND_URL, data=form) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    if not data.get("success"):
        raise RuntimeError(f"ZeroBounce upload failed: {data}")
    file_id: str = data["file_id"]
    log.info("Uploaded %d emails — file_id=%s", len(csv_bytes.decode().splitlines()) - 1, file_id)
    return file_id


async def poll_until_complete(session: aiohttp.ClientSession, api_key: str, file_id: str) -> None:
    """Block until ZeroBounce reports the file as Complete."""
    deadline = time.monotonic() + MAX_POLL_MINUTES * 60
    while True:
        async with session.get(
            ZB_STATUS_URL,
            params={"api_key": api_key, "file_id": file_id},
        ) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        status = data.get("file_status", "")
        pct = data.get("percentage_complete", 0)
        log.info("ZeroBounce status: %s (%s%% complete)", status, pct)

        if status == "Complete":
            return
        if status in ("Error", "Deleted"):
            raise RuntimeError(f"ZeroBounce file {file_id} entered terminal state: {data}")
        if time.monotonic() > deadline:
            raise TimeoutError(f"ZeroBounce did not finish within {MAX_POLL_MINUTES} minutes")

        await asyncio.sleep(POLL_INTERVAL_S)


async def download_results(session: aiohttp.ClientSession, api_key: str, file_id: str) -> list[dict]:
    """Download the completed result CSV and parse it into a list of dicts."""
    async with session.get(
        ZB_GET_URL,
        params={"api_key": api_key, "file_id": file_id},
    ) as resp:
        resp.raise_for_status()
        content = await resp.text()

    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


# ---------------------------------------------------------------------------
# Merge and write output
# ---------------------------------------------------------------------------

ZB_COLUMNS = [
    "zb_status",
    "zb_sub_status",
    "zb_free_email",
    "zb_did_you_mean",
    "zb_account",
    "zb_domain",
    "zb_mx_found",
    "zb_mx_record",
    "zb_processed_at",
]

# ZeroBounce result CSV column names → our output column names
_ZB_FIELD_MAP = {
    "ZB Status":        "zb_status",
    "ZB Sub Status":    "zb_sub_status",
    "ZB Free Email":    "zb_free_email",
    "ZB Did You Mean":  "zb_did_you_mean",
    "Account":          "zb_account",
    "Domain":           "zb_domain",
    "MX Found":         "zb_mx_found",
    "MX Record":        "zb_mx_record",
    "Processed At":     "zb_processed_at",
    # Alternate spellings ZeroBounce has used across API versions:
    "status":           "zb_status",
    "sub_status":       "zb_sub_status",
    "free_email":       "zb_free_email",
    "did_you_mean":     "zb_did_you_mean",
    "account":          "zb_account",
    "domain":           "zb_domain",
    "mx_found":         "zb_mx_found",
    "mx_record":        "zb_mx_record",
    "processed_at":     "zb_processed_at",
}


def _extract_zb_fields(zb_row: dict) -> dict:
    out: dict = {col: "" for col in ZB_COLUMNS}
    for src_key, dst_key in _ZB_FIELD_MAP.items():
        if src_key in zb_row and zb_row[src_key]:
            out[dst_key] = zb_row[src_key]
    return out


def merge_and_write(
    db_rows: list[dict],
    zb_results: list[dict],
    out_path: Path,
) -> tuple[int, int]:
    """Join DB rows with ZeroBounce results on email, write CSV. Returns (written, unmatched)."""
    # ZeroBounce returns one row per email — build lookup keyed by email (lowercase)
    zb_by_email: dict[str, dict] = {}
    for zb_row in zb_results:
        # ZeroBounce uses "Email Address" or "email_address" depending on version
        email = (
            zb_row.get("Email Address")
            or zb_row.get("email_address")
            or zb_row.get("email")
            or ""
        ).lower().strip()
        if email:
            zb_by_email[email] = zb_row

    pipeline_cols = [
        "unique_id", "business_name", "agent_name", "state",
        "candidate_email", "candidate_domain", "mx_provider",
        "final_verdict", "racknerd_status", "bbops_status",
        "zuhal_status", "confidence_score", "discovery_source",
        "strategy", "created_at", "updated_at",
    ]
    fieldnames = pipeline_cols + ZB_COLUMNS

    written = 0
    unmatched = 0

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in db_rows:
            email_key = (row.get("candidate_email") or "").lower().strip()
            zb_row = zb_by_email.get(email_key, {})
            if not zb_row:
                unmatched += 1
            merged = {**row, **_extract_zb_fields(zb_row)}
            writer.writerow(merged)
            written += 1

    return written, unmatched


# ---------------------------------------------------------------------------
# Per-DB processing
# ---------------------------------------------------------------------------

async def process_db(db_path: Path, api_key: str, session: aiohttp.ClientSession) -> None:
    log.info("── Processing %s", db_path)

    rows = await fetch_validated_emails(db_path)
    if not rows:
        log.info("  No validated emails found — skipping")
        return

    log.info("  Found %d emails (final_verdict: valid/catch_all)", len(rows))

    # Deduplicate emails before uploading (same email may appear on multiple records)
    seen: set[str] = set()
    unique_rows: list[dict] = []
    for r in rows:
        email = (r.get("candidate_email") or "").lower().strip()
        if email and email not in seen:
            seen.add(email)
            unique_rows.append(r)

    if len(unique_rows) < len(rows):
        log.info("  Deduped to %d unique emails", len(unique_rows))

    csv_bytes = _build_upload_csv(unique_rows)
    file_id = await upload_file(session, api_key, csv_bytes)
    await poll_until_complete(session, api_key, file_id)
    zb_results = await download_results(session, api_key, file_id)
    log.info("  Downloaded %d ZeroBounce result rows", len(zb_results))

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = db_path.parent / f"zerobounce_results_{ts}.csv"
    written, unmatched = merge_and_write(rows, zb_results, out_path)

    log.info("  Written %d rows → %s", written, out_path)
    if unmatched:
        log.warning("  %d rows had no ZeroBounce match (check email column mapping)", unmatched)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run VALIDATED pipeline emails through ZeroBounce batch API"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--db",
        metavar="PATH",
        type=Path,
        help="Path to a single pipeline.db file",
    )
    group.add_argument(
        "--output-dir",
        metavar="DIR",
        type=Path,
        help="Auto-discover all pipeline.db files under this directory",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("ZEROBOUNCE_API_KEY", ""),
        help="ZeroBounce API key (default: $ZEROBOUNCE_API_KEY)",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    api_key = args.api_key
    if not api_key:
        log.error(
            "No ZeroBounce API key. Set $ZEROBOUNCE_API_KEY or pass --api-key"
        )
        sys.exit(1)

    if args.db:
        db_paths = [args.db]
    else:
        db_paths = sorted(args.output_dir.rglob("pipeline.db"))
        if not db_paths:
            log.error("No pipeline.db files found under %s", args.output_dir)
            sys.exit(1)
        log.info("Found %d database(s) under %s", len(db_paths), args.output_dir)

    connector = aiohttp.TCPConnector(limit=4)
    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for db_path in db_paths:
            await process_db(db_path, api_key, session)

    log.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
