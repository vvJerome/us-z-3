#!/usr/bin/env python3
"""
run_csv.py — Submit a pipeline CSV of valid emails to ZeroBounce bulk API.

Usage:
    python zerobounce/run_csv.py \
        --input output/mi_p2_validated.csv \
        --seen  output/zerobounce_master.csv \
        --out   output/zerobounce_master.csv \
        --api-key <key>

--seen   Path to existing ZeroBounce results CSV. Emails already in this file
         are skipped so they are never re-submitted.
--out    Where to write (or append) results. Defaults to zerobounce_master.csv
         next to the input file. Using the same path for --seen and --out means
         each run extends the master file without duplication.
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("zerobounce")

ZB_SEND_URL   = "https://bulkapi.zerobounce.net/v2/sendfile"
ZB_STATUS_URL = "https://bulkapi.zerobounce.net/v2/filestatus"
ZB_GET_URL    = "https://bulkapi.zerobounce.net/v2/getfile"

POLL_INTERVAL_S  = 15
MAX_POLL_MINUTES = int(os.environ.get("ZB_MAX_POLL_MINUTES", "480"))

ZB_COLUMNS = [
    "zb_status", "zb_sub_status", "zb_free_email", "zb_did_you_mean",
    "zb_account", "zb_domain", "zb_mx_found", "zb_mx_record", "zb_processed_at",
]

_ZB_FIELD_MAP = {
    "ZB Status": "zb_status", "ZB Sub Status": "zb_sub_status",
    "ZB Sub status": "zb_sub_status",
    "ZB Free Email": "zb_free_email", "ZB Did You Mean": "zb_did_you_mean",
    "Account": "zb_account", "Domain": "zb_domain",
    "MX Found": "zb_mx_found", "MX Record": "zb_mx_record",
    "Processed At": "zb_processed_at",
    "status": "zb_status", "sub_status": "zb_sub_status",
    "free_email": "zb_free_email", "did_you_mean": "zb_did_you_mean",
    "account": "zb_account", "domain": "zb_domain",
    "mx_found": "zb_mx_found", "mx_record": "zb_mx_record",
    "processed_at": "zb_processed_at",
}


def load_seen_emails(seen_path: Path | None) -> set[str]:
    """Load emails already processed from an existing ZB results file."""
    if not seen_path or not seen_path.exists():
        return set()
    seen: set[str] = set()
    with seen_path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            email = (row.get("candidate_email") or row.get("email") or "").strip().lower()
            zb = row.get("zb_status", "").strip()
            if email and zb:
                seen.add(email)
    log.info("Loaded %d already-processed emails from %s", len(seen), seen_path)
    return seen


def load_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_upload_csv(rows: list[dict], skip: set[str]) -> tuple[bytes, list[str]]:
    """Build ZeroBounce upload CSV, skipping already-seen emails."""
    seen: set[str] = set(skip)
    unique_emails: list[str] = []
    for r in rows:
        email = (r.get("candidate_email") or r.get("email") or "").strip().lower()
        if email and email not in seen:
            seen.add(email)
            unique_emails.append(email)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["email_address"])
    for e in unique_emails:
        writer.writerow([e])
    return buf.getvalue().encode(), unique_emails


async def upload(session: aiohttp.ClientSession, api_key: str, csv_bytes: bytes, count: int) -> str:
    form = aiohttp.FormData()
    form.add_field("api_key", api_key)
    form.add_field("has_header_row", "true")
    form.add_field("email_address_column", "1")
    form.add_field("file", csv_bytes, filename="emails.csv", content_type="text/csv")
    async with session.post(ZB_SEND_URL, data=form) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    if not data.get("success"):
        raise RuntimeError(f"Upload failed: {data}")
    file_id: str = data["file_id"]
    log.info("Uploaded %d unique emails — file_id=%s", count, file_id)
    return file_id


async def poll(session: aiohttp.ClientSession, api_key: str, file_id: str) -> None:
    deadline = time.monotonic() + MAX_POLL_MINUTES * 60
    while True:
        async with session.get(ZB_STATUS_URL, params={"api_key": api_key, "file_id": file_id}) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
        status = data.get("file_status", "")
        pct = data.get("percentage_complete", 0)
        log.info("Status: %s (%s%% complete)", status, pct)
        if status == "Complete":
            return
        if status in ("Error", "Deleted"):
            raise RuntimeError(f"ZeroBounce terminal error: {data}")
        if time.monotonic() > deadline:
            raise TimeoutError(f"Timed out after {MAX_POLL_MINUTES} minutes")
        await asyncio.sleep(POLL_INTERVAL_S)


async def download(session: aiohttp.ClientSession, api_key: str, file_id: str) -> list[dict]:
    async with session.get(ZB_GET_URL, params={"api_key": api_key, "file_id": file_id}) as resp:
        resp.raise_for_status()
        content = await resp.text(encoding="utf-8-sig")
    return list(csv.DictReader(io.StringIO(content)))


def merge_and_write(rows: list[dict], zb_results: list[dict], out_path: Path, append: bool) -> int:
    """Merge ZB results into pipeline rows and write to out_path.

    If append=True, rows are appended to an existing file (no second header).
    """
    zb_by_email: dict[str, dict] = {}
    for zb in zb_results:
        email = (
            zb.get("Email Address") or zb.get("email_address") or zb.get("email") or ""
        ).lower().strip()
        if email:
            zb_by_email[email] = zb

    pipeline_cols = list(rows[0].keys()) if rows else []
    fieldnames = pipeline_cols + [c for c in ZB_COLUMNS if c not in pipeline_cols]

    mode = "a" if append else "w"
    with out_path.open(mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not append:
            writer.writeheader()
        for row in rows:
            email_key = (row.get("candidate_email") or row.get("email") or "").lower().strip()
            zb = zb_by_email.get(email_key, {})
            merged = {**row, **{col: "" for col in ZB_COLUMNS}}
            for src, dst in _ZB_FIELD_MAP.items():
                if src in zb:
                    merged[dst] = zb[src]
            writer.writerow(merged)
    return len(rows)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True, help="Input CSV with email column")
    parser.add_argument("--seen", type=Path, default=None, help="Existing ZB results to skip (dedup)")
    parser.add_argument("--out", type=Path, default=None, help="Output/master results file (default: zerobounce_master.csv next to input)")
    parser.add_argument("--api-key", default=os.environ.get("ZEROBOUNCE_API_KEY", ""), help="ZeroBounce API key")
    args = parser.parse_args()

    if not args.api_key:
        log.error("No API key — set $ZEROBOUNCE_API_KEY or pass --api-key")
        sys.exit(1)

    out_path = args.out or (args.input.parent / "zerobounce_master.csv")
    seen_path = args.seen or (out_path if out_path.exists() else None)

    seen_emails = load_seen_emails(seen_path)

    rows = load_csv(args.input)
    log.info("Loaded %d rows from %s", len(rows), args.input)

    csv_bytes, unique_emails = build_upload_csv(rows, seen_emails)
    skipped = len(rows) - len(unique_emails)
    if skipped:
        log.info("Skipped %d already-processed emails", skipped)
    log.info("%d new unique emails to submit", len(unique_emails))

    if not unique_emails:
        log.info("Nothing new to submit — all emails already processed.")
        return

    connector = aiohttp.TCPConnector(limit=2)
    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        file_id = await upload(session, args.api_key, csv_bytes, len(unique_emails))
        # Release rows and csv_bytes from memory during the poll wait — only disk matters now
        del rows, csv_bytes
        await poll(session, args.api_key, file_id)
        zb_results = await download(session, args.api_key, file_id)
        log.info("Downloaded %d result rows from ZeroBounce", len(zb_results))

    # Reload from disk for merge (memory was freed during poll)
    rows = load_csv(args.input)
    append = out_path.exists()
    written = merge_and_write(rows, zb_results, out_path, append=append)
    log.info("Done — %d rows written to %s (append=%s)", written, out_path, append)

    zb_statuses: dict[str, int] = {}
    for zb in zb_results:
        s = (zb.get("ZB Status") or zb.get("ZB Sub status") or zb.get("status") or "unknown").strip()
        zb_statuses[s] = zb_statuses.get(s, 0) + 1
    log.info("ZeroBounce result breakdown: %s", zb_statuses)


if __name__ == "__main__":
    asyncio.run(main())
