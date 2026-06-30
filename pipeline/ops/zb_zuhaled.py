"""Submit /zuhaled CSVs to ZeroBounce, save results to /zerobounced.

Usage:
    # Process a specific file
    scripts/zb_zuhaled.sh --input "jerome/part5_unknown_for_zb.csv"

    # Process every CSV in /zuhaled (in parallel)
    scripts/zb_zuhaled.sh

    # Resume all polling/uploading batches recorded in the manifest
    scripts/zb_zuhaled.sh --resume-all

Reads each input CSV (must have a column named Email/email/candidate_email),
dedups against emails already recorded in the manifest, submits the new
addresses to the ZeroBounce bulk API, polls for completion, and writes the
results to /zerobounced/<same-name>.csv with the original columns plus
zb_status, zb_sub_status, zb_free_email, zb_did_you_mean, zb_account,
zb_domain, zb_mx_found, zb_mx_record, zb_processed_at.

Batches are submitted concurrently; file_ids are persisted to the manifest
immediately after upload so a mid-flight crash can resume without log scraping.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import io
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from pipeline import manifest
from zerobounce.run_csv import (
    ZB_COLUMNS,
    _ZB_FIELD_MAP,
    download,
    poll,
    upload,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("zb_zuhaled")

ROOT = Path(__file__).resolve().parents[2]
ZUHALED = ROOT / "output" / "backup" / "us_output" / "zuhaled"
ZEROBOUNCED = ROOT / "output" / "backup" / "us_output" / "zerobounced"

EMAIL_KEYS = ("Email", "email", "email_address", "candidate_email")
CONFIDENCE_KEYS = ("confidence_score", "domain_confidence")

_PART_RE = re.compile(r"(part\d|w_officer|wo_officer|part1)", re.IGNORECASE)


def email_of(row: dict) -> str:
    for k in EMAIL_KEYS:
        if k in row:
            v = (row[k] or "").strip().lower()
            if v:
                return v
    return ""


def confidence_of(row: dict) -> float | None:
    """Read a confidence score from the row, or None if no confidence column present."""
    for k in CONFIDENCE_KEYS:
        if k in row and str(row[k]).strip():
            try:
                return float(row[k])
            except ValueError:
                return None
    return None


def batch_id_for(path: Path) -> tuple[str, str, str]:
    """Derive (batch_id, operator, part) from a path under ZUHALED."""
    try:
        rel = path.resolve().relative_to(ZUHALED.resolve())
        operator = rel.parts[0] if len(rel.parts) > 1 else ""
    except ValueError:
        operator = ""
    m = _PART_RE.search(path.stem.lower())
    part = m.group(1).lower() if m else ""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"{operator or 'na'}_{path.stem}_{stamp}", operator, part


def build_upload_csv(
    rows: list[dict], skip: set[str], min_confidence: float = 0.0
) -> tuple[bytes, list[str]]:
    seen = set(skip)
    unique: list[str] = []
    for r in rows:
        e = email_of(r)
        if not e or e in seen:
            continue
        # Don't spend a ZeroBounce credit on a low-confidence address. Rows without a
        # confidence column are always submitted (can't gate what we can't read).
        if min_confidence > 0.0:
            conf = confidence_of(r)
            if conf is not None and conf < min_confidence:
                continue
        seen.add(e)
        unique.append(e)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["email_address"])
    for e in unique:
        w.writerow([e])
    return buf.getvalue().encode(), unique


def write_results(in_rows: list[dict], zb_results: list[dict], out_path: Path) -> None:
    zb_by_email: dict[str, dict] = {}
    for zb in zb_results:
        e = (
            zb.get("Email Address")
            or zb.get("email_address")
            or zb.get("email")
            or ""
        ).lower().strip()
        if e:
            zb_by_email[e] = zb

    pipeline_cols = list(in_rows[0].keys()) if in_rows else []
    other_cols = [c for c in pipeline_cols if c.lower() not in {"email", "email_address"}]
    fieldnames = ["email"] + other_cols + list(ZB_COLUMNS)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in in_rows:
            e = email_of(r)
            zb = zb_by_email.get(e, {})
            merged: dict = {"email": e}
            for col in other_cols:
                merged[col] = r.get(col, "")
            for col in ZB_COLUMNS:
                merged[col] = ""
            for src, dst in _ZB_FIELD_MAP.items():
                if src in zb:
                    merged[dst] = zb[src]
            writer.writerow(merged)


async def process_one(
    input_path: Path,
    out_path: Path,
    api_key: str,
    conn: sqlite3.Connection,
    resume_file_id: str | None = None,
    batch_id: str | None = None,
    operator: str = "",
    part: str = "",
    seen_emails: set[str] | None = None,
    min_confidence: float = 0.0,
) -> None:
    with input_path.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    log.info("[%s] Loaded %d rows", input_path.name, len(rows))

    bid = batch_id or batch_id_for(input_path)[0]
    if not resume_file_id:
        manifest.start_batch(conn, bid, operator, part, "zb", str(input_path))

    connector = aiohttp.TCPConnector(limit=2)
    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as sess:
        if resume_file_id:
            log.info("[%s] Resuming ZB job %s", input_path.name, resume_file_id)
            file_id = resume_file_id
        else:
            skip = seen_emails if seen_emails is not None else manifest.seen_by_zb(conn)
            log.info("[%s] Dedup pool: %d emails", input_path.name, len(skip))
            csv_bytes, unique = build_upload_csv(rows, skip, min_confidence)
            log.info(
                "[%s] Submitting %d unique new emails (skipped %d)",
                input_path.name, len(unique), len(rows) - len(unique),
            )
            if not unique:
                log.info("[%s] Nothing new; writing pass-through", input_path.name)
                write_results(rows, [], out_path)
                manifest.finish_batch(conn, bid, row_count=0)
                return
            file_id = await upload(sess, api_key, csv_bytes, len(unique))
            manifest.record_file_id(conn, bid, file_id, len(unique))
            log.info("[%s] Uploaded file_id=%s (persisted to manifest)",
                     input_path.name, file_id)
        await poll(sess, api_key, file_id)
        zb_results = await download(sess, api_key, file_id)
    log.info("[%s] Downloaded %d ZB result rows", input_path.name, len(zb_results))

    write_results(rows, zb_results, out_path)
    log.info("[%s] Wrote %s", input_path.name, out_path)
    manifest.finish_batch(conn, bid, row_count=len(zb_results))


def out_path_for(input_path: Path) -> Path:
    try:
        rel = input_path.resolve().relative_to(ZUHALED.resolve())
        return ZEROBOUNCED / rel
    except ValueError:
        return ZEROBOUNCED / input_path.name


async def main_async() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=str, default=None,
        help="Filename (or path) in /zuhaled. Default: every CSV in /zuhaled, in parallel.",
    )
    parser.add_argument(
        "--api-key", default=os.environ.get("ZEROBOUNCE_API_KEY", ""),
        help="ZeroBounce API key (or set $ZEROBOUNCE_API_KEY)",
    )
    parser.add_argument(
        "--file-id", default=None,
        help="Resume a single submitted ZB job by file_id (requires --input).",
    )
    parser.add_argument(
        "--resume-all", action="store_true",
        help="Resume every batch in the manifest whose status is uploading|polling.",
    )
    parser.add_argument(
        "--db", type=Path, default=manifest.DEFAULT_DB_PATH,
        help=f"Manifest DB (default: {manifest.DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--min-confidence", type=float, default=0.0,
        help="Skip rows whose confidence_score/domain_confidence is below this "
             "(0.0 = submit everything). Saves ZeroBounce credits on weak candidates.",
    )
    args = parser.parse_args()

    if not args.api_key:
        log.error("No API key. Set ZEROBOUNCE_API_KEY or pass --api-key.")
        sys.exit(1)

    conn = manifest.connect(args.db)

    if args.resume_all:
        unfinished = manifest.get_unfinished_batches(conn)
        if not unfinished:
            log.info("No unfinished batches in manifest.")
            return
        tasks = []
        for b in unfinished:
            if not b["zb_file_id"]:
                log.warning("Skipping %s: no zb_file_id recorded", b["batch_id"])
                continue
            ipath = Path(b["input_path"])
            opath = out_path_for(ipath)
            tasks.append(process_one(
                ipath, opath, args.api_key, conn,
                resume_file_id=b["zb_file_id"],
                batch_id=b["batch_id"],
                operator=b["operator"] or "",
                part=b["part"] or "",
            ))
        log.info("Resuming %d unfinished batch(es) in parallel", len(tasks))
        await asyncio.gather(*tasks)
        return

    if args.input:
        p = Path(args.input)
        inputs = [p if p.is_absolute() else ZUHALED / args.input]
    else:
        inputs = sorted(ZUHALED.rglob("*.csv"))

    if not inputs:
        log.error("No CSVs to process in %s", ZUHALED)
        sys.exit(1)

    if args.file_id and len(inputs) != 1:
        log.error("--file-id requires exactly one --input (got %d)", len(inputs))
        sys.exit(1)

    for path in inputs:
        if not path.exists():
            log.error("Input not found: %s", path)
            sys.exit(1)

    seen = manifest.seen_by_zb(conn) if not args.file_id else None
    if seen is not None:
        log.info("Loaded %d already-seen emails from manifest", len(seen))

    tasks = []
    for path in inputs:
        bid, operator, part = batch_id_for(path)
        out = out_path_for(path)
        log.info("=== %s -> %s (batch=%s) ===",
                 path.name, out.relative_to(ZEROBOUNCED) if out.is_relative_to(ZEROBOUNCED) else out, bid)
        tasks.append(process_one(
            path, out, args.api_key, conn,
            resume_file_id=args.file_id,
            batch_id=bid, operator=operator, part=part,
            seen_emails=seen,
            min_confidence=args.min_confidence,
        ))

    if len(tasks) == 1:
        await tasks[0]
    else:
        log.info("Running %d batches in parallel", len(tasks))
        await asyncio.gather(*tasks)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
