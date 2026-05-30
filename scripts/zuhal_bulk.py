#!/usr/bin/env python3
"""Submit a NEEDS_ZUHAL CSV to the Zuhal Bulk API in 1k-email chunks.

Uses direct API calls (not pipeline.utils.zuhal_client.bulk_validate, which
has latent bugs around nested-JSON response parsing and a status-string
mismatch — production silently falls back to single-validate).

Usage:
    python scripts/zuhal_bulk.py \
        --input output/.../alpha_part2_needs_zuhal_all.csv \
        --out-dir output/.../zuhaled
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import io
import logging
import os
import sys
from collections import Counter
from pathlib import Path

import aiohttp

BASE = "https://zuhal.io/api/v1/bulk"
CHUNK_SIZE = 1000
CONCURRENCY = 5
POLL_INTERVAL_S = 15
MAX_POLL_MINUTES = 30
TERMINAL_STATUSES = {"completed", "failed", "error"}

_EMAIL_COLUMNS = ("candidate_email", "Email", "email")


def _input_has_required_columns(header: list[str]) -> bool:
    """Inputs must carry unique_id and an email column. Prevents the Jerome P5
    failure mode where the input was {Email}-only and standalone results lost EIDs.
    """
    return "unique_id" in header and any(c in header for c in _EMAIL_COLUMNS)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("zuhal_bulk")


async def upload(session: aiohttp.ClientSession, key: str, emails: list[str]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["email"])
    for e in emails:
        w.writerow([e])
    form = aiohttp.FormData()
    form.add_field("file", buf.getvalue().encode(), filename="emails.csv", content_type="text/csv")
    async with session.post(
        f"{BASE}/upload",
        data=form,
        headers={"Authorization": f"Bearer {key}"},
    ) as resp:
        body = await resp.json()
        if resp.status != 200:
            raise RuntimeError(f"upload {resp.status}: {body}")
    job_id = body.get("data", {}).get("job_id")
    if not job_id:
        raise RuntimeError(f"upload returned no job_id: {body}")
    return job_id


async def poll(session: aiohttp.ClientSession, key: str, job_id: str) -> None:
    deadline = asyncio.get_running_loop().time() + MAX_POLL_MINUTES * 60
    while True:
        async with session.get(
            f"{BASE}/status/{job_id}",
            headers={"Authorization": f"Bearer {key}"},
        ) as resp:
            body = await resp.json()
        st = body.get("data", {}).get("status", "")
        if st == "completed":
            return
        if st in TERMINAL_STATUSES:
            raise RuntimeError(f"job {job_id} terminal status {st}: {body}")
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError(f"job {job_id} did not complete within {MAX_POLL_MINUTES}min")
        await asyncio.sleep(POLL_INTERVAL_S)


async def download(session: aiohttp.ClientSession, key: str, job_id: str) -> dict[str, str]:
    async with session.get(
        f"{BASE}/download/{job_id}",
        headers={"Authorization": f"Bearer {key}"},
    ) as resp:
        body = await resp.json()
    url = body.get("data", {}).get("download_link")
    if not url:
        raise RuntimeError(f"download for {job_id} returned no link: {body}")
    async with session.get(url, headers={"Authorization": f"Bearer {key}"}) as resp:
        text = await resp.text(encoding="utf-8-sig")
    out: dict[str, str] = {}
    for r in csv.DictReader(io.StringIO(text)):
        e = (r.get("email") or r.get("Email") or "").strip().lower()
        v = (r.get("status") or r.get("Status") or r.get("email_status") or "unknown").strip().lower()
        if v == "accept-all":
            v = "catch_all"
        if e:
            out[e] = v
    return out


async def process_chunk(session, key, idx, total, emails) -> dict[str, str]:
    job_id = await upload(session, key, emails)
    log.info("[chunk %d/%d] uploaded %d emails -> %s", idx, total, len(emails), job_id)
    await poll(session, key, job_id)
    results = await download(session, key, job_id)
    log.info("[chunk %d/%d] downloaded %d results", idx, total, len(results))
    return results


async def main_async() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY)
    parser.add_argument(
        "--allow-email-only", action="store_true",
        help="Allow inputs lacking unique_id (results will lose EID linkage).",
    )
    args = parser.parse_args()

    key = os.environ.get("ZUHAL_API_KEY", "")
    if not key:
        log.error("ZUHAL_API_KEY not set")
        sys.exit(1)
    if not args.input.exists():
        log.error("input not found: %s", args.input)
        sys.exit(1)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.out_dir / f"{args.input.stem}.zuhal.csv"

    rows: list[dict] = []
    with args.input.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        if not _input_has_required_columns(header):
            log.error(
                "input %s missing required columns. Got %r. "
                "Need 'unique_id' plus one of {'candidate_email','Email','email'}. "
                "Pass --allow-email-only to bypass (results lose EID linkage).",
                args.input.name, header,
            )
            if not args.allow_email_only:
                sys.exit(2)
        for r in reader:
            rows.append(r)
    log.info("loaded %d rows from %s", len(rows), args.input.name)

    seen: set[str] = set()
    unique_emails: list[str] = []
    for r in rows:
        e = (r.get("candidate_email") or r.get("Email") or r.get("email") or "").strip().lower()
        if e and e not in seen:
            seen.add(e)
            unique_emails.append(e)
    log.info("unique emails to submit: %d", len(unique_emails))

    chunks = [unique_emails[i : i + args.chunk_size] for i in range(0, len(unique_emails), args.chunk_size)]
    log.info("splitting into %d chunks of %d (concurrency=%d)", len(chunks), args.chunk_size, args.concurrency)

    sem = asyncio.Semaphore(args.concurrency)
    all_results: dict[str, str] = {}
    failures = 0

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=900)) as session:
        async def bounded(idx, chunk):
            nonlocal failures
            async with sem:
                try:
                    return await process_chunk(session, key, idx, len(chunks), chunk)
                except Exception as e:
                    log.error("chunk %d failed: %s", idx, e)
                    failures += 1
                    return {}

        tasks = [bounded(i + 1, c) for i, c in enumerate(chunks)]
        for r in await asyncio.gather(*tasks):
            all_results.update(r)

    log.info("aggregated %d verdicts from %d chunks (%d failures)", len(all_results), len(chunks), failures)

    verdict_counts: Counter = Counter()
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "unique_id", "business_name", "agent_name",
            "candidate_email", "candidate_domain", "mx_provider",
            "zuhal_verdict",
        ])
        for r in rows:
            e = (r.get("candidate_email") or r.get("Email") or r.get("email") or "").strip().lower()
            verdict = all_results.get(e, "no_result") if e else ""
            verdict_counts[verdict] += 1
            writer.writerow([
                r.get("unique_id", ""),
                r.get("business_name", ""),
                r.get("agent_name", ""),
                e,
                r.get("candidate_domain", ""),
                r.get("mx_provider", ""),
                verdict,
            ])
    log.info("wrote %s", out_path)
    log.info("verdict mix:")
    for v, n in verdict_counts.most_common():
        log.info("  %-12s %d", v, n)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
