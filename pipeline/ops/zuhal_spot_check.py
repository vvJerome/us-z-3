"""
Spot-check Zuhal false negatives against ZeroBounce.

Samples a percentage of VALIDATION_FAILED records where Zuhal said invalid/unknown,
calls ZeroBounce on each, and reports how many ZeroBounce finds valid (false negatives).

Usage:
    python -m pipeline.ops.zuhal_spot_check --db output/run/pipeline.db
    python -m pipeline.ops.zuhal_spot_check --db output/run/pipeline.db --sample 10
    python -m pipeline.ops.zuhal_spot_check --db output/run/pipeline.db --dry-run
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
import random
import sys
from pathlib import Path

import aiohttp
import aiosqlite
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
logger = logging.getLogger("zuhal_spot_check")

ZB_VALIDATE_URL = "https://api.zerobounce.net/v2/validate"
ZB_CREDITS_URL = "https://api.zerobounce.net/v2/getcredits"
ZB_COST_PER_EMAIL = 0.008


async def _get_zb_credits(session: aiohttp.ClientSession, api_key: str) -> int:
    async with session.get(ZB_CREDITS_URL, params={"api_key": api_key}) as r:
        data = await r.json()
        return int(data.get("Credits", 0))


async def _zb_verify(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    api_key: str,
    email: str,
    dry_run: bool,
) -> str:
    if dry_run:
        return "valid"
    async with semaphore:
        try:
            params = {"api_key": api_key, "email": email, "ip_address": ""}
            async with session.get(
                ZB_VALIDATE_URL, params=params, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
            status = data.get("status", "unknown")
            # Normalise to match pipeline verdict labels
            if status == "catch-all":
                status = "catch_all"
            return str(status)
        except Exception as exc:
            logger.warning("ZeroBounce error for %s: %s", email, exc)
            return "error"


async def run(
    db_path: str,
    output_path: str,
    sample_pct: float,
    concurrency: int,
    dry_run: bool,
) -> None:
    api_key = os.environ.get("ZEROBOUNCE_API_KEY", "")
    if not api_key and not dry_run:
        sys.exit("ZEROBOUNCE_API_KEY not set in .env")

    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            """
            SELECT unique_id, business_name, candidate_email, zuhal_status
              FROM records
             WHERE record_state = 'VALIDATION_FAILED'
               AND zuhal_status IN ('invalid', 'error', 'unknown')
               AND candidate_email IS NOT NULL
            """
        ) as cur:
            rows = [dict(r) for r in await cur.fetchall()]

    if not rows:
        sys.exit("No Zuhal invalid/unknown records found in DB.")

    k = max(1, round(len(rows) * sample_pct / 100))
    sample = random.sample(rows, min(k, len(rows)))

    print(f"\n{'─' * 52}")
    print(f"  DB:                    {db_path}")
    print(f"  Zuhal invalid/unknown: {len(rows):,}")
    print(f"  Sample ({sample_pct}%):         {len(sample):,} emails")
    if not dry_run:
        print(f"  Est. cost:             ~${len(sample) * ZB_COST_PER_EMAIL:.2f} (ZeroBounce)")
    print(f"{'─' * 52}\n")

    confirm = input("Proceed? [y/N] ").strip().lower()
    if confirm != "y":
        sys.exit("Aborted.")

    if not dry_run:
        async with aiohttp.ClientSession() as session:
            credits = await _get_zb_credits(session, api_key)
        print(f"ZeroBounce credits available: {credits:,}")
        if credits < len(sample):
            sys.exit(f"Not enough credits ({credits:,} available, {len(sample):,} needed)")

    semaphore = asyncio.Semaphore(concurrency)
    results: list[dict] = []
    completed = 0

    async def _check(rec: dict) -> None:
        nonlocal completed
        async with aiohttp.ClientSession() as s:
            zb = await _zb_verify(s, semaphore, api_key, rec["candidate_email"], dry_run)

        zuhal = rec["zuhal_status"] or "unknown"
        missed = "YES" if zb in ("valid", "catch_all", "catch-all") else "no"

        results.append({
            "email":          rec["candidate_email"],
            "business_name":  rec["business_name"] or "",
            "unique_id":      rec["unique_id"],
            "zuhal_verdict":  zuhal,
            "zb_verdict":     zb,
            "missed":         missed,
        })
        completed += 1
        if completed % 50 == 0 or completed == len(sample):
            logger.info("Progress: %d / %d", completed, len(sample))

    await asyncio.gather(*[_check(r) for r in sample])

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["email", "business_name", "unique_id", "zuhal_verdict", "zb_verdict", "missed"],
        )
        writer.writeheader()
        # Missed emails first so they're easy to spot at the top
        writer.writerows(sorted(results, key=lambda r: r["missed"], reverse=True))

    false_negatives = sum(1 for r in results if r["missed"] == "YES")
    errors = sum(1 for r in results if r["zb_verdict"] == "error")
    checked = len(results) - errors
    fn_pct = false_negatives / checked * 100 if checked else 0.0

    print(f"\n{'─' * 52}")
    print(f"  Checked:         {len(results):,}")
    print(f"  ZB errors:       {errors:,}  (skipped)")
    print(f"  False negatives: {false_negatives:,}  ({fn_pct:.1f}%) ← Zuhal said invalid, ZB says valid")
    print(f"  Output:          {output_path}")
    if fn_pct <= 10:
        print(f"\n  Zuhal false-negative rate OK ({fn_pct:.1f}%)")
    else:
        print(f"\n  WARNING: {fn_pct:.1f}% false-negative rate — Zuhal may be too aggressive")
    print(f"{'─' * 52}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Spot-check Zuhal results against ZeroBounce")
    parser.add_argument("--db",          required=True, help="Path to pipeline.db")
    parser.add_argument("--output",      help="Output CSV path (default: <db_dir>/zuhal_spot_check.csv)")
    parser.add_argument("--sample",      type=float, default=5.0, help="Sample percentage (default: 5)")
    parser.add_argument("--concurrency", type=int,   default=10,  help="Parallel ZB requests (default: 10)")
    parser.add_argument("--dry-run",     action="store_true",     help="Mock ZB calls — no credits used")
    args = parser.parse_args()

    output = args.output or str(Path(args.db).parent / "zuhal_spot_check.csv")
    asyncio.run(run(args.db, output, args.sample, args.concurrency, args.dry_run))


if __name__ == "__main__":
    main()
