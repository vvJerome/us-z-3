"""Standalone Zuhal rescue pass.

Reads VALIDATION_FAILED records where both SMTP backends returned 'invalid'
and the zuhal_status is NULL or 'not_run', then calls Zuhal for each one.
Upgrades VALIDATION_FAILED → VALIDATED when Zuhal confirms deliverable.

Usage:
    scripts/zuhal_rescue.sh --db runs/<run>/v2/pipeline.db

Rate: 20 calls/hour by default (1 per 3 min). Runs until all eligible
records are processed or Ctrl-C.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

import aiohttp
import aiosqlite

from pipeline.db import State
from pipeline.utils.rate_limiter import TokenBucket
from pipeline.utils.zuhal_client import ZuhalClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
)
logger = logging.getLogger("zuhal_rescue")


async def run(db_path: str, rate_limit: int, dry_run: bool) -> None:
    api_key = os.environ.get("ZUHAL_API_KEY", "")
    if not api_key and not dry_run:
        sys.exit("ZUHAL_API_KEY not set. Pass --dry-run to test without calling the API.")

    bucket = TokenBucket(
        capacity=rate_limit,
        refill_rate=rate_limit / 3600,
        initial_tokens=0,
    )

    async with aiohttp.ClientSession() as session:
        client = ZuhalClient(
            api_key, session, bucket,
            concurrency=1,
            dry_run=dry_run,
            max_attempts=1,
        )

        async with aiosqlite.connect(db_path) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA journal_mode=WAL")

            while True:
                async with conn.execute("""
                    SELECT unique_id, candidate_email,
                           racknerd_status, bbops_status, zuhal_status
                    FROM records
                    WHERE record_state = ?
                      AND racknerd_status = 'invalid'
                      AND bbops_status   = 'invalid'
                      AND (zuhal_status IS NULL OR zuhal_status = 'not_run'
                           OR zuhal_status LIKE 'dual_%')
                    LIMIT 1
                """, (State.VALIDATION_FAILED,)) as cur:
                    row = await cur.fetchone()

                if row is None:
                    logger.info("No more eligible records. Done.")
                    break

                uid = row["unique_id"]
                email = row["candidate_email"]
                logger.info("Probing %s → %s", uid, email)

                try:
                    result = await client.validate(email)
                except Exception as exc:
                    logger.warning("Zuhal error for %s: %s — skipping", email, exc)
                    await conn.execute(
                        "UPDATE records SET zuhal_status = 'error' WHERE unique_id = ?",
                        (uid,),
                    )
                    await conn.commit()
                    continue

                status = result.status if hasattr(result, "status") else getattr(result, "verdict", "error")
                if status == "accept-all":
                    status = "catch_all"

                logger.info("  → %s: %s", email, status)

                if status in ("valid", "catch_all"):
                    await conn.execute("""
                        UPDATE records
                        SET record_state  = ?,
                            zuhal_status  = ?,
                            final_verdict = ?
                        WHERE unique_id = ?
                    """, (State.VALIDATED, status, status, uid))
                    logger.info("  Upgraded to VALIDATED")
                else:
                    await conn.execute(
                        "UPDATE records SET zuhal_status = ? WHERE unique_id = ?",
                        (status, uid),
                    )

                await conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Zuhal rescue pass over VALIDATION_FAILED records")
    parser.add_argument("--db", required=True, help="Path to pipeline.db")
    parser.add_argument("--rate-limit", type=int, default=20, help="Zuhal calls per hour (default: 20)")
    parser.add_argument("--dry-run", action="store_true", help="Mock Zuhal calls (no API key needed)")
    args = parser.parse_args()

    asyncio.run(run(args.db, args.rate_limit, args.dry_run))


if __name__ == "__main__":
    main()
