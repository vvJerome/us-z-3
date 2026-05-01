from __future__ import annotations

import asyncio
import csv
import json
import logging
import signal
import sys
import time
from pathlib import Path

import aiohttp

from pipeline.cli import parse_args
from pipeline.config import PipelineConfig
from pipeline.consumer import ConsumerWorker, confidence_tier
from pipeline.producer import ProducerWorker
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.logger import setup_logging, get_logger
from pipeline.utils.rate_limiter import TokenBucket
from pipeline.utils.zuhal_client import ZuhalClient
from pipeline import db
from pipeline.metrics import serve_metrics


async def cmd_run(args, config: PipelineConfig) -> None:
    """Execute the pipeline (producer + consumer or one of them)."""
    setup_logging(config)
    logger = get_logger("pipeline")

    conn = await db.init_db(config.db_path)
    logger.info("Database initialized: %s", config.db_path)

    stop_event = asyncio.Event()

    # Graceful shutdown via signals
    def _signal_handler():
        logger.info("Shutdown signal received — stopping workers gracefully")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for all signals
            pass

    session = aiohttp.ClientSession()
    cost_tracker = CostTracker(config.max_cost)
    base_run_id = config.run_id or f"run_{int(time.time())}"
    # In split mode (producer-only or consumer-only), tag the run_id with the
    # worker role so both processes write to separate stats rows and don't
    # overwrite each other on the shared database.
    if config.producer_only:
        run_id = f"{base_run_id}-producer"
    elif config.consumer_only:
        run_id = f"{base_run_id}-consumer"
    else:
        run_id = base_run_id

    tasks: list[asyncio.Task] = []

    try:
        if not config.consumer_only:
            producer = ProducerWorker(config, conn, cost_tracker, session, stop_event)
            tasks.append(asyncio.create_task(producer.run(), name="producer"))
            logger.info("Producer worker started")

        if not config.producer_only:
            bucket = TokenBucket(
                capacity=config.zuhal_rate_limit,
                refill_rate=config.zuhal_rate_limit / 3600,
            )
            zuhal = ZuhalClient(
                config.zuhal_api_key, session, bucket,
                dry_run=config.dry_run,
                max_attempts=config.max_attempts,
                jitter=config.backoff_jitter,
            )
            consumer = ConsumerWorker(config, conn, cost_tracker, zuhal, stop_event)
            tasks.append(asyncio.create_task(consumer.run(), name="consumer"))
            logger.info("Consumer worker started")

        if not tasks:
            logger.error("No workers to run — check flags")
            return

        metrics_task = asyncio.create_task(
            serve_metrics(conn, stop_event), name="metrics"
        )
        try:
            await asyncio.gather(*tasks)
        finally:
            stop_event.set()
            await metrics_task

    except Exception:
        logger.exception("Pipeline error")
        stop_event.set()
        raise
    finally:
        # Gather record counts to populate all stats columns
        status_counts: dict[str, int] = {}
        async with conn.execute(
            "SELECT record_state, COUNT(*) FROM records GROUP BY record_state"
        ) as _cur:
            async for _row in _cur:
                status_counts[_row[0]] = _row[1]

        total = sum(status_counts.values())
        validated_n = status_counts.get("VALIDATED", 0)
        failed_n = status_counts.get("VALIDATION_FAILED", 0)
        disc_failed_n = status_counts.get("DISCOVERY_FAILED", 0)
        disc_hits_n = max(total - disc_failed_n - status_counts.get("RAW", 0), 0)

        # Skip writing a stats row if nothing was processed (no phantom zero rows)
        if total > 0 or cost_tracker.total_cost > 0:
            await db.upsert_stats(
                conn, run_id,
                estimated_cost_usd=cost_tracker.total_cost,
                total_input=total,
                producer_processed=total,
                discovery_hits=disc_hits_n,
                discovery_misses=disc_failed_n,
                validated=validated_n,
                validation_failed=failed_n,
                **{f"{k}_calls": v for k, v in cost_tracker.counts.items()},
            )

        # Write output files
        await _write_outputs(conn, config)

        await session.close()
        await conn.close()
        logger.info("Pipeline shutdown complete. Cost: $%.4f", cost_tracker.total_cost)


async def cmd_status(args) -> None:
    """Print pipeline status summary."""
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return

    conn = await db.init_db(db_path)

    while True:
        summary = await db.get_status_summary(conn)
        _print_status(summary)

        if not args.watch:
            break
        await asyncio.sleep(args.watch)

    await conn.close()


async def cmd_reset(args) -> None:
    """Re-queue failed records."""
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return

    conn = await db.init_db(db_path)

    if args.dry_run:
        # Count without modifying
        async with conn.execute(
            "SELECT COUNT(*) FROM records WHERE record_state = ?", (args.status,)
        ) as cursor:
            row = await cursor.fetchone()
            count = row[0] if row else 0
        print(f"Would re-queue {count} records with status '{args.status}'")
    else:
        count = await db.reset_failed_records(conn, args.status, args.phase)
        print(f"Re-queued {count} records")

    await conn.close()


def _print_status(summary: dict) -> None:
    print("\n=== Pipeline Status ===\n")
    print(f"Total records: {summary.get('total_records', 0)}")
    print(f"Producer offset: {summary.get('producer_offset', 0)}")
    print(f"Producer done: {summary.get('producer_done', False)}")

    print("\nRecords by state:")
    for status, count in sorted(summary.get("records_by_state", {}).items()):
        print(f"  {status:.<30} {count:>8}")

    failures = summary.get("failures_by_phase", {})
    if failures:
        print("\nFailures by phase:")
        for phase, count in sorted(failures.items()):
            print(f"  {phase:.<30} {count:>8}")

    stats = summary.get("stats")
    if stats:
        cost = stats.get("estimated_cost_usd", 0)
        print(f"\nEstimated cost: ${cost:.4f}")

    print()


def _validation_method(zuhal_status: str | None) -> str:
    if zuhal_status == "ms_valid":
        return "ms_probe"
    if zuhal_status == "bbops_valid":
        return "bbops"
    if zuhal_status in ("valid", "accept-all", "catch_all"):
        return "zuhal"
    return "unknown"


async def _write_outputs(conn, config: PipelineConfig) -> None:
    """Write three output artefacts: pipeline.db (already on disk), results.json, valid_emails.csv."""
    logger = get_logger("pipeline")
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- valid_emails.csv ---
    csv_path = output_dir / "valid_emails.csv"
    async with conn.execute(
        "SELECT unique_id, business_name, agent_name, state, candidate_email, "
        "zuhal_status, zuhal_score, discovery_source FROM records WHERE record_state = 'VALIDATED'"
    ) as cursor:
        rows = await cursor.fetchall()

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "unique_id", "business_name", "agent_name", "state",
            "email", "zuhal_status", "confidence_tier",
            "discovery_method", "validation_method",
        ])
        for row in rows:
            writer.writerow([
                row["unique_id"], row["business_name"], row["agent_name"],
                row["state"], row["candidate_email"], row["zuhal_status"],
                confidence_tier(int(row["zuhal_score"] or 0)),
                row["discovery_source"] or "unknown",
                _validation_method(row["zuhal_status"]),
            ])
    logger.info("Wrote %d validated emails to %s", len(rows), csv_path)

    # --- results.json ---
    summary = await db.get_status_summary(conn)
    results_path = output_dir / "results.json"
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info("Wrote run summary to %s", results_path)


async def main() -> None:
    args = parse_args()

    if args.subcommand == "status":
        await cmd_status(args)
        return

    if args.subcommand == "reset":
        await cmd_reset(args)
        return

    # Build PipelineConfig from args
    config_kwargs: dict = {}
    for field_name in [
        "input_path",
        "limit", "start_offset", "ignore_checkpoint", "chunk_size",
        "producer_only", "consumer_only", "strategy",
        "dns_concurrency", "serper_concurrency", "zuhal_concurrency",
        "zuhal_rate_limit", "serper_rate_limit",
        "consumer_poll_interval",
        "max_attempts", "backoff_base_dns", "backoff_base_serper", "backoff_base_zuhal",
        "backoff_max_dns", "backoff_max_serper", "backoff_max_zuhal", "backoff_jitter",
        "max_cost", "dry_run", "enrichment_source", "run_id", "notify_pipe",
    ]:
        val = getattr(args, field_name, None)
        if val is not None:
            config_kwargs[field_name] = val

    # Resolve output paths: output/<name>/ if --name given, else output/ (overwrites)
    name = getattr(args, "name", None)
    base_dir = Path("output") / name if name else Path("output")
    config_kwargs["output_dir"] = args.output_dir or str(base_dir)
    config_kwargs["db_path"] = args.db or str(base_dir / "pipeline.db")
    config_kwargs["log_dir"] = args.log_dir or str(base_dir / "logs")
    if name and not config_kwargs.get("run_id"):
        config_kwargs["run_id"] = name

    config = PipelineConfig(**config_kwargs)
    await cmd_run(args, config)


if __name__ == "__main__":
    asyncio.run(main())
