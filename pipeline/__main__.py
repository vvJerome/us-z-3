from __future__ import annotations

import asyncio
import csv
import json
import logging
import signal
import sys
import time
from pathlib import Path

import aiodns
import aiohttp

from pipeline.cli import parse_args
from pipeline.config import PipelineConfig
from pipeline.consumers.bbops_async import BbopsAsyncConsumer
from pipeline.consumers.racknerd import RacknerdConfig, RacknerdConsumer
from pipeline.dispatcher import Dispatcher, confidence_tier
from pipeline.producer import ProducerWorker
from pipeline.tunnels.ssh_socks import SshSocksTunnel, TunnelConfig
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.logger import setup_logging, get_logger
from pipeline.utils.rate_limiter import TokenBucket
from pipeline.utils.serper_client import SerperClient
from pipeline.utils.zuhal_client import ZuhalClient
from pipeline import db
from pipeline.metrics import serve_metrics


class _NullRacknerd:
    """Stub used when --no-racknerd is set; always returns not_run so bbops handles validation."""
    async def verify(self, email: str):
        from pipeline.models import BackendVerdict
        return BackendVerdict(status="not_run", message="racknerd disabled", verified_at="")

    def is_up(self) -> bool:
        return False


async def cmd_run(args, config: PipelineConfig) -> None:
    """Execute the pipeline (producer + dispatcher or one of them)."""
    setup_logging(config)
    logger = get_logger("pipeline")

    conn = await db.init_db(config.db_path)
    logger.info("Database initialized: %s", config.db_path)

    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received — stopping workers gracefully")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    session = aiohttp.ClientSession()
    cost_tracker = CostTracker(config.max_cost)
    base_run_id = config.run_id or f"run_{int(time.time())}"
    if config.producer_only:
        run_id = f"{base_run_id}-producer"
    elif config.consumer_only:
        run_id = f"{base_run_id}-dispatcher"
    else:
        run_id = base_run_id

    tasks: list[asyncio.Task] = []
    tunnel: SshSocksTunnel | None = None
    bbops_consumer: BbopsAsyncConsumer | None = None

    try:
        if not config.consumer_only:
            producer = ProducerWorker(config, conn, cost_tracker, session, stop_event)
            tasks.append(asyncio.create_task(producer.run(), name="producer"))
            logger.info("Producer worker started")

        if not config.producer_only:
            # --- Racknerd consumer setup ---
            shared_resolver = aiodns.DNSResolver(timeout=3, tries=1)
            if config.racknerd_enabled and config.racknerd_direct:
                logger.info("Racknerd in direct mode (no SOCKS5 tunnel)")
                tunnel = None
                rk_config = RacknerdConfig(
                    concurrency=config.racknerd_concurrency,
                    smtp_timeout_s=config.racknerd_smtp_timeout_s,
                    direct=True,
                )
                racknerd = RacknerdConsumer(None, rk_config, resolver=shared_resolver)
            elif config.racknerd_enabled:
                tunnel_cfg = TunnelConfig(
                    host=config.racknerd_host,
                    user=config.racknerd_ssh_user,
                    port=config.racknerd_ssh_port,
                    socks_port=config.racknerd_socks_port,
                    ssh_key=config.racknerd_ssh_key,
                    autorestart=True,
                )
                tunnel = SshSocksTunnel(tunnel_cfg)
                logger.info("Starting SSH SOCKS5 tunnel to %s", config.racknerd_host)
                await tunnel.start(ready_timeout_s=30.0)
                logger.info("SSH tunnel ready")
                rk_config = RacknerdConfig(
                    socks_port=config.racknerd_socks_port,
                    concurrency=config.racknerd_concurrency,
                    smtp_timeout_s=config.racknerd_smtp_timeout_s,
                )
                racknerd = RacknerdConsumer(tunnel, rk_config, resolver=shared_resolver)
            else:
                logger.info("Racknerd disabled (--no-racknerd) — bbops + Zuhal only")
                tunnel = None
                racknerd = _NullRacknerd()

            # --- bbops async consumer ---
            bbops_consumer = BbopsAsyncConsumer(
                conn=conn,
                session=session,
                base_url=config.bbops_base_url,
                batch_size=config.bbops_batch_size,
                min_batch_size=config.bbops_min_batch_size,
                max_inflight=config.bbops_max_inflight,
                flush_interval_s=config.bbops_flush_interval_s,
                poll_interval_s=config.bbops_poll_interval_s,
                poll_timeout_s=config.bbops_poll_timeout_s,
                health_fail_threshold=config.bbops_health_fail_threshold,
                health_ok_threshold=config.bbops_health_ok_threshold,
            )
            await bbops_consumer.start()
            await bbops_consumer.recover_inflight()
            logger.info("bbops async consumer started and recovered")

            # --- Zuhal fallback (optional — only if api key is set) ---
            zuhal_client: ZuhalClient | None = None
            if config.zuhal_api_key:
                _zuhal_bucket = TokenBucket(
                    capacity=config.zuhal_rate_limit,
                    refill_rate=config.zuhal_rate_limit / 3600,
                )
                zuhal_client = ZuhalClient(
                    config.zuhal_api_key,
                    session,
                    _zuhal_bucket,
                    concurrency=config.zuhal_concurrency,
                    dry_run=config.dry_run,
                    max_attempts=config.max_attempts,
                    jitter=config.backoff_jitter,
                )
                logger.info(
                    "Zuhal fallback enabled (concurrency=%d, rate_limit=%d/hr)",
                    config.zuhal_concurrency,
                    config.zuhal_rate_limit,
                )
            else:
                logger.info("Zuhal fallback disabled (ZUHAL_API_KEY not set)")

            # --- Serper fallback (dispatcher calls this after patterns exhausted) ---
            dispatcher_serper = SerperClient(
                api_key=config.serper_api_key,
                session=session,
                rate_limiter=TokenBucket(
                    capacity=config.serper_rate_limit,
                    refill_rate=config.serper_rate_limit / 3600,
                ),
                dry_run=config.dry_run,
                max_attempts=config.max_attempts,
            )

            # --- Dispatcher ---
            dispatcher = Dispatcher(
                config=config,
                conn=conn,
                racknerd=racknerd,
                bbops=bbops_consumer,
                cost_tracker=cost_tracker,
                stop_event=stop_event,
                zuhal=zuhal_client,
                serper=dispatcher_serper,
            )
            tasks.append(asyncio.create_task(dispatcher.run(), name="dispatcher"))
            logger.info("Dispatcher started (concurrency=%d)", config.dispatch_concurrency)

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
        # Graceful teardown
        if bbops_consumer:
            await bbops_consumer.stop()
        if tunnel:
            await tunnel.stop()

        # Write final stats
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
        disc_hits_n = sum(
            status_counts.get(s, 0)
            for s in ("DISCOVERED", "VALIDATING", "VALIDATED", "VALIDATION_FAILED", "COST_SKIPPED")
        )

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

        await _write_outputs(conn, config)

        await session.close()
        await conn.close()
        logger.info("Pipeline shutdown complete. Cost: $%.4f", cost_tracker.total_cost)


async def cmd_status(args) -> None:
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
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return

    conn = await db.init_db(db_path)

    if args.dry_run:
        state_map = {
            "discovery_failed": "DISCOVERY_FAILED",
            "validation_failed": "VALIDATION_FAILED",
            "cost_skipped": "COST_SKIPPED",
        }
        state = state_map.get(args.status, args.status.upper())
        async with conn.execute(
            "SELECT COUNT(*) FROM records WHERE record_state = ?", (state,)
        ) as cursor:
            row = await cursor.fetchone()
            count = row[0] if row else 0
        print(f"Would re-queue {count} records with status '{args.status}'")
    else:
        state_map = {
            "discovery_failed": "DISCOVERY_FAILED",
            "validation_failed": "VALIDATION_FAILED",
            "cost_skipped": "COST_SKIPPED",
        }
        state = state_map.get(args.status, args.status.upper())
        count = await db.reset_failed_records(conn, state, args.phase)
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

    verdicts = summary.get("records_by_verdict", {})
    if verdicts:
        print("\nRecords by final verdict:")
        for verdict, count in sorted(verdicts.items()):
            print(f"  {verdict:.<30} {count:>8}")

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


def _is_verified(final_verdict: str | None) -> bool:
    return final_verdict in ("valid", "catch_all")


def _validation_method(zuhal_status: str | None, final_verdict: str | None) -> str:
    if zuhal_status == "ms_valid":
        return "ms_probe"
    if zuhal_status and zuhal_status.startswith("dual_"):
        return "racknerd+bbops"
    if zuhal_status and not zuhal_status.startswith("dual_"):
        return "zuhal_fallback"
    return "unknown"


async def _write_outputs(conn, config: PipelineConfig) -> None:
    logger = get_logger("pipeline")
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- valid_emails.csv ---
    csv_path = output_dir / "valid_emails.csv"
    async with conn.execute(
        """
        SELECT unique_id, business_name, agent_name, state,
               candidate_email, zuhal_status, zuhal_score,
               discovery_source, final_verdict,
               racknerd_status, bbops_status
          FROM records WHERE record_state = 'VALIDATED'
        """
    ) as cursor:
        rows = await cursor.fetchall()

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "unique_id", "business_name", "agent_name", "state",
            "email", "final_verdict", "confidence_tier", "verified",
            "discovery_method", "validation_method",
            "racknerd_status", "bbops_status",
        ])
        for row in rows:
            fv = row["final_verdict"] or row["zuhal_status"]
            writer.writerow([
                row["unique_id"], row["business_name"], row["agent_name"],
                row["state"], row["candidate_email"], fv,
                confidence_tier(int(row["zuhal_score"] or 0)),
                _is_verified(fv),
                row["discovery_source"] or "unknown",
                _validation_method(row["zuhal_status"], fv),
                row["racknerd_status"] or "",
                row["bbops_status"] or "",
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

    config_kwargs: dict = {}
    for field_name in [
        "input_path",
        "limit", "start_offset", "ignore_checkpoint", "chunk_size",
        "producer_only", "consumer_only", "strategy",
        "dns_concurrency", "serper_concurrency",
        "dispatch_concurrency", "dispatch_backend_timeout_s",
        "dispatch_poll_interval_s", "dispatch_chunk_size",
        "racknerd_enabled", "racknerd_direct", "racknerd_host", "racknerd_ssh_user", "racknerd_ssh_key",
        "racknerd_ssh_port", "racknerd_socks_port",
        "racknerd_concurrency", "racknerd_smtp_timeout_s",
        "bbops_base_url", "bbops_batch_size", "bbops_max_inflight",
        "serper_rate_limit",
        "max_attempts", "backoff_base_dns", "backoff_base_serper",
        "backoff_max_dns", "backoff_max_serper", "backoff_jitter",
        "max_cost", "max_consecutive_errors", "dry_run",
        "enrichment_source", "run_id", "notify_pipe",
        "zuhal_concurrency", "zuhal_rate_limit",
    ]:
        val = getattr(args, field_name, None)
        if val is not None:
            config_kwargs[field_name] = val

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
