from __future__ import annotations

import asyncio
import csv
import json
import logging
import signal
import time
from pathlib import Path

import aiodns
import aiohttp

from pipeline.cli import parse_args
from pipeline.config import PipelineConfig
from pipeline.consumers.bbops_async import BbopsAsyncConsumer
from pipeline.consumers.racknerd import RacknerdConfig, RacknerdConsumer
from pipeline.dispatcher import Dispatcher
from pipeline._dispatch_helpers import confidence_tier
from pipeline.utils.text import domain_confidence_tier
from pipeline.utils.owner_inference import owner_confidence_tier
from pipeline.zuhal_dispatcher import ZuhalDispatcher
from pipeline.producer import ProducerWorker
from pipeline.tunnels.ssh_socks import SshSocksTunnel, TunnelConfig
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.logger import setup_logging
from pipeline.utils.rate_limiter import TokenBucket
from pipeline.utils.serper_client import SerperClient
from pipeline.utils.zuhal_client import ZuhalClient
from pipeline import db
from pipeline.constants import (
    DNS_RESOLVER_TIMEOUT_S,
    DNS_RESOLVER_TRIES,
    SERPER_BUCKET_CAPACITY,
    SERPER_BUCKET_REFILL_RATE,
)
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
    logger = logging.getLogger("pipeline")

    conn = await db.init_db(config.db_path)
    logger.info("Database initialized: %s", config.db_path)

    cache_conn = conn
    if config.enrichment_cache_db:
        cache_conn = await db.init_db(config.enrichment_cache_db)
        logger.info("Persistent Serper cache enabled: %s", config.enrichment_cache_db)

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
            producer = ProducerWorker(config, conn, cost_tracker, session, stop_event, cache_conn=cache_conn)
            tasks.append(asyncio.create_task(producer.run(), name="producer"))
            logger.info("Producer worker started")

        if not config.producer_only:
            # --- Racknerd consumer setup ---
            shared_resolver = aiodns.DNSResolver(timeout=DNS_RESOLVER_TIMEOUT_S, tries=DNS_RESOLVER_TRIES)
            rk_helo_kwargs: dict = {}
            if config.racknerd_helo_hostname:
                rk_helo_kwargs["helo_hostname"] = config.racknerd_helo_hostname

            if config.racknerd_enabled and config.racknerd_direct:
                logger.info("Racknerd in direct mode (no SOCKS5 tunnel)")
                tunnel = None
                rk_config = RacknerdConfig(
                    concurrency=config.racknerd_concurrency,
                    smtp_timeout_s=config.racknerd_smtp_timeout_s,
                    direct=True,
                    **rk_helo_kwargs,
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
                    **rk_helo_kwargs,
                )
                racknerd = RacknerdConsumer(tunnel, rk_config, resolver=shared_resolver)
                logger.info("Racknerd MAIL FROM domain: %s", rk_config.helo_hostname)
            else:
                logger.info("Racknerd disabled (--no-racknerd) — bbops + Zuhal only")
                tunnel = None
                racknerd = _NullRacknerd()  # type: ignore[assignment]

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
                    initial_tokens=0,
                )
                zuhal_client = ZuhalClient(
                    config.zuhal_api_key,
                    session,
                    _zuhal_bucket,
                    concurrency=config.zuhal_concurrency,
                    dry_run=config.dry_run,
                    max_attempts=1,  # paid call fires once — never retried in-call
                    jitter=config.backoff_jitter,
                )
                logger.info(
                    "Zuhal fallback enabled (concurrency=%d, rate_limit=%d/hr)",
                    config.zuhal_concurrency,
                    config.zuhal_rate_limit,
                )
                remaining = await zuhal_client.check_credits()
                if remaining is not None:
                    logger.info("Zuhal credits OK — %d remaining", remaining)
                else:
                    logger.info("Zuhal credits check passed (balance not reported)")
            else:
                logger.info("Zuhal fallback disabled (ZUHAL_API_KEY not set)")

            # --- Serper fallback (dispatcher calls this after patterns exhausted) ---
            dispatcher_serper = SerperClient(
                api_key=config.serper_api_key,
                session=session,
                rate_limiter=TokenBucket(
                    capacity=SERPER_BUCKET_CAPACITY,
                    refill_rate=SERPER_BUCKET_REFILL_RATE,
                    initial_tokens=0,
                ),
                dry_run=config.dry_run,
                max_attempts=5,
                ignore_cache=config.ignore_cache,
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
                cache_conn=cache_conn,
            )
            smtp_done_event = asyncio.Event()

            async def _smtp_dispatcher_task() -> None:
                try:
                    await dispatcher.run()
                finally:
                    smtp_done_event.set()

            tasks.append(asyncio.create_task(_smtp_dispatcher_task(), name="dispatcher"))
            logger.info("Dispatcher started (concurrency=%d)", config.dispatch_concurrency)

            if zuhal_client is not None and config.zuhal_decoupled:
                zuhal_dispatcher = ZuhalDispatcher(
                    config=config,
                    conn=conn,
                    zuhal=zuhal_client,
                    cost_tracker=cost_tracker,
                    stop_event=stop_event,
                    smtp_done_event=smtp_done_event,
                )
                tasks.append(asyncio.create_task(zuhal_dispatcher.run(), name="zuhal-dispatcher"))
                logger.info(
                    "Zuhal dispatcher started — decoupled rescue worker (concurrency=%d)",
                    config.zuhal_concurrency,
                )

        if not tasks:
            logger.error("No workers to run — check flags")
            return

        metrics_task = asyncio.create_task(
            serve_metrics(conn, stop_event), name="metrics"
        )

        if config.master_db:
            from pipeline.ops.master_db import flush_from_pipeline_db

            async def _master_db_flush_loop() -> None:
                while not stop_event.is_set():
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(stop_event.wait()), timeout=60.0
                        )
                    except asyncio.TimeoutError:
                        pass
                    if stop_event.is_set():
                        break
                    try:
                        ins, upd = await asyncio.to_thread(
                            flush_from_pipeline_db, config.master_db, config.db_path
                        )
                        if ins or upd:
                            logger.info(
                                "Master DB flush: %d new, %d updated → %s",
                                ins, upd, config.master_db,
                            )
                    except Exception as exc:
                        logger.warning("Master DB flush failed: %s", exc)

            tasks.append(asyncio.create_task(_master_db_flush_loop(), name="master-db-flush"))
            logger.info("Master DB flush enabled — flushing every 500 records to %s", config.master_db)

        try:
            await asyncio.gather(*tasks)
        finally:
            stop_event.set()
            await metrics_task
            # Final flush on shutdown
            if config.master_db:
                try:
                    ins, upd = await asyncio.to_thread(
                        flush_from_pipeline_db, config.master_db, config.db_path, flush_every=0
                    )
                    if ins or upd:
                        logger.info(
                            "Master DB final flush: %d new, %d updated → %s",
                            ins, upd, config.master_db,
                        )
                except Exception as exc:
                    logger.warning("Master DB final flush failed: %s", exc)

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
                serper_cache_hits=cost_tracker.cache_hits,
                **{f"{k}_calls": v for k, v in cost_tracker.counts.items()},
            )

        await _write_outputs(conn, config)

        await session.close()
        await conn.close()
        if cache_conn is not conn:
            await cache_conn.close()
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

    by_state = summary.get("records_by_state", {})
    t1 = summary.get("terminal_last_1min", 0)
    t5 = summary.get("terminal_last_5min", 0)
    t15 = summary.get("terminal_last_15min", 0)
    r1 = t1 / 1.0
    r5 = t5 / 5.0
    r15 = t15 / 15.0

    pending_states = ("RAW", "DISCOVERING", "DISCOVERED", "VALIDATING", "NEEDS_ZUHAL", "ZUHAL_VALIDATING")
    pending = sum(by_state.get(s, 0) for s in pending_states)
    retry_backlog = summary.get("retry_backlog", 0)
    fresh = pending - retry_backlog

    needs_zuhal = by_state.get("NEEDS_ZUHAL", 0) + by_state.get("ZUHAL_VALIDATING", 0)
    zuhal_rate = summary.get("zuhal_terminal_last_5min", 0) / 5.0

    terminal_by_state = summary.get("terminal_by_state_5min", {})

    if any((r1, r5, r15)):
        print("\nThroughput:")
        print(f"  1 min:  {r1:>7.1f} records/min")
        print(f"  5 min:  {r5:>7.1f} records/min")
        print(f"  15 min: {r15:>7.1f} records/min")

        if terminal_by_state:
            print("\n  Per-state (last 5 min):")
            label_map = {
                "VALIDATED": "validated",
                "VALIDATION_FAILED": "validation_failed",
                "DISCOVERY_FAILED": "discovery_failed",
                "COST_SKIPPED": "cost_skipped",
            }
            for state, label in label_map.items():
                count = terminal_by_state.get(state, 0)
                if count:
                    print(f"    {label:.<26} {count / 5.0:>6.1f}/min")

        if needs_zuhal and zuhal_rate > 0:
            print(f"\n  Zuhal queue: {needs_zuhal:,} pending  ({zuhal_rate:.1f}/min draining)")

    if pending > 0:
        rate = r5 or r15 or r1
        if rate > 0:
            eta_min = pending / rate
            if eta_min < 60:
                eta_str = f"{eta_min:.0f} min"
            elif eta_min < 1440:
                eta_str = f"{eta_min / 60:.1f} hr"
            else:
                eta_str = f"{eta_min / 1440:.1f} days"
            pending_detail = f"{fresh:,} fresh + {retry_backlog:,} retries" if retry_backlog else f"{pending:,}"
            print(f"\nPending: {pending_detail}  →  ETA: {eta_str}")
        else:
            print(f"\nPending: {pending:,}  (throughput window empty — ETA unavailable)")
    else:
        print("\nAll records processed.")

    print()


def _is_verified(final_verdict: str | None) -> bool:
    return final_verdict in ("valid", "catch_all")


def _validation_method(
    racknerd_status: str | None,
    bbops_status: str | None,
    zuhal_status: str | None,
) -> str:
    if zuhal_status == "ms_valid":
        return "ms_probe"
    if zuhal_status and zuhal_status.startswith("dual_"):
        rk_ok = racknerd_status in ("valid", "catch_all")
        bb_ok = bbops_status in ("valid", "catch_all")
        if rk_ok and bb_ok:
            return "smtp_both"
        if rk_ok:
            return "smtp_racknerd"
        if bb_ok:
            return "smtp_bbops"
        return "smtp_both"
    if zuhal_status in ("valid", "catch_all", "accept-all"):
        return "zuhal_rescue"
    return "unknown"


def _zuhal_verdict(zuhal_status: str | None) -> str:
    if not zuhal_status:
        return "not_run"
    if zuhal_status == "ms_valid" or zuhal_status.startswith("dual_"):
        return "not_run"
    return zuhal_status


async def _write_outputs(conn, config: PipelineConfig) -> None:
    logger = logging.getLogger("pipeline")
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- valid_emails.csv ---
    csv_path = output_dir / "valid_emails.csv"
    async with conn.execute(
        """
        SELECT unique_id, business_name, agent_name, state,
               candidate_email, zuhal_status, confidence_score, domain_confidence,
               owner_confidence, discovery_source, final_verdict,
               racknerd_status, bbops_status,
               canonical_status, canonical_source, zb_status, zb_sub_status
          FROM records WHERE record_state = 'VALIDATED'
        """
    ) as cursor:
        rows = await cursor.fetchall()

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "unique_id", "business_name", "agent_name", "state",
            "email", "canonical_status", "canonical_source",
            "final_verdict", "confidence_tier", "confidence_score",
            "domain_confidence", "domain_confidence_tier",
            "owner_confidence", "owner_confidence_tier", "verified",
            "discovery_method", "validation_method",
            "racknerd_verdict", "bbops_verdict", "zuhal_verdict",
            "zb_status", "zb_sub_status",
        ])
        for row in rows:
            fv = row["final_verdict"] or row["zuhal_status"]
            rk = row["racknerd_status"] or ""
            bb = row["bbops_status"] or ""
            zs = row["zuhal_status"]
            dc = row["domain_confidence"]
            oc = row["owner_confidence"]
            writer.writerow([
                row["unique_id"], row["business_name"], row["agent_name"],
                row["state"], row["candidate_email"],
                row["canonical_status"] or "", row["canonical_source"] or "",
                fv,
                confidence_tier(int(row["confidence_score"] or 0)),
                int(row["confidence_score"] or 0),
                round(dc, 3) if dc is not None else "",
                domain_confidence_tier(dc) if dc is not None else "",
                round(oc, 3) if oc is not None else "",
                owner_confidence_tier(oc) if oc is not None else "",
                _is_verified(fv),
                row["discovery_source"] or "unknown",
                _validation_method(rk, bb, zs),
                rk,
                bb,
                _zuhal_verdict(zs),
                row["zb_status"] or "", row["zb_sub_status"] or "",
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
        "racknerd_concurrency", "racknerd_smtp_timeout_s", "racknerd_helo_hostname",
        "harvest_enabled",
        "bbops_base_url", "bbops_batch_size", "bbops_min_batch_size", "bbops_max_inflight",
        "max_attempts", "backoff_jitter",
        "max_cost", "max_dispatch_attempts", "max_requeue_count", "dry_run",
        "ignore_cache", "run_id", "notify_pipe",
        "zuhal_concurrency", "zuhal_rate_limit", "zuhal_on_both_invalid",
    ]:
        val = getattr(args, field_name, None)
        if val is not None:
            config_kwargs[field_name] = val

    name = getattr(args, "name", None)
    base_dir = Path("output") / name if name else Path("output")
    config_kwargs["output_dir"] = args.output_dir or str(base_dir)
    config_kwargs["db_path"] = args.db or str(base_dir / "pipeline.db")
    config_kwargs["log_dir"] = args.log_dir or str(base_dir / "logs")
    if getattr(args, "master_db", None):
        config_kwargs["master_db"] = args.master_db
    if getattr(args, "enrichment_cache_db", None):
        config_kwargs["enrichment_cache_db"] = args.enrichment_cache_db
    if name and not config_kwargs.get("run_id"):
        config_kwargs["run_id"] = name

    config = PipelineConfig(**config_kwargs)
    await cmd_run(args, config)


if __name__ == "__main__":
    asyncio.run(main())
