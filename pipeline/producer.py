from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from pathlib import Path

import time

import aiodns
import aiohttp
import aiosqlite

from pipeline.config import PipelineConfig
from pipeline.constants import FALLBACK_DOMAIN_BLOCKLIST
from pipeline.models import InputRecord, PipelineHaltError
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.dns_probe import probe_domains
from pipeline.utils.email_patterns import generate_ranked_candidates
from pipeline.utils.rate_limiter import TokenBucket
from pipeline.utils.serper_client import SerperClient
from pipeline.utils.text import assign_email_strategy, is_org_agent, parse_name
from pipeline.utils.notify import create_notify_pipe, signal_consumer
from pipeline import db
from pipeline.db import State

logger = logging.getLogger("pipeline.producer")


def _is_transient_enrichment_error(exc: Exception) -> bool:
    if isinstance(exc, (asyncio.TimeoutError, aiohttp.ClientConnectionError)):
        return True
    msg = str(exc)
    return any(code in msg for code in ("HTTP 429", "HTTP 500", "HTTP 503"))


class ProducerWorker:
    def __init__(
        self,
        config: PipelineConfig,
        conn: aiosqlite.Connection,
        cost_tracker: CostTracker,
        session: aiohttp.ClientSession,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self.config = config
        self.conn = conn
        self.cost_tracker = cost_tracker
        self.session = session
        self.stop_event = stop_event or asyncio.Event()

        self._dns_sem = asyncio.Semaphore(config.dns_concurrency)
        self._enrichment_sem = asyncio.Semaphore(config.serper_concurrency)
        # Shared resolver: one c-ares context per producer → avoids per-record setup
        # overhead and enables negative-TTL caching across records.
        self._dns_resolver = aiodns.DNSResolver(timeout=3, tries=1)

        # Dynamic fallback domain blocklist.
        # Starts from the static seed; grows when a domain is seen as first-organic
        # fallback for 2+ different businesses within this run.
        self._fallback_blocklist: set[str] = set(FALLBACK_DOMAIN_BLOCKLIST)
        self._fallback_seen: Counter[str] = Counter()
        self._notify_pipe: Path | None = None

        _serper_bucket = TokenBucket(
            capacity=config.serper_rate_limit,
            refill_rate=config.serper_rate_limit / 3600,
        )

        self._serper = SerperClient(
            config.serper_api_key, session, _serper_bucket,
            dry_run=config.dry_run,
            max_attempts=config.max_attempts,
            jitter=config.backoff_jitter,
        )

    async def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await db.upsert_producer_heartbeat(self.conn)
            except Exception:
                pass
            try:
                await asyncio.wait_for(asyncio.shield(self.stop_event.wait()), timeout=30.0)
            except asyncio.TimeoutError:
                pass

    async def run(self) -> None:
        config = self.config

        # Determine start offset
        if config.ignore_checkpoint:
            offset = config.start_offset
        else:
            saved = await db.get_checkpoint(self.conn, "producer_offset")
            offset = int(saved) if saved else config.start_offset

        if config.notify_pipe:
            self._notify_pipe = Path(config.notify_pipe)
            await create_notify_pipe(self._notify_pipe)

        logger.info("Producer starting at offset %d", offset)

        _hb = asyncio.create_task(self._heartbeat_loop(), name="producer-heartbeat")

        input_path = Path(config.input_path)
        if not input_path.exists():
            logger.error("Input file not found: %s", input_path)
            self.stop_event.set()
            raise FileNotFoundError(f"Input file not found: {input_path}")

        total_processed = 0

        with open(input_path, "r", encoding="utf-8") as f:
            # Skip to offset
            for _ in range(offset):
                line = f.readline()
                if not line:
                    logger.info("Input file shorter than offset %d — nothing to do", offset)
                    await db.upsert_checkpoint(self.conn, "producer_done", "true")
                    return

            while not self.stop_event.is_set():
                chunk: list[InputRecord] = []

                # Respect --limit: only read as many as needed
                remaining = config.limit - total_processed if config.limit else config.chunk_size
                read_size = min(config.chunk_size, remaining)

                for _ in range(read_size):
                    line = f.readline()
                    if not line:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = InputRecord.from_dict(json.loads(line))
                        chunk.append(record)
                    except (json.JSONDecodeError, KeyError, ValueError) as exc:
                        logger.warning("Skipping invalid record at offset %d: %s", offset + total_processed, exc)

                if not chunk:
                    logger.info("Producer exhausted input file at offset %d", offset)
                    break

                # Process chunk concurrently
                results = await self._process_chunk(chunk)

                # Atomic write + checkpoint advance
                new_offset = offset + len(chunk)
                await db.insert_records_batch(self.conn, results, new_offset)
                if self._notify_pipe:
                    await signal_consumer(self._notify_pipe)
                offset = new_offset

                total_processed += len(chunk)
                logger.info(
                    "Chunk written: %d records, offset now %d, total processed %d",
                    len(chunk), offset, total_processed,
                )

                # Check cost ceiling
                if self.cost_tracker.ceiling_reached():
                    logger.warning("Cost ceiling reached — producer stopping")
                    break

                # Check limit
                if config.limit and total_processed >= config.limit:
                    logger.info("Producer limit reached (%d/%d records)", total_processed, config.limit)
                    break

        if not self.stop_event.is_set() and self.config.max_discovery_retries > 0:
            await self._run_discovery_retries()

        _hb.cancel()
        await db.upsert_checkpoint(self.conn, "producer_done", "true")
        logger.info("Producer finished. Total processed: %d", total_processed)

    async def _run_discovery_retries(self) -> None:
        for attempt in range(1, self.config.max_discovery_retries + 1):
            retried = 0
            # Drain all pending_discovery records this round, chunk by chunk
            while True:
                rows = await db.fetch_pending_discovery(self.conn, limit=self.config.chunk_size)
                if not rows:
                    break
                retried += len(rows)
                attempts_by_id = {row["unique_id"]: (row["discovery_attempts"] or 1) for row in rows}
                records = [
                    InputRecord(
                        unique_id=row["unique_id"],
                        business_name=row["business_name"] or "",
                        agent_name=row["agent_name"] or "",
                        state=row["state"] or "",
                        jurisdiction=row["jurisdiction"] or "",
                        position_type=row["position_type"] or "",
                        name_entity_type=row["name_entity_type"] or "",
                    )
                    for row in rows
                ]
                results = await asyncio.gather(*[self._process_record(r) for r in records])
                for result in results:
                    result["discovery_attempts"] = attempts_by_id.get(result["unique_id"], 1) + 1
                    await db.update_record_discovery(self.conn, result)
                if self.cost_tracker.ceiling_reached():
                    return

            if retried == 0:
                logger.info("Discovery retry %d/%d: nothing pending — done", attempt, self.config.max_discovery_retries)
                break
            logger.info("Discovery retry %d/%d: retried %d records", attempt, self.config.max_discovery_retries, retried)

    async def _process_chunk(self, chunk: list[InputRecord]) -> list[dict]:
        tasks = [self._process_record(record) for record in chunk]
        results = await asyncio.gather(*tasks)
        counts = Counter(r["record_state"] for r in results)
        logger.info(
            "Chunk buckets — pv:%d pd:%d df:%d",
            counts.get(State.DISCOVERED, 0),
            counts.get(State.DISCOVERING, 0),
            counts.get(State.DISCOVERY_FAILED, 0),
        )
        return results

    async def _process_record(self, record: InputRecord) -> dict:
        config = self.config

        # Determine strategy
        if config.strategy == "auto":
            strategy = assign_email_strategy(record)
        else:
            strategy = config.strategy

        org_agent = is_org_agent(record)
        result = self._base_result(record, strategy, org_agent)

        # Short-circuit: existing email
        existing_email = record.email_biz or record.email_agent
        if existing_email:
            result["candidate_email"] = existing_email
            result["candidate_emails"] = json.dumps([existing_email])
            result["discovery_source"] = "input"
            result["record_state"] = State.DISCOVERED
            result["_trace"].append({"stage": "input", "outcome": "hit", "ms": 0})
            result["process_trace"] = json.dumps(result.pop("_trace"))
            return result

        # Phase 1: DNS probe
        _dns_t0 = time.monotonic()
        domain, mx_host = await probe_domains(
            record.business_name,
            self._dns_sem,
            resolver=self._dns_resolver,
            max_attempts=config.max_attempts,
            jitter=config.backoff_jitter,
            dry_run=config.dry_run,
        )
        _dns_ms = int((time.monotonic() - _dns_t0) * 1000)

        candidate_emails: list[str] = []
        had_transient_error = False
        transient_error_source = ""

        # Parse name once for both pattern generation and search queries
        first, _, last = parse_name(record.agent_name)
        parsed_agent = f"{first} {last}".strip() if first and last else record.agent_name

        result["_trace"].append({"stage": "dns", "outcome": "hit" if domain else "miss", "ms": _dns_ms, "domain": domain})

        if domain:
            result["candidate_domain"] = domain
            result["discovery_source"] = "dns"
            result["mx_provider"] = mx_host

            # Query per-MX success stats to reorder templates by historical hit rate
            rankings = await db.get_pattern_rankings(self.conn, mx_host) if mx_host else []
            patterns = generate_ranked_candidates(first, last, domain, strategy, rankings=rankings)
            candidate_emails.extend(patterns)
            result["_trace"].append({"stage": "patterns", "outcome": "gen", "n": len(patterns)})

        # Phase 2: Serper enrichment (find emails in snippets; find domain if DNS missed)
        enrichment_emails: list[str] = []
        enrichment_domain: str | None = None
        all_subdomain_emails: list[str] = []

        try:
            async with self._enrichment_sem:
                serper_result = await self._serper.enrich(
                    record.business_name,
                    parsed_agent if strategy == "with" else None,
                    record.state,
                    domain,
                    strategy,
                    fallback_blocklist=self._fallback_blocklist,
                    conn=self.conn,
                )
                self.cost_tracker.record_call("serper")
                # Record any extra cost from site: fallback retries
                for _ in range(self._serper._fallback_calls):
                    self.cost_tracker.record_call("serper")
                self._serper._fallback_calls = 0

            _serper_ms = int((time.monotonic() - _dns_t0) * 1000) - _dns_ms
            enrichment_emails.extend(serper_result.candidate_emails)
            all_subdomain_emails.extend(serper_result.subdomain_emails)
            result["_trace"].append({"stage": "serper", "outcome": "hit" if serper_result.candidate_emails or serper_result.candidate_domain else "miss", "ms": _serper_ms})
            if not domain and serper_result.candidate_domain:
                enrichment_domain = serper_result.candidate_domain
                result["discovery_source"] = "serper"
                if serper_result.is_fallback_domain:
                    self._record_fallback_domain(serper_result.candidate_domain)
            elif not domain and serper_result.candidate_emails:
                # Email found in snippet with no domain — still came from Serper
                result["discovery_source"] = "serper"

        except PipelineHaltError:
            raise
        except Exception as exc:
            if _is_transient_enrichment_error(exc):
                had_transient_error = True
                transient_error_source = "serper"
                result["_trace"].append({"stage": "serper", "outcome": "error"})
                logger.warning("Serper transient error for %s: %s", record.unique_id, exc)
            else:
                result["_trace"].append({"stage": "serper", "outcome": "error"})
                logger.warning("Serper error for %s: %s", record.unique_id, exc)

        # If enrichment found a domain but DNS didn't, generate patterns from it
        if not domain and enrichment_domain:
            result["candidate_domain"] = enrichment_domain
            patterns = generate_ranked_candidates(first, last, enrichment_domain, strategy, rankings=[])
            candidate_emails.extend(patterns)

        # Prepend any emails found directly in search snippets (they're already validated-looking)
        all_candidates: list[str] = []
        seen: set[str] = set()
        for email in enrichment_emails + candidate_emails:
            lower = email.lower()
            if lower not in seen:
                seen.add(lower)
                all_candidates.append(lower)

        # Cap at reasonable limit
        all_candidates = all_candidates[:10]

        if all_subdomain_emails:
            result["subdomain_emails"] = json.dumps(list(dict.fromkeys(all_subdomain_emails)))

        effective_domain = domain or enrichment_domain
        result["discovery_attempts"] = 1

        if all_candidates:
            result["candidate_emails"] = json.dumps(all_candidates)
            result["candidate_email"] = all_candidates[0]
            result["record_state"] = State.DISCOVERED
            logger.info(
                "BUCKET pv | %s | domain=%s emails=%d",
                record.unique_id, effective_domain, len(all_candidates),
            )
        elif had_transient_error and not effective_domain:
            result["record_state"] = State.DISCOVERING
            logger.warning(
                "BUCKET pd | %s | transient=%s",
                record.unique_id, transient_error_source,
            )
        else:
            result["record_state"] = State.DISCOVERY_FAILED
            logger.info(
                "BUCKET df | %s | domain=%s",
                record.unique_id, effective_domain,
            )

        result["process_trace"] = json.dumps(result.pop("_trace"))
        return result

    def _record_fallback_domain(self, domain: str) -> None:
        """Track domains used as first-organic fallback. Promotes to blocklist on 2nd hit."""
        self._fallback_seen[domain] += 1
        if self._fallback_seen[domain] >= 2:
            if domain not in self._fallback_blocklist:
                self._fallback_blocklist.add(domain)
                logger.info("Dynamic blocklist: added %s (seen %d times as fallback)", domain, self._fallback_seen[domain])

    @staticmethod
    def _base_result(record: InputRecord, strategy: str, org_agent: bool) -> dict:
        return {
            "unique_id": record.unique_id,
            "business_name": record.business_name,
            "agent_name": record.agent_name,
            "state": record.state,
            "jurisdiction": record.jurisdiction,
            "position_type": record.position_type,
            "name_entity_type": record.name_entity_type,
            "candidate_email": None,
            "candidate_emails": None,
            "subdomain_emails": None,
            "candidate_domain": None,
            "discovery_source": None,
            "discovery_attempts": 0,
            "strategy": strategy,
            "is_org_agent": org_agent,
            "mx_provider": None,
            "record_state": State.RAW,
            "_trace": [],
        }
