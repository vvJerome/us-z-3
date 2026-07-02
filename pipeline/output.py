"""Pipeline output helpers: status display and CSV/JSON writes."""
from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import cast

import aiosqlite

from pipeline._dispatch_helpers import confidence_tier
from pipeline.config import PipelineConfig
from pipeline.db.row_types import RecordRow
from pipeline.utils.text import domain_confidence_tier
from pipeline.utils.owner_inference import owner_confidence_tier
from pipeline import db

logger = logging.getLogger("pipeline")


def print_status(summary: dict) -> None:
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


async def write_outputs(conn: aiosqlite.Connection, config: PipelineConfig) -> None:
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
        rows = [cast(RecordRow, r) for r in await cursor.fetchall()]

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
