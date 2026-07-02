"""Integration tests for schema v12: probe_host/smtp_provider columns + smtp_outcomes DAO."""

import pytest

from pipeline.db import record_smtp_outcome, get_worker_provider_stats


async def test_records_have_fleet_telemetry_columns(db_conn):
    async with db_conn.execute("PRAGMA table_info(records)") as cur:
        cols = {row[1] async for row in cur}
    assert {"probe_host", "smtp_provider"} <= cols


async def test_record_smtp_outcome_increments_status_column(db_conn):
    await record_smtp_outcome(db_conn, "cherry-1", "google", "valid")
    rows = await get_worker_provider_stats(db_conn, "cherry-1")
    assert rows == [
        {"worker_id": "cherry-1", "provider": "google", "valid": 1, "invalid": 0,
         "catch_all": 0, "blocked": 0, "error": 0, "updated_at": rows[0]["updated_at"]}
    ]


async def test_ms_valid_rolls_up_into_valid(db_conn):
    await record_smtp_outcome(db_conn, "cherry-1", "microsoft", "ms_valid")
    rows = await get_worker_provider_stats(db_conn, "cherry-1")
    assert rows[0]["valid"] == 1


async def test_record_smtp_outcome_accumulates_on_conflict(db_conn):
    await record_smtp_outcome(db_conn, "cherry-1", "yahoo", "blocked")
    await record_smtp_outcome(db_conn, "cherry-1", "yahoo", "blocked")
    rows = await get_worker_provider_stats(db_conn, "cherry-1")
    assert rows[0]["blocked"] == 2


async def test_not_run_is_not_recorded(db_conn):
    await record_smtp_outcome(db_conn, "cherry-1", "google", "not_run")
    rows = await get_worker_provider_stats(db_conn)
    assert rows == []


async def test_get_worker_provider_stats_filters_by_worker(db_conn):
    await record_smtp_outcome(db_conn, "cherry-1", "google", "valid")
    await record_smtp_outcome(db_conn, "cherry-2", "google", "invalid")
    rows = await get_worker_provider_stats(db_conn, "cherry-2")
    assert [r["worker_id"] for r in rows] == ["cherry-2"]


async def test_get_worker_provider_stats_unfiltered_returns_all(db_conn):
    await record_smtp_outcome(db_conn, "cherry-2", "google", "invalid")
    await record_smtp_outcome(db_conn, "cherry-1", "google", "valid")
    rows = await get_worker_provider_stats(db_conn)
    assert [r["worker_id"] for r in rows] == ["cherry-1", "cherry-2"]


@pytest.mark.parametrize("status,column", [
    ("valid", "valid"), ("invalid", "invalid"), ("catch_all", "catch_all"),
    ("blocked", "blocked"), ("error", "error"),
])
async def test_each_status_maps_to_its_column(db_conn, status, column):
    await record_smtp_outcome(db_conn, "w", "p", status)
    rows = await get_worker_provider_stats(db_conn, "w")
    assert rows[0][column] == 1
