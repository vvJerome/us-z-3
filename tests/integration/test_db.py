"""Integration tests for database operations using in-memory SQLite."""

import json
from pathlib import Path

import aiosqlite
import pytest

from pipeline import db
from pipeline.db import State


pytestmark = pytest.mark.asyncio


@pytest.fixture
async def test_db() -> aiosqlite.Connection:
    """Create an in-memory test database with schema."""
    conn = await aiosqlite.connect(":memory:")
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA busy_timeout=10000")
    await conn.execute("PRAGMA cache_size=-64000")
    await conn.execute("PRAGMA mmap_size=268435456")
    await conn.execute("PRAGMA wal_autocheckpoint=1000")
    await conn.execute("PRAGMA temp_store=MEMORY")
    await conn.executescript(db.SCHEMA_SQL)
    await conn.commit()
    conn.row_factory = aiosqlite.Row
    yield conn
    await conn.close()


class TestInitDb:
    """Test database initialization."""

    async def test_init_db_creates_schema(self, tmp_path: Path):
        """init_db creates all required tables."""
        db_path = tmp_path / "test.db"
        conn = await db.init_db(db_path)

        # Check that tables exist
        async with conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ) as cursor:
            tables = [row[0] async for row in cursor]

        assert "records" in tables
        assert "checkpoints" in tables
        assert "stats" in tables
        assert "failures" in tables
        assert "pattern_stats" in tables
        assert "enrichment_cache" in tables

        await conn.close()

    async def test_init_db_creates_indexes(self, tmp_path: Path):
        """init_db creates required indexes."""
        db_path = tmp_path / "test.db"
        conn = await db.init_db(db_path)

        async with conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ) as cursor:
            indexes = [row[0] async for row in cursor]

        assert "idx_records_state" in indexes
        assert "idx_records_unique_id" in indexes

        await conn.close()

    async def test_migration_v10_to_v11_adds_owner_confidence(self, tmp_path: Path):
        """An existing v10 DB (no owner_confidence) gains the column via the v11 ALTER migration."""
        db_path = tmp_path / "old.db"
        # Build a real v11 DB, then roll it back to a pre-v11 state: drop the column and the version.
        conn = await db.init_db(db_path)
        await conn.execute("INSERT INTO records (unique_id, record_state) VALUES ('r1', 'RAW')")
        await conn.execute("ALTER TABLE records DROP COLUMN owner_confidence")
        await conn.execute("PRAGMA user_version = 10")
        await conn.commit()
        await conn.close()

        # Re-open: init_db must run _V11_MIGRATIONS and re-add the column.
        conn = await db.init_db(db_path)
        try:
            async with conn.execute("PRAGMA table_info(records)") as cur:
                cols = {r[1] for r in await cur.fetchall()}
            assert "owner_confidence" in cols
            async with conn.execute("PRAGMA user_version") as cur:
                assert (await cur.fetchone())[0] == db.SCHEMA_VERSION
            # Existing row survived the migration; new column defaults to NULL.
            async with conn.execute(
                "SELECT owner_confidence FROM records WHERE unique_id = 'r1'"
            ) as cur:
                assert (await cur.fetchone())[0] is None
        finally:
            await conn.close()


class TestCheckpoints:
    """Test checkpoint get/set operations."""

    async def test_get_nonexistent_checkpoint(self, test_db):
        """Getting nonexistent checkpoint returns None."""
        result = await db.get_checkpoint(test_db, "missing_key")
        assert result is None

    async def test_upsert_and_get_checkpoint(self, test_db):
        """Checkpoint is stored and retrieved correctly."""
        await db.upsert_checkpoint(test_db, "producer_offset", "42")
        result = await db.get_checkpoint(test_db, "producer_offset")
        assert result == "42"

    async def test_update_checkpoint(self, test_db):
        """Checkpoint can be updated."""
        await db.upsert_checkpoint(test_db, "key", "value1")
        await db.upsert_checkpoint(test_db, "key", "value2")
        result = await db.get_checkpoint(test_db, "key")
        assert result == "value2"

    async def test_multiple_checkpoints(self, test_db):
        """Multiple checkpoints can coexist."""
        await db.upsert_checkpoint(test_db, "offset", "100")
        await db.upsert_checkpoint(test_db, "done", "true")
        assert await db.get_checkpoint(test_db, "offset") == "100"
        assert await db.get_checkpoint(test_db, "done") == "true"


class TestInsertRecords:
    """Test record insertion."""

    async def test_insert_single_record(self, test_db):
        """Single record is inserted correctly."""
        records = [
            {
                "unique_id": "id1",
                "business_name": "Acme Corp",
                "agent_name": "John Doe",
                "state": "NY",
                "record_state": State.RAW,
            }
        ]
        await db.insert_records_batch(test_db, records, new_offset=1)

        async with test_db.execute(
            "SELECT * FROM records WHERE unique_id = ?", ("id1",)
        ) as cursor:
            row = await cursor.fetchone()

        assert row is not None
        assert row["business_name"] == "Acme Corp"
        assert row["agent_name"] == "John Doe"

    async def test_insert_persists_owner_confidence(self, test_db):
        """owner_confidence (schema v11) round-trips through insert_records_batch."""
        records = [{
            "unique_id": "oc1", "business_name": "Smith Plumbing LLC",
            "agent_name": "John Smith", "state": "NC",
            "owner_confidence": 0.9, "record_state": State.DISCOVERED,
        }]
        await db.insert_records_batch(test_db, records, new_offset=1)

        async with test_db.execute(
            "SELECT owner_confidence FROM records WHERE unique_id = ?", ("oc1",)
        ) as cursor:
            row = await cursor.fetchone()
        assert row["owner_confidence"] == 0.9

    async def test_insert_batch(self, test_db):
        """Multiple records inserted atomically."""
        records = [
            {"unique_id": f"id{i}", "business_name": f"Corp{i}", "record_state": State.RAW}
            for i in range(5)
        ]
        await db.insert_records_batch(test_db, records, new_offset=5)

        async with test_db.execute("SELECT COUNT(*) FROM records") as cursor:
            count = (await cursor.fetchone())[0]

        assert count == 5

    async def test_insert_updates_checkpoint(self, test_db):
        """insert_records_batch updates producer_offset checkpoint."""
        records = [{"unique_id": "id1", "record_state": State.RAW}]
        await db.insert_records_batch(test_db, records, new_offset=42)

        offset = await db.get_checkpoint(test_db, "producer_offset")
        assert offset == "42"

    async def test_insert_duplicate_unique_id_ignored(self, test_db):
        """Duplicate unique_id is ignored (INSERT OR IGNORE)."""
        records1 = [
            {"unique_id": "id1", "business_name": "Corp1", "record_state": State.RAW}
        ]
        await db.insert_records_batch(test_db, records1, new_offset=1)

        records2 = [
            {"unique_id": "id1", "business_name": "Corp2", "record_state": State.RAW}
        ]
        await db.insert_records_batch(test_db, records2, new_offset=2)

        async with test_db.execute(
            "SELECT business_name FROM records WHERE unique_id = ?", ("id1",)
        ) as cursor:
            row = await cursor.fetchone()

        # Original value preserved
        assert row[0] == "Corp1"


class TestFetchPendingValidation:
    """Test atomic claiming of validation records."""

    async def test_fetch_pending_returns_discovered(self, test_db):
        """fetch_pending_validation returns DISCOVERED records."""
        await test_db.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("id1", State.DISCOVERED),
        )
        await test_db.commit()

        rows = await db.fetch_pending_validation(test_db, limit=10)
        assert len(rows) == 1
        assert rows[0]["unique_id"] == "id1"

    async def test_fetch_pending_claims_atomically(self, test_db):
        """fetch_pending_validation changes state to VALIDATING."""
        await test_db.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("id1", State.DISCOVERED),
        )
        await test_db.commit()

        rows = await db.fetch_pending_validation(test_db, limit=10)
        assert rows[0]["record_state"] == "VALIDATING"

    async def test_fetch_pending_respects_limit(self, test_db):
        """fetch_pending_validation respects limit parameter."""
        for i in range(5):
            await test_db.execute(
                "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
                (f"id{i}", State.DISCOVERED),
            )
        await test_db.commit()

        rows = await db.fetch_pending_validation(test_db, limit=2)
        assert len(rows) == 2

    async def test_fetch_pending_skips_other_states(self, test_db):
        """fetch_pending_validation only returns DISCOVERED, not other states."""
        await test_db.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("id1", State.VALIDATING),
        )
        await test_db.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("id2", State.DISCOVERED),
        )
        await test_db.commit()

        rows = await db.fetch_pending_validation(test_db, limit=10)
        assert len(rows) == 1
        assert rows[0]["unique_id"] == "id2"

    async def test_fetch_pending_empty(self, test_db):
        """fetch_pending_validation returns empty when no DISCOVERED."""
        rows = await db.fetch_pending_validation(test_db, limit=10)
        assert rows == []


class TestHasPendingValidation:
    """Test non-claiming existence check."""

    async def test_has_pending_returns_true(self, test_db):
        """has_pending_validation returns True if DISCOVERED exists."""
        await test_db.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("id1", State.DISCOVERED),
        )
        await test_db.commit()

        result = await db.has_pending_validation(test_db)
        assert result is True

    async def test_has_pending_returns_false(self, test_db):
        """has_pending_validation returns False if none exist."""
        result = await db.has_pending_validation(test_db)
        assert result is False

    async def test_has_pending_ignores_other_states(self, test_db):
        """has_pending_validation ignores other states."""
        await test_db.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("id1", State.VALIDATING),
        )
        await test_db.commit()

        result = await db.has_pending_validation(test_db)
        assert result is False


class TestUpdateRecordStatus:
    """Test record status updates."""

    async def test_update_record_state(self, test_db):
        """update_record_status changes record_state."""
        await test_db.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("id1", State.RAW),
        )
        await test_db.commit()

        await db.update_record_status(test_db, "id1", State.VALIDATED)

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("id1",)
        ) as cursor:
            row = await cursor.fetchone()

        assert row[0] == State.VALIDATED

    async def test_update_with_extra_fields(self, test_db):
        """update_record_status updates extra fields."""
        await test_db.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("id1", State.RAW),
        )
        await test_db.commit()

        await db.update_record_status(
            test_db,
            "id1",
            State.VALIDATED,
            candidate_email="test@example.com",
            confidence_score=3,
        )

        async with test_db.execute(
            "SELECT record_state, candidate_email, confidence_score FROM records WHERE unique_id = ?",
            ("id1",),
        ) as cursor:
            row = await cursor.fetchone()

        assert row[0] == State.VALIDATED
        assert row[1] == "test@example.com"
        assert row[2] == 3

    async def test_update_nonexistent_record(self, test_db):
        """update_record_status on nonexistent record doesn't error."""
        await db.update_record_status(test_db, "nonexistent", State.VALIDATED)
        # Should not raise


class TestRecoverStaleValidating:
    """Test recovery of orphaned VALIDATING records."""

    async def test_recover_stale_validating(self, test_db):
        """recover_stale_validating resets old VALIDATING to DISCOVERED."""
        # Insert a record with old updated_at
        await test_db.execute(
            """
            INSERT INTO records (unique_id, record_state, updated_at)
            VALUES (?, ?, datetime('now', '-10 minutes'))
            """,
            ("id1", State.VALIDATING),
        )
        await test_db.commit()

        count = await db.recover_stale_validating(test_db, timeout_minutes=5)
        assert count == 1

        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("id1",)
        ) as cursor:
            row = await cursor.fetchone()

        assert row[0] == State.DISCOVERED

    async def test_recover_respects_timeout(self, test_db):
        """recover_stale_validating respects timeout_minutes."""
        # Insert a record with recent updated_at
        await test_db.execute(
            """
            INSERT INTO records (unique_id, record_state, updated_at)
            VALUES (?, ?, datetime('now', '-1 minute'))
            """,
            ("id1", State.VALIDATING),
        )
        await test_db.commit()

        count = await db.recover_stale_validating(test_db, timeout_minutes=5)
        assert count == 0

        # Record should still be VALIDATING
        async with test_db.execute(
            "SELECT record_state FROM records WHERE unique_id = ?", ("id1",)
        ) as cursor:
            row = await cursor.fetchone()

        assert row[0] == State.VALIDATING


class TestPatternStats:
    """Test pattern ranking and recording."""

    async def test_get_pattern_rankings_empty(self, test_db):
        """get_pattern_rankings returns empty for unknown provider."""
        rankings = await db.get_pattern_rankings(test_db, "unknown_provider")
        assert rankings == []

    async def test_record_pattern_result_insert(self, test_db):
        """record_pattern_result inserts new stats."""
        await db.record_pattern_result(test_db, "gmail.com", "first.last", success=True)

        rankings = await db.get_pattern_rankings(test_db, "gmail.com")
        assert len(rankings) == 1
        assert rankings[0]["template"] == "first.last"
        assert rankings[0]["success_count"] == 1
        assert rankings[0]["total_count"] == 1

    async def test_record_pattern_result_update(self, test_db):
        """record_pattern_result increments counters."""
        await db.record_pattern_result(test_db, "gmail.com", "first.last", success=True)
        await db.record_pattern_result(test_db, "gmail.com", "first.last", success=False)

        rankings = await db.get_pattern_rankings(test_db, "gmail.com")
        assert rankings[0]["success_count"] == 1
        assert rankings[0]["total_count"] == 2

    async def test_pattern_rankings_ordered_by_success_rate(self, test_db):
        """get_pattern_rankings orders by success rate descending."""
        await db.record_pattern_result(test_db, "gmail.com", "flast", success=True)
        for _ in range(9):
            await db.record_pattern_result(test_db, "gmail.com", "flast", success=True)

        await db.record_pattern_result(test_db, "gmail.com", "first.last", success=True)
        for _ in range(4):
            await db.record_pattern_result(test_db, "gmail.com", "first.last", success=False)

        rankings = await db.get_pattern_rankings(test_db, "gmail.com")
        # flast: 10/10 = 100%, first.last: 1/5 = 20%
        assert rankings[0]["template"] == "flast"
        assert rankings[1]["template"] == "first.last"


class TestEnrichmentCache:
    """Test enrichment cache operations."""

    async def test_set_and_get_enrichment_cache(self, test_db):
        """Enrichment cache stores and retrieves data."""
        data = {"domain": "example.com", "emails": ["test@example.com"]}
        await db.set_enrichment_cache(
            test_db,
            "Acme Corp",
            "John Doe",
            "DISCOVERING",
            "serper",
            json.dumps(data),
        )

        result = await db.get_enrichment_cache(
            test_db, "Acme Corp", "John Doe", "DISCOVERING", "serper"
        )
        assert result is not None
        cached_data = json.loads(result)
        assert cached_data["domain"] == "example.com"

    async def test_get_enrichment_cache_normalization(self, test_db):
        """get_enrichment_cache normalizes keys (lowercase, strip)."""
        data = {"test": True}
        await db.set_enrichment_cache(
            test_db, "ACME CORP  ", "  JOHN DOE", "DISCOVERING", "serper", json.dumps(data)
        )

        # Retrieve with different case/spacing
        result = await db.get_enrichment_cache(
            test_db, "acme corp", "john doe", "DISCOVERING", "serper"
        )
        assert result is not None

    async def test_get_enrichment_cache_expired(self, test_db):
        """Expired cache entries are not returned."""
        # Insert with very old timestamp
        await test_db.execute(
            """
            INSERT INTO enrichment_cache
                (business_name_norm, agent_name_norm, state, provider, response_json, cached_at)
            VALUES (?, ?, ?, ?, ?, datetime('now', '-40 days'))
            """,
            ("acme corp", "john doe", "DISCOVERING", "serper", '{"test": true}'),
        )
        await test_db.commit()

        result = await db.get_enrichment_cache(
            test_db, "Acme Corp", "John Doe", "DISCOVERING", "serper", ttl_days=30
        )
        assert result is None

    async def test_get_enrichment_cache_miss(self, test_db):
        """Missing cache returns None."""
        result = await db.get_enrichment_cache(
            test_db, "Nonexistent", "Nobody", "DISCOVERING", "serper"
        )
        assert result is None


class TestProcessTrace:
    """Test process trace append operations."""

    async def test_append_process_trace(self, test_db):
        """Process trace is appended as JSON array."""
        await test_db.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("id1", State.DISCOVERING),
        )
        await test_db.commit()

        entry = {"stage": "dns_probe", "outcome": "success", "mx": "gmail.com"}
        await db.append_process_trace(test_db, "id1", entry)

        async with test_db.execute(
            "SELECT process_trace FROM records WHERE unique_id = ?", ("id1",)
        ) as cursor:
            row = await cursor.fetchone()

        trace = json.loads(row[0])
        assert len(trace) == 1
        assert trace[0]["stage"] == "dns_probe"

    async def test_append_multiple_trace_entries(self, test_db):
        """Multiple trace entries accumulate in JSON array."""
        await test_db.execute(
            "INSERT INTO records (unique_id, record_state) VALUES (?, ?)",
            ("id1", State.DISCOVERING),
        )
        await test_db.commit()

        await db.append_process_trace(test_db, "id1", {"stage": "stage1"})
        await db.append_process_trace(test_db, "id1", {"stage": "stage2"})

        async with test_db.execute(
            "SELECT process_trace FROM records WHERE unique_id = ?", ("id1",)
        ) as cursor:
            row = await cursor.fetchone()

        trace = json.loads(row[0])
        assert len(trace) == 2
        assert trace[0]["stage"] == "stage1"
        assert trace[1]["stage"] == "stage2"
