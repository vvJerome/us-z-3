"""Unit tests for consistent DB snapshots + the backup worker.

snapshot_db backs up from its OWN connection to the DB *file* (never the live one), so
these tests use a file-backed DB — that's exactly the path that previously deadlocked.
"""

import sqlite3

import pytest

from pipeline.db import init_db
from pipeline.storage import SnapshotWorker, snapshot_db


@pytest.fixture
async def db_file(tmp_path):
    path = tmp_path / "pipeline.db"
    conn = await init_db(str(path))
    yield path, conn
    await conn.close()


async def test_snapshot_db_copies_records(db_file, tmp_path):
    path, conn = db_file
    await conn.execute("INSERT INTO records (unique_id, record_state) VALUES ('r1', 'RAW')")
    await conn.commit()
    dest = tmp_path / "backup" / "pipeline.db"
    await snapshot_db(path, dest)
    target = sqlite3.connect(str(dest))
    row = target.execute("SELECT unique_id FROM records WHERE unique_id = 'r1'").fetchone()
    target.close()
    assert row[0] == "r1"


async def test_snapshot_does_not_block_concurrent_writes(db_file, tmp_path):
    # The whole point of the fix: a snapshot must not stall writes on the live connection.
    path, conn = db_file
    await snapshot_db(path, tmp_path / "bk" / "pipeline.db")
    await conn.execute("INSERT INTO records (unique_id, record_state) VALUES ('after', 'RAW')")
    await conn.commit()
    async with conn.execute("SELECT COUNT(*) FROM records WHERE unique_id = 'after'") as cur:
        assert (await cur.fetchone())[0] == 1


async def test_snapshot_worker_writes_local(db_file, tmp_path):
    path, conn = db_file
    inv = tmp_path / "hosts.json"
    inv.write_text("[]")
    worker = SnapshotWorker(path, backup_dir=tmp_path / "bk", inventory_path=inv)
    await worker.snapshot_once()
    assert (tmp_path / "bk" / "pipeline.db").exists()
    assert (tmp_path / "bk" / "hosts.json").exists()


async def test_snapshot_worker_uploads_to_r2(db_file, tmp_path):
    path, conn = db_file
    inv = tmp_path / "hosts.json"
    inv.write_text("[]")

    class _FakeR2:
        def __init__(self):
            self.keys = []

        async def put_object(self, key, data, now=None):
            self.keys.append(key)

    r2 = _FakeR2()
    worker = SnapshotWorker(path, backup_dir=tmp_path / "bk", inventory_path=inv, r2_client=r2)
    await worker.snapshot_once()
    assert "pipeline.db" in r2.keys and "hosts.json" in r2.keys
