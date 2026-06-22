"""Unit tests for consistent DB snapshots + the backup worker."""

import aiosqlite

from pipeline.storage import SnapshotWorker, snapshot_db


async def test_snapshot_db_copies_records(db_conn, tmp_path):
    await db_conn.execute("INSERT INTO records (unique_id, record_state) VALUES ('r1', 'RAW')")
    await db_conn.commit()
    dest = tmp_path / "backup" / "pipeline.db"
    await snapshot_db(db_conn, dest)
    target = await aiosqlite.connect(str(dest))
    async with target.execute("SELECT unique_id FROM records WHERE unique_id = 'r1'") as cur:
        row = await cur.fetchone()
    await target.close()
    assert row[0] == "r1"


async def test_snapshot_worker_writes_local(db_conn, tmp_path):
    inv = tmp_path / "hosts.json"
    inv.write_text("[]")
    worker = SnapshotWorker(db_conn, backup_dir=tmp_path / "bk", inventory_path=inv)
    await worker.snapshot_once()
    assert (tmp_path / "bk" / "pipeline.db").exists()
    assert (tmp_path / "bk" / "hosts.json").exists()


async def test_snapshot_worker_uploads_to_r2(db_conn, tmp_path):
    inv = tmp_path / "hosts.json"
    inv.write_text("[]")

    class _FakeR2:
        def __init__(self):
            self.keys = []

        async def put_object(self, key, data, now=None):
            self.keys.append(key)

    r2 = _FakeR2()
    worker = SnapshotWorker(db_conn, backup_dir=tmp_path / "bk", inventory_path=inv, r2_client=r2)
    await worker.snapshot_once()
    assert "pipeline.db" in r2.keys and "hosts.json" in r2.keys
