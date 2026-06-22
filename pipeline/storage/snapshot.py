"""Consistent SQLite snapshots + inventory backup to a durable directory.

Uses SQLite's online backup API (via aiosqlite) so the copy is consistent even while
the pipeline writes. The destination is any directory path — a local disk, or an
rclone/s3fs mount backed by R2/S3 for offsite durability (item 2).
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
from pathlib import Path

import aiosqlite

from pipeline.storage.r2 import R2Client

logger = logging.getLogger("pipeline.storage")


async def snapshot_db(conn: aiosqlite.Connection, dest_path: Path | str) -> None:
    """Write a consistent copy of the live DB to dest_path via the SQLite backup API."""
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    target = await aiosqlite.connect(str(dest))
    try:
        await conn.backup(target)
    finally:
        await target.close()


class SnapshotWorker:
    """Periodically snapshots pipeline.db + inventory to a local dir and/or R2 until stopped."""

    def __init__(
        self,
        conn: aiosqlite.Connection,
        *,
        backup_dir: Path | str = "",
        db_name: str = "pipeline.db",
        inventory_path: Path | str = "output/fleet/hosts.json",
        interval_s: float = 300.0,
        r2_client: R2Client | None = None,
        r2_prefix: str = "",
    ) -> None:
        self.conn = conn
        self.backup_dir = Path(backup_dir) if backup_dir else None
        self.db_name = db_name
        self.inventory_path = Path(inventory_path)
        self.interval_s = interval_s
        self.r2_client = r2_client
        self.r2_prefix = r2_prefix

    async def snapshot_once(self) -> None:
        workdir = self.backup_dir or Path(tempfile.gettempdir()) / "ecc_snapshot"
        workdir.mkdir(parents=True, exist_ok=True)
        db_path = workdir / self.db_name
        await snapshot_db(self.conn, db_path)
        if self.backup_dir is not None and self.inventory_path.exists():
            shutil.copy2(self.inventory_path, self.backup_dir / self.inventory_path.name)
        if self.r2_client is not None:
            await self.r2_client.put_object(f"{self.r2_prefix}{self.db_name}", db_path.read_bytes())
            if self.inventory_path.exists():
                await self.r2_client.put_object(
                    f"{self.r2_prefix}{self.inventory_path.name}", self.inventory_path.read_bytes()
                )
        logger.info("snapshot complete (dir=%s, r2=%s)", workdir, self.r2_client is not None)

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self.snapshot_once()
            except Exception as exc:
                logger.error("snapshot failed: %s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.interval_s)
            except asyncio.TimeoutError:
                pass
