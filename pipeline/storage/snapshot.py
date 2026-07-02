"""Consistent SQLite snapshots + inventory backup to a durable directory.

Uses SQLite's online backup API so the copy is consistent even while the pipeline writes.
The backup runs on its OWN short-lived sqlite3 connections in a worker thread — never the
pipeline's live aiosqlite connection — so it can't serialize behind (or deadlock) the
dispatcher's writes. The destination is any directory path — a local disk, or an
rclone/s3fs mount backed by R2/S3 for offsite durability (item 2).
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import sqlite3
import tempfile
from pathlib import Path

from pipeline.storage.r2 import R2Client

logger = logging.getLogger("pipeline.storage")


async def snapshot_db(src_path: Path | str, dest_path: Path | str) -> None:
    """Copy the DB file at src_path to dest_path via SQLite's online backup API.

    Opens its own sqlite3 connections (not the pipeline's live aiosqlite connection) and
    runs the blocking backup in a thread, so concurrent dispatcher writes are never blocked.
    WAL mode lets the separate reader take a consistent snapshot while writes continue.
    """
    src = Path(src_path)
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)

    def _backup() -> None:
        source = sqlite3.connect(str(src))
        try:
            target = sqlite3.connect(str(dest))
            try:
                source.backup(target)
            finally:
                target.close()
        finally:
            source.close()

    await asyncio.to_thread(_backup)


class SnapshotWorker:
    """Periodically snapshots pipeline.db + inventory to a local dir and/or R2 until stopped."""

    def __init__(
        self,
        db_path: Path | str,
        *,
        backup_dir: Path | str = "",
        db_name: str = "pipeline.db",
        inventory_path: Path | str = "output/fleet/hosts.json",
        interval_s: float = 300.0,
        r2_client: R2Client | None = None,
        r2_prefix: str = "",
    ) -> None:
        self.db_path = Path(db_path)
        self.backup_dir = Path(backup_dir) if backup_dir else None
        self.db_name = db_name
        self.inventory_path = Path(inventory_path)
        self.interval_s = interval_s
        self.r2_client = r2_client
        self.r2_prefix = r2_prefix

    async def snapshot_once(self) -> None:
        workdir = self.backup_dir or Path(tempfile.gettempdir()) / "ecc_snapshot"
        workdir.mkdir(parents=True, exist_ok=True)
        dest_db = workdir / self.db_name
        await snapshot_db(self.db_path, dest_db)
        if self.backup_dir is not None and self.inventory_path.exists():
            shutil.copy2(self.inventory_path, self.backup_dir / self.inventory_path.name)
        if self.r2_client is not None:
            await self.r2_client.put_object(f"{self.r2_prefix}{self.db_name}", dest_db.read_bytes())
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
