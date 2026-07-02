"""Durable external-storage backup of authoritative pipeline state (item 2).

The central-coordinator architecture already keeps no authoritative state on any VPS
worker (they are stateless SMTP egress IPs). This package adds an optional, periodic
consistent snapshot of pipeline.db + the fleet inventory to a configured directory —
point it at an rclone/s3fs-mounted R2/S3 bucket for offsite durability. Off by default.
"""
from __future__ import annotations

from pipeline.storage.r2 import R2Client, signing_key  # noqa: F401
from pipeline.storage.snapshot import SnapshotWorker, snapshot_db  # noqa: F401
