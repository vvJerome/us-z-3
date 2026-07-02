"""SMTP worker fleet — Cherry Servers provisioning, health, routing, and failover.

Replaces the single RackNerd VPS with a live, self-managing pool of SMTP egress
workers (Improve-Existing items 1, 2, 5, 6). Each worker is an SSH SOCKS5 egress
IP; the pool implements the dispatcher's `verify(email) -> BackendVerdict` seam.
"""
from __future__ import annotations

from pipeline.fleet.cherry_client import CherryAPIError, CherryClient  # noqa: F401
