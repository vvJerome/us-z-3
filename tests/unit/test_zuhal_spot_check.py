"""Unit tests for pipeline.ops.zuhal_spot_check."""
from __future__ import annotations

import asyncio

import pytest

from pipeline.ops.zuhal_spot_check import _zb_verify


class TestZbVerifyDryRun:
    async def test_dry_run_returns_string(self):
        import aiohttp
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(1)
            result = await _zb_verify(session, semaphore, "fake_key", "test@acme.com", dry_run=True)
        assert isinstance(result, str)
        assert result != ""
