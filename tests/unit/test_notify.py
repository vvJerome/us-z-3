"""Unit tests for the named-pipe (FIFO) notify channel. Uses real FIFOs under
tmp_path — this is local filesystem IPC, not network, so no mocking needed."""
from __future__ import annotations

import asyncio
import os

import pytest

from pipeline.utils.notify import create_notify_pipe, open_notify_reader, signal_consumer

pytestmark = pytest.mark.asyncio


class TestCreateNotifyPipe:
    async def test_creates_fifo_when_missing(self, tmp_path):
        pipe_path = tmp_path / "notify.pipe"
        await create_notify_pipe(pipe_path)
        assert pipe_path.exists()
        assert not pipe_path.is_file()

    async def test_safe_to_call_twice(self, tmp_path):
        pipe_path = tmp_path / "notify.pipe"
        await create_notify_pipe(pipe_path)
        await create_notify_pipe(pipe_path)  # must not raise FileExistsError
        assert pipe_path.exists()


class TestSignalConsumer:
    async def test_no_reader_open_is_silently_ignored(self, tmp_path):
        """Writing to a FIFO with no reader raises ENXIO — must be swallowed, not propagate."""
        pipe_path = tmp_path / "notify.pipe"
        await create_notify_pipe(pipe_path)
        await signal_consumer(pipe_path)  # must not raise


class TestNotifyRoundTrip:
    async def test_signal_wakes_a_waiting_reader(self, tmp_path):
        pipe_path = tmp_path / "notify.pipe"
        await create_notify_pipe(pipe_path)

        reader = await open_notify_reader(pipe_path)

        # Keep the write end open until the signal is sent — otherwise there's
        # a race where signal_consumer's open+write can outrun the reader setup.
        hold_fd = os.open(str(pipe_path), os.O_WRONLY | os.O_NONBLOCK)
        try:
            await signal_consumer(pipe_path)
            data = await asyncio.wait_for(reader.read(1), timeout=2.0)
        finally:
            os.close(hold_fd)

        assert data == b"\n"
