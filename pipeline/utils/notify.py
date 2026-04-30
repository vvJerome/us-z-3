"""Named-pipe (FIFO) notification channel between producer and consumer.

Producer calls signal_consumer() after each batch insert.
Consumer replaces asyncio.sleep() with wait_for_signal() which wakes in <1ms
instead of the 5–30s polling window.  A 30s wallclock timeout ensures the
stale-record sweep still fires even when the producer is idle.
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path


async def create_notify_pipe(path: Path) -> None:
    """Create the FIFO if it doesn't exist. Safe to call from producer before consumer opens it."""
    if not path.exists():
        os.mkfifo(str(path))


async def signal_consumer(path: Path) -> None:
    """Write a single byte to the FIFO to wake the consumer. Non-blocking."""
    try:
        fd = os.open(str(path), os.O_WRONLY | os.O_NONBLOCK)
        try:
            os.write(fd, b"\n")
        finally:
            os.close(fd)
    except OSError:
        # Consumer hasn't opened the read end yet, or pipe buffer full — safe to ignore
        pass


async def open_notify_reader(path: Path) -> asyncio.StreamReader:
    """Open the FIFO for reading and return an asyncio StreamReader."""
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    fd = os.open(str(path), os.O_RDONLY | os.O_NONBLOCK)
    await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(reader), os.fdopen(fd, "rb", 0)
    )
    return reader
