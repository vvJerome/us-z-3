"""Pytest configuration and shared fixtures."""

import sys
from pathlib import Path

# Add project root to sys.path so `import pipeline` and `import orchestrator` work
_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

import pytest


@pytest.fixture
def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


@pytest.fixture
async def db_conn():
    """In-memory aiosqlite connection with full pipeline schema."""
    from pipeline.db import init_db
    conn = await init_db(":memory:")
    yield conn
    await conn.close()
