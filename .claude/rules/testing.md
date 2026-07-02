# Testing Rules

## Framework

- `pytest` with `asyncio_mode = auto` (configured in `pytest.ini`).
- No `@pytest.mark.asyncio` decorator needed — all async test functions are automatically picked up.
- Virtual environment: always run via `.venv/bin/python -m pytest`, never system `pytest`.

## What to test

Every new function that contains logic (not just delegation) needs a test. Specifically:
- New `db.py` helpers → integration test with real SQLite in `tmp_path`
- New `utils/` functions → unit test, pure Python
- New CLI flags → e2e test via `subprocess.run([sys.executable, "-m", "pipeline", ...])`

## What not to mock

- **Never mock SQLite** — use `tmp_path` with `init_db()`. Mocked DB tests silently pass while real behaviour breaks.
- **Never mock `asyncio.sleep`** in timing-sensitive tests — parameterize the sleep duration to 0 instead.
- **Never mock `db.py` functions** in integration tests — test the real function.

## What to mock

- External HTTP (Serper, Zuhal, Microsoft API) — use `aioresponses` or `unittest.mock.patch`.
- `aiodns.DNSResolver.query` — patch to return fake MX records without real DNS. Pass an
  explicit `resolver=` (e.g. `AsyncMock()`) to anything that constructs one (`RacknerdConsumer`,
  `pipeline.fleet.wiring.build_fleet`) — the default-constructs-a-real-`aiodns.DNSResolver()`
  fallback is a C extension (pycares) that is harmless on macOS but can leave the process unable
  to exit cleanly on Linux CI. This is exactly what caused two consecutive broken `main` merges.
- File system only if you can't use `tmp_path` (rare).
- Don't let ambient machine state (a local `.env`, real env vars) leak into a `PipelineConfig()`
  under test — set every field the test's assertions depend on explicitly. `RACKNERD_HOST` from
  a developer's own `.env` passing locally while failing on a `.env`-less CI runner is the other
  half of the same incident above.

## Network is blocked by default

`pytest.ini` sets `--disable-socket --allow-hosts=127.0.0.1,::1` — any test that touches a real
external host fails immediately with a clear `SocketBlockedError` instead of hanging or silently
depending on network reachability. Loopback is allowed (covers aiohttp connectors, local test
servers, the event loop's self-pipe). If a test legitimately needs a real socket, mark it
explicitly: `@pytest.mark.enable_socket` (see `pytest-socket` docs) — don't disable this globally.

## Test structure

```python
async def test_<what>_<condition>():
    # Arrange
    ...
    # Act
    result = await ...
    # Assert
    assert result == expected
```

One logical assertion per test. Use `pytest.raises` for expected exceptions — not try/except.

## Fixtures

Shared fixtures live in `tests/conftest.py`. Don't define the same fixture in multiple test files.

## Passing bar

901 tests, 0 failures, 0 errors. mypy 0 errors across 76 source files. Coverage gate enforced at 80% (`--cov-fail-under=80` in `pytest.ini`; actual is ~88%) — a PR that drops it below 80% fails `make check` outright. Any regression is a blocker.

Run `make check` before marking any task complete (runs pytest + mypy). If you don't have `make`, run both manually:
```bash
.venv/bin/python -m pytest tests/ -q
.venv/bin/mypy pipeline/
```

## E2e test isolation

E2e tests that call real APIs must explicitly zero out keys:
```python
env = {k: v for k, v in os.environ.items() if k not in ("SERPER_API_KEY", "ZUHAL_API_KEY")}
env.update({"SERPER_API_KEY": "", "ZUHAL_API_KEY": ""})
result = subprocess.run([...], env=env, ...)
```
