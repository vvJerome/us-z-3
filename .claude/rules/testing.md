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
- `aiodns.DNSResolver.query` — patch to return fake MX records without real DNS.
- File system only if you can't use `tmp_path` (rare).

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

648 tests, 0 failures, 0 errors. Any regression is a blocker. Run `pytest tests/ -q` before marking any task complete.

## E2e test isolation

E2e tests that call real APIs must explicitly zero out keys:
```python
env = {k: v for k, v in os.environ.items() if k not in ("SERPER_API_KEY", "ZUHAL_API_KEY")}
env.update({"SERPER_API_KEY": "", "ZUHAL_API_KEY": ""})
result = subprocess.run([...], env=env, ...)
```
