# Test Suite for Email Discovery Pipeline

Comprehensive test suite covering unit, integration, and end-to-end tests for the email-discovery pipeline.

## Structure

```
tests/
├── conftest.py              # Shared fixtures and pytest configuration
├── unit/                    # Fast unit tests (no IO, no DB)
│   ├── test_email_patterns.py
│   ├── test_scoring.py
│   ├── test_ms_verify.py
│   └── test_rate_limiter.py
├── integration/             # Tests with real in-memory DB, mocked HTTP
│   ├── test_db.py
│   └── test_pipeline_flow.py
└── e2e/                     # End-to-end tests via CLI
    └── test_full_run.py
```

## Running Tests

### All tests

```bash
pytest tests/
```

### By category

```bash
# Unit tests only
pytest tests/unit/

# Integration tests only
pytest tests/integration/

# E2E tests only
pytest tests/e2e/
```

### By name

```bash
# Run a specific test
pytest tests/unit/test_email_patterns.py::TestGeneratePersonalPatterns::test_generates_all_templates

# Run tests matching a pattern
pytest -k "confidence" tests/unit/

# Exclude E2E (faster runs during development)
pytest -m "not e2e" tests/
```

### With coverage

```bash
pytest --cov=pipeline tests/ --cov-report=html
```

## Test Coverage

### Unit Tests (`tests/unit/`)

#### `test_email_patterns.py`
- **TestGeneratePersonalPatterns**: Personal template generation, ordering, missing names
- **TestGenerateGenericPatterns**: Generic template generation (info, contact, etc.)
- **TestGenerateRankedCandidates**: Ranking with success rates, strategy-aware ordering
- **TestEmailToTemplate**: Reverse mapping of emails to template names
- **TestIntegrationPatternRanking**: Combined pattern generation and ranking

**Coverage:**
- All 13 personal templates
- All 8 generic templates
- Fuzzy matching with rankings
- Edge cases: empty names, special characters, single letters

#### `test_scoring.py`
- **TestComputeConfidenceScore**: Score computation for all branches
  - Domain matching (fuzzy >= 85%)
  - Name matching in email local part
  - Generic vs. non-generic prefixes
  - Catch-all vs. valid emails
  - "With" vs. "without" strategy
- **TestConfidenceTier**: Classification into high (≥3), medium (2), low (≤1)
- **TestIntegrationScoring**: End-to-end scoring workflows

**Coverage:**
- Score range 0–4 for "with" strategy
- Score range 0–3 for "without" strategy
- All generic prefixes (info, contact, hello, admin, support, sales, help)
- Edge cases: None/empty domain, None/empty agent_name

#### `test_ms_verify.py`
- **TestIsMicrosoftMx**: MX provider detection
  - Outlook.com, Hotmail.com, Microsoft.com patterns
  - Subdomain matching
  - Case-insensitivity
- **TestCheckSync**: Synchronous API response handling
  - Domain type detection (managed, federated, consumer)
  - IfExistsResult codes (0=exists, 1=not exists, 2=throttled)
  - Throttle handling
  - Error responses (HTTP non-200)

**Coverage:**
- DOMAIN_MANAGED (3): reliable
- DOMAIN_FEDERATED (4): unreliable
- DOMAIN_CONSUMER (2): unreliable
- User-Agent header randomization
- Request body format validation

#### `test_rate_limiter.py`
- **TestTokenBucket**: Async rate limiter
  - Token depletion and refill
  - Waiting when empty
  - Refill rate accuracy
  - Concurrent acquire serialization
  - Capacity ceiling enforcement

**Coverage:**
- Various capacities and refill rates
- Concurrent tasks
- Time-based refill accuracy
- Stress tests with 100+ concurrent acquires

### Integration Tests (`tests/integration/`)

#### `test_db.py`
Uses in-memory SQLite (`:memory:`) with full schema.

- **TestInitDb**: Schema and index creation
- **TestCheckpoints**: Get/set/update checkpoints
- **TestInsertRecords**: Batch insertion, atomicity, duplicate handling
- **TestFetchPendingValidation**: Atomic DISCOVERED → VALIDATING state transition
- **TestHasPendingValidation**: Non-claiming existence check
- **TestUpdateRecordStatus**: State and field updates
- **TestRecoverStaleValidating**: Timeout-based recovery of orphaned VALIDATING records
- **TestPatternStats**: Pattern ranking learning and ordering
- **TestEnrichmentCache**: Cache insertion, TTL, normalization
- **TestProcessTrace**: JSON array accumulation of trace entries

**Coverage:**
- All State constants (RAW, DISCOVERING, DISCOVERED, VALIDATING, VALIDATED, etc.)
- All db.py functions with real async/await
- Atomic transitions
- TTL expiry logic
- JSON storage and retrieval

#### `test_pipeline_flow.py`
Mocked HTTP, real in-memory DB.

- **TestRecordInsertionWorkflow**: Records with discovery data
- **TestValidationWorkflow**: DISCOVERED → VALIDATING → VALIDATED states
- **TestConsumerWorkerIntegration**: ConsumerWorker with mocked Zuhal
- **TestStateTransitions**: Full state machine paths
- **TestPatternLearning**: Ranking accuracy across runs
- **TestErrorHandling**: Missing data, invalid JSON

**Coverage:**
- Record flows through entire pipeline
- Cost tracking with limits
- Pattern ranking updates during validation
- Multi-provider ranking separation

### End-to-End Tests (`tests/e2e/`)

#### `test_full_run.py`
Subprocess invocation of `python -m pipeline --dry-run ...`

- **TestFullPipelineRun**: CLI invocation and exit codes
  - Full run with --dry-run
  - Output directory creation
  - --limit parameter respected
  - Database file creation
  - Environment variables (SERPER_API_KEY, ZUHAL_API_KEY)
  - Invalid input handling
  - --chunk-size parameter
  - --dry-run requirement (no real API calls)

- **TestPipelineOutputGeneration**: Output validation
  - Dry run doesn't make API calls
  - No error logs for API failures

**Coverage:**
- CLI argument parsing
- File I/O (JSONL input, DB creation)
- Exit code validation
- Dry-run mode isolation

## Dependencies

### Runtime
- `pytest`
- `pytest-asyncio` (for async tests)
- `aiosqlite` (in-memory DB for integration tests)
- `rapidfuzz` (fuzzy matching, used by pipeline)
- `pydantic` (config validation)

### Test-only
- `unittest.mock` (built-in, used for HTTP mocking)

### Install

```bash
# From project root
pip install -r scraper/requirements.txt
pip install pytest pytest-asyncio
```

## Writing New Tests

### Unit Test Template

```python
import pytest
from pipeline.utils.some_module import some_function

class TestSomeFunction:
    """Test some_function behavior."""

    def test_basic_case(self):
        """Describe what is being tested."""
        result = some_function("input")
        assert result == "expected"

    def test_edge_case(self):
        """Test boundary conditions."""
        result = some_function("")
        assert result is None
```

### Async Test Template

```python
import pytest

pytestmark = pytest.mark.asyncio

class TestAsyncFunction:
    """Test async function behavior."""

    async def test_basic_case(self):
        """Describe the test."""
        result = await async_function()
        assert result == "expected"
```

### Integration Test Template

```python
import pytest
import aiosqlite
from pipeline import db

pytestmark = pytest.mark.asyncio

@pytest.fixture
async def test_db():
    """In-memory test database."""
    conn = await aiosqlite.connect(":memory:")
    await conn.executescript(db.SCHEMA_SQL)
    conn.row_factory = aiosqlite.Row
    yield conn
    await conn.close()

class TestDbFunction:
    """Test database operations."""

    async def test_operation(self, test_db):
        """Test description."""
        await db.some_operation(test_db, ...)
        # Assert results
```

## Mocking HTTP Calls

Use `unittest.mock.patch` and `MagicMock`:

```python
from unittest.mock import patch, MagicMock

@patch("pipeline.utils.ms_verify.requests.post")
def test_ms_verify(mock_post):
    """Test MS verification with mocked HTTP."""
    mock_post.return_value = MagicMock(
        status_code=200,
        json=lambda: {"IfExistsResult": 0, "EstsProperties": {"DomainType": 3}}
    )
    result = _check_sync("user@example.com")
    assert result["status"] == "valid"
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: "3.10"
      - run: pip install -r scraper/requirements.txt pytest pytest-asyncio
      - run: pytest tests/ -v --tb=short
```

## Performance Notes

- **Unit tests**: ~100ms total (no I/O)
- **Integration tests**: ~1-2s (in-memory DB)
- **E2E tests**: ~5-10s per test (subprocess overhead)
- Use `pytest -m "not e2e"` during development for fast feedback loops

## Known Issues

- E2E tests require `python -m pipeline` to work from `scraper/` directory
- Async tests require `pytest-asyncio` with `asyncio_mode = auto`
- In-memory DB doesn't support all SQLite pragmas (WAL, mmap disabled in conftest)

## Contributing

When adding new features:

1. Write unit tests first (TDD)
2. Add integration tests for DB interactions
3. Add E2E tests for CLI features
4. Ensure no real API calls in test runs
5. Mock all HTTP requests
6. Use fixtures for common setup (conftest.py)
7. Add docstrings explaining test intent
