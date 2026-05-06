"""Unit tests for TokenBucket rate limiter."""

import asyncio
import time

import pytest


pytestmark = pytest.mark.asyncio


class TestTokenBucket:
    """Test async token bucket rate limiter."""

    async def test_initialization(self):
        """TokenBucket initializes with correct capacity."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=10, refill_rate=2.0)
        assert bucket.capacity == 10
        assert bucket.refill_rate == 2.0
        assert bucket._tokens == 10.0

    async def test_acquire_succeeds_with_available_tokens(self):
        """Acquire succeeds immediately when tokens are available."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        start = time.monotonic()
        await bucket.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1
        assert bucket._tokens < 10.0  # at least one token was consumed

    async def test_acquire_depletes_tokens(self):
        """Multiple acquires deplete tokens."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=5, refill_rate=1.0)
        for _ in range(5):
            await bucket.acquire()
        # Tokens should be at or near 0 (small refill during execution is ok)
        assert bucket._tokens < 0.1

    async def test_acquire_waits_when_no_tokens(self):
        """Acquire waits when bucket is empty."""
        from pipeline.utils.rate_limiter import TokenBucket

        # capacity=1 so first acquire empties it; refill_rate=2 means ~0.5s to refill 1 token
        bucket = TokenBucket(capacity=1, refill_rate=2.0)
        await bucket.acquire()
        start = time.monotonic()
        await bucket.acquire()
        elapsed = time.monotonic() - start
        assert 0.4 < elapsed < 0.8

    async def test_capacity_ceiling(self):
        """Tokens never exceed capacity."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=5, refill_rate=10.0)
        await asyncio.sleep(1.0)
        # Even with high refill rate, should cap at capacity
        assert bucket._tokens <= 5.0

    async def test_concurrent_acquires_serialized(self):
        """Concurrent acquires are properly serialized by lock."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=10, refill_rate=100.0)
        tasks = [bucket.acquire() for _ in range(5)]
        await asyncio.gather(*tasks)
        # All 5 succeeded; tokens close to 5.0 (allow for tiny refill during execution)
        assert 4.9 < bucket._tokens < 5.1

    async def test_high_concurrency_respects_capacity(self):
        """High concurrency still respects capacity ceiling."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        tasks = [bucket.acquire() for _ in range(10)]
        await asyncio.gather(*tasks)
        assert bucket._tokens < 0.1

    async def test_acquire_after_empty_waits_for_refill(self):
        """After depletion, acquire waits for refill."""
        from pipeline.utils.rate_limiter import TokenBucket

        # capacity=1 guarantees empty after single acquire; refill_rate=2 → ~0.5s wait
        bucket = TokenBucket(capacity=1, refill_rate=2.0)
        await bucket.acquire()
        start = time.monotonic()
        await bucket.acquire()
        elapsed = time.monotonic() - start
        assert 0.4 < elapsed < 0.8

    async def test_refill_on_acquire(self):
        """_refill() is called on each acquire."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        await bucket.acquire()
        initial_tokens = bucket._tokens
        await asyncio.sleep(0.1)  # ~0.1 token refill
        await bucket.acquire()
        # One token was consumed, but ~0.1 was refilled; net change is roughly -0.9
        assert bucket._tokens < initial_tokens

    async def test_rapid_fire_acquires(self):
        """Rapid sequential acquires deplete then wait."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=3, refill_rate=10.0)
        start = time.monotonic()
        for _ in range(6):
            await bucket.acquire()
        elapsed = time.monotonic() - start
        # First 3 are instant; next 3 need ~0.1s each at 10 tokens/sec
        assert elapsed > 0.2

    async def test_lock_prevents_race_conditions(self):
        """Multiple concurrent acquires don't cause race conditions."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=100, refill_rate=10.0)
        tasks = [bucket.acquire() for _ in range(50)]
        await asyncio.gather(*tasks)
        # ~50 tokens remain (allow for small refill during execution)
        assert 49.5 < bucket._tokens < 51.0

    async def test_variable_capacity(self):
        """Different capacities work correctly."""
        from pipeline.utils.rate_limiter import TokenBucket

        for capacity in [1, 5, 50, 1000]:
            bucket = TokenBucket(capacity=capacity, refill_rate=1.0)
            for _ in range(capacity):
                await bucket.acquire()
            assert bucket._tokens < 0.1

    async def test_variable_refill_rate(self):
        """Different refill rates produce correct wait times."""
        from pipeline.utils.rate_limiter import TokenBucket

        # capacity=1 ensures we wait; refill_rate=5 means ~0.2s for 1 token
        bucket = TokenBucket(capacity=1, refill_rate=5.0)
        await bucket.acquire()
        start = time.monotonic()
        await bucket.acquire()
        elapsed = time.monotonic() - start
        assert 0.15 < elapsed < 0.35

    async def test_stress_many_concurrent_tasks(self):
        """Stress test with many concurrent tasks."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=100, refill_rate=50.0)
        tasks = [bucket.acquire() for _ in range(100)]
        start = time.monotonic()
        await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start
        assert elapsed < 1.0
        # All 100 consumed; allow for slight refill
        assert bucket._tokens < 0.1

    async def test_time_moves_forward_between_acquires(self):
        """Token bucket respects wall-clock time between acquires."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=10, refill_rate=10.0)
        await bucket.acquire()
        tokens_after_first = bucket._tokens
        await asyncio.sleep(0.1)  # 1 token refills
        # Trigger refill by calling _refill directly
        bucket._refill()
        assert bucket._tokens > tokens_after_first

    async def test_initial_tokens_zero_starts_empty(self):
        """initial_tokens=0 means bucket starts with no tokens."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=10, refill_rate=1.0, initial_tokens=0)
        assert bucket._tokens == 0.0

    async def test_initial_tokens_zero_first_acquire_waits(self):
        """initial_tokens=0 forces even the first acquire to wait for refill."""
        from pipeline.utils.rate_limiter import TokenBucket

        # rate=2/s → ~0.5s to accumulate 1 token from empty
        bucket = TokenBucket(capacity=5, refill_rate=2.0, initial_tokens=0)
        start = time.monotonic()
        await bucket.acquire()
        elapsed = time.monotonic() - start
        assert elapsed > 0.3

    async def test_initial_tokens_custom_value(self):
        """initial_tokens sets an arbitrary starting level below capacity."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=10, refill_rate=1.0, initial_tokens=3.0)
        assert bucket._tokens == 3.0

    async def test_initial_tokens_default_preserves_full_start(self):
        """Without initial_tokens, bucket starts at capacity (existing behaviour unchanged)."""
        from pipeline.utils.rate_limiter import TokenBucket

        bucket = TokenBucket(capacity=7, refill_rate=1.0)
        assert bucket._tokens == 7.0
