"""Unit tests for pure fleet-worker health classification."""

from pipeline.fleet.health import Health, HealthThresholds, WorkerHealthInput, classify

_T = HealthThresholds()


def _inp(**kw):
    base = dict(tunnel_up=True, samples=0, blocked=0, errors=0, consecutive_failures=0)
    base.update(kw)
    return WorkerHealthInput(**base)


def test_tunnel_down_is_transient():
    assert classify(_inp(tunnel_up=False, samples=100, blocked=100)) is Health.DEGRADED_TRANSIENT


def test_high_block_rate_is_reputation():
    assert classify(_inp(samples=20, blocked=10)) is Health.DEGRADED_REPUTATION


def test_high_error_rate_is_reputation():
    assert classify(_inp(samples=20, errors=14)) is Health.DEGRADED_REPUTATION


def test_consecutive_failures_is_reputation():
    assert classify(_inp(samples=2, consecutive_failures=8)) is Health.DEGRADED_REPUTATION


def test_low_rates_are_healthy():
    assert classify(_inp(samples=50, blocked=2, errors=2)) is Health.HEALTHY


def test_insufficient_samples_does_not_judge_reputation():
    # 5 blocked out of 5 is 100% but below min_samples → not yet reputation-degraded.
    assert classify(_inp(samples=5, blocked=5)) is Health.HEALTHY


def test_block_rate_threshold_is_inclusive():
    samples = _T.min_samples
    blocked = round(samples * _T.block_rate)
    assert classify(_inp(samples=samples, blocked=blocked)) is Health.DEGRADED_REPUTATION
