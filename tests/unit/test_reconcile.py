"""Unit tests for OR-of-valids reconciliation logic."""

from pipeline.dispatcher import reconcile
from pipeline.models import BackendVerdict


def _v(status: str, message: str = "") -> BackendVerdict:
    return BackendVerdict(status=status, message=message, verified_at="2026-05-04T00:00:00Z")


class TestReconcileOrOfValids:
    def test_racknerd_valid_wins(self):
        result = reconcile(_v("valid", "250 OK"), _v("invalid", "550"))
        assert result.final_verdict == "valid"
        assert result.should_write is True
        assert result.is_terminal is True

    def test_bbops_valid_wins(self):
        result = reconcile(_v("invalid", "550"), _v("valid", "250 OK"))
        assert result.final_verdict == "valid"
        assert result.should_write is True

    def test_both_valid(self):
        result = reconcile(_v("valid"), _v("valid"))
        assert result.final_verdict == "valid"

    def test_catch_all_on_racknerd(self):
        result = reconcile(_v("catch_all"), _v("invalid"))
        assert result.final_verdict == "catch_all"
        assert result.should_write is True

    def test_catch_all_on_bbops(self):
        result = reconcile(_v("error"), _v("catch_all"))
        assert result.final_verdict == "catch_all"
        assert result.should_write is True

    def test_both_invalid_clean(self):
        result = reconcile(_v("invalid"), _v("invalid"))
        assert result.final_verdict == "invalid"
        assert result.should_write is True
        assert result.is_terminal is False

    def test_invalid_plus_error_gives_unknown(self):
        result = reconcile(_v("invalid"), _v("error"))
        assert result.final_verdict == "unknown"
        assert result.should_write is False

    def test_error_plus_invalid_gives_unknown(self):
        result = reconcile(_v("error"), _v("invalid"))
        assert result.final_verdict == "unknown"
        assert result.should_write is False

    def test_both_error_gives_unknown(self):
        result = reconcile(_v("error"), _v("error"))
        assert result.final_verdict == "unknown"
        assert result.should_write is False

    def test_both_blocked_gives_unknown(self):
        result = reconcile(_v("blocked"), _v("blocked"))
        assert result.final_verdict == "unknown"
        assert result.should_write is False

    def test_tunnel_down_does_not_burn_attempt(self):
        result = reconcile(_v("error", "tunnel not up"), _v("invalid"))
        assert result.final_verdict == "unknown"
        assert result.should_write is False

    def test_bbops_valid_overrides_tunnel_down(self):
        # Co-equal: a positive bbops verdict is honored even when Racknerd's tunnel is down.
        result = reconcile(_v("error", "tunnel not up"), _v("valid"))
        assert result.final_verdict == "valid"
        assert result.should_write is True


def test_greylisting_retry_after_is_future_within_jitter():
    import datetime
    from pipeline.reconcile import greylisting_retry_after
    ts = greylisting_retry_after(minutes=15.0, jitter=0.4)
    dt = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
    delta_min = (dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds() / 60
    assert 8.0 <= delta_min <= 22.0  # 15 ± 40%, with margin

    def test_not_run_backends(self):
        result = reconcile(_v("not_run"), _v("not_run"))
        assert result.final_verdict == "unknown"
        assert result.should_write is False

    def test_none_racknerd_bbops_valid(self):
        result = reconcile(None, _v("valid"))
        assert result.final_verdict == "valid"
        assert result.should_write is True

    def test_racknerd_valid_none_bbops(self):
        result = reconcile(_v("valid"), None)
        assert result.final_verdict == "valid"

    def test_both_none(self):
        result = reconcile(None, None)
        assert result.final_verdict == "unknown"
        assert result.should_write is False
