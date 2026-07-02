"""SMTP backend reconciliation — the OR-of-valids policy and its helpers.

Pure decision logic, no I/O: given the Racknerd and bbops verdicts, decide the
reconciled outcome and whether it should be written or re-queued. Kept separate
from the Dispatcher so the policy is testable and the dispatcher stays small.
"""
from __future__ import annotations

import datetime
import random

from pipeline.models import BackendVerdict, ReconcileResult


def valid_email_format(email: str) -> bool:
    """Return False for emails whose local part violates RFC 5321 basics (e.g. ...@domain)."""
    parts = email.split("@")
    if len(parts) != 2:
        return False
    local = parts[0]
    return bool(local) and not local.startswith(".") and not local.endswith(".") and ".." not in local


# Statuses that indicate the backend actually ran and returned a definitive answer
DEFINITIVE: frozenset[str] = frozenset({"valid", "invalid", "catch_all"})
# Statuses that mean "couldn't reach server" — should not count as invalid
INCONCLUSIVE: frozenset[str] = frozenset({"error", "blocked", "not_run"})


def reconcile(
    racknerd: BackendVerdict | None,
    bbops: BackendVerdict | None,
) -> ReconcileResult:
    """
    OR-of-valids policy:
    - Either backend valid/catch_all → accept
    - Both definitively invalid (no errors) → reject
    - Mixed error/inconclusive → unknown, re-queue without burning attempt
    - Tunnel down special-case → re-queue without burning attempt
    """
    rk = racknerd.status if racknerd else "not_run"
    bb = bbops.status if bbops else "not_run"

    # OR-of-valids: a positive from EITHER co-equal checker wins — even if the other
    # backend's infra is down (tunnel not up). Checked before the tunnel-down short-circuit
    # so a definitive bbops verdict is never discarded when Racknerd is unreachable.
    if rk == "valid" or bb == "valid":
        return ReconcileResult(final_verdict="valid", should_write=True, is_terminal=True)

    if rk == "catch_all" or bb == "catch_all":
        return ReconcileResult(final_verdict="catch_all", should_write=True, is_terminal=True)

    # Tunnel-down with no positive signal: pure infra, re-queue without burning attempt.
    if rk == "error" and (racknerd and "tunnel not up" in racknerd.message):
        return ReconcileResult(final_verdict="unknown", should_write=False, is_terminal=False)

    # Both definitively invalid (no errors mixed in)
    if rk == "invalid" and bb == "invalid":
        return ReconcileResult(final_verdict="invalid", should_write=True, is_terminal=False)

    if rk == "invalid" and bb in INCONCLUSIVE:
        # not_run = backend intentionally disabled; treat as definitive invalid
        if bb == "not_run":
            return ReconcileResult(final_verdict="invalid", should_write=True, is_terminal=False)
        # One said invalid, one errored — can't trust the invalid verdict alone
        return ReconcileResult(final_verdict="unknown", should_write=False, is_terminal=False)

    if rk in INCONCLUSIVE and bb == "invalid":
        # not_run = backend intentionally disabled; treat as definitive invalid
        if rk == "not_run":
            return ReconcileResult(final_verdict="invalid", should_write=True, is_terminal=False)
        return ReconcileResult(final_verdict="unknown", should_write=False, is_terminal=False)

    # Both inconclusive
    return ReconcileResult(final_verdict="unknown", should_write=False, is_terminal=False)


def greylisting_retry_after(minutes: float = 15.0, jitter: float = 0.4) -> str:
    """Return an ISO timestamp ~`minutes` from now (±jitter) for a greylisting hold.

    Greylisters accept the retry once a min-age passes (commonly ~5 min); jitter keeps a
    batch of deferrals from retrying in lockstep (RFC 6647).
    """
    delay = minutes * (1.0 + random.uniform(-jitter, jitter))
    dt = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=delay)
    return dt.strftime("%Y-%m-%d %H:%M:%S")
