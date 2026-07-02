"""Run-flag defaults: mirrored numeric flags must be None so PipelineConfig/.env wins."""

from pipeline.cli import parse_args
from pipeline.config import PipelineConfig


def test_unset_run_flags_default_to_none():
    # When the operator does not pass these flags, argparse must yield None so the
    # __main__ merge skips them and PipelineConfig (and thus .env) is the source of truth.
    args = parse_args(["run", "-i", "input/x.jsonl"])
    for flag in (
        "dispatch_concurrency", "dispatch_backend_timeout_s", "dispatch_poll_interval_s",
        "dispatch_chunk_size", "dns_concurrency", "serper_concurrency",
        "racknerd_concurrency", "racknerd_smtp_timeout_s", "racknerd_ssh_port",
        "racknerd_socks_port",
    ):
        assert getattr(args, flag) is None, f"{flag} should default to None, not a literal"


def test_config_defaults_are_the_effective_run_defaults():
    # The values an unflagged run actually uses come straight from PipelineConfig.
    # _env_file=None keeps the test hermetic (ignore any local .env overrides).
    cfg = PipelineConfig(_env_file=None, serper_api_key="x", racknerd_host="localhost")
    assert cfg.dispatch_concurrency == 50
    assert cfg.racknerd_concurrency == 25
    assert cfg.racknerd_smtp_timeout_s == 8.0


def test_explicit_run_flag_still_parses():
    args = parse_args(["run", "-i", "input/x.jsonl", "--dispatch-concurrency", "7"])
    assert args.dispatch_concurrency == 7
