from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="ECC Pipeline — Email Discovery & Dual-Backend Validation",
    )
    subparsers = parser.add_subparsers(dest="subcommand")

    # --- run (default) ---
    run_parser = subparsers.add_parser("run", help="Execute the pipeline")
    _add_run_flags(run_parser)

    # Also accept run flags directly on the root parser (no subcommand = run)
    _add_run_flags(parser)

    # --- status ---
    status_parser = subparsers.add_parser("status", help="Show pipeline status")
    status_parser.add_argument("--db", default="output/pipeline.db", help="Database path")
    status_parser.add_argument("--watch", type=int, default=None, metavar="N",
                               help="Refresh every N seconds")

    # --- reset ---
    reset_parser = subparsers.add_parser("reset", help="Re-queue failed records")
    reset_parser.add_argument("--db", default="output/pipeline.db", help="Database path")
    reset_parser.add_argument("--status", default="discovery_failed",
                              choices=["discovery_failed", "validation_failed", "cost_skipped"],
                              help="Which status to re-queue")
    reset_parser.add_argument("--phase", default=None,
                              choices=["dns", "serper"],
                              help="Filter by failure phase")
    reset_parser.add_argument("--dry-run", action="store_true",
                              help="Print count without making changes")

    return parser


def _add_run_flags(parser: argparse.ArgumentParser) -> None:
    # I/O
    parser.add_argument("-i", "--input", dest="input_path", help="Input JSONL file path")
    parser.add_argument("--name", default=None,
                        help="Run name — outputs go to output/<name>/")
    parser.add_argument("-o", "--output-dir", default=None, help="Override output directory")
    parser.add_argument("--db", default=None, help="Override SQLite database path")
    parser.add_argument("--log-dir", default=None, help="Override log directory")

    # Scope
    parser.add_argument("--limit", type=int, default=None, help="Max records to process")
    parser.add_argument("--start-offset", type=int, default=0, help="Skip first N lines")
    parser.add_argument("--ignore-checkpoint", action="store_true",
                        help="Ignore saved checkpoint, start from --start-offset")
    parser.add_argument("--chunk-size", type=int, default=100, help="Records per chunk (1-10000)")

    # Worker mode
    parser.add_argument("--producer-only", action="store_true", help="Run producer only")
    parser.add_argument("--consumer-only", action="store_true", help="Run dispatcher only")

    # Strategy
    parser.add_argument("--strategy", default="auto", choices=["auto", "with", "without"],
                        help="Email discovery strategy")

    # Producer concurrency
    parser.add_argument("--dns-concurrency", type=int, default=100, help="DNS semaphore size")
    parser.add_argument("--serper-concurrency", type=int, default=15, help="Serper semaphore size")

    # Dispatcher
    parser.add_argument("--dispatch-concurrency", type=int, default=20,
                        help="Concurrent records in dispatcher")
    parser.add_argument("--dispatch-backend-timeout-s", type=float, default=60.0,
                        help="Per-backend timeout in seconds")
    parser.add_argument("--dispatch-poll-interval-s", type=float, default=5.0,
                        help="Dispatcher poll interval when queue is empty")
    parser.add_argument("--dispatch-chunk-size", type=int, default=50,
                        help="Records claimed per poll cycle")

    # Racknerd
    parser.add_argument("--racknerd-enabled", action="store_true", default=None,
                        help="Enable Racknerd SMTP backend (default: True unless --producer-only)")
    parser.add_argument("--no-racknerd", dest="racknerd_enabled", action="store_false",
                        help="Disable Racknerd SMTP backend")
    parser.add_argument("--racknerd-direct", action="store_true", default=None,
                        help="Use Racknerd in direct TCP mode (no SOCKS5 tunnel — use when running on the egress VPS)")
    parser.add_argument("--racknerd-host", default=None, help="Racknerd VPS hostname")
    parser.add_argument("--racknerd-ssh-user", default="egress", help="SSH username")
    parser.add_argument("--racknerd-ssh-key", default=None, help="SSH private key path")
    parser.add_argument("--racknerd-ssh-port", type=int, default=22, help="SSH port")
    parser.add_argument("--racknerd-socks-port", type=int, default=1080, help="Local SOCKS5 port")
    parser.add_argument("--racknerd-concurrency", type=int, default=10,
                        help="Concurrent SMTP probes via Racknerd")
    parser.add_argument("--racknerd-smtp-timeout-s", type=float, default=15.0,
                        help="Per-SMTP-operation timeout")

    # bbops
    parser.add_argument("--bbops-base-url", default=None, help="bbops.io base URL")
    parser.add_argument("--bbops-batch-size", type=int, default=500, help="Emails per batch")
    parser.add_argument("--bbops-min-batch-size", type=int, default=8,
                        help="Minimum emails before flushing a bbops batch")
    parser.add_argument("--bbops-max-inflight", type=int, default=12,
                        help="Concurrent in-flight bbops batches")

    # Rate limits
    parser.add_argument("--serper-rate-limit", type=int, default=500,
                        help="Serper calls/hour ceiling")
    parser.add_argument("--zuhal-rate-limit", type=int, default=100,
                        help="Zuhal calls/hour ceiling")
    parser.add_argument("--zuhal-concurrency", type=int, default=5,
                        help="Concurrent Zuhal fallback probes")

    # Backoff
    parser.add_argument("--max-attempts", type=int, default=3, help="Max retries per phase")
    parser.add_argument("--backoff-base-dns", type=float, default=0.5)
    parser.add_argument("--backoff-base-serper", type=float, default=1.0)
    parser.add_argument("--backoff-max-dns", type=float, default=8.0)
    parser.add_argument("--backoff-max-serper", type=float, default=32.0)
    parser.add_argument("--backoff-jitter", type=float, default=0.2)

    # Cost / safety
    parser.add_argument("--max-cost", type=float, default=None, help="USD cost ceiling")
    parser.add_argument("--max-consecutive-errors", type=int, default=10,
                        help="Halt pipeline after this many consecutive errors")
    parser.add_argument("--dry-run", action="store_true", help="Mock all API calls")

    # Enrichment
    parser.add_argument("--enrichment-source", default="serper",
                        choices=["serper"],
                        help="Phase 2 search API source")
    parser.add_argument("--ignore-cache", action="store_true",
                        help="Bypass Serper enrichment cache (forces live API call)")

    # Run ID
    parser.add_argument("--run-id", default="", help="Run identifier for stats")

    # IPC notify pipe
    parser.add_argument("--notify-pipe", default="", help="Named pipe path for producer→dispatcher wake signal")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.subcommand is None:
        args.subcommand = "run"

    if args.subcommand == "run":
        if not getattr(args, "consumer_only", False) and not getattr(args, "input_path", None):
            parser.error("-i/--input is required for the run subcommand (unless --consumer-only)")

    return args
