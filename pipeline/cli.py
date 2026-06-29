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
    parser.add_argument("--master-db", default=None, help="Master DB path — flush verified records here every 500 validations")
    parser.add_argument("--enrichment-cache-db", default=None,
                        help="Persistent Serper enrichment cache, shared across runs (default: cache is per-run only)")

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
    parser.add_argument("--harvest", dest="harvest_enabled", action="store_true", default=None,
                        help="Enable free website harvest (emails + officers) before paid Serper fallback")
    parser.add_argument("--racknerd-direct", action="store_true", default=None,
                        help="Use Racknerd in direct TCP mode (no SOCKS5 tunnel — use when running on the egress VPS)")
    parser.add_argument("--racknerd-host", default=None, help="Racknerd VPS hostname")
    parser.add_argument("--racknerd-ssh-user", default=None, help="SSH username")
    parser.add_argument("--racknerd-ssh-key", default=None, help="SSH private key path")
    parser.add_argument("--racknerd-ssh-port", type=int, default=22, help="SSH port")
    parser.add_argument("--racknerd-socks-port", type=int, default=1080, help="Local SOCKS5 port")
    parser.add_argument("--racknerd-concurrency", type=int, default=10,
                        help="Concurrent SMTP probes via Racknerd")
    parser.add_argument("--racknerd-smtp-timeout-s", type=float, default=15.0,
                        help="Per-SMTP-operation timeout")
    parser.add_argument("--racknerd-helo", dest="racknerd_helo_hostname", default=None,
                        help="SMTP EHLO/MAIL FROM domain (overrides RACKNERD_HELO_HOSTNAME env). "
                             "Use a real FQDN; IP literals are rejected by most MX servers.")

    # bbops
    parser.add_argument("--bbops-base-url", default=None, help="bbops.io base URL")
    parser.add_argument("--bbops-batch-size", type=int, default=500, help="Emails per batch")
    parser.add_argument("--bbops-min-batch-size", type=int, default=8,
                        help="Minimum emails before flushing a bbops batch")
    parser.add_argument("--bbops-max-inflight", type=int, default=12,
                        help="Concurrent in-flight bbops batches")
    parser.add_argument("--bbops-flush-interval-s", type=float, default=None,
                        help="Seconds before flushing a partial bbops batch")
    parser.add_argument("--bbops-poll-interval-s", type=float, default=None,
                        help="Seconds between bbops result-polling cycles")

    # Rate limits
    parser.add_argument("--zuhal-rate-limit", type=int, default=100,
                        help="Zuhal calls/hour ceiling")
    parser.add_argument("--zuhal-concurrency", type=int, default=5,
                        help="Concurrent Zuhal fallback probes")
    parser.add_argument("--zuhal-on-both-invalid", action="store_true", default=False,
                        help="Run Zuhal rescue even when both SMTP backends return invalid")
    parser.add_argument("--zuhal-decoupled", dest="zuhal_decoupled", action="store_true", default=None,
                        help="Run Zuhal rescue in a separate worker pool (default: on)")
    parser.add_argument("--no-zuhal-decoupled", dest="zuhal_decoupled", action="store_false",
                        help="Run Zuhal inline inside the SMTP dispatcher (legacy behavior)")
    parser.add_argument("--zuhal-poll-interval-s", type=float, default=None,
                        help="Zuhal worker poll interval when queue is empty")
    parser.add_argument("--zuhal-chunk-size", type=int, default=None,
                        help="Records claimed per Zuhal-worker poll cycle")
    parser.add_argument("--zuhal-concurrency-min", type=int, default=None,
                        help="Minimum concurrency when adaptive scaling backs off")
    parser.add_argument("--zuhal-concurrency-max", type=int, default=None,
                        help="Maximum concurrency adaptive scaling can reach")
    parser.add_argument("--zuhal-backpressure-threshold", type=int, default=None,
                        help="Pause SMTP handoffs when NEEDS_ZUHAL exceeds this (0=disabled)")
    parser.add_argument("--zuhal-bulk-threshold", type=int, default=None,
                        help="Switch to bulk CSV upload mode when backlog exceeds this")
    parser.add_argument("--zuhal-bulk-batch-size", type=int, default=None,
                        help="Emails per bulk upload batch")
    parser.add_argument("--zuhal-bulk-concurrent-jobs", type=int, default=None,
                        help="Concurrent bulk upload jobs per worker (default 1)")

    # Backoff (per-service base/max delays live in constants.SERVICE_BACKOFF)
    parser.add_argument("--max-attempts", type=int, default=3, help="Max retries per phase")
    parser.add_argument("--backoff-jitter", type=float, default=0.2)

    # Cost / safety
    parser.add_argument("--max-cost", type=float, default=None, help="USD cost ceiling")
    parser.add_argument("--max-dispatch-attempts", type=int, default=None,
                        help="Max real-verdict attempts before marking VALIDATION_FAILED (default: 5)")
    parser.add_argument("--max-requeue-count", type=int, default=None,
                        help="Max total re-queues (safety valve against infra loops, default: 15)")
    parser.add_argument("--dry-run", action="store_true", help="Mock all API calls")

    # Enrichment
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
