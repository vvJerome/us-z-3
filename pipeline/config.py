from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- API Keys (from .env only) ---
    serper_api_key: str = ""
    zuhal_api_key: str = ""

    # --- I/O ---
    input_path: Path = Path("input/records.jsonl")
    output_dir: Path = Path("output")
    db_path: Path = Path("output/pipeline.db")
    log_dir: Path = Path("output/logs")

    # --- Scope ---
    limit: int | None = None
    start_offset: int = 0
    ignore_checkpoint: bool = False
    chunk_size: int = Field(default=100, ge=1, le=10000)

    # --- Worker mode ---
    producer_only: bool = False
    consumer_only: bool = False

    # --- Strategy ---
    strategy: Literal["auto", "with", "without"] = "auto"

    # --- Producer concurrency ---
    dns_concurrency: int = Field(default=100, le=200)
    serper_concurrency: int = 15

    # --- Dispatcher ---
    dispatch_concurrency: int = Field(default=50, ge=1)
    dispatch_backend_timeout_s: float = 60.0
    dispatch_poll_interval_s: float = 5.0
    dispatch_chunk_size: int = Field(default=50, ge=1)

    # --- Racknerd backend ---
    racknerd_enabled: bool = True
    racknerd_direct: bool = False  # skip SSH tunnel, probe SMTP directly from this machine
    racknerd_host: str = ""
    racknerd_ssh_user: str = "egress"
    racknerd_ssh_key: str = "~/.ssh/racknerd_egress"
    racknerd_ssh_port: int = 22
    racknerd_socks_port: int = 1080
    racknerd_concurrency: int = Field(default=25, ge=1)
    racknerd_smtp_timeout_s: float = 8.0
    racknerd_helo_hostname: str | None = None  # override SMTP EHLO/MAIL FROM domain; falls back to _default_helo_hostname()

    # --- Cherry Servers SMTP fleet (Improve-Existing items 1/5/6) ---
    cherry_enabled: bool = False
    cherry_project_id: int = 0
    cherry_team_id: int = 0
    cherry_plan: str = "B2-1-1gb-20s-shared"
    cherry_region: str = "EU-Nord-1"
    cherry_failover_region: str = "US-Chicago"
    cherry_image: str = "ubuntu_22_04"
    cherry_fleet_size: int = Field(default=4, ge=1)
    cherry_ssh_user: str = "root"
    cherry_ssh_key: str = "~/.ssh/cherry_fleet"
    smtp_hosts: list[str] = Field(default_factory=list)  # explicit worker IPs; else read inventory
    fleet_block_cooldown_s: float = 300.0
    fleet_max_reroutes: int = Field(default=2, ge=0)
    fleet_credit_floor_eur: float = 0.10
    fleet_max_reprovisions: int = Field(default=10, ge=0)
    fleet_scale_min: int = Field(default=1, ge=1)
    fleet_scale_max: int = Field(default=10, ge=1)
    fleet_autoscale: bool = False
    fleet_monitor_interval_s: float = 15.0

    # --- Durable state backup (item 2; off by default) ---
    backup_enabled: bool = False
    backup_dir: str = ""  # local dir; optional alongside R2
    backup_r2_endpoint: str = ""  # S3-compatible R2 endpoint incl. bucket; creds via env
    backup_interval_s: float = 300.0

    # --- bbops async backend ---
    bbops_base_url: str = "https://email-verifier.bbops.io"
    bbops_batch_size: int = Field(default=500, ge=1)
    bbops_min_batch_size: int = Field(default=8, ge=1)
    bbops_max_inflight: int = Field(default=12, ge=1)
    bbops_flush_interval_s: float = 2.0
    bbops_poll_interval_s: float = 10.0
    bbops_poll_timeout_s: float = 600.0
    bbops_health_fail_threshold: int = Field(default=3, ge=1)
    bbops_health_ok_threshold: int = Field(default=2, ge=1)

    # --- Rate limits (calls per hour) ---
    zuhal_rate_limit: int = 100

    # --- Zuhal fallback backend ---
    zuhal_concurrency: int = Field(default=5, ge=1)
    zuhal_concurrency_min: int = Field(default=2, ge=1)
    zuhal_concurrency_max: int = Field(default=50, ge=1)
    zuhal_on_both_invalid: bool = False
    zuhal_decoupled: bool = True
    # Identity/deliverability gates (0.0 = disabled, current behavior).
    # zuhal_min_confidence: candidates scoring below this skip paid Zuhal rescue.
    # catch_all_min_confidence: catch-all verdicts below this are not auto-accepted.
    zuhal_min_confidence: float = 0.0
    catch_all_min_confidence: float = 0.0
    zuhal_poll_interval_s: float = 5.0
    zuhal_chunk_size: int = Field(default=20, ge=1)
    # Backpressure: pause SMTP handoffs when NEEDS_ZUHAL exceeds this (0 = disabled)
    zuhal_backpressure_threshold: int = Field(default=5000, ge=0)
    zuhal_backpressure_sleep_s: float = 2.0
    # Bulk API: use CSV upload when backlog exceeds threshold
    zuhal_bulk_threshold: int = Field(default=200, ge=1)
    zuhal_bulk_batch_size: int = Field(default=1000, ge=1)
    zuhal_bulk_poll_interval_s: float = 30.0
    zuhal_bulk_concurrent_jobs: int = Field(default=1, ge=1)
    zuhal_bulk_stale_timeout_minutes: int = Field(default=120, ge=5)

    # --- Website harvest (pipeline.harvest) — free fallback before paid Serper ---
    harvest_enabled: bool = False  # opt-in via --harvest
    harvest_timeout_s: float = 8.0  # per-request cap
    harvest_rps: float = Field(default=2.0, gt=0)  # global politeness throttle across all harvests

    # --- Backoff (per-service base/max delays live in constants.SERVICE_BACKOFF) ---
    max_attempts: int = 3
    backoff_jitter: float = 0.2

    # --- Cost / safety ---
    max_cost: float | None = None
    dry_run: bool = False
    max_discovery_retries: int = Field(default=3, ge=0)
    max_dispatch_attempts: int = Field(default=5, ge=1)
    max_requeue_count: int = Field(default=15, ge=1)

    # --- Enrichment ---
    ignore_cache: bool = False

    # --- Run identity ---
    run_id: str = ""

    # --- IPC: named pipe path for producer→dispatcher notification ---
    notify_pipe: str = ""

    @model_validator(mode="after")
    def _saturate_dispatcher_chunk(self) -> PipelineConfig:
        if self.dispatch_chunk_size < self.dispatch_concurrency:
            logging.getLogger("pipeline").warning(
                "dispatch_chunk_size=%d < dispatch_concurrency=%d — auto-bumping chunk_size to %d "
                "so the worker pool stays saturated",
                self.dispatch_chunk_size, self.dispatch_concurrency,
                self.dispatch_concurrency * 2,
            )
            self.dispatch_chunk_size = self.dispatch_concurrency * 2
        return self

    @model_validator(mode="after")
    def _validate_flags(self) -> PipelineConfig:
        if self.producer_only and self.consumer_only:
            raise ValueError("--producer-only and --consumer-only are mutually exclusive")

        if (
            self.racknerd_enabled and not self.racknerd_direct and not self.racknerd_host
            and not self.producer_only and not self.cherry_enabled and not self.smtp_hosts
        ):
            raise ValueError(
                "RACKNERD_HOST must be set when racknerd_enabled=True (not direct mode). "
                "Set RACKNERD_HOST in .env, pass --racknerd-host, --smtp-hosts, "
                "--cherry-enabled, or use --racknerd-direct."
            )

        if self.ignore_checkpoint and self.start_offset == 0:
            logging.getLogger("pipeline").warning(
                "--ignore-checkpoint without --start-offset is a no-op (starts from line 0)"
            )

        if self.consumer_only and self.strategy != "auto":
            logging.getLogger("pipeline").warning(
                f"--strategy {self.strategy} has no effect with --consumer-only"
            )

        return self
