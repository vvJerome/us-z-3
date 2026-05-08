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

    # --- bbops async backend ---
    bbops_base_url: str = "https://email-verifier.bbops.io"
    bbops_batch_size: int = Field(default=500, ge=1)
    bbops_min_batch_size: int = Field(default=8, ge=1)
    bbops_max_inflight: int = Field(default=12, ge=1)
    bbops_flush_interval_s: float = 2.0
    bbops_poll_interval_s: float = 10.0
    bbops_poll_timeout_s: float = 1800.0
    bbops_health_fail_threshold: int = Field(default=3, ge=1)
    bbops_health_ok_threshold: int = Field(default=2, ge=1)

    # --- Rate limits (calls per hour) ---
    serper_rate_limit: int = 500
    zuhal_rate_limit: int = 100

    # --- Zuhal fallback backend ---
    zuhal_concurrency: int = Field(default=5, ge=1)

    # --- Backoff ---
    max_attempts: int = 3
    backoff_base_dns: float = 0.5
    backoff_base_serper: float = 1.0
    backoff_max_dns: float = 8.0
    backoff_max_serper: float = 32.0
    backoff_jitter: float = 0.2

    # --- Cost / safety ---
    max_cost: float | None = None
    dry_run: bool = False
    max_consecutive_errors: int = Field(default=10, ge=1)
    max_discovery_retries: int = Field(default=3, ge=0)
    max_dispatch_attempts: int = Field(default=3, ge=1)

    # --- Enrichment ---
    enrichment_source: Literal["serper"] = "serper"
    ignore_cache: bool = False

    # --- Run identity ---
    run_id: str = ""

    # --- IPC: named pipe path for producer→dispatcher notification ---
    notify_pipe: str = ""

    @model_validator(mode="after")
    def _validate_flags(self) -> PipelineConfig:
        if self.producer_only and self.consumer_only:
            raise ValueError("--producer-only and --consumer-only are mutually exclusive")

        if self.racknerd_enabled and not self.racknerd_direct and not self.racknerd_host and not self.producer_only:
            raise ValueError(
                "RACKNERD_HOST must be set when racknerd_enabled=True (not direct mode). "
                "Set RACKNERD_HOST in .env, pass --racknerd-host, or use --racknerd-direct."
            )

        needs_dispatcher = not self.producer_only and not self.dry_run
        if needs_dispatcher and not self.zuhal_api_key:
            raise ValueError(
                "ZUHAL_API_KEY must be set — Zuhal is required as the rescue backend. "
                "Set ZUHAL_API_KEY in .env or use --dry-run / --producer-only."
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
