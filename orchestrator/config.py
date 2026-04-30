from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = PROJECT_ROOT / "input"
RUNS_DIR = PROJECT_ROOT / "runs"


@dataclass(frozen=True)
class RunPaths:
    run_dir: Path
    v2_input: Path
    v2_dir: Path
    v2_db: Path
    v2_output_dir: Path
    v2_bbops_csv: Path
    v2_stderr_log: Path
    merged_csv: Path
    manifest: Path

    @classmethod
    def for_run(cls, run_name: str) -> "RunPaths":
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        slug = _slugify(run_name)
        run_dir = RUNS_DIR / f"{slug}_{ts}"
        return cls._build(run_dir)

    @classmethod
    def attach(cls, run_dir: Path) -> "RunPaths":
        return cls._build(run_dir)

    @classmethod
    def _build(cls, run_dir: Path) -> "RunPaths":
        v2_dir = run_dir / "v2"
        return cls(
            run_dir=run_dir,
            v2_input=run_dir / "v2_input.jsonl",
            v2_dir=v2_dir,
            v2_db=v2_dir / "pipeline.db",
            v2_output_dir=v2_dir / "output",
            v2_bbops_csv=v2_dir / "bbops_valid_emails.csv",
            v2_stderr_log=v2_dir / "stderr.log",
            merged_csv=run_dir / "merged_valid_emails.csv",
            manifest=run_dir / "manifest.json",
        )

    def ensure(self) -> None:
        for p in (self.run_dir, self.v2_dir, self.v2_output_dir):
            p.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class Env:
    bbops_base_url: str
    serper_api_key: str
    zuhal_api_key: str

    @classmethod
    def load(cls) -> "Env":
        if load_dotenv is not None:
            load_dotenv(PROJECT_ROOT / ".env")
        return cls(
            bbops_base_url=os.environ.get("BBOPS_BASE_URL", "https://email-verifier.bbops.io"),
            serper_api_key=os.environ.get("SERPER_API_KEY", ""),
            zuhal_api_key=os.environ.get("ZUHAL_API_KEY", ""),
        )


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", name.strip()).strip("_")
    return slug or "run"
