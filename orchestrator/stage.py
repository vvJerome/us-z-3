from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from .config import PROJECT_ROOT, RunPaths


def run(paths: RunPaths, env: dict[str, str]) -> None:
    """Three sequential sub-steps:

    1. Producer  — DNS probe + Serper enrichment → fills DISCOVERED queue.
    2. bbops.io  — verify_emails.py → SMTP-style validation via bbops.io API.
    3. Consumer  — MS probe + Zuhal validation for anything bbops didn't settle.
    """
    paths.v2_dir.mkdir(parents=True, exist_ok=True)
    paths.v2_output_dir.mkdir(parents=True, exist_ok=True)

    _run_producer(paths, env)
    _run_bbops(paths, env)
    _run_consumer(paths, env)


def _run_producer(paths: RunPaths, env: dict[str, str]) -> None:
    cmd = [
        sys.executable, "-m", "pipeline", "run",
        "-i", str(paths.v2_input.resolve()),
        "--db", str(paths.v2_db.resolve()),
        "-o", str(paths.v2_output_dir.resolve()),
        "--log-dir", str((paths.v2_dir / "logs").resolve()),
        "--producer-only",
    ]
    _invoke(cmd, PROJECT_ROOT, paths.v2_stderr_log, env, label="v2-producer")


def _run_bbops(paths: RunPaths, env: dict[str, str]) -> None:
    cmd = [
        sys.executable, "-m", "pipeline.bbops",
        "--db", str(paths.v2_db.resolve()),
        "--out", str(paths.v2_bbops_csv.resolve()),
    ]
    merged_env = {**os.environ, **env,
                  "PIPELINE_DB": str(paths.v2_db.resolve()),
                  "OUTPUT_CSV": str(paths.v2_bbops_csv.resolve())}
    _invoke(cmd, PROJECT_ROOT, paths.v2_stderr_log, merged_env, label="v2-bbops",
            env_is_full=True)


def _run_consumer(paths: RunPaths, env: dict[str, str]) -> None:
    cmd = [
        sys.executable, "-m", "pipeline", "run",
        "--db", str(paths.v2_db.resolve()),
        "-o", str(paths.v2_output_dir.resolve()),
        "--log-dir", str((paths.v2_dir / "logs").resolve()),
        "--consumer-only",
    ]
    _invoke(cmd, PROJECT_ROOT, paths.v2_stderr_log, env, label="v2-consumer")


def _invoke(cmd: list[str], cwd: Path, errlog_path: Path,
            env: dict[str, str], label: str,
            env_is_full: bool = False) -> None:
    merged = env if env_is_full else {**os.environ, **env}
    with errlog_path.open("a", encoding="utf-8") as errlog:
        errlog.write(f"\n=== {label} ===\n")
        errlog.flush()
        proc = subprocess.run(cmd, cwd=str(cwd), stderr=errlog, env=merged)
    if proc.returncode != 0:
        raise RuntimeError(
            f"{label} exited with code {proc.returncode}. See {errlog_path}"
        )
