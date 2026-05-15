"""
Adapter entrypoint: translates Kestra env vars into us-z-3 orchestrator calls.

Env vars injected by Kestra:
  JOB_ID          — UUID assigned by FastAPI
  INPUT_FILE_KEY  — relative path within DATA_DIR (e.g. inputs/abc123/input.jsonl)
  CONFIG          — JSON: {"enable_proxy": false, "skip_duplicates": true}
  DATA_DIR        — absolute path to the shared data volume (default: /data)
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from pathlib import Path


def _setup_logging(log_path: Path) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )
    return logging.getLogger("entrypoint")


def _read_env() -> tuple[str, Path, bool, bool, Path]:
    job_id = os.environ.get("JOB_ID", "").strip()
    input_file_key = os.environ.get("INPUT_FILE_KEY", "").strip()
    config_json = os.environ.get("CONFIG", "{}")
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))

    if not job_id:
        print("FATAL: JOB_ID env var is required", file=sys.stderr)
        sys.exit(1)
    if not input_file_key:
        print("FATAL: INPUT_FILE_KEY env var is required", file=sys.stderr)
        sys.exit(1)
    if ".." in input_file_key:
        print(
            f"FATAL: path traversal in INPUT_FILE_KEY: {input_file_key}",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        config: dict = json.loads(config_json)
    except json.JSONDecodeError as exc:
        print(f"FATAL: CONFIG is not valid JSON: {exc}", file=sys.stderr)
        sys.exit(1)

    enable_proxy: bool = bool(config.get("enable_proxy", False))
    skip_duplicates: bool = bool(config.get("skip_duplicates", True))
    input_path = data_dir / input_file_key

    return job_id, input_path, enable_proxy, skip_duplicates, data_dir


def _prepare_v2_input(src: Path, dst: Path) -> int:
    """Write JSONL with per-officer composite unique_id (filing_id__agent_id)."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    written = skipped_parse = skipped_no_id = 0
    with src.open("r", encoding="utf-8") as sf, dst.open("w", encoding="utf-8") as df:
        for lineno, line in enumerate(sf, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                skipped_parse += 1
                logging.getLogger("entrypoint").debug(
                    "Line %d: invalid JSON — skipped", lineno
                )
                continue
            filing_id = str(
                rec.get("unique_id") or rec.get("raw_unique_id") or ""
            ).strip()
            agent_id = str(rec.get("unique_agent_id") or "").strip()
            if not filing_id:
                skipped_no_id += 1
                logging.getLogger("entrypoint").debug(
                    "Line %d: missing unique_id — skipped", lineno
                )
                continue
            rec["unique_id"] = f"{filing_id}__{agent_id}" if agent_id else filing_id
            rec["filing_id"] = filing_id
            rec["agent_id"] = agent_id
            df.write(json.dumps(rec) + "\n")
            written += 1
    if skipped_parse or skipped_no_id:
        logging.getLogger("entrypoint").warning(
            "Input prep: %d written, %d skipped (parse errors), %d skipped (no unique_id)",
            written,
            skipped_parse,
            skipped_no_id,
        )
    return written


def main() -> int:
    job_id, input_path, enable_proxy, skip_duplicates, data_dir = _read_env()

    # Directories for this job under the shared data volume
    run_dir = data_dir / "jobs" / job_id
    output_path = data_dir / "outputs" / job_id / "result.csv"
    log_path = data_dir / "logs" / job_id / "run.log"

    for directory in (run_dir, output_path.parent, log_path.parent):
        directory.mkdir(parents=True, exist_ok=True)

    log = _setup_logging(log_path)
    log.info("JOB_ID=%s  INPUT_FILE_KEY=%s  DATA_DIR=%s", job_id, input_path, data_dir)
    log.info("enable_proxy=%s  skip_duplicates=%s", enable_proxy, skip_duplicates)

    if not input_path.exists():
        log.error("Input file not found: %s", input_path)
        return 1

    # Apply config-driven env overrides before us-z-3 imports read them
    if not enable_proxy:
        os.environ["RACKNERD_ENABLED"] = "false"
        log.info("Racknerd SMTP backend disabled (enable_proxy=false)")

    # Import after env overrides are set
    from orchestrator import merge_outputs, stage  # noqa: E402
    from orchestrator.config import Env, RunPaths  # noqa: E402

    # Build RunPaths mapped to this job's directory (bypasses timestamp naming)
    paths = RunPaths.attach(run_dir)
    paths.ensure()

    log.info("Preparing V2 input → %s", paths.v2_input)
    written = _prepare_v2_input(input_path, paths.v2_input)
    log.info("  %d records prepared", written)

    if written == 0:
        log.error("Input file contains no valid records: %s", input_path)
        return 1

    env = Env.load()
    env_extra = {
        k: v
        for k, v in {
            "SERPER_API_KEY": env.serper_api_key,
            "ZUHAL_API_KEY": env.zuhal_api_key,
        }.items()
        if v
    }

    log.info("Running pipeline stages: producer → bbops → consumer")
    try:
        stage.run(paths, env_extra)
    except RuntimeError as exc:
        log.error("Pipeline stage failed: %s", exc)
        log.error("Check stderr log at: %s", paths.v2_stderr_log)
        return 1

    log.info("Merging outputs (skip_duplicates=%s)", skip_duplicates)
    counts = merge_outputs.merge(paths.v2_db, paths.merged_csv)
    log.info("Merged: total=%d  duplicates=%d", counts["total"], counts["duplicates"])

    if counts["total"] == 0:
        log.warning("Pipeline completed but produced zero validated records")

    shutil.copy(paths.merged_csv, output_path)
    log.info("Output written to %s", output_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
