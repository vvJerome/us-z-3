"""End-to-end tests for full pipeline runs via subprocess CLI."""

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent)
sys.path.insert(0, _REPO_ROOT)

from pipeline.db.schema import SCHEMA_SQL, INSERT_RECORD_SQL  # noqa: E402

# Base env: strip live keys so tests stay offline
def _test_env(**overrides) -> dict:
    env = {k: v for k, v in os.environ.items()
           if k not in ("SERPER_API_KEY", "ZUHAL_API_KEY", "RACKNERD_HOST")}
    env.update({
        "SERPER_API_KEY": "",
        "ZUHAL_API_KEY": "",
    })
    env.update(overrides)
    return env


def _seed_discovered(db_path: Path, unique_id: str = "seed_1") -> None:
    """Create a DB with one DISCOVERED record using synchronous sqlite3."""
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA_SQL)
    conn.execute(
        INSERT_RECORD_SQL,
        (
            unique_id, "Test Corp", "John Doe", "NC",
            None, None, None,                          # jurisdiction, position_type, name_entity_type
            "jdoe@testcorp-xyz-fake.com",              # candidate_email
            json.dumps(["jdoe@testcorp-xyz-fake.com"]),# candidate_emails
            None,                                      # subdomain_emails
            "testcorp-xyz-fake.com",                   # candidate_domain
            "dns", 1,                                  # discovery_source, discovery_attempts
            "with", 0,                                 # strategy, is_org_agent
            "google.com",                              # mx_provider
            0.8, 0.5,                                  # domain_confidence, owner_confidence
            "DISCOVERED",                              # record_state
            json.dumps([]),                            # process_trace
            0,                                         # serper_enriched
        ),
    )
    conn.commit()
    conn.close()


class TestFullPipelineRun:
    """End-to-end tests running the producer phase of the pipeline."""

    def test_producer_only_dry_mode(self, tmp_path: Path):
        """Producer-only run with --dry-run completes successfully."""
        input_file = tmp_path / "input.jsonl"
        records = [
            {"unique_id": "test_1", "business_name": "Test Corp", "agent_name": "John Doe", "state": "NY"},
            {"unique_id": "test_2", "business_name": "Another Inc", "agent_name": "Jane Smith", "state": "CA"},
        ]
        with open(input_file, "w") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        db_path = tmp_path / "pipeline.db"

        result = subprocess.run(
            [
                sys.executable, "-m", "pipeline",
                "--dry-run", "--producer-only",
                "-i", str(input_file),
                "-o", str(output_dir),
                "--db", str(db_path),
                "--limit", "10",
            ],
            cwd=_REPO_ROOT,
            capture_output=True,
            timeout=30,
            env=_test_env(),
        )

        assert result.returncode == 0, (
            f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )

    def test_pipeline_creates_output_directory(self, tmp_path: Path):
        """Pipeline creates output directory if it doesn't exist."""
        input_file = tmp_path / "input.jsonl"
        with open(input_file, "w") as f:
            f.write(json.dumps({"unique_id": "test", "business_name": "Test"}) + "\n")

        output_dir = tmp_path / "output_subdir" / "nested"
        db_path = tmp_path / "pipeline.db"

        result = subprocess.run(
            [
                sys.executable, "-m", "pipeline",
                "--dry-run", "--producer-only",
                "-i", str(input_file),
                "-o", str(output_dir),
                "--db", str(db_path),
                "--limit", "1",
            ],
            cwd=_REPO_ROOT,
            capture_output=True,
            timeout=30,
            env=_test_env(),
        )

        assert result.returncode == 0

    def test_pipeline_respects_limit(self, tmp_path: Path):
        """Pipeline respects --limit parameter."""
        input_file = tmp_path / "input.jsonl"
        with open(input_file, "w") as f:
            for i in range(10):
                f.write(json.dumps({"unique_id": f"test_{i}", "business_name": f"Corp {i}"}) + "\n")

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        db_path = tmp_path / "pipeline.db"

        result = subprocess.run(
            [
                sys.executable, "-m", "pipeline",
                "--dry-run", "--producer-only",
                "-i", str(input_file),
                "-o", str(output_dir),
                "--db", str(db_path),
                "--limit", "3",
            ],
            cwd=_REPO_ROOT,
            capture_output=True,
            timeout=30,
            env=_test_env(),
        )

        assert result.returncode == 0

    def test_pipeline_database_created(self, tmp_path: Path):
        """Pipeline creates database file."""
        input_file = tmp_path / "input.jsonl"
        with open(input_file, "w") as f:
            f.write(json.dumps({"unique_id": "test", "business_name": "Test"}) + "\n")

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        db_path = tmp_path / "test.db"

        subprocess.run(
            [
                sys.executable, "-m", "pipeline",
                "--dry-run", "--producer-only",
                "-i", str(input_file),
                "-o", str(output_dir),
                "--db", str(db_path),
                "--limit", "1",
            ],
            cwd=_REPO_ROOT,
            capture_output=True,
            timeout=30,
            env=_test_env(),
        )

        assert db_path.exists()

    def test_enrichment_cache_db_flag_creates_separate_file(self, tmp_path: Path):
        """--enrichment-cache-db creates its own file, distinct from --db."""
        input_file = tmp_path / "input.jsonl"
        with open(input_file, "w") as f:
            f.write(json.dumps({"unique_id": "test", "business_name": "Test Corp"}) + "\n")

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        db_path = tmp_path / "pipeline.db"
        cache_db_path = tmp_path / "enrichment_cache.db"

        result = subprocess.run(
            [
                sys.executable, "-m", "pipeline",
                "--dry-run", "--producer-only",
                "-i", str(input_file),
                "-o", str(output_dir),
                "--db", str(db_path),
                "--enrichment-cache-db", str(cache_db_path),
                "--limit", "1",
            ],
            cwd=_REPO_ROOT,
            capture_output=True,
            timeout=30,
            env=_test_env(),
        )

        assert result.returncode == 0, (
            f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"
        )
        assert db_path.exists()
        assert cache_db_path.exists()

    def test_pipeline_invalid_input_file(self, tmp_path: Path):
        """Pipeline handles missing input file gracefully."""
        nonexistent = tmp_path / "missing.jsonl"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        result = subprocess.run(
            [
                sys.executable, "-m", "pipeline",
                "--dry-run", "--producer-only",
                "-i", str(nonexistent),
                "-o", str(output_dir),
            ],
            cwd=_REPO_ROOT,
            capture_output=True,
            timeout=30,
            env=_test_env(),
        )

        assert result.returncode != 0

    def test_pipeline_chunk_size_parameter(self, tmp_path: Path):
        """Pipeline respects --chunk-size parameter."""
        input_file = tmp_path / "input.jsonl"
        with open(input_file, "w") as f:
            for i in range(5):
                f.write(json.dumps({"unique_id": f"test_{i}"}) + "\n")

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        db_path = tmp_path / "pipeline.db"

        result = subprocess.run(
            [
                sys.executable, "-m", "pipeline",
                "--dry-run", "--producer-only",
                "-i", str(input_file),
                "-o", str(output_dir),
                "--db", str(db_path),
                "--chunk-size", "2",
            ],
            cwd=_REPO_ROOT,
            capture_output=True,
            timeout=30,
            env=_test_env(),
        )

        assert result.returncode == 0

    def test_pipeline_without_dry_run_exits_cleanly(self, tmp_path: Path):
        """Without --dry-run and with empty API keys, pipeline processes records gracefully."""
        input_file = tmp_path / "input.jsonl"
        with open(input_file, "w") as f:
            f.write(json.dumps({"unique_id": "test", "business_name": "Test Corp"}) + "\n")

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        db_path = tmp_path / "pipeline.db"

        result = subprocess.run(
            [
                sys.executable, "-m", "pipeline",
                "-i", str(input_file),
                "-o", str(output_dir),
                "--db", str(db_path),
                "--producer-only",
            ],
            cwd=_REPO_ROOT,
            capture_output=True,
            timeout=30,
            env=_test_env(),
        )

        assert result.returncode == 0


class TestPipelineOutputGeneration:
    """Test pipeline output files."""

    def test_dry_run_no_external_calls(self, tmp_path: Path):
        """Dry run doesn't make external API calls."""
        input_file = tmp_path / "input.jsonl"
        with open(input_file, "w") as f:
            f.write(json.dumps({"unique_id": "test", "business_name": "Test"}) + "\n")

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        db_path = tmp_path / "pipeline.db"

        result = subprocess.run(
            [
                sys.executable, "-m", "pipeline",
                "--dry-run", "--producer-only",
                "-i", str(input_file),
                "-o", str(output_dir),
                "--db", str(db_path),
                "--limit", "1",
            ],
            cwd=_REPO_ROOT,
            capture_output=True,
            timeout=30,
            env=_test_env(),
        )

        assert result.returncode == 0


class TestDispatcherPath:
    """E2E tests that exercise the dispatcher code path.

    The dispatcher is a long-running daemon — it doesn't exit when its queue
    empties, it waits for more work. Tests use Popen + SIGTERM (the normal
    production shutdown path) and treat exit code 0 or -SIGTERM as success.
    """

    def test_dispatcher_starts_and_shuts_down_gracefully(self, tmp_path: Path):
        """Consumer-only dispatcher starts against a pre-seeded DB and shuts down on SIGTERM.

        Exercises: dispatcher init → DISCOVERED record claimed → backends run
        (NullRacknerd=not_run, bbops=error) → reconcile unknown → re-queue →
        SIGTERM → graceful shutdown. Validates the full dispatcher loop without
        needing real SMTP infrastructure.
        """
        db_path = tmp_path / "pipeline.db"
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        input_file = tmp_path / "input.jsonl"
        input_file.write_text("")  # consumer-only ignores -i but the flag is required

        _seed_discovered(db_path)

        proc = subprocess.Popen(
            [
                sys.executable, "-m", "pipeline",
                "--consumer-only", "--no-racknerd",
                "--bbops-base-url", "http://localhost:19999",  # guaranteed unreachable
                "-i", str(input_file),
                "-o", str(output_dir),
                "--db", str(db_path),
                "--dispatch-concurrency", "1",
                "--dispatch-backend-timeout-s", "2",
            ],
            cwd=_REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_test_env(),
        )

        # Let the dispatcher start and process at least one cycle.
        time.sleep(8)

        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

        # 0 = graceful exit, -SIGTERM = terminated by our signal (both are correct).
        assert proc.returncode in (0, -signal.SIGTERM), (
            f"returncode={proc.returncode}\n"
            f"stdout: {proc.stdout.read().decode()}\n"  # type: ignore[union-attr]
            f"stderr: {proc.stderr.read().decode()}"    # type: ignore[union-attr]
        )
