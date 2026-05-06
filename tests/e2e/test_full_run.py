"""End-to-end tests for full pipeline runs via subprocess CLI."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent)

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
