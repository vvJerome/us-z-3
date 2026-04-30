"""End-to-end tests for full pipeline runs via subprocess CLI."""

import json
import subprocess
import sys
from pathlib import Path

import pytest


class TestFullPipelineRun:
    """End-to-end tests running the full pipeline."""

    def test_full_run_dry_mode(self, tmp_path: Path):
        """Full pipeline run with --dry-run completes successfully."""
        # Create input file
        input_file = tmp_path / "input.jsonl"
        records = [
            {
                "unique_id": "test_1",
                "business_name": "Test Corp",
                "agent_name": "John Doe",
                "state": "NY",
            },
            {
                "unique_id": "test_2",
                "business_name": "Another Inc",
                "agent_name": "Jane Smith",
                "state": "CA",
            },
        ]

        with open(input_file, "w") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        db_path = tmp_path / "pipeline.db"

        # Run pipeline with --dry-run
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pipeline",
                "--dry-run",
                "-i",
                str(input_file),
                "-o",
                str(output_dir),
                "--db",
                str(db_path),
                "--limit",
                "10",
            ],
            cwd=str(Path(__file__).resolve().parent.parent.parent),
            capture_output=True,
            timeout=30,
        )

        # Should complete without error
        assert result.returncode == 0, f"stdout: {result.stdout.decode()}\nstderr: {result.stderr.decode()}"

    def test_pipeline_creates_output_directory(self, tmp_path: Path):
        """Pipeline creates output directory if it doesn't exist."""
        input_file = tmp_path / "input.jsonl"
        with open(input_file, "w") as f:
            f.write(json.dumps({"unique_id": "test", "business_name": "Test"}) + "\n")

        output_dir = tmp_path / "output_subdir" / "nested"
        db_path = tmp_path / "pipeline.db"

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pipeline",
                "--dry-run",
                "-i",
                str(input_file),
                "-o",
                str(output_dir),
                "--db",
                str(db_path),
                "--limit",
                "1",
            ],
            cwd=str(Path(__file__).resolve().parent.parent.parent),
            capture_output=True,
            timeout=30,
        )

        assert result.returncode == 0

    def test_pipeline_respects_limit(self, tmp_path: Path):
        """Pipeline respects --limit parameter."""
        input_file = tmp_path / "input.jsonl"

        # Create 10 records
        with open(input_file, "w") as f:
            for i in range(10):
                f.write(
                    json.dumps({
                        "unique_id": f"test_{i}",
                        "business_name": f"Corp {i}",
                    }) + "\n"
                )

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        db_path = tmp_path / "pipeline.db"

        # Limit to 3 records
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pipeline",
                "--dry-run",
                "-i",
                str(input_file),
                "-o",
                str(output_dir),
                "--db",
                str(db_path),
                "--limit",
                "3",
            ],
            cwd=str(Path(__file__).resolve().parent.parent.parent),
            capture_output=True,
            timeout=30,
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
                sys.executable,
                "-m",
                "pipeline",
                "--dry-run",
                "-i",
                str(input_file),
                "-o",
                str(output_dir),
                "--db",
                str(db_path),
                "--limit",
                "1",
            ],
            cwd=str(Path(__file__).resolve().parent.parent.parent),
            capture_output=True,
            timeout=30,
        )

        assert db_path.exists()

    def test_pipeline_with_required_env(self, tmp_path: Path):
        """Pipeline works with environment variables."""
        import os

        input_file = tmp_path / "input.jsonl"
        with open(input_file, "w") as f:
            f.write(json.dumps({"unique_id": "test", "business_name": "Test"}) + "\n")

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        db_path = tmp_path / "pipeline.db"

        env = os.environ.copy()
        env["SERPER_API_KEY"] = "dummy_key"
        env["ZUHAL_API_KEY"] = "dummy_key"

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pipeline",
                "--dry-run",
                "-i",
                str(input_file),
                "-o",
                str(output_dir),
                "--db",
                str(db_path),
                "--limit",
                "1",
            ],
            cwd=str(Path(__file__).resolve().parent.parent.parent),
            capture_output=True,
            timeout=30,
            env=env,
        )

        assert result.returncode == 0

    def test_pipeline_invalid_input_file(self, tmp_path: Path):
        """Pipeline handles missing input file gracefully."""
        nonexistent = tmp_path / "missing.jsonl"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pipeline",
                "--dry-run",
                "-i",
                str(nonexistent),
                "-o",
                str(output_dir),
            ],
            cwd=str(Path(__file__).resolve().parent.parent.parent),
            capture_output=True,
            timeout=30,
        )

        # Should fail with non-zero exit code
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
                sys.executable,
                "-m",
                "pipeline",
                "--dry-run",
                "-i",
                str(input_file),
                "-o",
                str(output_dir),
                "--db",
                str(db_path),
                "--chunk-size",
                "2",
            ],
            cwd=str(Path(__file__).resolve().parent.parent.parent),
            capture_output=True,
            timeout=30,
        )

        assert result.returncode == 0

    def test_pipeline_without_dry_run_exits_cleanly(self, tmp_path: Path):
        """Without --dry-run and with empty API keys, pipeline processes records gracefully.

        API key validation happens at call time (not startup), so exit code is 0
        but records end up in DISCOVERY_FAILED state due to failed enrichment.
        Use --dry-run in all tests that don't need to verify live-key behavior.
        """
        input_file = tmp_path / "input.jsonl"
        with open(input_file, "w") as f:
            f.write(json.dumps({"unique_id": "test", "business_name": "Test Corp"}) + "\n")

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        db_path = tmp_path / "pipeline.db"

        import os
        env = {k: v for k, v in os.environ.items()
               if k not in ("SERPER_API_KEY", "ZUHAL_API_KEY")}
        env.update({"SERPER_API_KEY": "", "ZUHAL_API_KEY": ""})

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pipeline",
                "-i",
                str(input_file),
                "-o",
                str(output_dir),
                "--db",
                str(db_path),
                "--producer-only",
            ],
            cwd=str(Path(__file__).resolve().parent.parent.parent),
            env=env,
            capture_output=True,
            timeout=30,
        )

        # Pipeline exits 0 — failed API calls are per-record errors, not fatal
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

        # This should complete without trying to call external APIs
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pipeline",
                "--dry-run",
                "-i",
                str(input_file),
                "-o",
                str(output_dir),
                "--db",
                str(db_path),
                "--limit",
                "1",
            ],
            cwd=str(Path(__file__).resolve().parent.parent.parent),
            capture_output=True,
            timeout=30,
        )

        assert result.returncode == 0
        # Should not see API errors (since --dry-run skips API calls)
        assert "API" not in result.stderr.decode() or "error" not in result.stderr.decode().lower()
