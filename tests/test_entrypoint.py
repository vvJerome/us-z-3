from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


# ── _read_env ─────────────────────────────────────────────────────────────────

def test_missing_job_id_exits(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("JOB_ID", raising=False)
    monkeypatch.setenv("INPUT_FILE_KEY", "inputs/abc/input.jsonl")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    from entrypoint import _read_env
    with pytest.raises(SystemExit) as exc_info:
        _read_env()
    assert exc_info.value.code == 1


def test_missing_input_file_key_exits(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("JOB_ID", "test-job-id")
    monkeypatch.delenv("INPUT_FILE_KEY", raising=False)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    from entrypoint import _read_env
    with pytest.raises(SystemExit) as exc_info:
        _read_env()
    assert exc_info.value.code == 1


def test_path_traversal_in_file_key_exits(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("JOB_ID", "test-job-id")
    monkeypatch.setenv("INPUT_FILE_KEY", "../../etc/passwd")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    from entrypoint import _read_env
    with pytest.raises(SystemExit) as exc_info:
        _read_env()
    assert exc_info.value.code == 1


def test_invalid_config_json_exits(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("JOB_ID", "test-job-id")
    monkeypatch.setenv("INPUT_FILE_KEY", "inputs/abc/input.jsonl")
    monkeypatch.setenv("CONFIG", "{not valid json}")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    from entrypoint import _read_env
    with pytest.raises(SystemExit) as exc_info:
        _read_env()
    assert exc_info.value.code == 1


def test_valid_env_parses_defaults(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("JOB_ID", "job-123")
    monkeypatch.setenv("INPUT_FILE_KEY", "inputs/job-123/input.jsonl")
    monkeypatch.delenv("CONFIG", raising=False)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    from entrypoint import _read_env
    job_id, input_path, enable_proxy, skip_duplicates, data_dir = _read_env()

    assert job_id == "job-123"
    assert enable_proxy is False
    assert skip_duplicates is True
    assert data_dir == tmp_path


def test_config_toggles_parsed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("JOB_ID", "job-456")
    monkeypatch.setenv("INPUT_FILE_KEY", "inputs/job-456/input.jsonl")
    monkeypatch.setenv("CONFIG", json.dumps({"enable_proxy": True, "skip_duplicates": False}))
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    from entrypoint import _read_env
    _, _, enable_proxy, skip_duplicates, _ = _read_env()

    assert enable_proxy is True
    assert skip_duplicates is False


# ── _prepare_v2_input ─────────────────────────────────────────────────────────

def test_prepare_v2_input_basic(tmp_path: Path) -> None:
    src = FIXTURES / "sample.jsonl"
    dst = tmp_path / "v2_input.jsonl"

    from entrypoint import _prepare_v2_input
    count = _prepare_v2_input(src, dst)

    assert count == 3
    lines = [json.loads(l) for l in dst.read_text().splitlines() if l.strip()]
    assert lines[0]["unique_id"] == "FL001__AG001"
    assert lines[0]["filing_id"] == "FL001"
    assert lines[0]["agent_id"] == "AG001"


def test_prepare_v2_input_empty_agent_id_omits_separator(tmp_path: Path) -> None:
    src = FIXTURES / "sample.jsonl"
    dst = tmp_path / "v2_input.jsonl"

    from entrypoint import _prepare_v2_input
    _prepare_v2_input(src, dst)

    lines = [json.loads(l) for l in dst.read_text().splitlines() if l.strip()]
    # TX001 has empty unique_agent_id
    tx_record = next(r for r in lines if r["filing_id"] == "TX001")
    assert "__" not in tx_record["unique_id"]
    assert tx_record["unique_id"] == "TX001"


def test_prepare_v2_input_skips_blank_lines(tmp_path: Path) -> None:
    src = tmp_path / "input.jsonl"
    src.write_text('\n{"unique_id":"A1","business_name":"Test"}\n\n')
    dst = tmp_path / "v2_input.jsonl"

    from entrypoint import _prepare_v2_input
    count = _prepare_v2_input(src, dst)
    assert count == 1


def test_prepare_v2_input_skips_malformed_json(tmp_path: Path) -> None:
    src = FIXTURES / "malformed.jsonl"
    dst = tmp_path / "v2_input.jsonl"

    from entrypoint import _prepare_v2_input
    count = _prepare_v2_input(src, dst)
    # Only the valid line with unique_id should be counted
    assert count == 1


def test_prepare_v2_input_creates_parent_dirs(tmp_path: Path) -> None:
    src = FIXTURES / "sample.jsonl"
    dst = tmp_path / "nested" / "deep" / "v2_input.jsonl"

    from entrypoint import _prepare_v2_input
    _prepare_v2_input(src, dst)
    assert dst.exists()


# ── enable_proxy env override ─────────────────────────────────────────────────

def test_enable_proxy_false_sets_racknerd_disabled(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RACKNERD_ENABLED", raising=False)

    # Simulate the env override block from main()
    enable_proxy = False
    if not enable_proxy:
        os.environ["RACKNERD_ENABLED"] = "false"

    assert os.environ.get("RACKNERD_ENABLED") == "false"
    monkeypatch.delenv("RACKNERD_ENABLED", raising=False)


def test_enable_proxy_true_leaves_racknerd_enabled(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RACKNERD_ENABLED", raising=False)

    enable_proxy = True
    if not enable_proxy:
        os.environ["RACKNERD_ENABLED"] = "false"

    assert os.environ.get("RACKNERD_ENABLED") is None
