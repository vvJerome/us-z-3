from __future__ import annotations

import argparse
import json
import sys
import urllib.request
import urllib.error
from pathlib import Path

from . import merge_outputs, stage
from .config import Env, RunPaths


def main(argv: list[str] | None = None) -> int:
    args = _parse(argv)

    if args.resume:
        paths = RunPaths.attach(Path(args.resume).resolve())
    else:
        paths = RunPaths.for_run(args.run_name)
    paths.ensure()

    env = Env.load()
    env_extra = _api_env(env)

    manifest = _load_manifest(paths.manifest)
    manifest.setdefault("run_name", args.run_name)
    if args.input:
        manifest.setdefault("input", str(Path(args.input).resolve()))
    _save_manifest(paths.manifest, manifest)

    print(f"[us-z-3] run dir: {paths.run_dir}", flush=True)

    if not args.skip_preflight:
        _preflight(env)

    # Write V2 input: add per-officer composite unique_id natively
    if not manifest.get("v2_done"):
        raw_input = Path(args.input or manifest["input"])
        print(f"[us-z-3] preparing V2 input (per-officer ids) → {paths.v2_input}", flush=True)
        written = _write_v2_input(raw_input, paths.v2_input)
        print(f"[us-z-3]   {written} records", flush=True)

        print("[us-z-3] stage V2: producer + bbops + Zuhal", flush=True)
        stage.run(paths, env_extra)
        manifest["v2_done"] = True
        _save_manifest(paths.manifest, manifest)
    else:
        print("[us-z-3] skipping V2 (already done)", flush=True)

    # Merge outputs
    counts = merge_outputs.merge(paths.v2_db, paths.merged_csv)
    print(f"[us-z-3] merged: total={counts['total']}  dupes={counts['duplicates']}  "
          f"→ {paths.merged_csv}", flush=True)
    manifest["merged"] = counts
    _save_manifest(paths.manifest, manifest)

    return 0


def _write_v2_input(src: Path, dst: Path) -> int:
    """Write V2 input JSONL with per-officer composite unique_id.

    Each record gets unique_id = f"{filing_id}__{agent_id}" so the pipeline
    deduplicates by officer, not by filing.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with src.open("r", encoding="utf-8") as sf, dst.open("w", encoding="utf-8") as df:
        for line in sf:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            filing_id = str(rec.get("unique_id") or rec.get("raw_unique_id") or "").strip()
            agent_id = str(rec.get("unique_agent_id") or "").strip()
            if not filing_id:
                continue
            rec["unique_id"] = f"{filing_id}__{agent_id}" if agent_id else filing_id
            rec["filing_id"] = filing_id
            rec["agent_id"] = agent_id
            df.write(json.dumps(rec) + "\n")
            written += 1
    return written


def _parse(argv):
    p = argparse.ArgumentParser(
        prog="orchestrator",
        description="us-z-3 V2 email discovery/validation orchestrator",
    )
    p.add_argument("--input", required=False,
                   help="Input JSONL (business+agent records). Required unless --resume.")
    p.add_argument("--run-name", default="run",
                   help="Human-readable run label; used in the run directory name.")
    p.add_argument("--resume", default=None,
                   help="Path to an existing runs/<slug>_<ts>/ directory to resume from manifest.")
    p.add_argument("--skip-v2", action="store_true", help="Skip V2 stage (merge only).")
    p.add_argument("--skip-preflight", action="store_true",
                   help="Skip bbops.io reachability check.")
    args = p.parse_args(argv)
    if not args.resume and not args.input:
        p.error("--input is required unless --resume is passed")
    return args


def _preflight(env: Env) -> None:
    try:
        _get(f"{env.bbops_base_url.rstrip('/')}/health", timeout=10)
        print(f"[preflight] OK  bbops.io at {env.bbops_base_url}", flush=True)
    except Exception as e:
        print(f"[preflight] WARN  bbops.io /health check failed: {e}. "
              f"Pass --skip-preflight to bypass.",
              file=sys.stderr)


def _get(url: str, timeout: int) -> None:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"HTTP {resp.status}")


def _api_env(env: Env) -> dict[str, str]:
    return {k: v for k, v in {
        "SERPER_API_KEY": env.serper_api_key,
        "ZUHAL_API_KEY": env.zuhal_api_key,
    }.items() if v}


def _load_manifest(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}


def _save_manifest(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
