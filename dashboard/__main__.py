from __future__ import annotations

import argparse
import sys
from pathlib import Path

import uvicorn

from .app import create_app


def main() -> int:
    p = argparse.ArgumentParser(prog="dashboard", description="Live pipeline dashboard")
    p.add_argument(
        "--output-dir", default="output",
        help="Root directory to scan for run_*/pipeline.db files (default: output/)",
    )
    p.add_argument(
        "--db", action="append", dest="extra_dbs", default=[], metavar="DB_PATH",
        help="Add a specific DB path directly (repeatable; supplements auto-discovery)",
    )
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--poll-ttl", type=float, default=1.5, help="Server-side cache TTL (seconds)")
    p.add_argument("--cost-ceiling", type=float, default=600.0, help="Cost ceiling for the gauge")
    args = p.parse_args()

    out_dir = Path(args.output_dir)
    if not out_dir.is_dir():
        print(f"Output directory not found: {out_dir}", file=sys.stderr)
        return 2

    app = create_app(
        str(out_dir.resolve()),
        extra_dbs=args.extra_dbs,
        poll_ttl=args.poll_ttl,
        cost_ceiling=args.cost_ceiling,
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="info", access_log=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
