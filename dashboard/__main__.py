from __future__ import annotations

import argparse
import sys
from pathlib import Path

import uvicorn

from .app import create_app


def main() -> int:
    p = argparse.ArgumentParser(prog="dashboard", description="Live pipeline dashboard")
    p.add_argument("--db", required=True, help="Path to pipeline.db")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--poll-ttl", type=float, default=1.5, help="Server-side cache TTL (seconds)")
    p.add_argument("--cost-ceiling", type=float, default=600.0, help="Cost ceiling for the gauge")
    args = p.parse_args()

    db = Path(args.db)
    if not db.is_file():
        print(f"DB not found: {db}", file=sys.stderr)
        return 2

    app = create_app(str(db.resolve()), poll_ttl=args.poll_ttl, cost_ceiling=args.cost_ceiling)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info", access_log=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
