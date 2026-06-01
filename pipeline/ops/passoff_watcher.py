"""Drip-feed watcher: ingest new zuhaled/zerobounced CSVs into the manifest and
append newly-confirmed rows to each operator's combined passoff CSV.

Runs forever (or one-shot with --once). Idempotent: rows already marked
in_passoff=1 are never appended twice.
"""
from __future__ import annotations

import argparse
import csv
import logging
import signal
import time
from pathlib import Path

from pipeline import manifest
from pipeline.manifest import (
    OPERATORS,
    eid_of,
    email_of,
    ingest_zb_file,
    ingest_zuhal_file,
    is_zb_results_file,
    is_zuhal_results_file,
    normalize_zuhal_verdict,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("passoff_watcher")

ROOT = Path(__file__).resolve().parents[2]
US_OUT = ROOT / "output" / "us_output"

PASSOFF_COLS = [
    "email", "zb_status", "zb_sub_status", "zb_free_email", "zb_did_you_mean",
    "zb_account", "zb_domain", "zb_mx_found", "zb_mx_record", "zb_processed_at",
    "source", "eid",
]

_running = True


def _on_signal(signum, frame):
    global _running
    _running = False
    log.info("Caught signal %d, stopping after current cycle", signum)


def passoff_path(operator: str) -> Path:
    return US_OUT / "passoff" / operator / f"{operator}_combined.csv"


def ensure_passoff_header(path: Path) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=PASSOFF_COLS).writeheader()


def append_confirmed_from_zb(conn, src: Path, operator: str) -> int:
    """Append rows where zb_status in {valid, catch-all} that aren't yet in passoff."""
    out_path = passoff_path(operator)
    ensure_passoff_header(out_path)

    new_rows: list[dict] = []
    with src.open(encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            email = email_of(row)
            if not email:
                continue
            zb_status = (row.get("zb_status") or "").strip().lower()
            if zb_status not in ("valid", "catch-all"):
                continue
            if manifest.is_passed_off(conn, email):
                continue
            new_rows.append({
                "email": email,
                "zb_status": row.get("zb_status", "").strip(),
                "zb_sub_status": row.get("zb_sub_status", ""),
                "zb_free_email": row.get("zb_free_email", ""),
                "zb_did_you_mean": row.get("zb_did_you_mean", ""),
                "zb_account": row.get("zb_account", ""),
                "zb_domain": row.get("zb_domain", ""),
                "zb_mx_found": row.get("zb_mx_found", ""),
                "zb_mx_record": row.get("zb_mx_record", ""),
                "zb_processed_at": row.get("zb_processed_at", ""),
                "source": "zb_drip",
                "eid": eid_of(row),
            })

    if not new_rows:
        return 0

    appended = 0
    with out_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PASSOFF_COLS)
        for r in new_rows:
            writer.writerow(r)
            manifest.mark_passed_off(conn, r["email"])
            appended += 1
    return appended


def append_confirmed_from_zuhal(conn, src: Path, operator: str) -> int:
    """Append rows where zuhal_verdict in {valid, catch_all} that haven't been
    seen by ZB yet and aren't yet in passoff. ZB-confirmed paths take precedence
    when both signals exist for the same email.
    """
    out_path = passoff_path(operator)
    ensure_passoff_header(out_path)

    new_rows: list[dict] = []
    with src.open(encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            email = email_of(row)
            if not email:
                continue
            raw_verdict = row.get("zuhal_verdict") or row.get("Status") or ""
            verdict = normalize_zuhal_verdict(raw_verdict)
            if verdict not in ("valid", "catch_all"):
                continue
            existing = manifest.get_email(conn, email)
            if existing and existing.get("in_passoff"):
                continue
            if existing and existing.get("zerobounced"):
                continue
            new_rows.append({
                "email": email,
                "zb_status": "valid" if verdict == "valid" else "catch-all",
                "zb_sub_status": "",
                "zb_free_email": "",
                "zb_did_you_mean": "",
                "zb_account": "",
                "zb_domain": "",
                "zb_mx_found": "",
                "zb_mx_record": "",
                "zb_processed_at": "",
                "source": "zuhal_drip",
                "eid": eid_of(row),
            })

    if not new_rows:
        return 0

    appended = 0
    with out_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PASSOFF_COLS)
        for r in new_rows:
            writer.writerow(r)
            manifest.mark_passed_off(conn, r["email"])
            appended += 1
    return appended


def file_fingerprint(path: Path) -> tuple[int, int]:
    st = path.stat()
    return (int(st.st_mtime), st.st_size)


def scan_once(conn, seen_fingerprints: dict[Path, tuple[int, int]]) -> None:
    for op in OPERATORS:
        for kind, root in (
            ("zuhal", US_OUT / "zuhaled" / op),
            ("zb", US_OUT / "zerobounced" / op),
        ):
            if not root.exists():
                continue
            for path in sorted(root.glob("*.csv")):
                fp = file_fingerprint(path)
                if seen_fingerprints.get(path) == fp:
                    continue
                if kind == "zuhal" and is_zuhal_results_file(path):
                    rows = ingest_zuhal_file(conn, path, op)
                    appended = append_confirmed_from_zuhal(conn, path, op)
                    log.info("zuhal %s/%s rows=%d passoff+=%d", op, path.name, rows, appended)
                elif kind == "zb" and is_zb_results_file(path):
                    rows = ingest_zb_file(conn, path, op)
                    appended = append_confirmed_from_zb(conn, path, op)
                    log.info("zb    %s/%s rows=%d passoff+=%d", op, path.name, rows, appended)
                seen_fingerprints[path] = fp


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db", type=Path, default=manifest.DEFAULT_DB_PATH,
        help=f"Manifest DB (default: {manifest.DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--interval", type=int, default=30,
        help="Seconds between scans (default: 30)",
    )
    parser.add_argument("--once", action="store_true", help="Scan once and exit")
    args = parser.parse_args()

    conn = manifest.connect(args.db)
    log.info("Manifest at %s, interval=%ds", args.db, args.interval)

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    seen: dict[Path, tuple[int, int]] = {}

    while _running:
        try:
            scan_once(conn, seen)
        except Exception:
            log.exception("Scan failed; continuing")
        if args.once:
            break
        for _ in range(args.interval):
            if not _running:
                break
            time.sleep(1)

    c = manifest.counts(conn)
    log.info(
        "Stopping. Totals: emails=%d zuhaled=%d zerobounced=%d in_passoff=%d",
        c["total"], c["zuhaled"], c["zerobounced"], c["in_passoff"],
    )


if __name__ == "__main__":
    main()
