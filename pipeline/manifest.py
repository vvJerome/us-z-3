"""SQLite state store for zuhaled/zerobounced/passoff tracking.

Single source of truth for which emails have been processed by each service
and which have been written to the operator's combined passoff CSV.
"""
from __future__ import annotations

import csv
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_DB_PATH = Path("output/us_output/manifest.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS emails (
    email           TEXT PRIMARY KEY,
    eid             TEXT,
    operator        TEXT,
    part            TEXT,
    source          TEXT,
    zuhaled         INTEGER NOT NULL DEFAULT 0,
    zuhal_verdict   TEXT,
    zuhal_at        TEXT,
    zerobounced     INTEGER NOT NULL DEFAULT 0,
    zb_status       TEXT,
    zb_sub_status   TEXT,
    zb_at           TEXT,
    in_passoff      INTEGER NOT NULL DEFAULT 0,
    passoff_at      TEXT
);

CREATE INDEX IF NOT EXISTS idx_emails_op_part ON emails(operator, part);
CREATE INDEX IF NOT EXISTS idx_emails_zuhaled ON emails(zuhaled);
CREATE INDEX IF NOT EXISTS idx_emails_zerobounced ON emails(zerobounced);
CREATE INDEX IF NOT EXISTS idx_emails_in_passoff ON emails(in_passoff);

CREATE TABLE IF NOT EXISTS batches (
    batch_id        TEXT PRIMARY KEY,
    operator        TEXT,
    part            TEXT,
    kind            TEXT,
    input_path      TEXT,
    zb_file_id      TEXT,
    uploaded_at     TEXT,
    completed_at    TEXT,
    row_count       INTEGER,
    status          TEXT
);

CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(status);
"""

_STATE_PREFIX_RE = re.compile(r"^[A-Z]+-")


def strip_state_prefix(uid: str | None) -> str:
    return _STATE_PREFIX_RE.sub("", uid) if uid else ""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: Path | str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(_SCHEMA)
    return conn


def mark_zuhaled(
    conn: sqlite3.Connection,
    email: str,
    eid: str,
    operator: str,
    part: str,
    verdict: str,
    source: str,
) -> None:
    conn.execute(
        """
        INSERT INTO emails (email, eid, operator, part, source,
                            zuhaled, zuhal_verdict, zuhal_at)
        VALUES (?, ?, ?, ?, ?, 1, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
            eid           = COALESCE(NULLIF(excluded.eid, ''), emails.eid),
            operator      = COALESCE(excluded.operator, emails.operator),
            part          = COALESCE(excluded.part, emails.part),
            source        = COALESCE(emails.source, excluded.source),
            zuhaled       = 1,
            zuhal_verdict = excluded.zuhal_verdict,
            zuhal_at      = excluded.zuhal_at
        """,
        (email, eid, operator, part, source, verdict, _now()),
    )


def mark_zerobounced(
    conn: sqlite3.Connection,
    email: str,
    zb_status: str,
    zb_sub_status: str = "",
    operator: str | None = None,
    part: str | None = None,
    eid: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO emails (email, eid, operator, part,
                            zerobounced, zb_status, zb_sub_status, zb_at)
        VALUES (?, ?, ?, ?, 1, ?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
            eid          = COALESCE(NULLIF(excluded.eid, ''), emails.eid),
            operator     = COALESCE(emails.operator, excluded.operator),
            part         = COALESCE(emails.part, excluded.part),
            zerobounced  = 1,
            zb_status    = excluded.zb_status,
            zb_sub_status = excluded.zb_sub_status,
            zb_at        = excluded.zb_at
        """,
        (email, eid or "", operator, part, zb_status, zb_sub_status, _now()),
    )


def mark_passed_off(conn: sqlite3.Connection, email: str) -> None:
    conn.execute(
        "UPDATE emails SET in_passoff=1, passoff_at=? WHERE email=?",
        (_now(), email),
    )


def is_passed_off(conn: sqlite3.Connection, email: str) -> bool:
    row = conn.execute(
        "SELECT in_passoff FROM emails WHERE email=?", (email,)
    ).fetchone()
    return bool(row and row[0])


def seen_by_zb(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT email FROM emails WHERE zerobounced=1")}


def seen_by_zuhal(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT email FROM emails WHERE zuhaled=1")}


def get_email(conn: sqlite3.Connection, email: str) -> dict | None:
    cursor = conn.execute("SELECT * FROM emails WHERE email=?", (email,))
    row = cursor.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cursor.description]
    return dict(zip(cols, row))


def start_batch(
    conn: sqlite3.Connection,
    batch_id: str,
    operator: str,
    part: str,
    kind: str,
    input_path: str,
) -> None:
    conn.execute(
        """
        INSERT INTO batches (batch_id, operator, part, kind, input_path,
                             uploaded_at, status)
        VALUES (?, ?, ?, ?, ?, ?, 'uploading')
        ON CONFLICT(batch_id) DO UPDATE SET
            operator     = excluded.operator,
            part         = excluded.part,
            kind         = excluded.kind,
            input_path   = excluded.input_path,
            uploaded_at  = excluded.uploaded_at,
            status       = 'uploading'
        """,
        (batch_id, operator, part, kind, input_path, _now()),
    )


def record_file_id(
    conn: sqlite3.Connection, batch_id: str, zb_file_id: str, row_count: int
) -> None:
    conn.execute(
        """
        UPDATE batches
        SET zb_file_id=?, row_count=?, status='polling'
        WHERE batch_id=?
        """,
        (zb_file_id, row_count, batch_id),
    )


def finish_batch(
    conn: sqlite3.Connection, batch_id: str, row_count: int | None = None
) -> None:
    if row_count is None:
        conn.execute(
            "UPDATE batches SET completed_at=?, status='complete' WHERE batch_id=?",
            (_now(), batch_id),
        )
    else:
        conn.execute(
            """
            UPDATE batches
            SET completed_at=?, status='complete', row_count=?
            WHERE batch_id=?
            """,
            (_now(), row_count, batch_id),
        )


def fail_batch(conn: sqlite3.Connection, batch_id: str) -> None:
    conn.execute(
        "UPDATE batches SET status='failed' WHERE batch_id=?", (batch_id,)
    )


def get_unfinished_batches(conn: sqlite3.Connection) -> list[dict]:
    cursor = conn.execute(
        """
        SELECT batch_id, operator, part, kind, input_path, zb_file_id,
               uploaded_at, row_count, status
        FROM batches
        WHERE status IN ('uploading', 'polling')
        ORDER BY uploaded_at
        """
    )
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, r)) for r in cursor.fetchall()]


def counts(conn: sqlite3.Connection) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT
            COUNT(*),
            SUM(zuhaled),
            SUM(zerobounced),
            SUM(in_passoff)
        FROM emails
        """
    ).fetchone()
    return {
        "total": row[0] or 0,
        "zuhaled": row[1] or 0,
        "zerobounced": row[2] or 0,
        "in_passoff": row[3] or 0,
    }


OPERATORS: tuple[str, ...] = ("alpha", "jerome", "sara")

_EMAIL_KEYS = ("email", "Email", "candidate_email", "email_address")
_EID_KEYS = ("unique_id",)
_ZUHAL_VERDICT_KEYS = ("zuhal_verdict", "Status")

_ZUHAL_VERDICT_MAP: dict[str, str] = {
    "valid": "valid",
    "invalid": "invalid",
    "catch_all": "catch_all",
    "catch-all": "catch_all",
    "accept-all": "catch_all",
    "unknown": "unknown",
    "no_result": "unknown",
    "disposable account": "invalid",
    "disposable": "invalid",
}

_PART_FROM_FILENAME_RE = re.compile(r"(part\d|w_officer|wo_officer|part1)", re.IGNORECASE)


def email_of(row: dict) -> str:
    for k in _EMAIL_KEYS:
        v = row.get(k)
        if v:
            return v.strip().lower()
    return ""


def eid_of(row: dict) -> str:
    for k in _EID_KEYS:
        v = row.get(k)
        if v:
            return strip_state_prefix(v.strip())
    return ""


def normalize_zuhal_verdict(v: str) -> str:
    return _ZUHAL_VERDICT_MAP.get((v or "").strip().lower(), (v or "").strip().lower())


def part_from_filename(path: Path) -> str:
    m = _PART_FROM_FILENAME_RE.search(path.stem.lower())
    return m.group(1).lower() if m else ""


def is_zuhal_results_file(path: Path) -> bool:
    name = path.stem.lower()
    return (
        name.endswith("_zuhaled")
        or name.endswith("_zuhaled_v2")
        or name.endswith(".zuhal")
    )


def is_zb_results_file(path: Path) -> bool:
    name = path.stem.lower()
    return (
        name.endswith("_zerobounced")
        or name.endswith("_unknown_for_zb")
        or name.endswith("_valid_for_zb")
        or name.endswith("_valid_catchall_for_zb")
    )


def ingest_zuhal_file(conn: sqlite3.Connection, path: Path, operator: str) -> int:
    part = part_from_filename(path)
    n = 0
    with path.open(encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            email = email_of(row)
            if not email:
                continue
            verdict_raw = next(
                (row[k] for k in _ZUHAL_VERDICT_KEYS if k in row and row[k]), ""
            )
            mark_zuhaled(
                conn,
                email=email,
                eid=eid_of(row),
                operator=operator,
                part=part,
                verdict=normalize_zuhal_verdict(verdict_raw),
                source="standalone_zuhal",
            )
            n += 1
    return n


def ingest_zb_file(conn: sqlite3.Connection, path: Path, operator: str) -> int:
    part = part_from_filename(path)
    n = 0
    with path.open(encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            email = email_of(row)
            if not email:
                continue
            zb_status = (row.get("zb_status") or "").strip()
            if not zb_status:
                continue
            mark_zerobounced(
                conn,
                email=email,
                zb_status=zb_status,
                zb_sub_status=(row.get("zb_sub_status") or "").strip(),
                operator=operator,
                part=part,
                eid=eid_of(row),
            )
            n += 1
    return n


def ingest_passoff_file(conn: sqlite3.Connection, path: Path, operator: str) -> int:
    n = 0
    with path.open(encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            email = email_of(row)
            if not email:
                continue
            mark_passed_off(conn, email)
            n += 1
    return n
