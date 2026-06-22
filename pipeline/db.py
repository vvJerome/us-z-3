from __future__ import annotations

import json
import logging
from pathlib import Path

import aiosqlite

_log = logging.getLogger("pipeline.db")

SCHEMA_VERSION = 9


# ---------------------------------------------------------------------------
# Status taxonomy constants — single source of truth
# ---------------------------------------------------------------------------
class State:
    RAW               = "RAW"
    DISCOVERING       = "DISCOVERING"
    DISCOVERED        = "DISCOVERED"
    VALIDATING        = "VALIDATING"
    VALIDATED         = "VALIDATED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    DISCOVERY_FAILED  = "DISCOVERY_FAILED"
    COST_SKIPPED      = "COST_SKIPPED"
    NEEDS_ZUHAL       = "NEEDS_ZUHAL"
    ZUHAL_VALIDATING  = "ZUHAL_VALIDATING"


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS records (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    unique_id           TEXT NOT NULL UNIQUE,
    business_name       TEXT,
    agent_name          TEXT,
    state               TEXT,
    jurisdiction        TEXT,
    position_type       TEXT,
    name_entity_type    TEXT,

    -- Discovery outputs
    candidate_email     TEXT,
    candidate_emails    TEXT,
    subdomain_emails    TEXT,
    candidate_domain    TEXT,
    discovery_source    TEXT,
    discovery_attempts  INTEGER DEFAULT 0,
    strategy            TEXT,
    is_org_agent        INTEGER DEFAULT 0,
    mx_provider         TEXT,

    -- Reconciliation path (encodes which backends ran and what Zuhal did)
    zuhal_status        TEXT,
    confidence_score    REAL,

    -- Per-backend verdicts
    racknerd_status     TEXT,
    racknerd_message    TEXT,
    racknerd_verified_at TEXT,
    bbops_status        TEXT,
    bbops_message       TEXT,
    bbops_verified_at   TEXT,
    final_verdict       TEXT,
    dispatch_attempts   INTEGER DEFAULT 0,

    -- Enrichment tracking
    serper_enriched     INTEGER DEFAULT 0,

    -- State machine
    record_state        TEXT NOT NULL DEFAULT 'RAW',
    process_trace       TEXT,

    -- Re-queue tracking
    requeue_count         INTEGER DEFAULT 0,
    tunnel_requeue_count  INTEGER DEFAULT 0,
    bbops_requeue_count   INTEGER DEFAULT 0,
    retry_after           TEXT,

    -- Why VALIDATION_FAILED: 'infra_loop' (never tested) or 'max_attempts' (all tested invalid)
    failure_reason        TEXT,

    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS checkpoints (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS stats (
    run_id                   TEXT PRIMARY KEY,
    total_input              INTEGER DEFAULT 0,
    producer_processed       INTEGER DEFAULT 0,
    discovery_hits           INTEGER DEFAULT 0,
    discovery_misses         INTEGER DEFAULT 0,
    validated                INTEGER DEFAULT 0,
    validation_failed        INTEGER DEFAULT 0,
    serper_producer_calls    INTEGER DEFAULT 0,
    serper_dispatcher_calls  INTEGER DEFAULT 0,
    zuhal_calls              INTEGER DEFAULT 0,
    racknerd_probes          INTEGER DEFAULT 0,
    bbops_probes             INTEGER DEFAULT 0,
    backend_disagreements    INTEGER DEFAULT 0,
    estimated_cost_usd       REAL DEFAULT 0.0,
    last_producer_heartbeat  TEXT,
    last_dispatcher_heartbeat TEXT,
    updated_at               TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS failures (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    unique_id       TEXT NOT NULL,
    phase           TEXT NOT NULL,
    attempt         INTEGER NOT NULL,
    error_type      TEXT,
    error_detail    TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pattern_stats (
    mx_provider   TEXT NOT NULL,
    template      TEXT NOT NULL,
    success_count INTEGER DEFAULT 0,
    total_count   INTEGER DEFAULT 0,
    PRIMARY KEY (mx_provider, template)
);

CREATE TABLE IF NOT EXISTS enrichment_cache (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    business_name_norm  TEXT NOT NULL,
    agent_name_norm     TEXT NOT NULL,
    state               TEXT NOT NULL,
    provider            TEXT NOT NULL,
    response_json       TEXT,
    cached_at           TEXT DEFAULT (datetime('now')),
    UNIQUE(business_name_norm, agent_name_norm, state, provider)
);

-- Crash-recovery table for in-flight bbops batch jobs
CREATE TABLE IF NOT EXISTS bbops_jobs (
    record_id       INTEGER NOT NULL,
    email           TEXT NOT NULL,
    job_id          TEXT,
    batch_id        TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'submitted',
    result_status   TEXT,
    result_message  TEXT,
    submitted_at    TEXT DEFAULT (datetime('now')),
    completed_at    TEXT,
    PRIMARY KEY (record_id, email)
);

CREATE INDEX IF NOT EXISTS idx_records_state ON records(record_state);
CREATE INDEX IF NOT EXISTS idx_records_unique_id ON records(unique_id);
CREATE INDEX IF NOT EXISTS idx_failures_unique_id ON failures(unique_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_cache_key ON enrichment_cache(business_name_norm, agent_name_norm, state, provider);
CREATE INDEX IF NOT EXISTS idx_bbops_jobs_batch ON bbops_jobs(batch_id);
CREATE INDEX IF NOT EXISTS idx_bbops_jobs_record ON bbops_jobs(record_id);
"""

# Migration statements from schema v3 → v4
_V4_MIGRATIONS: list[str] = [
    # v5
    "ALTER TABLE records ADD COLUMN serper_enriched INTEGER DEFAULT 0",
    "ALTER TABLE stats ADD COLUMN serper_producer_calls INTEGER DEFAULT 0",
    "ALTER TABLE stats ADD COLUMN serper_dispatcher_calls INTEGER DEFAULT 0",
    "ALTER TABLE records ADD COLUMN racknerd_status TEXT",
    "ALTER TABLE records ADD COLUMN racknerd_message TEXT",
    "ALTER TABLE records ADD COLUMN racknerd_verified_at TEXT",
    "ALTER TABLE records ADD COLUMN bbops_status TEXT",
    "ALTER TABLE records ADD COLUMN bbops_message TEXT",
    "ALTER TABLE records ADD COLUMN bbops_verified_at TEXT",
    "ALTER TABLE records ADD COLUMN final_verdict TEXT",
    "ALTER TABLE records ADD COLUMN dispatch_attempts INTEGER DEFAULT 0",
    "ALTER TABLE stats ADD COLUMN racknerd_probes INTEGER DEFAULT 0",
    "ALTER TABLE stats ADD COLUMN bbops_probes INTEGER DEFAULT 0",
    "ALTER TABLE stats ADD COLUMN backend_disagreements INTEGER DEFAULT 0",
    "ALTER TABLE stats ADD COLUMN last_dispatcher_heartbeat TEXT",
    """
    CREATE TABLE IF NOT EXISTS bbops_jobs (
        record_id       INTEGER NOT NULL,
        email           TEXT NOT NULL,
        job_id          TEXT,
        batch_id        TEXT NOT NULL,
        status          TEXT NOT NULL DEFAULT 'submitted',
        result_status   TEXT,
        result_message  TEXT,
        submitted_at    TEXT DEFAULT (datetime('now')),
        completed_at    TEXT,
        PRIMARY KEY (record_id, email)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_bbops_jobs_batch ON bbops_jobs(batch_id)",
    "CREATE INDEX IF NOT EXISTS idx_bbops_jobs_record ON bbops_jobs(record_id)",
    "ALTER TABLE stats ADD COLUMN zuhal_calls INTEGER DEFAULT 0",
]

# Migration statements for schema v7 (applied on top of v4–v6 DBs)
_V7_MIGRATIONS: list[str] = [
    # Add confidence_score; copy existing zuhal_score values if present.
    # Uses a two-step approach because SQLite does not support RENAME COLUMN
    # before v3.25 and we want this to be idempotent on any existing DB.
    "ALTER TABLE records ADD COLUMN confidence_score REAL",
    "UPDATE records SET confidence_score = zuhal_score WHERE confidence_score IS NULL AND zuhal_score IS NOT NULL",
]

# Migration statements for schema v9
_V9_MIGRATIONS: list[str] = [
    "ALTER TABLE records ADD COLUMN tunnel_requeue_count INTEGER DEFAULT 0",
    "ALTER TABLE records ADD COLUMN bbops_requeue_count INTEGER DEFAULT 0",
    "ALTER TABLE records ADD COLUMN failure_reason TEXT",
]

# Migration statements for schema v8
_V8_MIGRATIONS: list[str] = [
    # requeue_count: total re-queues (including infra transients) — safety valve against
    # infinite loops when dispatch_attempts does not increment for infra failures.
    "ALTER TABLE records ADD COLUMN requeue_count INTEGER DEFAULT 0",
    # retry_after: ISO timestamp; when set, fetch_pending_validation skips the record until
    # this time passes, implementing a greylisting hold (SMTP 4xx temporary deferral).
    "ALTER TABLE records ADD COLUMN retry_after TEXT",
]

INSERT_RECORD_SQL = """
INSERT OR IGNORE INTO records (
    unique_id, business_name, agent_name, state, jurisdiction,
    position_type, name_entity_type, candidate_email, candidate_emails,
    subdomain_emails, candidate_domain, discovery_source, discovery_attempts,
    strategy, is_org_agent, mx_provider, record_state, process_trace, serper_enriched
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

UPSERT_CHECKPOINT_SQL = """
INSERT INTO checkpoints (key, value, updated_at)
VALUES (?, ?, datetime('now'))
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')
"""


async def init_db(db_path: Path | str) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(str(db_path), isolation_level=None)
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA busy_timeout=10000")
    await conn.execute("PRAGMA cache_size=-64000")
    await conn.execute("PRAGMA mmap_size=268435456")
    await conn.execute("PRAGMA wal_autocheckpoint=1000")
    await conn.execute("PRAGMA temp_store=MEMORY")
    await conn.executescript(SCHEMA_SQL)
    await conn.commit()
    await _run_migrations(conn)
    conn.row_factory = aiosqlite.Row
    return conn


async def _run_migrations(conn: aiosqlite.Connection) -> None:
    async with conn.execute("PRAGMA user_version") as cur:
        row = await cur.fetchone()
        current_version = row[0] if row else 0

    if current_version >= SCHEMA_VERSION:
        return

    _log.info("Migrating DB schema from v%d to v%d", current_version, SCHEMA_VERSION)

    migration_sets: list[tuple[int, list[str]]] = [
        (6, _V4_MIGRATIONS),
        (7, _V7_MIGRATIONS),
        (8, _V8_MIGRATIONS),
        (9, _V9_MIGRATIONS),
    ]
    for target_version, stmts in migration_sets:
        if current_version >= target_version:
            continue
        for stmt in stmts:
            try:
                await conn.execute(stmt)
            except Exception as exc:
                exc_lower = str(exc).lower()
                # Suppress expected non-fatal migration failures:
                # - "duplicate column name" / "already exists": column was already added
                # - "no such column": best-effort backfill targeting a column from an
                #   older schema that never existed on this install (e.g. zuhal_score)
                expected = any(p in exc_lower for p in (
                    "duplicate column name", "already exists", "no such column",
                ))
                if not expected:
                    _log.warning("Migration statement skipped (%s): %.120s", exc, stmt.strip())

    await conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    await conn.commit()
    _log.info("Schema migration to v%d complete", SCHEMA_VERSION)


async def get_checkpoint(conn: aiosqlite.Connection, key: str) -> str | None:
    async with conn.execute(
        "SELECT value FROM checkpoints WHERE key = ?", (key,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else None


async def upsert_checkpoint(conn: aiosqlite.Connection, key: str, value: str) -> None:
    await conn.execute(UPSERT_CHECKPOINT_SQL, (key, value))


async def insert_records_batch(
    conn: aiosqlite.Connection,
    records: list[dict],
    new_offset: int,
) -> None:
    """Atomically insert a batch of records and advance the producer checkpoint."""
    async with conn.cursor() as cur:
        await cur.execute("BEGIN")
        try:
            inserted = 0
            for r in records:
                await cur.execute(INSERT_RECORD_SQL, (
                    r["unique_id"],
                    r.get("business_name", ""),
                    r.get("agent_name", ""),
                    r.get("state", ""),
                    r.get("jurisdiction", ""),
                    r.get("position_type", ""),
                    r.get("name_entity_type", ""),
                    r.get("candidate_email"),
                    r.get("candidate_emails"),
                    r.get("subdomain_emails"),
                    r.get("candidate_domain"),
                    r.get("discovery_source"),
                    r.get("discovery_attempts", 0),
                    r.get("strategy"),
                    1 if r.get("is_org_agent") else 0,
                    r.get("mx_provider"),
                    r.get("record_state", State.RAW),
                    r.get("process_trace"),
                    1 if r.get("serper_enriched") else 0,
                ))
                inserted += cur.rowcount
            if inserted < len(records):
                dropped = len(records) - inserted
                _log.warning(
                    "insert_records_batch: %d record(s) dropped — duplicate unique_id in input",
                    dropped,
                )
            await cur.execute(UPSERT_CHECKPOINT_SQL, ("producer_offset", str(new_offset)))
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


async def fetch_pending_validation(
    conn: aiosqlite.Connection,
    limit: int = 10,
) -> list[aiosqlite.Row]:
    """Atomically claim up to `limit` DISCOVERED rows by setting them to VALIDATING.

    Skips records whose retry_after timestamp has not yet passed (greylisting hold).
    """
    async with conn.execute(
        """
        UPDATE records
           SET record_state = 'VALIDATING', updated_at = datetime('now')
         WHERE id IN (
             SELECT id FROM records
              WHERE record_state = 'DISCOVERED'
                AND (retry_after IS NULL OR retry_after <= datetime('now'))
              LIMIT ?
         )
        RETURNING *
        """,
        (limit,),
    ) as cursor:
        return await cursor.fetchall()  # type: ignore[return-value]


async def has_pending_validation(conn: aiosqlite.Connection) -> bool:
    """Non-claiming check: True if any DISCOVERED rows exist."""
    async with conn.execute(
        "SELECT 1 FROM records WHERE record_state = 'DISCOVERED' LIMIT 1"
    ) as cursor:
        return await cursor.fetchone() is not None


async def fetch_pending_discovery(
    conn: aiosqlite.Connection,
    limit: int = 100,
) -> list[aiosqlite.Row]:
    async with conn.execute(
        "SELECT * FROM records WHERE record_state = 'DISCOVERING' LIMIT ?",
        (limit,),
    ) as cursor:
        return await cursor.fetchall()  # type: ignore[return-value]


async def update_record_discovery(conn: aiosqlite.Connection, result: dict) -> None:
    """UPDATE discovery fields on an existing record (used by the retry loop)."""
    await conn.execute(
        """UPDATE records SET
               record_state = ?, candidate_email = ?, candidate_emails = ?,
               subdomain_emails = ?, candidate_domain = ?,
               discovery_source = ?, discovery_attempts = ?,
               mx_provider = ?,
               updated_at = datetime('now')
           WHERE unique_id = ?""",
        (
            result.get("record_state", State.DISCOVERING),
            result.get("candidate_email"),
            result.get("candidate_emails"),
            result.get("subdomain_emails"),
            result.get("candidate_domain"),
            result.get("discovery_source"),
            result.get("discovery_attempts", 1),
            result.get("mx_provider"),
            result["unique_id"],
        ),
    )
    await conn.commit()


async def requeue_record(
    conn: aiosqlite.Connection,
    unique_id: str,
    *,
    increment_attempts: bool = True,
    retry_after: str | None = None,
    infra_type: str | None = None,
) -> None:
    """Return a record to DISCOVERED.

    increment_attempts=True when at least one backend returned a real verdict
    (valid/invalid/catch_all). False for infra transients (tunnel down, Zuhal
    circuit open, bbops unhealthy) that should not penalise the dispatch budget.

    requeue_count always increments. infra_type="tunnel" or "bbops" additionally
    increments the per-backend counter used to enforce per-infra requeue limits.

    retry_after (ISO timestamp) sets a hold: fetch_pending_validation skips
    the record until that time passes. Use for greylisting (SMTP 4xx).
    """
    infra_fragment = ""
    if infra_type == "tunnel":
        infra_fragment = ", tunnel_requeue_count = tunnel_requeue_count + 1"
    elif infra_type == "bbops":
        infra_fragment = ", bbops_requeue_count = bbops_requeue_count + 1"

    if increment_attempts:
        sql = (
            "UPDATE records"
            " SET record_state = 'DISCOVERED',"
            " dispatch_attempts = dispatch_attempts + 1,"
            f" requeue_count = requeue_count + 1{infra_fragment},"
            " retry_after = ?,"
            " updated_at = datetime('now')"
            " WHERE unique_id = ?"
        )
    else:
        sql = (
            "UPDATE records"
            " SET record_state = 'DISCOVERED',"
            f" requeue_count = requeue_count + 1{infra_fragment},"
            " retry_after = ?,"
            " updated_at = datetime('now')"
            " WHERE unique_id = ?"
        )
    await conn.execute(sql, (retry_after, unique_id))
    await conn.commit()


async def update_record_status(
    conn: aiosqlite.Connection,
    unique_id: str,
    record_state: str,
    **extra_fields: object,
) -> None:
    sets = ["record_state = ?", "updated_at = datetime('now')"]
    values: list[object] = [record_state]

    for col, val in extra_fields.items():
        sets.append(f"{col} = ?")
        values.append(val)

    values.append(unique_id)

    sql = f"UPDATE records SET {', '.join(sets)} WHERE unique_id = ?"
    await conn.execute(sql, values)
    await conn.commit()


async def update_record_dual(
    conn: aiosqlite.Connection,
    unique_id: str,
    record_state: str,
    *,
    racknerd_status: str | None,
    racknerd_message: str | None,
    racknerd_verified_at: str | None,
    bbops_status: str | None,
    bbops_message: str | None,
    bbops_verified_at: str | None,
    final_verdict: str,
    candidate_email: str | None = None,
    confidence_score: float | None = None,
    dispatch_attempts_delta: int = 1,
    zuhal_status_override: str | None = None,
    failure_reason: str | None = None,
) -> None:
    """Write dual-backend verdicts and advance dispatch_attempts atomically."""
    sets = [
        "record_state = ?",
        "racknerd_status = ?",
        "racknerd_message = ?",
        "racknerd_verified_at = ?",
        "bbops_status = ?",
        "bbops_message = ?",
        "bbops_verified_at = ?",
        "final_verdict = ?",
        "dispatch_attempts = dispatch_attempts + ?",
        "updated_at = datetime('now')",
    ]
    values: list[object] = [
        record_state,
        racknerd_status,
        racknerd_message,
        racknerd_verified_at,
        bbops_status,
        bbops_message,
        bbops_verified_at,
        final_verdict,
        dispatch_attempts_delta,
    ]

    if candidate_email is not None:
        sets.append("candidate_email = ?")
        values.append(candidate_email)
        sets.append("zuhal_status = ?")
        values.append(zuhal_status_override if zuhal_status_override is not None else f"dual_{final_verdict}")

    if confidence_score is not None:
        sets.append("confidence_score = ?")
        values.append(confidence_score)

    if failure_reason is not None:
        sets.append("failure_reason = ?")
        values.append(failure_reason)

    values.append(unique_id)
    sql = f"UPDATE records SET {', '.join(sets)} WHERE unique_id = ?"
    await conn.execute(sql, values)
    await conn.commit()


async def recover_stale_validating(
    conn: aiosqlite.Connection,
    timeout_minutes: int = 5,
) -> int:
    """Reset rows orphaned in VALIDATING by a crashed dispatcher back to DISCOVERED."""
    cursor = await conn.execute(
        """
        UPDATE records
           SET record_state = 'DISCOVERED',
               dispatch_attempts = dispatch_attempts + 1,
               updated_at = datetime('now')
         WHERE record_state = 'VALIDATING'
           AND updated_at < datetime('now', ?)
        """,
        (f"-{timeout_minutes} minutes",),
    )
    await conn.commit()
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Zuhal-queue helpers (decoupled rescue worker)
# ---------------------------------------------------------------------------

async def handoff_to_zuhal(
    conn: aiosqlite.Connection,
    unique_id: str,
    *,
    racknerd_status: str | None,
    racknerd_message: str | None,
    racknerd_verified_at: str | None,
    bbops_status: str | None,
    bbops_message: str | None,
    bbops_verified_at: str | None,
    candidate_email: str,
) -> None:
    """Persist SMTP verdicts and route the record to the Zuhal queue.

    Atomic: SMTP-side outcome lands on the row in the same UPDATE that moves
    state to NEEDS_ZUHAL. Does NOT write final_verdict (Zuhal worker owns that)
    and does NOT increment dispatch_attempts (the handoff is a transition, not
    a retry; the SMTP attempt is already counted).
    """
    await conn.execute(
        """
        UPDATE records
           SET record_state = 'NEEDS_ZUHAL',
               racknerd_status = ?,
               racknerd_message = ?,
               racknerd_verified_at = ?,
               bbops_status = ?,
               bbops_message = ?,
               bbops_verified_at = ?,
               candidate_email = ?,
               updated_at = datetime('now')
         WHERE unique_id = ?
        """,
        (
            racknerd_status,
            racknerd_message,
            racknerd_verified_at,
            bbops_status,
            bbops_message,
            bbops_verified_at,
            candidate_email,
            unique_id,
        ),
    )
    await conn.commit()


async def fetch_pending_zuhal(
    conn: aiosqlite.Connection,
    limit: int = 10,
) -> list[aiosqlite.Row]:
    """Atomically claim up to `limit` NEEDS_ZUHAL rows by setting them to ZUHAL_VALIDATING."""
    async with conn.execute(
        """
        UPDATE records
           SET record_state = 'ZUHAL_VALIDATING', updated_at = datetime('now')
         WHERE id IN (
             SELECT id FROM records WHERE record_state = 'NEEDS_ZUHAL' LIMIT ?
         )
        RETURNING *
        """,
        (limit,),
    ) as cursor:
        return await cursor.fetchall()  # type: ignore[return-value]


async def has_pending_zuhal(conn: aiosqlite.Connection) -> bool:
    """Non-claiming check: True if any NEEDS_ZUHAL or ZUHAL_VALIDATING rows exist."""
    async with conn.execute(
        "SELECT 1 FROM records "
        "WHERE record_state IN ('NEEDS_ZUHAL', 'ZUHAL_VALIDATING') LIMIT 1"
    ) as cursor:
        return await cursor.fetchone() is not None


async def count_needs_zuhal(conn: aiosqlite.Connection) -> int:
    """Return current size of the NEEDS_ZUHAL backlog (non-claiming)."""
    async with conn.execute(
        "SELECT COUNT(*) FROM records WHERE record_state = 'NEEDS_ZUHAL'"
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else 0


async def touch_zuhal_validating(conn: aiosqlite.Connection, unique_ids: list[str]) -> None:
    """Refresh updated_at on ZUHAL_VALIDATING rows to prevent stale-recovery eviction."""
    if not unique_ids:
        return
    placeholders = ",".join("?" * len(unique_ids))
    await conn.execute(
        f"UPDATE records SET updated_at = datetime('now') "
        f"WHERE unique_id IN ({placeholders}) AND record_state = 'ZUHAL_VALIDATING'",
        unique_ids,
    )
    await conn.commit()


async def recover_stale_zuhal_validating(
    conn: aiosqlite.Connection,
    timeout_minutes: int = 5,
) -> int:
    """Return rows orphaned in ZUHAL_VALIDATING back to NEEDS_ZUHAL for retry."""
    cursor = await conn.execute(
        """
        UPDATE records
           SET record_state = 'NEEDS_ZUHAL',
               updated_at = datetime('now')
         WHERE record_state = 'ZUHAL_VALIDATING'
           AND updated_at < datetime('now', ?)
        """,
        (f"-{timeout_minutes} minutes",),
    )
    await conn.commit()
    return cursor.rowcount


async def requeue_zuhal(conn: aiosqlite.Connection, unique_id: str) -> None:
    """Return a ZUHAL_VALIDATING row to NEEDS_ZUHAL (circuit-open or 429 re-queue)."""
    await conn.execute(
        """
        UPDATE records
           SET record_state = 'NEEDS_ZUHAL',
               updated_at = datetime('now')
         WHERE unique_id = ?
        """,
        (unique_id,),
    )
    await conn.commit()


async def flush_process_trace(
    conn: aiosqlite.Connection,
    unique_id: str,
    entries: list[dict],
) -> None:
    """Append all accumulated trace entries in a single UPDATE (one commit per record)."""
    if not entries:
        return
    await conn.execute(
        """
        UPDATE records
           SET process_trace = (
               SELECT json_group_array(value)
               FROM (
                   SELECT value FROM json_each(COALESCE(
                       (SELECT process_trace FROM records WHERE unique_id = ?), '[]'
                   ))
                   UNION ALL
                   SELECT value FROM json_each(?)
               )
           )
         WHERE unique_id = ?
        """,
        (unique_id, json.dumps(entries), unique_id),
    )
    await conn.commit()


async def append_process_trace(
    conn: aiosqlite.Connection,
    unique_id: str,
    entry: dict,
) -> None:
    """Append a stage-outcome entry to the record's process_trace JSON array."""
    await conn.execute(
        """
        UPDATE records
           SET process_trace = json_insert(COALESCE(process_trace, '[]'), '$[#]', json(?))
         WHERE unique_id = ?
        """,
        (json.dumps(entry), unique_id),
    )
    await conn.commit()


async def insert_failure(
    conn: aiosqlite.Connection,
    unique_id: str,
    phase: str,
    attempt: int,
    error_type: str,
    error_detail: str,
) -> None:
    await conn.execute(
        "INSERT INTO failures (unique_id, phase, attempt, error_type, error_detail) "
        "VALUES (?, ?, ?, ?, ?)",
        (unique_id, phase, attempt, error_type, error_detail),
    )
    await conn.commit()


async def upsert_stats(
    conn: aiosqlite.Connection,
    run_id: str,
    **fields: object,
) -> None:
    cols = ["run_id"]
    vals: list[object] = [run_id]
    updates = ["updated_at = datetime('now')"]

    for col, val in fields.items():
        cols.append(col)
        vals.append(val)
        updates.append(f"{col} = excluded.{col}")

    placeholders = ", ".join(["?"] * len(cols))
    col_str = ", ".join(cols)
    update_str = ", ".join(updates)

    sql = (
        f"INSERT INTO stats ({col_str}) VALUES ({placeholders}) "
        f"ON CONFLICT(run_id) DO UPDATE SET {update_str}"
    )
    await conn.execute(sql, vals)
    await conn.commit()


async def upsert_producer_heartbeat(conn: aiosqlite.Connection) -> None:
    await conn.execute(
        """
        UPDATE stats SET last_producer_heartbeat = datetime('now'), updated_at = datetime('now')
        WHERE rowid = (SELECT MAX(rowid) FROM stats)
        """
    )
    await conn.commit()


async def upsert_dispatcher_heartbeat(conn: aiosqlite.Connection) -> None:
    await conn.execute(
        """
        UPDATE stats SET last_dispatcher_heartbeat = datetime('now'), updated_at = datetime('now')
        WHERE rowid = (SELECT MAX(rowid) FROM stats)
        """
    )
    await conn.commit()


async def get_status_summary(conn: aiosqlite.Connection) -> dict:
    summary: dict = {}

    async with conn.execute(
        "SELECT record_state, COUNT(*) FROM records GROUP BY record_state"
    ) as cursor:
        summary["records_by_state"] = {row[0]: row[1] async for row in cursor}

    async with conn.execute("SELECT COUNT(*) FROM records") as cursor:
        row = await cursor.fetchone()
        summary["total_records"] = row[0] if row else 0

    offset = await get_checkpoint(conn, "producer_offset")
    summary["producer_offset"] = int(offset) if offset else 0

    done = await get_checkpoint(conn, "producer_done")
    summary["producer_done"] = done == "true"

    async with conn.execute("SELECT * FROM stats LIMIT 1") as cursor:
        row = await cursor.fetchone()
        if row:
            summary["stats"] = dict(row)

    async with conn.execute(
        "SELECT phase, COUNT(*) FROM failures GROUP BY phase"
    ) as cursor:
        summary["failures_by_phase"] = {row[0]: row[1] async for row in cursor}

    async with conn.execute(
        "SELECT final_verdict, COUNT(*) FROM records WHERE final_verdict IS NOT NULL GROUP BY final_verdict"
    ) as cursor:
        summary["records_by_verdict"] = {row[0]: row[1] async for row in cursor}

    return summary


async def reset_failed_records(
    conn: aiosqlite.Connection,
    record_state: str = State.DISCOVERY_FAILED,
    phase: str | None = None,
) -> int:
    if phase:
        sql = """
            UPDATE records SET record_state = 'RAW', discovery_attempts = 0, updated_at = datetime('now')
            WHERE record_state = ? AND unique_id IN (
                SELECT DISTINCT unique_id FROM failures WHERE phase = ?
            )
        """
        cursor = await conn.execute(sql, (record_state, phase))
    else:
        if record_state in (State.VALIDATION_FAILED, State.COST_SKIPPED):
            sql = """
                UPDATE records SET record_state = 'DISCOVERED', dispatch_attempts = 0,
                validation_attempts = 0, updated_at = datetime('now')
                WHERE record_state = ?
            """
        else:
            sql = """
                UPDATE records SET record_state = 'RAW', discovery_attempts = 0,
                updated_at = datetime('now')
                WHERE record_state = ?
            """
        cursor = await conn.execute(sql, (record_state,))

    await conn.commit()
    return cursor.rowcount


# ---------------------------------------------------------------------------
# pattern_stats helpers
# ---------------------------------------------------------------------------

async def get_pattern_rankings(
    conn: aiosqlite.Connection,
    mx_provider: str,
) -> list[dict]:
    """Return templates ordered by success rate for this MX provider."""
    async with conn.execute(
        """
        SELECT template, success_count, total_count
          FROM pattern_stats
         WHERE mx_provider = ? AND total_count > 0
         ORDER BY CAST(success_count AS REAL) / total_count DESC
        """,
        (mx_provider,),
    ) as cursor:
        return [
            {"template": row[0], "success_count": row[1], "total_count": row[2]}
            async for row in cursor
        ]


async def record_pattern_result(
    conn: aiosqlite.Connection,
    mx_provider: str,
    template: str,
    success: bool,
) -> None:
    await conn.execute(
        """
        INSERT INTO pattern_stats (mx_provider, template, success_count, total_count)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(mx_provider, template) DO UPDATE SET
            success_count = success_count + ?,
            total_count = total_count + 1
        """,
        (mx_provider, template, 1 if success else 0, 1 if success else 0),
    )
    await conn.commit()


async def mark_serper_enriched(conn: aiosqlite.Connection, unique_id: str) -> None:
    """Mark a record as having been enriched by Serper (prevents duplicate calls)."""
    await conn.execute(
        "UPDATE records SET serper_enriched = 1, updated_at = datetime('now') WHERE unique_id = ?",
        (unique_id,),
    )
    await conn.commit()


# ---------------------------------------------------------------------------
# enrichment_cache helpers
# ---------------------------------------------------------------------------

async def get_enrichment_cache(
    conn: aiosqlite.Connection,
    business_name: str,
    agent_name: str,
    state: str,
    provider: str,
    ttl_days: int = 30,
) -> str | None:
    """Return cached response JSON if within TTL, else None."""
    async with conn.execute(
        """
        SELECT response_json FROM enrichment_cache
         WHERE business_name_norm = ?
           AND agent_name_norm = ?
           AND state = ?
           AND provider = ?
           AND cached_at > datetime('now', ?)
        """,
        (
            business_name.lower().strip(),
            agent_name.lower().strip(),
            state,
            provider,
            f"-{ttl_days} days",
        ),
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else None


async def set_enrichment_cache(
    conn: aiosqlite.Connection,
    business_name: str,
    agent_name: str,
    state: str,
    provider: str,
    response_json: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO enrichment_cache
            (business_name_norm, agent_name_norm, state, provider, response_json, cached_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(business_name_norm, agent_name_norm, state, provider) DO UPDATE SET
            response_json = excluded.response_json,
            cached_at = datetime('now')
        """,
        (
            business_name.lower().strip(),
            agent_name.lower().strip(),
            state,
            provider,
            response_json,
        ),
    )
    await conn.commit()


# ---------------------------------------------------------------------------
# bbops_jobs helpers (crash recovery for in-flight batches)
# ---------------------------------------------------------------------------

async def insert_bbops_jobs(
    conn: aiosqlite.Connection,
    jobs: list[dict],
) -> None:
    """Persist bbops job mappings BEFORE polling — enables crash recovery."""
    for job in jobs:
        await conn.execute(
            """
            INSERT OR IGNORE INTO bbops_jobs
                (record_id, email, job_id, batch_id, submitted_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            """,
            (job["record_id"], job["email"], job.get("job_id", ""), job["batch_id"]),
        )
    await conn.commit()


async def mark_bbops_job_done(
    conn: aiosqlite.Connection,
    job_id: str,
    result_status: str,
    result_message: str,
) -> None:
    await conn.execute(
        """
        UPDATE bbops_jobs
           SET status = 'done', result_status = ?, result_message = ?,
               completed_at = datetime('now')
         WHERE job_id = ?
        """,
        (result_status, result_message, job_id),
    )
    await conn.commit()


async def fetch_inflight_bbops_batches(
    conn: aiosqlite.Connection,
) -> dict[str, list[dict]]:
    """Return all submitted-but-not-done batches grouped by batch_id for crash recovery."""
    async with conn.execute(
        """
        SELECT batch_id, record_id, email
          FROM bbops_jobs
         WHERE status = 'submitted'
        """
    ) as cursor:
        batches: dict[str, list[dict]] = {}
        async for row in cursor:
            batches.setdefault(row[0], []).append(
                {"record_id": row[1], "email": row[2]}
            )
    return batches
