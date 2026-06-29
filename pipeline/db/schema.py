from __future__ import annotations

import logging
from pathlib import Path

import aiosqlite

_log = logging.getLogger("pipeline.db")


_log = logging.getLogger("pipeline.db")

SCHEMA_VERSION = 14


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
    domain_confidence   REAL,
    owner_confidence    REAL,

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

    -- ZeroBounce (ground-truth final layer, ingested by a separate script)
    zb_status           TEXT,
    zb_sub_status       TEXT,
    zb_verified_at      TEXT,

    -- Canonical verdict: one normalized status everything reads (see pipeline/verdicts.py).
    -- canonical_source: which service set it (zerobounce > zuhal > smtp > ms_probe).
    -- reconciliation_path: de-overloads zuhal_status (dual_*/ms_valid live here now).
    canonical_status    TEXT,
    canonical_sub_status TEXT,
    canonical_source    TEXT,
    reconciliation_path TEXT,

    -- Enrichment tracking
    serper_enriched     INTEGER DEFAULT 0,

    -- State machine
    record_state        TEXT NOT NULL DEFAULT 'RAW',
    process_trace       TEXT,

    -- Re-queue tracking
    requeue_count       INTEGER DEFAULT 0,
    tunnel_requeue_count INTEGER DEFAULT 0,
    bbops_requeue_count INTEGER DEFAULT 0,
    retry_after         TEXT,

    -- Failure diagnostics
    failure_reason      TEXT,

    -- Domain-to-business match score (0.0–1.0, word overlap + fuzzy)
    domain_match_score  REAL,

    -- Which backend(s) confirmed this email
    verifier_agreement  TEXT,

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
    serper_cache_hits        INTEGER DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS zuhal_jobs (
    job_id          TEXT PRIMARY KEY,
    email_count     INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'polling',
    created_at      TEXT DEFAULT (datetime('now')),
    completed_at    TEXT
);

CREATE TABLE IF NOT EXISTS email_verification_cache (
    email_norm      TEXT PRIMARY KEY,
    verdict         TEXT NOT NULL,
    provider        TEXT,
    verified_at     TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_records_state ON records(record_state);
CREATE INDEX IF NOT EXISTS idx_records_unique_id ON records(unique_id);
CREATE INDEX IF NOT EXISTS idx_failures_unique_id ON failures(unique_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_cache_key ON enrichment_cache(business_name_norm, agent_name_norm, state, provider);
CREATE INDEX IF NOT EXISTS idx_bbops_jobs_batch ON bbops_jobs(batch_id);
CREATE INDEX IF NOT EXISTS idx_bbops_jobs_record ON bbops_jobs(record_id);
CREATE INDEX IF NOT EXISTS idx_zuhal_jobs_status ON zuhal_jobs(status);
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

# Migration statements for schema v11
_V11_MIGRATIONS: list[str] = [
    # owner_confidence: 0–1 likelihood the registered agent is the business owner,
    # computed at discovery (pipeline.utils.owner_inference).
    "ALTER TABLE records ADD COLUMN owner_confidence REAL",
]

# Migration statements for schema v12
_V12_MIGRATIONS: list[str] = [
    "ALTER TABLE records ADD COLUMN tunnel_requeue_count INTEGER DEFAULT 0",
    "ALTER TABLE records ADD COLUMN bbops_requeue_count INTEGER DEFAULT 0",
    "ALTER TABLE records ADD COLUMN failure_reason TEXT",
    "ALTER TABLE records ADD COLUMN domain_match_score REAL",
    """
    CREATE TABLE IF NOT EXISTS zuhal_jobs (
        job_id          TEXT PRIMARY KEY,
        email_count     INTEGER NOT NULL,
        status          TEXT NOT NULL DEFAULT 'polling',
        created_at      TEXT DEFAULT (datetime('now')),
        completed_at    TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_zuhal_jobs_status ON zuhal_jobs(status)",
    """
    CREATE TABLE IF NOT EXISTS email_verification_cache (
        email_norm      TEXT PRIMARY KEY,
        verdict         TEXT NOT NULL,
        provider        TEXT,
        verified_at     TEXT DEFAULT (datetime('now'))
    )
    """,
]

# Migration statements for schema v13
_V13_MIGRATIONS: list[str] = [
    "ALTER TABLE records ADD COLUMN verifier_agreement TEXT",
]

# Migration statements for schema v14
_V14_MIGRATIONS: list[str] = [
    "ALTER TABLE stats ADD COLUMN serper_cache_hits INTEGER DEFAULT 0",
]

# Migration statements for schema v10 — verdict-field standardization (additive).
_V10_MIGRATIONS: list[str] = [
    "ALTER TABLE records ADD COLUMN zb_status TEXT",
    "ALTER TABLE records ADD COLUMN zb_sub_status TEXT",
    "ALTER TABLE records ADD COLUMN zb_verified_at TEXT",
    "ALTER TABLE records ADD COLUMN canonical_status TEXT",
    "ALTER TABLE records ADD COLUMN canonical_sub_status TEXT",
    "ALTER TABLE records ADD COLUMN canonical_source TEXT",
    "ALTER TABLE records ADD COLUMN reconciliation_path TEXT",
    # Best-effort backfill of existing rows (no ZeroBounce history available):
    # canonical_status from final_verdict, normalized to the canonical set.
    "UPDATE records SET canonical_status = "
    "  CASE WHEN final_verdict IN ('valid','invalid','catch_all') THEN final_verdict END, "
    "       canonical_source = CASE WHEN final_verdict IS NOT NULL THEN 'smtp' END "
    " WHERE canonical_status IS NULL AND final_verdict IS NOT NULL",
    # Move the dual_*/ms_valid overload off zuhal_status into reconciliation_path.
    "UPDATE records SET reconciliation_path = zuhal_status "
    " WHERE reconciliation_path IS NULL AND zuhal_status IN "
    "       ('dual_valid','dual_catch_all','dual_invalid','ms_valid')",
]

# Migration statements for schema v9
_V9_MIGRATIONS: list[str] = [
    # domain_confidence: 0–1 business-to-domain match confidence computed at discovery.
    "ALTER TABLE records ADD COLUMN domain_confidence REAL",
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
    strategy, is_org_agent, mx_provider, domain_confidence, owner_confidence,
    record_state, process_trace, serper_enriched
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        (10, _V10_MIGRATIONS),
        (11, _V11_MIGRATIONS),
        (12, _V12_MIGRATIONS),
        (13, _V13_MIGRATIONS),
        (14, _V14_MIGRATIONS),
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
