"""SQLite data layer.

Split by responsibility — schema/migrations, record lifecycle, the Zuhal queue,
run metadata, pattern stats, enrichment cache, and bbops jobs each live in their
own module. This package re-exports the full public surface so existing call
sites (`from pipeline import db; db.func(...)`) keep working unchanged.
"""
from __future__ import annotations

from pipeline.db.schema import (  # noqa: F401
    State,
    SCHEMA_VERSION,
    SCHEMA_SQL,
    INSERT_RECORD_SQL,
    UPSERT_CHECKPOINT_SQL,
    init_db,
)
from pipeline.db.records import (  # noqa: F401
    insert_records_batch,
    fetch_pending_validation,
    has_pending_validation,
    fetch_pending_discovery,
    update_record_discovery,
    requeue_record,
    update_record_status,
    update_record_dual,
    recover_stale_validating,
    flush_process_trace,
    append_process_trace,
)
from pipeline.db.zuhal_queue import (  # noqa: F401
    handoff_to_zuhal,
    fetch_pending_zuhal,
    has_pending_zuhal,
    count_needs_zuhal,
    touch_zuhal_validating,
    recover_stale_zuhal_validating,
    requeue_zuhal,
    create_zuhal_job,
    update_zuhal_job_status,
    lookup_email_cache,
    write_email_cache,
)
from pipeline.db.meta import (  # noqa: F401
    get_checkpoint,
    upsert_checkpoint,
    insert_failure,
    upsert_stats,
    upsert_producer_heartbeat,
    upsert_dispatcher_heartbeat,
    get_status_summary,
    reset_failed_records,
)
from pipeline.db.patterns import (  # noqa: F401
    get_pattern_rankings,
    record_pattern_result,
)
from pipeline.db.enrichment import (  # noqa: F401
    mark_serper_enriched,
    get_enrichment_cache,
    set_enrichment_cache,
)
from pipeline.db.bbops_jobs import (  # noqa: F401
    insert_bbops_jobs,
    mark_bbops_job_done,
    fetch_inflight_bbops_batches,
)
