CREATE TABLE IF NOT EXISTS memory_invalidation_queue (
    invalidation_id          TEXT PRIMARY KEY,
    invalidation_key         TEXT NOT NULL UNIQUE,
    workspace_id             TEXT NOT NULL,
    agent_id                 TEXT NOT NULL,
    session_id               TEXT NOT NULL DEFAULT '',
    report_scope             TEXT NOT NULL DEFAULT 'AGENT',
    report_id                TEXT NOT NULL DEFAULT '',
    residency_tier           TEXT NOT NULL DEFAULT '',
    replica_kind             TEXT NOT NULL DEFAULT '',
    coherence_class          TEXT NOT NULL DEFAULT '',
    canonical_memory_id      TEXT NOT NULL DEFAULT '',
    cache_key                TEXT NOT NULL DEFAULT '',
    ref_kind                 TEXT NOT NULL,
    ref_id                   TEXT NOT NULL,
    previous_version_token   TEXT NOT NULL DEFAULT '',
    current_version_token    TEXT NOT NULL DEFAULT '',
    reason                   TEXT NOT NULL DEFAULT 'VERSION_CHANGED',
    state                    TEXT NOT NULL DEFAULT 'OPEN',
    metadata_json            TEXT NOT NULL DEFAULT '{}',
    delivered_at             TEXT NOT NULL DEFAULT '',
    acknowledged_at          TEXT NOT NULL DEFAULT '',
    created_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    updated_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now'))
);

CREATE INDEX IF NOT EXISTS idx_memory_invalidation_queue_agent
    ON memory_invalidation_queue(workspace_id, agent_id, state, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_memory_invalidation_queue_ref
    ON memory_invalidation_queue(workspace_id, ref_kind, ref_id, created_at DESC);
