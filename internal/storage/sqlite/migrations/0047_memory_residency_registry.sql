CREATE TABLE IF NOT EXISTS memory_residency_reports (
    report_id                   TEXT PRIMARY KEY,
    workspace_id                TEXT NOT NULL,
    agent_id                    TEXT NOT NULL,
    session_id                  TEXT NOT NULL DEFAULT '',
    report_scope                TEXT NOT NULL DEFAULT 'AGENT',
    p1_entry_count              INTEGER NOT NULL DEFAULT 0,
    p2_entry_count              INTEGER NOT NULL DEFAULT 0,
    p3_entry_count              INTEGER NOT NULL DEFAULT 0,
    hot_hit_rate                REAL NOT NULL DEFAULT 0,
    persistent_hit_rate         REAL NOT NULL DEFAULT 0,
    cluster_hit_rate            REAL NOT NULL DEFAULT 0,
    stale_read_rate             REAL NOT NULL DEFAULT 0,
    offload_ratio               REAL NOT NULL DEFAULT 0,
    invalidated_replica_count   INTEGER NOT NULL DEFAULT 0,
    notes_json                  TEXT NOT NULL DEFAULT '{}',
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    updated_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now'))
);

CREATE INDEX IF NOT EXISTS idx_memory_residency_reports_workspace_agent
    ON memory_residency_reports(workspace_id, agent_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_memory_residency_reports_workspace_scope
    ON memory_residency_reports(workspace_id, report_scope, updated_at DESC);

CREATE TABLE IF NOT EXISTS memory_replica_states (
    replica_state_id        TEXT PRIMARY KEY,
    report_id               TEXT NOT NULL REFERENCES memory_residency_reports(report_id) ON DELETE CASCADE,
    workspace_id            TEXT NOT NULL,
    agent_id                TEXT NOT NULL,
    residency_tier          TEXT NOT NULL,
    replica_kind            TEXT NOT NULL,
    coherence_class         TEXT NOT NULL,
    state                   TEXT NOT NULL,
    canonical_memory_id     TEXT NOT NULL DEFAULT '',
    cache_key               TEXT NOT NULL DEFAULT '',
    source_kind             TEXT NOT NULL DEFAULT '',
    source_id               TEXT NOT NULL DEFAULT '',
    version_guard_json      TEXT NOT NULL DEFAULT '[]',
    hit_count               INTEGER NOT NULL DEFAULT 0,
    stale_ref_count         INTEGER NOT NULL DEFAULT 0,
    last_accessed_at        TEXT NOT NULL DEFAULT '',
    last_validated_at       TEXT NOT NULL DEFAULT '',
    metadata_json           TEXT NOT NULL DEFAULT '{}',
    created_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    updated_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now'))
);

CREATE INDEX IF NOT EXISTS idx_memory_replica_states_report
    ON memory_replica_states(report_id, residency_tier, replica_kind, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_memory_replica_states_workspace_agent
    ON memory_replica_states(workspace_id, agent_id, residency_tier, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_memory_replica_states_memory
    ON memory_replica_states(workspace_id, canonical_memory_id, updated_at DESC);
