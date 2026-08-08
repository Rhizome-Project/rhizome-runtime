CREATE TABLE IF NOT EXISTS memory_access_stats (
    report_id                    TEXT PRIMARY KEY,
    workspace_id                 TEXT NOT NULL,
    agent_id                     TEXT NOT NULL,
    session_id                   TEXT NOT NULL DEFAULT '',
    report_scope                 TEXT NOT NULL DEFAULT 'AGENT',
    window_started_at            TEXT NOT NULL DEFAULT '',
    window_ended_at              TEXT NOT NULL DEFAULT '',
    lookup_count                 INTEGER NOT NULL DEFAULT 0,
    l1_hit_count                 INTEGER NOT NULL DEFAULT 0,
    l2_hit_count                 INTEGER NOT NULL DEFAULT 0,
    p3_hit_count                 INTEGER NOT NULL DEFAULT 0,
    stale_hit_count              INTEGER NOT NULL DEFAULT 0,
    promotion_count              INTEGER NOT NULL DEFAULT 0,
    promotion_reuse_count        INTEGER NOT NULL DEFAULT 0,
    flush_count                  INTEGER NOT NULL DEFAULT 0,
    flush_positive_count         INTEGER NOT NULL DEFAULT 0,
    local_consolidation_count    INTEGER NOT NULL DEFAULT 0,
    potential_shared_op_count    INTEGER NOT NULL DEFAULT 0,
    notes_json                   TEXT NOT NULL DEFAULT '{}',
    created_at                   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    updated_at                   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now'))
);

CREATE INDEX IF NOT EXISTS idx_memory_access_stats_workspace_agent
    ON memory_access_stats(workspace_id, agent_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_memory_access_stats_workspace_scope
    ON memory_access_stats(workspace_id, report_scope, updated_at DESC);
