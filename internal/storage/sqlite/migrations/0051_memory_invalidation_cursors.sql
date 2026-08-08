CREATE TABLE IF NOT EXISTS memory_invalidation_cursors (
    workspace_id                      TEXT NOT NULL,
    agent_id                          TEXT NOT NULL,
    session_id                        TEXT NOT NULL DEFAULT '',
    last_polled_at                    TEXT NOT NULL DEFAULT '',
    last_delivered_at                 TEXT NOT NULL DEFAULT '',
    last_delivered_invalidation_id    TEXT NOT NULL DEFAULT '',
    last_acknowledged_at              TEXT NOT NULL DEFAULT '',
    last_acknowledged_invalidation_id TEXT NOT NULL DEFAULT '',
    last_poll_count                   INTEGER NOT NULL DEFAULT 0,
    updated_at                        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    PRIMARY KEY (workspace_id, agent_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_memory_invalidation_cursors_workspace_agent
    ON memory_invalidation_cursors(workspace_id, agent_id, updated_at DESC);
