CREATE TABLE IF NOT EXISTS memory_projection_outbox (
    projection_id    TEXT PRIMARY KEY,
    workspace_id     TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    projection_kind  TEXT NOT NULL,
    origin_id        TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PENDING',
    attempt_count    INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT NOT NULL DEFAULT '',
    available_at     TEXT NOT NULL,
    enqueued_at      TEXT NOT NULL,
    started_at       TEXT,
    completed_at     TEXT,
    updated_at       TEXT NOT NULL,
    UNIQUE (workspace_id, projection_kind, origin_id)
);

CREATE INDEX IF NOT EXISTS idx_memory_projection_outbox_pending
    ON memory_projection_outbox(status, available_at, workspace_id, updated_at);

CREATE INDEX IF NOT EXISTS idx_memory_projection_outbox_workspace
    ON memory_projection_outbox(workspace_id, status, updated_at DESC);
