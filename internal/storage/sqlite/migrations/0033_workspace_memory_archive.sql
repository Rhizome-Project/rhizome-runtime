-- Archive/tombstone support for canonical workspace memory lifecycle.

ALTER TABLE workspace_memory ADD COLUMN archived_at TEXT;
ALTER TABLE workspace_memory ADD COLUMN archived_by TEXT;
ALTER TABLE workspace_memory ADD COLUMN archived_reason TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_workspace_memory_workspace_archived_updated
    ON workspace_memory(workspace_id, archived_at, updated_at DESC, memory_id DESC);
