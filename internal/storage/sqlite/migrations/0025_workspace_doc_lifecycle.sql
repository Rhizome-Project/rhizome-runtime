ALTER TABLE workspace_docs ADD COLUMN archived_at TEXT;
ALTER TABLE workspace_docs ADD COLUMN archived_by TEXT;

CREATE INDEX IF NOT EXISTS idx_workspace_docs_archived
  ON workspace_docs(workspace_id, archived_at, updated_at DESC);
