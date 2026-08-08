ALTER TABLE project_patch_queue_items ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1;
ALTER TABLE project_patch_queue_items ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 1;
ALTER TABLE project_patch_queue_items ADD COLUMN next_retry_at TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN dead_lettered_at TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_project_patch_queue_items_retry_contract
  ON project_patch_queue_items(workspace_id, project_id, state, attempt, max_attempts);
