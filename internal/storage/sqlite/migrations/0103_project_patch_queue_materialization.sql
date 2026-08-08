ALTER TABLE project_patch_queue_items
    ADD COLUMN materialization_schema TEXT NOT NULL DEFAULT '';

ALTER TABLE project_patch_queue_items
    ADD COLUMN materialization_accepted INTEGER NOT NULL DEFAULT 0;

ALTER TABLE project_patch_queue_items
    ADD COLUMN materialization_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE project_patch_queue_items
    ADD COLUMN materialization_digest TEXT NOT NULL DEFAULT '';

ALTER TABLE project_patch_queue_items
    ADD COLUMN materialization_recorded_by TEXT NOT NULL DEFAULT '';

ALTER TABLE project_patch_queue_items
    ADD COLUMN materialization_recorded_at TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_project_patch_queue_items_materialization
  ON project_patch_queue_items(workspace_id, project_id, materialization_digest)
  WHERE materialization_accepted = 1;
