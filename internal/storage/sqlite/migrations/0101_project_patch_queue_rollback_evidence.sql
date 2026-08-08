ALTER TABLE project_patch_queue_items ADD COLUMN rollback_evidence_schema TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN rollback_evidence_accepted INTEGER NOT NULL DEFAULT 0;
ALTER TABLE project_patch_queue_items ADD COLUMN rollback_evidence_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE project_patch_queue_items ADD COLUMN rollback_evidence_digest TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN rollback_recorded_by TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN rollback_recorded_at TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_project_patch_queue_items_rollback_evidence
  ON project_patch_queue_items(workspace_id, project_id, rollback_recorded_at)
  WHERE rollback_evidence_accepted = 1;
