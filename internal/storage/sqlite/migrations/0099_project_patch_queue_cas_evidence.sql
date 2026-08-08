ALTER TABLE project_patch_queue_items ADD COLUMN cas_evidence_schema TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN cas_evidence_accepted INTEGER NOT NULL DEFAULT 0;
ALTER TABLE project_patch_queue_items ADD COLUMN cas_status TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN cas_patch_digest TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN cas_evaluation_digest TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN cas_result_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE project_patch_queue_items ADD COLUMN cas_test_evidence_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE project_patch_queue_items ADD COLUMN cas_test_evidence_digest TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN cas_recorded_by TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN cas_recorded_at TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_project_patch_queue_items_cas_evidence
  ON project_patch_queue_items(workspace_id, project_id, cas_status)
  WHERE cas_evidence_accepted = 1;
