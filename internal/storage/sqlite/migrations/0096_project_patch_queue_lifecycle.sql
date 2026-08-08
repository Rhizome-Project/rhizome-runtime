ALTER TABLE project_patch_queue_items ADD COLUMN claimed_by TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN claim_token TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN claimed_at TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN claim_expires_at TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN decision_doc_key TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN decision_summary TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN decided_by TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN decided_at TEXT NOT NULL DEFAULT '';

DROP INDEX IF EXISTS idx_project_patch_queue_items_live_branch;
CREATE UNIQUE INDEX IF NOT EXISTS idx_project_patch_queue_items_live_branch
  ON project_patch_queue_items(workspace_id, branch_id)
  WHERE state IN ('PROPOSED', 'CLAIMED');

CREATE INDEX IF NOT EXISTS idx_project_patch_queue_items_claim
  ON project_patch_queue_items(workspace_id, project_id, state, claimed_by, claim_expires_at);
