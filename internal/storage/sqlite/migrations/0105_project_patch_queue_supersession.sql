ALTER TABLE project_patch_queue_items ADD COLUMN supersedes_queue_id TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN supersedes_item_id TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN evidence_doc_key TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_project_patch_queue_items_supersession
  ON project_patch_queue_items(workspace_id, project_id, supersedes_queue_id, supersedes_item_id)
  WHERE supersedes_item_id <> '';
