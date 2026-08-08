ALTER TABLE project_patch_queue_items ADD COLUMN task_id TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN session_id TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN run_id TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN agent_id TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN principal_type TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN principal_id TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN capability_snapshot_id TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN capability_snapshot_schema TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN repo_root TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN base_tree_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN base_file_hashes_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE project_patch_queue_items ADD COLUMN context_digest TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN repo_lease_id TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN lease_term INTEGER NOT NULL DEFAULT 0;
ALTER TABLE project_patch_queue_items ADD COLUMN operation_id TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN operation_kind TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_project_patch_queue_items_binding_refs
  ON project_patch_queue_items(workspace_id, project_id, task_id, run_id, operation_id)
  WHERE task_id <> '' OR run_id <> '' OR operation_id <> '';
