ALTER TABLE project_patch_queue_items ADD COLUMN operation_binding_schema TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN operation_binding_accepted INTEGER NOT NULL DEFAULT 0;
ALTER TABLE project_patch_queue_items ADD COLUMN operation_context_digest TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN operation_lease_context_digest TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN operation_mutation_paths_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE project_patch_queue_items ADD COLUMN operation_bound_by TEXT NOT NULL DEFAULT '';
ALTER TABLE project_patch_queue_items ADD COLUMN operation_bound_at TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_project_patch_queue_items_operation_binding
  ON project_patch_queue_items(workspace_id, project_id, operation_id)
  WHERE operation_id <> '';
