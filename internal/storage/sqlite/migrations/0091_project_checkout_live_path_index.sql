DROP INDEX IF EXISTS idx_project_checkout_registry_active_path;

CREATE UNIQUE INDEX IF NOT EXISTS idx_project_checkout_registry_active_path
  ON project_checkout_registry(workspace_id, machine_id, local_path)
  WHERE status IN ('ACTIVE', 'BLOCKED', 'STALE');
