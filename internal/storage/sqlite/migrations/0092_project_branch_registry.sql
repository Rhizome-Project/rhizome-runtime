CREATE TABLE IF NOT EXISTS project_branch_registry (
  branch_id        TEXT PRIMARY KEY,
  workspace_id     TEXT NOT NULL,
  project_id       TEXT NOT NULL,
  repo_id          TEXT NOT NULL,
  checkout_id      TEXT NOT NULL DEFAULT '',
  agent_id         TEXT NOT NULL DEFAULT '',
  active_task_id   TEXT NOT NULL DEFAULT '',
  active_claim_id  TEXT NOT NULL DEFAULT '',
  branch_name      TEXT NOT NULL,
  branch_kind      TEXT NOT NULL DEFAULT 'feature',
  base_branch      TEXT NOT NULL DEFAULT '',
  head_sha         TEXT NOT NULL DEFAULT '',
  base_sha         TEXT NOT NULL DEFAULT '',
  write_scope_json TEXT NOT NULL DEFAULT '',
  status           TEXT NOT NULL DEFAULT 'RESERVED',
  updated_by       TEXT NOT NULL DEFAULT '',
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE,
  FOREIGN KEY(repo_id) REFERENCES project_repositories(repo_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_project_branch_registry_project
  ON project_branch_registry(workspace_id, project_id, status);
CREATE INDEX IF NOT EXISTS idx_project_branch_registry_repo
  ON project_branch_registry(workspace_id, repo_id, status);
CREATE INDEX IF NOT EXISTS idx_project_branch_registry_agent
  ON project_branch_registry(workspace_id, agent_id, status);
CREATE INDEX IF NOT EXISTS idx_project_branch_registry_task
  ON project_branch_registry(workspace_id, active_task_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_project_branch_registry_live_name
  ON project_branch_registry(workspace_id, repo_id, branch_name)
  WHERE status IN ('RESERVED', 'ACTIVE', 'BLOCKED', 'READY_FOR_REVIEW');
