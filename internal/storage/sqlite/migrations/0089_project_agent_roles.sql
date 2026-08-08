CREATE TABLE IF NOT EXISTS project_agent_roles (
  role_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  role_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ACTIVE',
  write_scope_json TEXT NOT NULL DEFAULT '{}',
  lease_token TEXT NOT NULL DEFAULT '',
  lease_expires_at TEXT NOT NULL DEFAULT '',
  summary TEXT NOT NULL DEFAULT '',
  claimed_at TEXT NOT NULL DEFAULT '',
  released_at TEXT NOT NULL DEFAULT '',
  updated_by TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE,
  FOREIGN KEY(workspace_id, agent_id) REFERENCES agents(workspace_id, agent_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_project_agent_roles_project
  ON project_agent_roles(workspace_id, project_id, status, role_type);

CREATE INDEX IF NOT EXISTS idx_project_agent_roles_agent
  ON project_agent_roles(workspace_id, agent_id, status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_project_agent_roles_one_active_lead
  ON project_agent_roles(workspace_id, project_id, role_type)
  WHERE role_type = 'STRATEGIC_LEAD' AND status = 'ACTIVE';
