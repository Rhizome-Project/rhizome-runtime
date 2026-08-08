-- Projects entity for grouping tasks and artifacts
CREATE TABLE IF NOT EXISTS projects (
  project_id   TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  title        TEXT NOT NULL,
  description  TEXT DEFAULT '',
  status       TEXT NOT NULL DEFAULT 'ACTIVE',
  created_by   TEXT NOT NULL,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_projects_workspace ON projects(workspace_id);
CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(workspace_id, status);

-- Add project_id to existing tasks
ALTER TABLE tasks ADD COLUMN project_id TEXT DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_tasks_project ON tasks(project_id);
