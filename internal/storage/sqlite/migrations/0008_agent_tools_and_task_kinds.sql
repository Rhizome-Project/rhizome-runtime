ALTER TABLE tasks ADD COLUMN title TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN description TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN task_kind TEXT NOT NULL DEFAULT 'EXECUTION';
ALTER TABLE tasks ADD COLUMN task_template TEXT NOT NULL DEFAULT 'generic';

CREATE TABLE IF NOT EXISTS workspace_tools (
  workspace_id TEXT NOT NULL,
  tool_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  description TEXT NOT NULL,
  owner_user_id TEXT NOT NULL,
  owner_agent_id TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  version TEXT NOT NULL,
  access_level TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  capabilities_json TEXT NOT NULL,
  manifest_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (workspace_id, tool_id),
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tasks_kind_status ON tasks(task_kind, status, priority);
CREATE INDEX IF NOT EXISTS idx_workspace_tools_workspace_status ON workspace_tools(workspace_id, status, updated_at DESC);
