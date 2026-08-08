CREATE TABLE IF NOT EXISTS workspaces (
  workspace_id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT NOT NULL,
  created_by TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workspace_docs (
  workspace_id TEXT NOT NULL,
  doc_key TEXT NOT NULL,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  updated_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (workspace_id, doc_key),
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS agents (
  agent_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  owner_user_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  role TEXT NOT NULL,
  status TEXT NOT NULL,
  protocol_version TEXT NOT NULL,
  capabilities_json TEXT NOT NULL,
  summary TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_seen_at TEXT,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workspace_tasks (
  workspace_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  linked_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (workspace_id, task_id),
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS task_claims (
  task_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  claim_status TEXT NOT NULL,
  summary TEXT NOT NULL,
  claimed_at TEXT NOT NULL,
  released_at TEXT,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (workspace_id, task_id) REFERENCES workspace_tasks(workspace_id, task_id) ON DELETE CASCADE,
  FOREIGN KEY (agent_id) REFERENCES agents(agent_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS agent_updates (
  update_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  update_type TEXT NOT NULL,
  summary TEXT NOT NULL,
  payload_json TEXT,
  requires_human INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (agent_id) REFERENCES agents(agent_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_workspaces_status ON workspaces(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_workspace_docs_updated ON workspace_docs(workspace_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_agents_workspace_status ON agents(workspace_id, status, last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_workspace_tasks_workspace ON workspace_tasks(workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_task_claims_workspace_status ON task_claims(workspace_id, claim_status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_updates_workspace ON agent_updates(workspace_id, created_at DESC);
