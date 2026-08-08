CREATE TABLE IF NOT EXISTS agent_task_frontiers (
  workspace_id TEXT NOT NULL,
  generation_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  generated_at TEXT NOT NULL,
  selection_mode TEXT NOT NULL DEFAULT 'agent_self_select',
  candidate_task_ids_json TEXT NOT NULL DEFAULT '[]',
  candidate_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  PRIMARY KEY (workspace_id, generation_id),
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_agent_task_frontiers_agent_created
  ON agent_task_frontiers(workspace_id, agent_id, created_at DESC);
