CREATE TABLE IF NOT EXISTS task_claim_blockers (
  task_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  prior_exists INTEGER NOT NULL DEFAULT 0,
  prior_agent_id TEXT,
  prior_claim_status TEXT,
  prior_summary TEXT,
  prior_claimed_at TEXT,
  prior_released_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (workspace_id, task_id),
  FOREIGN KEY (workspace_id, task_id) REFERENCES workspace_tasks(workspace_id, task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_task_claim_blockers_workspace ON task_claim_blockers(workspace_id, updated_at DESC);
