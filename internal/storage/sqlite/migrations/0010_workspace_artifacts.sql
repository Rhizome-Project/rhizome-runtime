CREATE TABLE IF NOT EXISTS workspace_artifacts (
  artifact_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  task_id TEXT,
  update_id TEXT,
  title TEXT NOT NULL,
  artifact_ref TEXT NOT NULL,
  kind TEXT NOT NULL,
  content_type TEXT NOT NULL,
  created_by TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE SET NULL,
  FOREIGN KEY (update_id) REFERENCES agent_updates(update_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_workspace_artifacts_workspace_created
  ON workspace_artifacts(workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_workspace_artifacts_task
  ON workspace_artifacts(workspace_id, task_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_workspace_artifacts_update
  ON workspace_artifacts(workspace_id, update_id, created_at DESC);
