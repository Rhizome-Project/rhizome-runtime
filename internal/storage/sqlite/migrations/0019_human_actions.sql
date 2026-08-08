-- 0019: Human actions and action messages for Action Required feature
CREATE TABLE IF NOT EXISTS human_actions (
  action_id          TEXT PRIMARY KEY,
  workspace_id       TEXT NOT NULL,
  task_id            TEXT NOT NULL,
  agent_id           TEXT NOT NULL DEFAULT '',
  assigned_to        TEXT NOT NULL DEFAULT '',
  title              TEXT NOT NULL,
  description        TEXT NOT NULL DEFAULT '',
  status             TEXT NOT NULL DEFAULT 'PENDING',
  resolution_comment TEXT NOT NULL DEFAULT '',
  created_at         TEXT NOT NULL,
  resolved_at        TEXT,
  resolved_by        TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_human_actions_workspace_status ON human_actions(workspace_id, status);
CREATE INDEX IF NOT EXISTS idx_human_actions_task ON human_actions(task_id);

CREATE TABLE IF NOT EXISTS action_messages (
  message_id   TEXT PRIMARY KEY,
  action_id    TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  from_id      TEXT NOT NULL,
  content      TEXT NOT NULL,
  created_at   TEXT NOT NULL,
  FOREIGN KEY (action_id) REFERENCES human_actions(action_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_action_messages_action ON action_messages(action_id, created_at);
