CREATE TABLE IF NOT EXISTS tg_bridge_state (
  state_key TEXT PRIMARY KEY,
  state_value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tg_message_map (
  workspace_id TEXT NOT NULL,
  source_update_id TEXT NOT NULL,
  task_id TEXT,
  agent_id TEXT NOT NULL,
  target_user_id TEXT,
  telegram_chat_id INTEGER NOT NULL,
  telegram_message_id INTEGER NOT NULL,
  reply_update_id TEXT,
  sent_at TEXT NOT NULL,
  replied_at TEXT,
  PRIMARY KEY (telegram_chat_id, telegram_message_id),
  UNIQUE (source_update_id),
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (source_update_id) REFERENCES agent_updates(update_id) ON DELETE CASCADE,
  FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE SET NULL,
  FOREIGN KEY (agent_id) REFERENCES agents(agent_id) ON DELETE CASCADE,
  FOREIGN KEY (reply_update_id) REFERENCES agent_updates(update_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_tg_message_map_workspace_sent
  ON tg_message_map(workspace_id, sent_at DESC);

CREATE INDEX IF NOT EXISTS idx_tg_message_map_target_user
  ON tg_message_map(target_user_id, sent_at DESC);
