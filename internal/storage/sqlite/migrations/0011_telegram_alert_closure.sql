ALTER TABLE tg_message_map ADD COLUMN superseded_by_update_id TEXT;
ALTER TABLE tg_message_map ADD COLUMN superseded_at TEXT;

CREATE INDEX IF NOT EXISTS idx_tg_message_map_open
  ON tg_message_map(workspace_id, task_id, agent_id, replied_at, superseded_at, sent_at DESC);
