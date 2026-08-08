ALTER TABLE human_actions ADD COLUMN blocking INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_human_actions_task_blocking_status
  ON human_actions(task_id, blocking, status);
