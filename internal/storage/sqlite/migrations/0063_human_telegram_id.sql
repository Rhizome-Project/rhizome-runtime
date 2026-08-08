ALTER TABLE workspace_humans ADD COLUMN telegram_user_id INTEGER;

CREATE INDEX IF NOT EXISTS idx_workspace_humans_telegram_user_id
    ON workspace_humans(workspace_id, telegram_user_id);
