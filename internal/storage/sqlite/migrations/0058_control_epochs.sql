CREATE TABLE IF NOT EXISTS workspace_control_epochs (
    workspace_id TEXT PRIMARY KEY REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    current_epoch INTEGER NOT NULL DEFAULT 0,
    policy_mode TEXT NOT NULL DEFAULT 'shadow', -- 'shadow' or 'active'
    last_incremented_at TEXT NOT NULL
);
