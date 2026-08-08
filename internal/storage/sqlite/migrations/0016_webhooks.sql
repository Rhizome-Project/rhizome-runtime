-- Webhook subscriptions for workspace events
CREATE TABLE IF NOT EXISTS webhook_subscriptions (
  webhook_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  url TEXT NOT NULL,
  event_types_json TEXT NOT NULL DEFAULT '["*"]',
  secret TEXT NOT NULL DEFAULT '',
  active INTEGER NOT NULL DEFAULT 1,
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_webhook_workspace_active ON webhook_subscriptions(workspace_id, active);
