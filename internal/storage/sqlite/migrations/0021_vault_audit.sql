-- Vault audit log: track who accesses vault entries
CREATE TABLE IF NOT EXISTS vault_audit_log (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  workspace_id TEXT NOT NULL,
  entry_id     TEXT NOT NULL,
  entry_title  TEXT DEFAULT '',
  action       TEXT NOT NULL,  -- 'read', 'create', 'update', 'delete'
  actor        TEXT NOT NULL,  -- agent_id or 'developer'
  created_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_vault_audit_ws ON vault_audit_log(workspace_id, created_at);
