-- Vault: secure credentials storage (API keys, passwords, etc.)
CREATE TABLE IF NOT EXISTS vault_entries (
  entry_id     TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  title        TEXT NOT NULL,
  description  TEXT DEFAULT '',
  fields_json  TEXT DEFAULT '{}',
  created_by   TEXT DEFAULT '',
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_vault_entries_ws ON vault_entries(workspace_id);
