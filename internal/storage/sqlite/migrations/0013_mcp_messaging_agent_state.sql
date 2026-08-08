-- MCP Gateway: server registry and tool cache
CREATE TABLE IF NOT EXISTS mcp_servers (
  server_id       TEXT PRIMARY KEY,
  workspace_id    TEXT NOT NULL,
  display_name    TEXT NOT NULL,
  transport       TEXT NOT NULL DEFAULT 'streamable-http',
  url             TEXT,
  command         TEXT,
  args_json       TEXT DEFAULT '[]',
  env_json        TEXT DEFAULT '{}',
  headers_json    TEXT DEFAULT '{}',
  status          TEXT NOT NULL DEFAULT 'ACTIVE',
  registered_by   TEXT NOT NULL,
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mcp_servers_workspace ON mcp_servers(workspace_id, status);

CREATE TABLE IF NOT EXISTS mcp_server_tools (
  server_id     TEXT NOT NULL,
  tool_name     TEXT NOT NULL,
  description   TEXT DEFAULT '',
  input_schema  TEXT DEFAULT '{}',
  discovered_at TEXT NOT NULL,
  PRIMARY KEY (server_id, tool_name),
  FOREIGN KEY (server_id) REFERENCES mcp_servers(server_id)
);

-- Agent messaging
CREATE TABLE IF NOT EXISTS agent_messages (
  message_id    TEXT PRIMARY KEY,
  workspace_id  TEXT NOT NULL,
  from_agent_id TEXT NOT NULL,
  to_agent_id   TEXT,
  channel       TEXT NOT NULL DEFAULT 'default',
  content_type  TEXT NOT NULL DEFAULT 'text/plain',
  content       TEXT NOT NULL,
  metadata_json TEXT DEFAULT '{}',
  created_at    TEXT NOT NULL,
  read_at       TEXT
);

CREATE INDEX IF NOT EXISTS idx_agent_messages_to ON agent_messages(workspace_id, to_agent_id, created_at);
CREATE INDEX IF NOT EXISTS idx_agent_messages_channel ON agent_messages(workspace_id, channel, created_at);

-- Agent memory (per-agent key-value store)
CREATE TABLE IF NOT EXISTS agent_state (
  workspace_id TEXT NOT NULL,
  agent_id     TEXT NOT NULL,
  state_key    TEXT NOT NULL,
  state_value  TEXT,
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (workspace_id, agent_id, state_key)
);
