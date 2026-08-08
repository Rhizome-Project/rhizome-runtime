-- MCP Gateway: isolate servers securely by (workspace_id, server_id)

CREATE TABLE mcp_servers_new (
  workspace_id    TEXT NOT NULL,
  server_id       TEXT NOT NULL,
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
  updated_at      TEXT NOT NULL,
  PRIMARY KEY (workspace_id, server_id)
);

INSERT INTO mcp_servers_new (workspace_id, server_id, display_name, transport, url, command, args_json, env_json, headers_json, status, registered_by, created_at, updated_at)
SELECT workspace_id, server_id, display_name, transport, url, command, args_json, env_json, headers_json, status, registered_by, created_at, updated_at
FROM mcp_servers;

CREATE TABLE mcp_server_tools_new (
  workspace_id  TEXT NOT NULL,
  server_id     TEXT NOT NULL,
  tool_name     TEXT NOT NULL,
  description   TEXT DEFAULT '',
  input_schema  TEXT DEFAULT '{}',
  discovered_at TEXT NOT NULL,
  PRIMARY KEY (workspace_id, server_id, tool_name),
  FOREIGN KEY (workspace_id, server_id) REFERENCES mcp_servers_new(workspace_id, server_id) ON DELETE CASCADE
);

INSERT INTO mcp_server_tools_new (workspace_id, server_id, tool_name, description, input_schema, discovered_at)
SELECT s.workspace_id, t.server_id, t.tool_name, t.description, t.input_schema, t.discovered_at
FROM mcp_server_tools t
JOIN mcp_servers s ON t.server_id = s.server_id;

DROP TABLE mcp_server_tools;
DROP TABLE mcp_servers;

ALTER TABLE mcp_servers_new RENAME TO mcp_servers;
ALTER TABLE mcp_server_tools_new RENAME TO mcp_server_tools;

CREATE INDEX idx_mcp_servers_workspace ON mcp_servers(workspace_id, status);
