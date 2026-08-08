-- Agent-to-Agent RPC: request/response pattern
CREATE TABLE IF NOT EXISTS agent_requests (
  request_id    TEXT PRIMARY KEY,
  workspace_id  TEXT NOT NULL,
  from_agent_id TEXT NOT NULL,
  to_agent_id   TEXT NOT NULL,
  method        TEXT NOT NULL DEFAULT 'default',
  payload       TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'PENDING',
  response      TEXT,
  created_at    TEXT NOT NULL,
  responded_at  TEXT,
  timeout_sec   INTEGER NOT NULL DEFAULT 300
);

CREATE INDEX IF NOT EXISTS idx_agent_requests_to ON agent_requests(workspace_id, to_agent_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_agent_requests_from ON agent_requests(workspace_id, from_agent_id, created_at);
