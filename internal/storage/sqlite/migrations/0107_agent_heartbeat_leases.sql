CREATE TABLE IF NOT EXISTS agent_heartbeat_leases (
  workspace_id TEXT NOT NULL,
  agent_id     TEXT NOT NULL,
  heartbeat_id TEXT NOT NULL,
  owner_id     TEXT NOT NULL,
  lease_token  TEXT NOT NULL,
  locks_json   TEXT NOT NULL,
  acquired_at  TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  expires_at   TEXT NOT NULL,
  PRIMARY KEY (workspace_id, agent_id, heartbeat_id)
);

CREATE TABLE IF NOT EXISTS agent_heartbeat_lock_leases (
  workspace_id TEXT NOT NULL,
  agent_id     TEXT NOT NULL,
  lock_name    TEXT NOT NULL,
  heartbeat_id TEXT NOT NULL,
  owner_id     TEXT NOT NULL,
  lease_token  TEXT NOT NULL,
  expires_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (workspace_id, agent_id, lock_name)
);

CREATE INDEX IF NOT EXISTS idx_agent_heartbeat_leases_expiry
  ON agent_heartbeat_leases(workspace_id, agent_id, expires_at);

CREATE INDEX IF NOT EXISTS idx_agent_heartbeat_lock_leases_expiry
  ON agent_heartbeat_lock_leases(workspace_id, agent_id, expires_at);
