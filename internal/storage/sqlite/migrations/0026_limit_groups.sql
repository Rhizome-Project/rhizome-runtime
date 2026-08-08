-- Limit groups: subscription-based grouping for agent rate limits
CREATE TABLE IF NOT EXISTS limit_groups (
  group_id          TEXT PRIMARY KEY,
  workspace_id      TEXT NOT NULL,
  title             TEXT NOT NULL,
  owner_name        TEXT NOT NULL DEFAULT '',
  subscription_tier TEXT NOT NULL DEFAULT '',
  daily_limit       INTEGER NOT NULL DEFAULT 0,
  weekly_limit      INTEGER NOT NULL DEFAULT 0,
  daily_remaining   INTEGER NOT NULL DEFAULT 0,
  weekly_remaining  INTEGER NOT NULL DEFAULT 0,
  last_reported_at  TEXT NOT NULL DEFAULT '',
  created_at        TEXT NOT NULL,
  updated_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_limit_groups_ws ON limit_groups(workspace_id);

-- Agent membership in limit groups
CREATE TABLE IF NOT EXISTS limit_group_agents (
  group_id  TEXT NOT NULL,
  agent_id  TEXT NOT NULL,
  joined_at TEXT NOT NULL,
  PRIMARY KEY (group_id, agent_id),
  FOREIGN KEY (group_id) REFERENCES limit_groups(group_id) ON DELETE CASCADE,
  FOREIGN KEY (agent_id) REFERENCES agents(agent_id) ON DELETE CASCADE
);

-- Audit trail for limit report updates
CREATE TABLE IF NOT EXISTS limit_snapshots (
  snapshot_id      TEXT PRIMARY KEY,
  group_id         TEXT NOT NULL,
  agent_id         TEXT NOT NULL,
  daily_remaining  INTEGER NOT NULL,
  weekly_remaining INTEGER NOT NULL,
  reported_at      TEXT NOT NULL,
  FOREIGN KEY (group_id) REFERENCES limit_groups(group_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_limit_snapshots_group ON limit_snapshots(group_id, reported_at);
