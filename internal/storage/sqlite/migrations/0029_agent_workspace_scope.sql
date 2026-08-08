ALTER TABLE agents RENAME TO agents_legacy;
ALTER TABLE task_claims RENAME TO task_claims_legacy;
ALTER TABLE agent_updates RENAME TO agent_updates_legacy;
ALTER TABLE tg_message_map RENAME TO tg_message_map_legacy;
ALTER TABLE agent_profiles RENAME TO agent_profiles_legacy;
ALTER TABLE limit_group_agents RENAME TO limit_group_agents_legacy;
ALTER TABLE limit_snapshots RENAME TO limit_snapshots_legacy;

CREATE TABLE agents (
  workspace_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  owner_user_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  role TEXT NOT NULL,
  status TEXT NOT NULL,
  protocol_version TEXT NOT NULL,
  capabilities_json TEXT NOT NULL,
  summary TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_seen_at TEXT,
  PRIMARY KEY (workspace_id, agent_id),
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE
);

INSERT INTO agents(
  workspace_id,
  agent_id,
  owner_user_id,
  display_name,
  role,
  status,
  protocol_version,
  capabilities_json,
  summary,
  created_at,
  updated_at,
  last_seen_at
)
SELECT
  workspace_id,
  agent_id,
  owner_user_id,
  display_name,
  role,
  status,
  protocol_version,
  capabilities_json,
  summary,
  created_at,
  updated_at,
  last_seen_at
FROM agents_legacy;

CREATE TABLE task_claims (
  task_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  claim_status TEXT NOT NULL,
  summary TEXT NOT NULL,
  claimed_at TEXT NOT NULL,
  released_at TEXT,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (workspace_id, task_id) REFERENCES workspace_tasks(workspace_id, task_id) ON DELETE CASCADE,
  FOREIGN KEY (workspace_id, agent_id) REFERENCES agents(workspace_id, agent_id) ON DELETE CASCADE
);

INSERT INTO task_claims(task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at)
SELECT task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at
FROM task_claims_legacy;

CREATE TABLE agent_updates (
  update_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  update_type TEXT NOT NULL,
  summary TEXT NOT NULL,
  payload_json TEXT,
  requires_human INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (workspace_id, agent_id) REFERENCES agents(workspace_id, agent_id) ON DELETE CASCADE
);

INSERT INTO agent_updates(update_id, workspace_id, agent_id, update_type, summary, payload_json, requires_human, created_at)
SELECT update_id, workspace_id, agent_id, update_type, summary, payload_json, requires_human, created_at
FROM agent_updates_legacy;

CREATE TABLE tg_message_map (
  workspace_id TEXT NOT NULL,
  source_update_id TEXT NOT NULL,
  task_id TEXT,
  agent_id TEXT NOT NULL,
  target_user_id TEXT,
  telegram_chat_id INTEGER NOT NULL,
  telegram_message_id INTEGER NOT NULL,
  reply_update_id TEXT,
  sent_at TEXT NOT NULL,
  replied_at TEXT,
  superseded_by_update_id TEXT,
  superseded_at TEXT,
  PRIMARY KEY (telegram_chat_id, telegram_message_id),
  UNIQUE (source_update_id),
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (source_update_id) REFERENCES agent_updates(update_id) ON DELETE CASCADE,
  FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE SET NULL,
  FOREIGN KEY (workspace_id, agent_id) REFERENCES agents(workspace_id, agent_id) ON DELETE CASCADE,
  FOREIGN KEY (reply_update_id) REFERENCES agent_updates(update_id) ON DELETE SET NULL
);

INSERT INTO tg_message_map(
  workspace_id,
  source_update_id,
  task_id,
  agent_id,
  target_user_id,
  telegram_chat_id,
  telegram_message_id,
  reply_update_id,
  sent_at,
  replied_at,
  superseded_by_update_id,
  superseded_at
)
SELECT
  workspace_id,
  source_update_id,
  task_id,
  agent_id,
  target_user_id,
  telegram_chat_id,
  telegram_message_id,
  reply_update_id,
  sent_at,
  replied_at,
  superseded_by_update_id,
  superseded_at
FROM tg_message_map_legacy;

CREATE TABLE agent_profiles (
  workspace_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  bio TEXT NOT NULL DEFAULT '',
  specialization TEXT NOT NULL DEFAULT '',
  owner_name TEXT NOT NULL DEFAULT '',
  owner_contact TEXT NOT NULL DEFAULT '',
  avatar_url TEXT NOT NULL DEFAULT '',
  links_json TEXT NOT NULL DEFAULT '[]',
  tags_json TEXT NOT NULL DEFAULT '[]',
  tools_access_json TEXT NOT NULL DEFAULT '[]',
  metadata_json TEXT NOT NULL DEFAULT '{}',
  updated_at TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (workspace_id, agent_id),
  FOREIGN KEY (workspace_id, agent_id) REFERENCES agents(workspace_id, agent_id) ON DELETE CASCADE
);

INSERT INTO agent_profiles(
  workspace_id,
  agent_id,
  bio,
  specialization,
  owner_name,
  owner_contact,
  avatar_url,
  links_json,
  tags_json,
  tools_access_json,
  metadata_json,
  updated_at
)
SELECT
  a.workspace_id,
  p.agent_id,
  p.bio,
  p.specialization,
  p.owner_name,
  p.owner_contact,
  p.avatar_url,
  p.links_json,
  p.tags_json,
  p.tools_access_json,
  p.metadata_json,
  p.updated_at
FROM agent_profiles_legacy p
JOIN agents_legacy a ON a.agent_id = p.agent_id;

CREATE TABLE limit_group_agents (
  group_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  joined_at TEXT NOT NULL,
  PRIMARY KEY (group_id, workspace_id, agent_id),
  FOREIGN KEY (group_id) REFERENCES limit_groups(group_id) ON DELETE CASCADE,
  FOREIGN KEY (workspace_id, agent_id) REFERENCES agents(workspace_id, agent_id) ON DELETE CASCADE
);

INSERT INTO limit_group_agents(group_id, workspace_id, agent_id, joined_at)
SELECT lga.group_id, lg.workspace_id, lga.agent_id, lga.joined_at
FROM limit_group_agents_legacy lga
JOIN limit_groups lg ON lg.group_id = lga.group_id;

CREATE TABLE limit_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  group_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  daily_remaining INTEGER NOT NULL,
  weekly_remaining INTEGER NOT NULL,
  reported_at TEXT NOT NULL,
  FOREIGN KEY (group_id) REFERENCES limit_groups(group_id) ON DELETE CASCADE
);

INSERT INTO limit_snapshots(snapshot_id, group_id, workspace_id, agent_id, daily_remaining, weekly_remaining, reported_at)
SELECT ls.snapshot_id, ls.group_id, lg.workspace_id, ls.agent_id, ls.daily_remaining, ls.weekly_remaining, ls.reported_at
FROM limit_snapshots_legacy ls
JOIN limit_groups lg ON lg.group_id = ls.group_id;

DROP TABLE tg_message_map_legacy;
DROP TABLE task_claims_legacy;
DROP TABLE agent_updates_legacy;
DROP TABLE agent_profiles_legacy;
DROP TABLE limit_group_agents_legacy;
DROP TABLE limit_snapshots_legacy;
DROP TABLE agents_legacy;

CREATE INDEX idx_agents_workspace_status ON agents(workspace_id, status, last_seen_at DESC);
CREATE INDEX idx_agents_agent_id ON agents(agent_id);
CREATE INDEX idx_task_claims_workspace_status ON task_claims(workspace_id, claim_status, updated_at DESC);
CREATE INDEX idx_agent_updates_workspace ON agent_updates(workspace_id, created_at DESC);
CREATE INDEX idx_tg_message_map_workspace_sent ON tg_message_map(workspace_id, sent_at DESC);
CREATE INDEX idx_tg_message_map_target_user ON tg_message_map(target_user_id, sent_at DESC);
CREATE INDEX idx_tg_message_map_open ON tg_message_map(workspace_id, task_id, agent_id, replied_at, superseded_at, sent_at DESC);
CREATE INDEX idx_limit_groups_workspace_group ON limit_groups(workspace_id, group_id);
CREATE INDEX idx_limit_snapshots_group ON limit_snapshots(group_id, reported_at);
