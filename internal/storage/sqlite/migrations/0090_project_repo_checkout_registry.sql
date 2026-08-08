CREATE TABLE IF NOT EXISTS project_repositories (
  repo_id                   TEXT PRIMARY KEY,
  workspace_id              TEXT NOT NULL,
  project_id                TEXT NOT NULL,
  remote_url                TEXT NOT NULL DEFAULT '',
  remote_kind               TEXT NOT NULL DEFAULT 'unknown',
  owner                     TEXT NOT NULL DEFAULT '',
  name                      TEXT NOT NULL DEFAULT '',
  default_branch            TEXT NOT NULL DEFAULT 'main',
  integration_branch        TEXT NOT NULL DEFAULT '',
  credential_vault_entry_id TEXT NOT NULL DEFAULT '',
  repo_status               TEXT NOT NULL DEFAULT 'MISSING',
  is_canonical              INTEGER NOT NULL DEFAULT 0,
  created_by_agent_id       TEXT NOT NULL DEFAULT '',
  updated_by                TEXT NOT NULL DEFAULT '',
  created_at                TEXT NOT NULL,
  updated_at                TEXT NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_project_repositories_project ON project_repositories(workspace_id, project_id, repo_status);
CREATE INDEX IF NOT EXISTS idx_project_repositories_remote ON project_repositories(remote_kind, owner, name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_project_repositories_canonical
  ON project_repositories(workspace_id, project_id)
  WHERE is_canonical = 1 AND repo_status <> 'ARCHIVED';

CREATE TABLE IF NOT EXISTS project_checkout_registry (
  checkout_id      TEXT PRIMARY KEY,
  workspace_id     TEXT NOT NULL,
  project_id       TEXT NOT NULL,
  repo_id          TEXT NOT NULL,
  machine_id       TEXT NOT NULL,
  machine_label    TEXT NOT NULL DEFAULT '',
  owner_user_id    TEXT NOT NULL DEFAULT '',
  agent_id         TEXT NOT NULL DEFAULT '',
  local_path       TEXT NOT NULL,
  checkout_kind    TEXT NOT NULL DEFAULT 'clone',
  branch_name      TEXT NOT NULL DEFAULT '',
  base_branch      TEXT NOT NULL DEFAULT '',
  head_sha         TEXT NOT NULL DEFAULT '',
  base_sha         TEXT NOT NULL DEFAULT '',
  dirty_state      TEXT NOT NULL DEFAULT 'unknown',
  active_task_id   TEXT NOT NULL DEFAULT '',
  active_claim_id  TEXT NOT NULL DEFAULT '',
  status           TEXT NOT NULL DEFAULT 'ACTIVE',
  last_seen_at     TEXT NOT NULL,
  updated_by       TEXT NOT NULL DEFAULT '',
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE,
  FOREIGN KEY(repo_id) REFERENCES project_repositories(repo_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_project_checkout_registry_project ON project_checkout_registry(workspace_id, project_id, status);
CREATE INDEX IF NOT EXISTS idx_project_checkout_registry_agent ON project_checkout_registry(workspace_id, agent_id, status);
CREATE INDEX IF NOT EXISTS idx_project_checkout_registry_repo ON project_checkout_registry(workspace_id, repo_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_project_checkout_registry_active_path
  ON project_checkout_registry(workspace_id, machine_id, local_path)
  WHERE status IN ('ACTIVE', 'BLOCKED', 'STALE');
