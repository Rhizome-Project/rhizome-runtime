CREATE TABLE IF NOT EXISTS project_patch_queue_items (
  queue_id            TEXT NOT NULL,
  item_id             TEXT NOT NULL,
  workspace_id        TEXT NOT NULL,
  project_id          TEXT NOT NULL,
  repo_id             TEXT NOT NULL,
  branch_id           TEXT NOT NULL,
  review_doc_key      TEXT NOT NULL,
  repo_authority_mode TEXT NOT NULL DEFAULT 'patch_only_temp_repo',
  state               TEXT NOT NULL DEFAULT 'PROPOSED',
  pathset_json        TEXT NOT NULL DEFAULT '[]',
  base_ref            TEXT NOT NULL DEFAULT '',
  base_sha            TEXT NOT NULL DEFAULT '',
  head_sha            TEXT NOT NULL DEFAULT '',
  auto_merge          INTEGER NOT NULL DEFAULT 0,
  submitted_by        TEXT NOT NULL DEFAULT '',
  created_at          TEXT NOT NULL,
  updated_at          TEXT NOT NULL,
  PRIMARY KEY(queue_id, item_id),
  FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE,
  FOREIGN KEY(repo_id) REFERENCES project_repositories(repo_id) ON DELETE CASCADE,
  FOREIGN KEY(branch_id) REFERENCES project_branch_registry(branch_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_project_patch_queue_items_project
  ON project_patch_queue_items(workspace_id, project_id, state, updated_at);
CREATE INDEX IF NOT EXISTS idx_project_patch_queue_items_branch
  ON project_patch_queue_items(workspace_id, branch_id, state);
CREATE UNIQUE INDEX IF NOT EXISTS idx_project_patch_queue_items_live_branch
  ON project_patch_queue_items(workspace_id, branch_id)
  WHERE state IN ('PROPOSED');
