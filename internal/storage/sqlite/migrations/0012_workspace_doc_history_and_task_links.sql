CREATE TABLE IF NOT EXISTS workspace_doc_revisions (
  revision_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  doc_key TEXT NOT NULL,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  updated_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_workspace_doc_revisions_workspace_doc_created
  ON workspace_doc_revisions(workspace_id, doc_key, created_at DESC);

CREATE TABLE IF NOT EXISTS workspace_task_links (
  workspace_id TEXT NOT NULL,
  from_task_id TEXT NOT NULL,
  to_task_id TEXT NOT NULL,
  link_type TEXT NOT NULL,
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (workspace_id, from_task_id, to_task_id, link_type),
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (from_task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
  FOREIGN KEY (to_task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_workspace_task_links_workspace_type_created
  ON workspace_task_links(workspace_id, link_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_workspace_task_links_workspace_from
  ON workspace_task_links(workspace_id, from_task_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_workspace_task_links_workspace_to
  ON workspace_task_links(workspace_id, to_task_id, created_at DESC);
