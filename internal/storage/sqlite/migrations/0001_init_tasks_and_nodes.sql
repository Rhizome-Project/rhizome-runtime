CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  priority TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dag_nodes (
  node_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  node_type TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (task_id, node_id),
  FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS node_dependencies (
  task_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  depends_on_node_id TEXT NOT NULL,
  PRIMARY KEY (task_id, node_id, depends_on_node_id),
  FOREIGN KEY (task_id, node_id) REFERENCES dag_nodes(task_id, node_id) ON DELETE CASCADE,
  FOREIGN KEY (task_id, depends_on_node_id) REFERENCES dag_nodes(task_id, node_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tasks_owner_status ON tasks(owner_user_id, status);
CREATE INDEX IF NOT EXISTS idx_nodes_task_status ON dag_nodes(task_id, status);
