CREATE TABLE IF NOT EXISTS node_state_transitions (
  transition_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  from_status TEXT NOT NULL,
  to_status TEXT NOT NULL,
  reason TEXT,
  actor_id TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY (task_id, node_id) REFERENCES dag_nodes(task_id, node_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_node_transitions_task_node ON node_state_transitions(task_id, node_id, created_at);
