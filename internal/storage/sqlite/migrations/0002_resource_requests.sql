CREATE TABLE IF NOT EXISTS resource_requests (
  request_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  owner_user_id TEXT NOT NULL,
  resource_type TEXT NOT NULL,
  service_id TEXT NOT NULL,
  estimated_cost_usd REAL NOT NULL,
  justification TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  decision TEXT,
  decision_reason TEXT,
  created_at TEXT NOT NULL,
  decided_at TEXT,
  FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
  FOREIGN KEY (task_id, node_id) REFERENCES dag_nodes(task_id, node_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_rr_task_node ON resource_requests(task_id, node_id);
CREATE INDEX IF NOT EXISTS idx_rr_owner_service_created ON resource_requests(owner_user_id, service_id, created_at);
