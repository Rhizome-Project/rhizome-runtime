-- 0024_node_claims.sql

-- Node claims allow individual DAG nodes within a task to be claimed and executed by different agents

CREATE TABLE IF NOT EXISTS node_claims (
  task_id       TEXT NOT NULL,
  node_id       TEXT NOT NULL,
  workspace_id  TEXT NOT NULL,
  agent_id      TEXT NOT NULL,
  claim_status  TEXT NOT NULL, -- PENDING, CLAIMED, COMPLETED, FAILED, RELEASED
  summary       TEXT DEFAULT '',
  claimed_at    TEXT NOT NULL,
  released_at   TEXT,
  updated_at    TEXT NOT NULL,
  PRIMARY KEY (task_id, node_id, workspace_id),
  FOREIGN KEY (task_id, node_id) REFERENCES dag_nodes(task_id, node_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_node_claims_workspace ON node_claims(workspace_id, claim_status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_node_claims_agent ON node_claims(agent_id, claim_status);
