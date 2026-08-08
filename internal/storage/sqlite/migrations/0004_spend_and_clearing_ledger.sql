CREATE TABLE IF NOT EXISTS spend_transactions (
  tx_id TEXT PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  service_id TEXT NOT NULL,
  amount_usd REAL NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
  FOREIGN KEY (task_id, node_id) REFERENCES dag_nodes(task_id, node_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS clearing_ledger (
  entry_id TEXT PRIMARY KEY,
  debtor_user_id TEXT NOT NULL,
  creditor_user_id TEXT NOT NULL,
  resource_key TEXT NOT NULL,
  amount REAL NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_spend_task_node_created ON spend_transactions(task_id, node_id, created_at);
CREATE INDEX IF NOT EXISTS idx_spend_owner_created ON spend_transactions(owner_user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_clearing_debtor_created ON clearing_ledger(debtor_user_id, created_at);
