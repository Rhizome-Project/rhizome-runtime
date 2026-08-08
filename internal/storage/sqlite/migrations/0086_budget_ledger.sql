CREATE TABLE IF NOT EXISTS budget_accounts (
  account_id       TEXT PRIMARY KEY,
  principal_type   TEXT NOT NULL,
  principal_id     TEXT NOT NULL,
  workspace_id     TEXT NOT NULL,
  currency         TEXT NOT NULL DEFAULT 'USD',
  limit_micros     INTEGER NOT NULL,
  reserved_micros  INTEGER NOT NULL DEFAULT 0,
  spent_micros     INTEGER NOT NULL DEFAULT 0,
  refunded_micros  INTEGER NOT NULL DEFAULT 0,
  status           TEXT NOT NULL DEFAULT 'ACTIVE',
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  CHECK (limit_micros >= 0),
  CHECK (reserved_micros >= 0),
  CHECK (spent_micros >= 0),
  CHECK (refunded_micros >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_budget_accounts_principal_workspace_currency
  ON budget_accounts(principal_type, principal_id, workspace_id, currency);
CREATE INDEX IF NOT EXISTS idx_budget_accounts_workspace_status
  ON budget_accounts(workspace_id, status);

CREATE TABLE IF NOT EXISTS budget_reservations (
  reservation_id   TEXT PRIMARY KEY,
  idempotency_key  TEXT NOT NULL UNIQUE,
  account_id       TEXT NOT NULL,
  amount_micros    INTEGER NOT NULL,
  spent_micros     INTEGER NOT NULL DEFAULT 0,
  released_micros  INTEGER NOT NULL DEFAULT 0,
  status           TEXT NOT NULL DEFAULT 'OPEN',
  workspace_id     TEXT NOT NULL,
  agent_id         TEXT NOT NULL,
  task_id          TEXT NOT NULL,
  run_id           TEXT NOT NULL,
  provider_id      TEXT NOT NULL,
  model            TEXT NOT NULL,
  reason           TEXT NOT NULL DEFAULT '',
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  FOREIGN KEY (account_id) REFERENCES budget_accounts(account_id) ON DELETE CASCADE,
  CHECK (amount_micros > 0),
  CHECK (spent_micros >= 0),
  CHECK (released_micros >= 0),
  CHECK (spent_micros + released_micros <= amount_micros)
);

CREATE INDEX IF NOT EXISTS idx_budget_reservations_account_status
  ON budget_reservations(account_id, status);
CREATE INDEX IF NOT EXISTS idx_budget_reservations_scope
  ON budget_reservations(workspace_id, agent_id, task_id, run_id);

CREATE TABLE IF NOT EXISTS budget_ledger_entries (
  entry_id         TEXT PRIMARY KEY,
  idempotency_key  TEXT NOT NULL UNIQUE,
  account_id       TEXT NOT NULL,
  reservation_id   TEXT NOT NULL DEFAULT '',
  entry_type       TEXT NOT NULL,
  amount_micros    INTEGER NOT NULL,
  currency         TEXT NOT NULL DEFAULT 'USD',
  workspace_id     TEXT NOT NULL,
  agent_id         TEXT NOT NULL,
  task_id          TEXT NOT NULL,
  run_id           TEXT NOT NULL,
  provider_id      TEXT NOT NULL,
  model            TEXT NOT NULL,
  source_entry_id  TEXT NOT NULL DEFAULT '',
  reason           TEXT NOT NULL DEFAULT '',
  created_at       TEXT NOT NULL,
  FOREIGN KEY (account_id) REFERENCES budget_accounts(account_id) ON DELETE CASCADE,
  CHECK (entry_type IN ('RESERVATION', 'SPEND', 'RELEASE', 'REFUND')),
  CHECK (amount_micros > 0)
);

CREATE INDEX IF NOT EXISTS idx_budget_ledger_account_created
  ON budget_ledger_entries(account_id, created_at);
CREATE INDEX IF NOT EXISTS idx_budget_ledger_scope_created
  ON budget_ledger_entries(workspace_id, agent_id, task_id, run_id, created_at);
CREATE INDEX IF NOT EXISTS idx_budget_ledger_reservation
  ON budget_ledger_entries(reservation_id, created_at);
