CREATE TABLE IF NOT EXISTS service_direction_briefs (
  direction_id       TEXT PRIMARY KEY,
  idempotency_key    TEXT NOT NULL,
  workspace_id       TEXT NOT NULL,
  title              TEXT NOT NULL,
  description        TEXT NOT NULL DEFAULT '',
  constraints_json   TEXT NOT NULL DEFAULT '{}',
  budget_cap_micros  INTEGER NOT NULL DEFAULT 0,
  status             TEXT NOT NULL DEFAULT 'ACTIVE',
  created_by         TEXT NOT NULL,
  updated_by         TEXT NOT NULL,
  created_at         TEXT NOT NULL,
  updated_at         TEXT NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  CHECK (budget_cap_micros >= 0),
  CHECK (status IN ('DRAFT', 'ACTIVE', 'PAUSED', 'ARCHIVED'))
);

CREATE INDEX IF NOT EXISTS idx_service_direction_briefs_workspace_status
  ON service_direction_briefs(workspace_id, status, updated_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_service_direction_briefs_workspace_idempotency
  ON service_direction_briefs(workspace_id, idempotency_key);

CREATE TABLE IF NOT EXISTS service_candidates (
  candidate_id        TEXT PRIMARY KEY,
  idempotency_key     TEXT NOT NULL,
  workspace_id        TEXT NOT NULL,
  direction_id        TEXT NOT NULL,
  title               TEXT NOT NULL,
  target_user         TEXT NOT NULL DEFAULT '',
  user_pain           TEXT NOT NULL DEFAULT '',
  solution_summary    TEXT NOT NULL DEFAULT '',
  distribution        TEXT NOT NULL DEFAULT '',
  monetization        TEXT NOT NULL DEFAULT '',
  implementation_size TEXT NOT NULL DEFAULT '',
  risk_level          TEXT NOT NULL DEFAULT '',
  score               INTEGER NOT NULL DEFAULT 0,
  evidence_plan_json  TEXT NOT NULL DEFAULT '{}',
  status              TEXT NOT NULL DEFAULT 'PROPOSED',
  selected_by         TEXT NOT NULL DEFAULT '',
  selected_at         TEXT NOT NULL DEFAULT '',
  created_by          TEXT NOT NULL,
  updated_by          TEXT NOT NULL,
  created_at          TEXT NOT NULL,
  updated_at          TEXT NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (direction_id) REFERENCES service_direction_briefs(direction_id) ON DELETE CASCADE,
  CHECK (status IN ('PROPOSED', 'SELECTED', 'REJECTED', 'PARKED'))
);

CREATE INDEX IF NOT EXISTS idx_service_candidates_workspace_status
  ON service_candidates(workspace_id, status, updated_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_service_candidates_workspace_idempotency
  ON service_candidates(workspace_id, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_service_candidates_direction_status
  ON service_candidates(workspace_id, direction_id, status, score DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS service_runs (
  run_id                 TEXT PRIMARY KEY,
  idempotency_key        TEXT NOT NULL,
  workspace_id           TEXT NOT NULL,
  candidate_id           TEXT NOT NULL,
  project_id             TEXT NOT NULL,
  title                  TEXT NOT NULL,
  deploy_target          TEXT NOT NULL DEFAULT '',
  public_url             TEXT NOT NULL DEFAULT '',
  health_check_url       TEXT NOT NULL DEFAULT '',
  budget_account_id      TEXT NOT NULL DEFAULT '',
  budget_cap_micros      INTEGER NOT NULL DEFAULT 0,
  credential_policy      TEXT NOT NULL DEFAULT 'PENDING_APPROVAL',
  status                 TEXT NOT NULL DEFAULT 'PLANNED',
  started_by             TEXT NOT NULL,
  updated_by             TEXT NOT NULL,
  started_at             TEXT NOT NULL,
  updated_at             TEXT NOT NULL,
  completed_at           TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (candidate_id) REFERENCES service_candidates(candidate_id) ON DELETE CASCADE,
  FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE,
  CHECK (budget_cap_micros >= 0),
  CHECK (credential_policy IN ('PENDING_APPROVAL', 'FREE_TIER_ONLY', 'APPROVED')),
  CHECK (status IN ('PLANNED', 'ACTIVE', 'BLOCKED', 'DEPLOYED', 'MEASURING', 'COMPLETED', 'KILLED', 'CANCELLED'))
);

CREATE INDEX IF NOT EXISTS idx_service_runs_workspace_status
  ON service_runs(workspace_id, status, updated_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_service_runs_workspace_idempotency
  ON service_runs(workspace_id, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_service_runs_candidate
  ON service_runs(workspace_id, candidate_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_service_runs_project
  ON service_runs(workspace_id, project_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS service_approval_grants (
  grant_id        TEXT PRIMARY KEY,
  idempotency_key TEXT NOT NULL,
  workspace_id    TEXT NOT NULL,
  run_id          TEXT NOT NULL,
  grant_type      TEXT NOT NULL,
  scope_json      TEXT NOT NULL DEFAULT '{}',
  approval_ref    TEXT NOT NULL DEFAULT '',
  status          TEXT NOT NULL DEFAULT 'PENDING',
  approved_by     TEXT NOT NULL DEFAULT '',
  expires_at      TEXT NOT NULL DEFAULT '',
  created_by      TEXT NOT NULL,
  updated_by      TEXT NOT NULL,
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (run_id) REFERENCES service_runs(run_id) ON DELETE CASCADE,
  CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'EXPIRED', 'REVOKED'))
);

CREATE INDEX IF NOT EXISTS idx_service_approval_grants_run_status
  ON service_approval_grants(workspace_id, run_id, status, expires_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_service_approval_grants_workspace_idempotency
  ON service_approval_grants(workspace_id, idempotency_key);

CREATE TABLE IF NOT EXISTS service_provider_resources (
  resource_id               TEXT PRIMARY KEY,
  idempotency_key            TEXT NOT NULL,
  workspace_id               TEXT NOT NULL,
  run_id                     TEXT NOT NULL,
  provider                   TEXT NOT NULL,
  resource_type              TEXT NOT NULL,
  resource_ref               TEXT NOT NULL DEFAULT '',
  credential_vault_entry_id  TEXT NOT NULL DEFAULT '',
  approval_grant_id          TEXT NOT NULL DEFAULT '',
  paid                       INTEGER NOT NULL DEFAULT 0,
  cost_cap_micros            INTEGER NOT NULL DEFAULT 0,
  status                     TEXT NOT NULL DEFAULT 'PENDING_APPROVAL',
  ttl_expires_at             TEXT NOT NULL DEFAULT '',
  created_by                 TEXT NOT NULL,
  updated_by                 TEXT NOT NULL,
  created_at                 TEXT NOT NULL,
  updated_at                 TEXT NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (run_id) REFERENCES service_runs(run_id) ON DELETE CASCADE,
  CHECK (paid IN (0, 1)),
  CHECK (cost_cap_micros >= 0),
  CHECK (status IN ('PENDING_APPROVAL', 'PROVISIONED', 'ACTIVE', 'REVOKED', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_service_provider_resources_run_status
  ON service_provider_resources(workspace_id, run_id, status, updated_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_service_provider_resources_workspace_idempotency
  ON service_provider_resources(workspace_id, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_service_provider_resources_grant
  ON service_provider_resources(approval_grant_id);

CREATE TABLE IF NOT EXISTS service_spend_receipts (
  receipt_id           TEXT PRIMARY KEY,
  idempotency_key      TEXT NOT NULL,
  workspace_id         TEXT NOT NULL,
  run_id               TEXT NOT NULL,
  provider_resource_id TEXT NOT NULL DEFAULT '',
  ledger_entry_id      TEXT NOT NULL DEFAULT '',
  amount_micros        INTEGER NOT NULL,
  currency             TEXT NOT NULL DEFAULT 'USD',
  external_receipt_ref TEXT NOT NULL DEFAULT '',
  evidence_ref         TEXT NOT NULL,
  recorded_by          TEXT NOT NULL,
  recorded_at          TEXT NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (run_id) REFERENCES service_runs(run_id) ON DELETE CASCADE,
  CHECK (amount_micros >= 0)
);

CREATE INDEX IF NOT EXISTS idx_service_spend_receipts_run
  ON service_spend_receipts(workspace_id, run_id, recorded_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_service_spend_receipts_workspace_idempotency
  ON service_spend_receipts(workspace_id, idempotency_key);

CREATE TABLE IF NOT EXISTS service_revenue_observations (
  observation_id       TEXT PRIMARY KEY,
  idempotency_key      TEXT NOT NULL,
  workspace_id         TEXT NOT NULL,
  run_id               TEXT NOT NULL,
  amount_micros        INTEGER NOT NULL,
  currency             TEXT NOT NULL DEFAULT 'USD',
  source               TEXT NOT NULL,
  external_receipt_ref TEXT NOT NULL DEFAULT '',
  evidence_ref         TEXT NOT NULL,
  observed_at          TEXT NOT NULL,
  recorded_by          TEXT NOT NULL,
  created_at           TEXT NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (run_id) REFERENCES service_runs(run_id) ON DELETE CASCADE,
  CHECK (amount_micros >= 0)
);

CREATE INDEX IF NOT EXISTS idx_service_revenue_observations_run
  ON service_revenue_observations(workspace_id, run_id, observed_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_service_revenue_observations_workspace_idempotency
  ON service_revenue_observations(workspace_id, idempotency_key);

CREATE TABLE IF NOT EXISTS service_outcomes (
  outcome_id             TEXT PRIMARY KEY,
  idempotency_key        TEXT NOT NULL,
  workspace_id           TEXT NOT NULL,
  run_id                 TEXT NOT NULL,
  public_url             TEXT NOT NULL DEFAULT '',
  deploy_health_status   TEXT NOT NULL DEFAULT 'UNKNOWN',
  deploy_evidence_ref    TEXT NOT NULL DEFAULT '',
  analytics_json         TEXT NOT NULL DEFAULT '{}',
  analytics_evidence_ref TEXT NOT NULL DEFAULT '',
  spend_micros           INTEGER NOT NULL DEFAULT 0,
  spend_evidence_ref     TEXT NOT NULL DEFAULT '',
  revenue_micros         INTEGER NOT NULL DEFAULT 0,
  revenue_evidence_ref   TEXT NOT NULL DEFAULT '',
  quality_score          INTEGER NOT NULL DEFAULT 0,
  decision               TEXT NOT NULL,
  decision_reason        TEXT NOT NULL,
  evidence_refs_json     TEXT NOT NULL DEFAULT '[]',
  recorded_by            TEXT NOT NULL,
  recorded_at            TEXT NOT NULL,
  FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
  FOREIGN KEY (run_id) REFERENCES service_runs(run_id) ON DELETE CASCADE,
  CHECK (spend_micros >= 0),
  CHECK (revenue_micros >= 0),
  CHECK (deploy_health_status IN ('UNKNOWN', 'PASS', 'FAIL', 'WAIVED')),
  CHECK (decision IN ('CONTINUE', 'ITERATE', 'KILL', 'BLOCKED', 'HOLD'))
);

CREATE INDEX IF NOT EXISTS idx_service_outcomes_run_recorded
  ON service_outcomes(workspace_id, run_id, recorded_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_service_outcomes_workspace_idempotency
  ON service_outcomes(workspace_id, idempotency_key);
