-- PCAW Wave 2: first-class project coordination profile, phase history, and gates.

CREATE TABLE IF NOT EXISTS project_profiles (
  workspace_id                TEXT NOT NULL,
  project_id                  TEXT NOT NULL,
  goal                        TEXT NOT NULL DEFAULT '',
  current_phase               TEXT NOT NULL DEFAULT 'INTAKE',
  design_doc_id               TEXT NOT NULL DEFAULT '',
  implementation_plan_doc_id  TEXT NOT NULL DEFAULT '',
  repo_required               INTEGER NOT NULL DEFAULT 0,
  repo_status                 TEXT NOT NULL DEFAULT 'NOT_REQUIRED',
  repo_url                    TEXT NOT NULL DEFAULT '',
  repo_default_branch         TEXT NOT NULL DEFAULT '',
  updated_by                  TEXT NOT NULL DEFAULT '',
  created_at                  TEXT NOT NULL,
  updated_at                  TEXT NOT NULL,
  PRIMARY KEY (workspace_id, project_id),
  FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_project_profiles_phase
  ON project_profiles(workspace_id, current_phase, updated_at DESC);

CREATE TABLE IF NOT EXISTS project_phase_history (
  history_id      TEXT PRIMARY KEY,
  workspace_id    TEXT NOT NULL,
  project_id      TEXT NOT NULL,
  from_phase      TEXT NOT NULL DEFAULT '',
  to_phase        TEXT NOT NULL,
  transitioned_by TEXT NOT NULL DEFAULT '',
  reason          TEXT NOT NULL DEFAULT '',
  created_at      TEXT NOT NULL,
  FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_project_phase_history_project
  ON project_phase_history(workspace_id, project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS project_gate_status (
  workspace_id  TEXT NOT NULL,
  project_id    TEXT NOT NULL,
  gate_key      TEXT NOT NULL,
  state         TEXT NOT NULL DEFAULT 'UNKNOWN',
  summary       TEXT NOT NULL DEFAULT '',
  evidence_ref  TEXT NOT NULL DEFAULT '',
  updated_by    TEXT NOT NULL DEFAULT '',
  updated_at    TEXT NOT NULL,
  PRIMARY KEY (workspace_id, project_id, gate_key),
  FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_project_gate_status_project
  ON project_gate_status(workspace_id, project_id, state, updated_at DESC);

INSERT OR IGNORE INTO project_profiles (
  workspace_id, project_id, goal, current_phase, repo_required, repo_status, created_at, updated_at
)
SELECT workspace_id, project_id, description, 'INTAKE', 0, 'NOT_REQUIRED', created_at, updated_at
FROM projects;
