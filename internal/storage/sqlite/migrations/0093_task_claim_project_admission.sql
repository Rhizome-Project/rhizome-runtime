ALTER TABLE task_claims ADD COLUMN project_role_id TEXT NOT NULL DEFAULT '';
ALTER TABLE task_claims ADD COLUMN repo_id TEXT NOT NULL DEFAULT '';
ALTER TABLE task_claims ADD COLUMN checkout_id TEXT NOT NULL DEFAULT '';
ALTER TABLE task_claims ADD COLUMN branch_id TEXT NOT NULL DEFAULT '';
ALTER TABLE task_claims ADD COLUMN write_scope_json TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_task_claims_project_role
  ON task_claims(workspace_id, project_role_id, claim_status);
CREATE INDEX IF NOT EXISTS idx_task_claims_checkout
  ON task_claims(workspace_id, checkout_id, claim_status);
CREATE INDEX IF NOT EXISTS idx_task_claims_branch
  ON task_claims(workspace_id, branch_id, claim_status);
