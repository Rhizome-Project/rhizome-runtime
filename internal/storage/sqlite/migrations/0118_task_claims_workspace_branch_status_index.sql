CREATE INDEX IF NOT EXISTS idx_task_claims_workspace_branch_status
  ON task_claims(workspace_id, branch_id, claim_status);
