-- Knowledge claim lifecycle metadata: review/confirm/dispute/stale/supersede.

ALTER TABLE knowledge_claims ADD COLUMN lifecycle_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE knowledge_claims ADD COLUMN review_due_at TEXT;
ALTER TABLE knowledge_claims ADD COLUMN reviewed_at TEXT;
ALTER TABLE knowledge_claims ADD COLUMN reviewed_by TEXT;
ALTER TABLE knowledge_claims ADD COLUMN superseded_by_claim_id TEXT REFERENCES knowledge_claims(claim_id);

CREATE INDEX IF NOT EXISTS idx_knowledge_claims_workspace_review_due
    ON knowledge_claims(workspace_id, review_due_at, updated_at DESC)
    WHERE review_due_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_knowledge_claims_workspace_superseded_by
    ON knowledge_claims(workspace_id, superseded_by_claim_id)
    WHERE superseded_by_claim_id IS NOT NULL;
