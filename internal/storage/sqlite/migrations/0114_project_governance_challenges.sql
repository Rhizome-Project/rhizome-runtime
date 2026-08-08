CREATE TABLE IF NOT EXISTS governance_challenges (
  challenge_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  challenged_agent_id TEXT NOT NULL,
  challenger_agent_id TEXT NOT NULL,
  nominated_successor_agent_id TEXT NOT NULL DEFAULT '',
  lead_role_id TEXT NOT NULL DEFAULT '',
  tension_id TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL,
  current_round INTEGER NOT NULL DEFAULT 1,
  max_rounds INTEGER NOT NULL DEFAULT 3,
  stall_predicates_json TEXT NOT NULL DEFAULT '[]',
  predicate_results_json TEXT NOT NULL DEFAULT '[]',
  evidence_refs_json TEXT NOT NULL DEFAULT '[]',
  argument_doc_key TEXT NOT NULL DEFAULT '',
  defense_doc_key TEXT NOT NULL DEFAULT '',
  defense_stance TEXT NOT NULL DEFAULT '',
  round_opened_at TEXT NOT NULL,
  defense_deadline_at TEXT NOT NULL DEFAULT '',
  voting_deadline_at TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  resolved_at TEXT NOT NULL DEFAULT '',
  resolution TEXT NOT NULL DEFAULT '',
  CHECK (state IN ('RAISED', 'DEFENSE_OPEN', 'VOTING', 'NEGOTIATION', 'RESOLVED_UPHELD', 'RESOLVED_REASSIGNED', 'RESOLVED_DEFAULT', 'AUTO_WITHDRAWN')),
  CHECK (current_round >= 1),
  CHECK (max_rounds BETWEEN 1 AND 3),
  FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE CASCADE,
  FOREIGN KEY (workspace_id, challenged_agent_id) REFERENCES agents(workspace_id, agent_id) ON DELETE CASCADE,
  FOREIGN KEY (workspace_id, challenger_agent_id) REFERENCES agents(workspace_id, agent_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_governance_challenges_workspace_state
  ON governance_challenges(workspace_id, state, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_governance_challenges_project_state
  ON governance_challenges(workspace_id, project_id, state, updated_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_governance_challenges_one_open_lead
  ON governance_challenges(workspace_id, project_id, challenged_agent_id)
  WHERE state IN ('RAISED', 'DEFENSE_OPEN', 'VOTING', 'NEGOTIATION');

CREATE TABLE IF NOT EXISTS governance_votes (
  vote_id TEXT PRIMARY KEY,
  challenge_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  round INTEGER NOT NULL,
  voter_agent_id TEXT NOT NULL,
  ballot TEXT NOT NULL,
  rationale_doc_key TEXT NOT NULL DEFAULT '',
  cast_at TEXT NOT NULL,
  CHECK (ballot IN ('UPHOLD_LEAD', 'REASSIGN', 'ABSTAIN')),
  UNIQUE(challenge_id, round, voter_agent_id),
  FOREIGN KEY (challenge_id) REFERENCES governance_challenges(challenge_id) ON DELETE CASCADE,
  FOREIGN KEY (workspace_id, voter_agent_id) REFERENCES agents(workspace_id, agent_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_governance_votes_challenge_round
  ON governance_votes(challenge_id, round, cast_at);
