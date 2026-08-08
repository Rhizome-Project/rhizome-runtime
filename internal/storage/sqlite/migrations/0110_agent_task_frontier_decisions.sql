ALTER TABLE agent_task_frontiers
  ADD COLUMN decision_state TEXT NOT NULL DEFAULT 'offered';

ALTER TABLE agent_task_frontiers
  ADD COLUMN selected_task_id TEXT NOT NULL DEFAULT '';

ALTER TABLE agent_task_frontiers
  ADD COLUMN decision_summary TEXT NOT NULL DEFAULT '';

ALTER TABLE agent_task_frontiers
  ADD COLUMN decided_at TEXT NOT NULL DEFAULT '';

ALTER TABLE agent_task_frontiers
  ADD COLUMN consumed_task_id TEXT NOT NULL DEFAULT '';

ALTER TABLE agent_task_frontiers
  ADD COLUMN consumed_event_id TEXT NOT NULL DEFAULT '';

ALTER TABLE agent_task_frontiers
  ADD COLUMN consumed_at TEXT NOT NULL DEFAULT '';

ALTER TABLE agent_task_frontiers
  ADD COLUMN expires_at TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_agent_task_frontiers_decision
  ON agent_task_frontiers(workspace_id, agent_id, decision_state, generated_at DESC);
