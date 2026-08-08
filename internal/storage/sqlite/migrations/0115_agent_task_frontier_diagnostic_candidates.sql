ALTER TABLE agent_task_frontiers
ADD COLUMN diagnostic_candidate_task_ids_json TEXT NOT NULL DEFAULT '[]';
