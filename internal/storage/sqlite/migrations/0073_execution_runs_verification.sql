ALTER TABLE execution_runs
    ADD COLUMN verification_json TEXT NOT NULL DEFAULT '{}';
