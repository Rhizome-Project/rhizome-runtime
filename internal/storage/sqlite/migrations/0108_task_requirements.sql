ALTER TABLE tasks ADD COLUMN task_requirements_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE tasks ADD COLUMN write_scope_hints_json TEXT NOT NULL DEFAULT '[]';
