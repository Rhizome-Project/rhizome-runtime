ALTER TABLE tasks ADD COLUMN task_class TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN task_class_source TEXT NOT NULL DEFAULT 'UNSET';
ALTER TABLE tasks ADD COLUMN task_class_updated_at TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_tasks_class_source ON tasks(task_class, task_class_source, status);
