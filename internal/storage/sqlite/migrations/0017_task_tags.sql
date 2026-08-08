-- Add tags to tasks for categorization and auto-routing
ALTER TABLE tasks ADD COLUMN tags_json TEXT NOT NULL DEFAULT '[]';
