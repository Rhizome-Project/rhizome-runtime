-- 0018: Add close_reason column to tasks table
ALTER TABLE tasks ADD COLUMN close_reason TEXT NOT NULL DEFAULT '';
