-- PCAW-001/PCAW-002 task/project linkage hardening.
-- task_kind already exists from 0008 and remains the legacy
-- EXECUTION/COORDINATION switch. This migration adds the project-specific
-- taxonomy companions and records legacy project_id links that cannot be scoped
-- to an existing project in the same workspace.

CREATE TABLE IF NOT EXISTS task_project_orphan_report (
  task_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL DEFAULT '',
  orphan_project_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  reported_at TEXT NOT NULL,
  PRIMARY KEY (task_id, workspace_id, orphan_project_id)
);

INSERT OR IGNORE INTO task_project_orphan_report (
  task_id, workspace_id, orphan_project_id, reason, reported_at
)
SELECT
  t.task_id,
  wt.workspace_id,
  TRIM(t.project_id),
  'project_id_attached_to_multiple_workspaces',
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
FROM tasks t
JOIN workspace_tasks wt ON wt.task_id = t.task_id
WHERE TRIM(COALESCE(t.project_id, '')) <> ''
  AND (
    SELECT COUNT(DISTINCT wt2.workspace_id)
    FROM workspace_tasks wt2
    WHERE wt2.task_id = t.task_id
  ) > 1;

INSERT OR IGNORE INTO task_project_orphan_report (
  task_id, workspace_id, orphan_project_id, reason, reported_at
)
SELECT
  t.task_id,
  wt.workspace_id,
  TRIM(t.project_id),
  'project_id_missing_in_workspace',
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
FROM tasks t
JOIN workspace_tasks wt ON wt.task_id = t.task_id
WHERE TRIM(COALESCE(t.project_id, '')) <> ''
  AND (
    SELECT COUNT(DISTINCT wt2.workspace_id)
    FROM workspace_tasks wt2
    WHERE wt2.task_id = t.task_id
  ) = 1
  AND NOT EXISTS (
    SELECT 1
    FROM projects p
    WHERE p.project_id = TRIM(t.project_id)
      AND p.workspace_id = wt.workspace_id
  );

INSERT OR IGNORE INTO task_project_orphan_report (
  task_id, workspace_id, orphan_project_id, reason, reported_at
)
SELECT
  t.task_id,
  '',
  TRIM(t.project_id),
  'task_not_attached_to_workspace',
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
FROM tasks t
WHERE TRIM(COALESCE(t.project_id, '')) <> ''
  AND NOT EXISTS (
    SELECT 1
    FROM workspace_tasks wt
    WHERE wt.task_id = t.task_id
  );

UPDATE tasks AS t
SET project_id = ''
WHERE TRIM(COALESCE(t.project_id, '')) <> ''
  AND (
    (
      SELECT COUNT(DISTINCT wt.workspace_id)
      FROM workspace_tasks wt
      WHERE wt.task_id = t.task_id
    ) != 1
    OR NOT EXISTS (
      SELECT 1
      FROM workspace_tasks wt
      JOIN projects p
        ON p.project_id = TRIM(t.project_id)
       AND p.workspace_id = wt.workspace_id
      WHERE wt.task_id = t.task_id
    )
  );

ALTER TABLE tasks ADD COLUMN project_lane TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN requires_project_gate INTEGER NOT NULL DEFAULT 0;

UPDATE tasks
SET project_lane = LOWER(TRIM(task_kind))
WHERE TRIM(COALESCE(project_lane, '')) = ''
  AND UPPER(TRIM(COALESCE(task_kind, ''))) IN (
    'INTAKE',
    'SPEC',
    'PLANNING',
    'IMPLEMENTATION',
    'REVIEW',
    'INTEGRATION',
    'VALIDATION',
    'OPS'
  );

UPDATE tasks
SET task_kind = CASE
  WHEN UPPER(TRIM(COALESCE(task_kind, ''))) = 'COORDINATION' THEN 'COORDINATION'
  ELSE 'EXECUTION'
END
WHERE UPPER(TRIM(COALESCE(task_kind, ''))) NOT IN ('EXECUTION', 'COORDINATION')
   OR TRIM(COALESCE(task_kind, '')) <> UPPER(TRIM(COALESCE(task_kind, 'EXECUTION')));

CREATE INDEX IF NOT EXISTS idx_tasks_project_kind_lane
  ON tasks(project_id, task_kind, project_lane, status);

CREATE INDEX IF NOT EXISTS idx_tasks_project_gate
  ON tasks(project_id, requires_project_gate, status);
