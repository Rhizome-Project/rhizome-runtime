CREATE TABLE IF NOT EXISTS task_patch_queue_identities (
    task_id TEXT PRIMARY KEY REFERENCES tasks(task_id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    project_id TEXT NOT NULL,
    queue_id TEXT NOT NULL DEFAULT '',
    item_id TEXT NOT NULL DEFAULT '',
    branch_id TEXT NOT NULL DEFAULT '',
    head_sha TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_patch_queue_identity_queue
    ON task_patch_queue_identities(workspace_id, project_id, kind, queue_id, item_id, head_sha, task_id);

CREATE INDEX IF NOT EXISTS idx_task_patch_queue_identity_branch
    ON task_patch_queue_identities(workspace_id, project_id, kind, branch_id, head_sha, task_id);

CREATE INDEX IF NOT EXISTS idx_task_patch_queue_identity_head
    ON task_patch_queue_identities(workspace_id, project_id, kind, head_sha, task_id);

INSERT OR IGNORE INTO task_patch_queue_identities(
    task_id,
    workspace_id,
    project_id,
    queue_id,
    item_id,
    branch_id,
    head_sha,
    kind,
    created_at,
    updated_at
)
SELECT t.task_id,
       wt.workspace_id,
       COALESCE(NULLIF(json_extract(t.task_requirements_json, '$.project_id'), ''), COALESCE(t.project_id, '')),
       COALESCE(json_extract(t.task_requirements_json, '$.queue_id'), ''),
       COALESCE(json_extract(t.task_requirements_json, '$.item_id'), ''),
       COALESCE(json_extract(t.task_requirements_json, '$.branch_id'), ''),
       lower(COALESCE(json_extract(t.task_requirements_json, '$.head_sha'), '')),
       lower(COALESCE(json_extract(t.task_requirements_json, '$.patch_queue_task_kind'), '')),
       t.created_at,
       t.updated_at
  FROM tasks t
  JOIN workspace_tasks wt ON wt.task_id = t.task_id
 WHERE COALESCE(json_extract(t.task_requirements_json, '$.patch_queue_task_kind'), '') <> ''
   AND COALESCE(NULLIF(json_extract(t.task_requirements_json, '$.project_id'), ''), COALESCE(t.project_id, '')) <> '';
