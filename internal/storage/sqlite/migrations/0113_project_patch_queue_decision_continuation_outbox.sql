CREATE TABLE IF NOT EXISTS project_patch_queue_decision_continuation_outbox (
    outbox_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    project_id TEXT NOT NULL,
    queue_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    branch_id TEXT NOT NULL DEFAULT '',
    head_sha TEXT NOT NULL DEFAULT '',
    decision TEXT NOT NULL,
    followup_kind TEXT NOT NULL,
    continuation_task_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    decision_event_id TEXT NOT NULL REFERENCES runtime_events(event_id) ON DELETE CASCADE,
    decision_doc_key TEXT NOT NULL DEFAULT '',
    decision_summary TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(workspace_id, project_id, queue_id, item_id, decision)
);

CREATE INDEX IF NOT EXISTS idx_project_patch_queue_decision_continuation_pending
    ON project_patch_queue_decision_continuation_outbox(workspace_id, project_id, state, followup_kind, updated_at);

CREATE INDEX IF NOT EXISTS idx_project_patch_queue_decision_continuation_item
    ON project_patch_queue_decision_continuation_outbox(workspace_id, project_id, queue_id, item_id);
