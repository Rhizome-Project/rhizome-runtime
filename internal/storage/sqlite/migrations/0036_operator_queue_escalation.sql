-- Operator queue escalation metadata for overdue/SLA workflows.

ALTER TABLE operator_queue_items ADD COLUMN escalation_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE operator_queue_items ADD COLUMN last_escalated_at TEXT;
ALTER TABLE operator_queue_items ADD COLUMN last_escalated_by TEXT;
ALTER TABLE operator_queue_items ADD COLUMN escalation_reason TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_operator_queue_items_workspace_due_status
    ON operator_queue_items(workspace_id, status, due_at, escalation_count DESC, updated_at DESC);
