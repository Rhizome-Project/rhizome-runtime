CREATE TABLE IF NOT EXISTS workspace_replica_apply_state (
    workspace_id TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'workspace',
    replica_authority_node_id TEXT NOT NULL,
    leader_authority_node_id TEXT NOT NULL,
    authority_term INTEGER NOT NULL,
    apply_status TEXT NOT NULL DEFAULT 'IDLE',
    exported_head_commit_watermark INTEGER NOT NULL DEFAULT 0,
    attempted_through_commit_watermark INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    last_failure_at TEXT NOT NULL DEFAULT '',
    last_failure_reason TEXT NOT NULL DEFAULT '',
    next_retry_at TEXT NOT NULL DEFAULT '',
    dead_lettered_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (workspace_id, scope, replica_authority_node_id),
    CONSTRAINT chk_workspace_replica_apply_state_term_positive CHECK (authority_term > 0),
    CONSTRAINT chk_workspace_replica_apply_state_status CHECK (apply_status IN ('IDLE', 'RETRY_PENDING', 'DEAD_LETTER')),
    CONSTRAINT chk_workspace_replica_apply_state_nodes_distinct CHECK (leader_authority_node_id <> replica_authority_node_id),
    CONSTRAINT chk_workspace_replica_apply_state_exported_nonnegative CHECK (exported_head_commit_watermark >= 0),
    CONSTRAINT chk_workspace_replica_apply_state_attempted_nonnegative CHECK (attempted_through_commit_watermark >= 0),
    CONSTRAINT chk_workspace_replica_apply_state_attempted_le_exported CHECK (attempted_through_commit_watermark <= exported_head_commit_watermark),
    CONSTRAINT chk_workspace_replica_apply_state_failure_count_nonnegative CHECK (failure_count >= 0),
    CONSTRAINT chk_workspace_replica_apply_state_retry_fields CHECK (
        (apply_status = 'IDLE' AND next_retry_at = '' AND dead_lettered_at = '')
        OR (apply_status = 'RETRY_PENDING' AND next_retry_at <> '' AND dead_lettered_at = '')
        OR (apply_status = 'DEAD_LETTER' AND dead_lettered_at <> '')
    )
);

CREATE INDEX IF NOT EXISTS idx_workspace_replica_apply_state_workspace
    ON workspace_replica_apply_state(workspace_id, scope, authority_term DESC, updated_at DESC);
