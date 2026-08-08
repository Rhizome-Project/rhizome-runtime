CREATE TABLE IF NOT EXISTS workspace_replica_transport_state (
    workspace_id TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'workspace',
    replica_authority_node_id TEXT NOT NULL,
    leader_authority_node_id TEXT NOT NULL,
    authority_term INTEGER NOT NULL,
    exported_head_commit_watermark INTEGER NOT NULL DEFAULT 0,
    fetched_through_commit_watermark INTEGER NOT NULL DEFAULT 0,
    acknowledged_commit_watermark INTEGER NOT NULL DEFAULT 0,
    last_fetch_at TEXT NOT NULL DEFAULT '',
    last_acknowledged_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (workspace_id, scope, replica_authority_node_id),
    CHECK (authority_term > 0),
    CHECK (leader_authority_node_id <> replica_authority_node_id),
    CHECK (exported_head_commit_watermark >= 0),
    CHECK (fetched_through_commit_watermark >= 0),
    CHECK (acknowledged_commit_watermark >= 0),
    CHECK (fetched_through_commit_watermark <= exported_head_commit_watermark),
    CHECK (acknowledged_commit_watermark <= fetched_through_commit_watermark)
);

CREATE INDEX IF NOT EXISTS idx_workspace_replica_transport_state_workspace
ON workspace_replica_transport_state(workspace_id, scope, authority_term DESC, updated_at DESC, replica_authority_node_id);

CREATE INDEX IF NOT EXISTS idx_workspace_replica_transport_state_leader_term
ON workspace_replica_transport_state(leader_authority_node_id, authority_term DESC, updated_at DESC, workspace_id, scope);
