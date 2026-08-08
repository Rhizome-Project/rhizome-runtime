CREATE TABLE IF NOT EXISTS workspace_replica_install_state (
    workspace_id TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'workspace',
    replica_authority_node_id TEXT NOT NULL,
    leader_authority_node_id TEXT NOT NULL,
    authority_term INTEGER NOT NULL,
    install_status TEXT NOT NULL,
    base_commit_watermark INTEGER NOT NULL DEFAULT 0,
    install_started_at TEXT NOT NULL DEFAULT '',
    install_completed_at TEXT NOT NULL DEFAULT '',
    install_reason TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (workspace_id, scope, replica_authority_node_id),
    CHECK (install_status IN ('PENDING', 'INSTALLED')),
    CHECK (authority_term > 0),
    CHECK (base_commit_watermark >= 0),
    CHECK (leader_authority_node_id <> replica_authority_node_id),
    CHECK (
        (install_status = 'PENDING' AND install_completed_at = '') OR
        (install_status = 'INSTALLED' AND install_completed_at <> '')
    )
);

CREATE INDEX IF NOT EXISTS idx_workspace_replica_install_state_workspace_status
ON workspace_replica_install_state(workspace_id, scope, install_status, authority_term DESC, updated_at DESC, replica_authority_node_id);

CREATE INDEX IF NOT EXISTS idx_workspace_replica_install_state_leader_term
ON workspace_replica_install_state(leader_authority_node_id, authority_term DESC, updated_at DESC, workspace_id, scope);
