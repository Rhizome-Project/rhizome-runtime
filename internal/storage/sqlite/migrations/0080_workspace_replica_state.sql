CREATE TABLE IF NOT EXISTS workspace_replica_state (
    workspace_id               TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    scope                      TEXT NOT NULL DEFAULT 'workspace',
    replica_authority_node_id  TEXT NOT NULL REFERENCES runtime_nodes(authority_node_id) ON DELETE CASCADE,
    replica_role               TEXT NOT NULL,
    membership_state           TEXT NOT NULL,
    leader_authority_node_id   TEXT REFERENCES runtime_nodes(authority_node_id) ON DELETE SET NULL,
    authority_term             INTEGER NOT NULL DEFAULT 0,
    commit_watermark           INTEGER NOT NULL DEFAULT 0,
    applied_watermark          INTEGER NOT NULL DEFAULT 0,
    last_fetch_at              TEXT NOT NULL DEFAULT '',
    last_apply_at              TEXT NOT NULL DEFAULT '',
    membership_reason          TEXT NOT NULL DEFAULT '',
    updated_at                 TEXT NOT NULL,
    CHECK (replica_role IN ('HOLDER', 'FOLLOWER')),
    CHECK (membership_state IN ('PROVISIONAL', 'ACTIVE', 'CATCHING_UP', 'STALE', 'REJOIN_PENDING', 'REJECTED', 'DISBANDED')),
    CHECK (authority_term >= 0),
    CHECK (commit_watermark >= 0),
    CHECK (applied_watermark >= 0),
    CHECK (applied_watermark <= commit_watermark),
    PRIMARY KEY (workspace_id, scope, replica_authority_node_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_workspace_replica_state_unique_holder
    ON workspace_replica_state(workspace_id, scope)
    WHERE replica_role = 'HOLDER';

CREATE INDEX IF NOT EXISTS idx_workspace_replica_state_workspace_state
    ON workspace_replica_state(workspace_id, scope, membership_state, replica_role, updated_at DESC, replica_authority_node_id);

CREATE INDEX IF NOT EXISTS idx_workspace_replica_state_leader_term
    ON workspace_replica_state(leader_authority_node_id, authority_term DESC, updated_at DESC, workspace_id, scope)
    WHERE leader_authority_node_id IS NOT NULL;
