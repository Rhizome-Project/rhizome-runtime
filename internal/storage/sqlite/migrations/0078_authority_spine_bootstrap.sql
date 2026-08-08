CREATE TABLE IF NOT EXISTS runtime_nodes (
    authority_node_id TEXT PRIMARY KEY,
    node_kind         TEXT NOT NULL DEFAULT '',
    host_label        TEXT NOT NULL DEFAULT '',
    boot_instance_id  TEXT NOT NULL DEFAULT '',
    registered_at     TEXT NOT NULL,
    last_seen_at      TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_runtime_nodes_status_seen
    ON runtime_nodes(status, last_seen_at DESC, authority_node_id);

CREATE TABLE IF NOT EXISTS workspace_authority (
    workspace_id             TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    scope                    TEXT NOT NULL DEFAULT 'workspace',
    holder_authority_node_id TEXT REFERENCES runtime_nodes(authority_node_id) ON DELETE SET NULL,
    lease_token              TEXT NOT NULL DEFAULT '',
    term                     INTEGER NOT NULL DEFAULT 0,
    lease_expires_at         TEXT NOT NULL DEFAULT '',
    commit_watermark         INTEGER NOT NULL DEFAULT 0,
    applied_watermark        INTEGER NOT NULL DEFAULT 0,
    status                   TEXT NOT NULL DEFAULT 'EXPIRED',
    updated_at               TEXT NOT NULL,
    PRIMARY KEY (workspace_id, scope)
);

CREATE INDEX IF NOT EXISTS idx_workspace_authority_holder_status
    ON workspace_authority(holder_authority_node_id, status, updated_at DESC, workspace_id, scope);

CREATE INDEX IF NOT EXISTS idx_workspace_authority_status_term
    ON workspace_authority(status, term DESC, updated_at DESC, workspace_id, scope);
