CREATE TABLE IF NOT EXISTS workspace_effective_controls (
    workspace_id             TEXT NOT NULL REFERENCES workspaces(workspace_id),
    proto_cluster_id         TEXT NOT NULL DEFAULT '',
    epoch                    INTEGER NOT NULL DEFAULT 0,
    ttl_seconds              INTEGER NOT NULL DEFAULT 0,
    expires_at               TEXT NOT NULL DEFAULT '',
    control_mode             TEXT NOT NULL DEFAULT '',
    candidate_mode           TEXT NOT NULL DEFAULT '',
    candidate_controls_json  TEXT NOT NULL DEFAULT '{}',
    advisory_controls_json   TEXT NOT NULL DEFAULT '{}',
    effective_controls_json  TEXT NOT NULL DEFAULT '{}',
    resolved_from            TEXT NOT NULL DEFAULT '',
    match_score              INTEGER NOT NULL DEFAULT 0,
    basis_summary            TEXT NOT NULL DEFAULT '',
    generated_at             TEXT NOT NULL DEFAULT '',
    actor_id                 TEXT NOT NULL DEFAULT '',
    created_at               TEXT NOT NULL,
    updated_at               TEXT NOT NULL,
    PRIMARY KEY (workspace_id, proto_cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_workspace_effective_controls_workspace_expiry
    ON workspace_effective_controls(workspace_id, expires_at, updated_at DESC, proto_cluster_id DESC);
