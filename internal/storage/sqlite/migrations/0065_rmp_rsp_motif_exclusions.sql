-- 0065_rmp_rsp_motif_exclusions.sql
-- Enables rsp-1.2 motif detectors by providing a DB exclusion list for agents caught thrashing.

CREATE TABLE IF NOT EXISTS workspace_tension_exclusions (
    workspace_id TEXT NOT NULL,
    tension_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    expires_at DATETIME NOT NULL,
    reason TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, tension_id, agent_id),
    FOREIGN KEY (tension_id) REFERENCES workspace_tensions(tension_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_workspace_tension_exclusions_expiry
    ON workspace_tension_exclusions(workspace_id, expires_at);
