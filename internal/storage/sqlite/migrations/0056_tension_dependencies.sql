CREATE TABLE IF NOT EXISTS workspace_tension_dependencies (
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    tension_id TEXT NOT NULL REFERENCES workspace_tensions(tension_id) ON DELETE CASCADE,
    depends_on_tension_id TEXT NOT NULL REFERENCES workspace_tensions(tension_id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    PRIMARY KEY (tension_id, depends_on_tension_id)
);

CREATE INDEX IF NOT EXISTS idx_workspace_tension_deps_workspace
    ON workspace_tension_dependencies(workspace_id, tension_id);
