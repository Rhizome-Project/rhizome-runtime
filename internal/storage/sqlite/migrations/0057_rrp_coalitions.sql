CREATE TABLE IF NOT EXISTS workspace_coalitions (
    coalition_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    tension_id TEXT NOT NULL REFERENCES workspace_tensions(tension_id) ON DELETE CASCADE,
    success_criterion TEXT NOT NULL,
    synergy_score REAL NOT NULL DEFAULT 0.0,
    ttl_epochs INTEGER NOT NULL DEFAULT 3,
    status TEXT NOT NULL DEFAULT 'FORMING',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workspace_coalitions_workspace_tension
    ON workspace_coalitions(workspace_id, tension_id);

CREATE TABLE IF NOT EXISTS workspace_coalition_members (
    coalition_id TEXT NOT NULL REFERENCES workspace_coalitions(coalition_id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL,
    fit_score REAL NOT NULL DEFAULT 0.0,
    novelty_score REAL NOT NULL DEFAULT 0.0,
    min_stay_until_epoch INTEGER NOT NULL DEFAULT 0,
    joined_at TEXT NOT NULL,
    PRIMARY KEY (coalition_id, agent_id),
    FOREIGN KEY (workspace_id, agent_id) REFERENCES agents(workspace_id, agent_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_workspace_coalition_members_agent
    ON workspace_coalition_members(workspace_id, agent_id);
