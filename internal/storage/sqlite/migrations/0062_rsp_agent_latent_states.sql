CREATE TABLE IF NOT EXISTS rsp_agent_latent_states (
    workspace_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    state_focused REAL NOT NULL DEFAULT 0.0,
    state_exploring REAL NOT NULL DEFAULT 0.0,
    state_saturated REAL NOT NULL DEFAULT 0.0,
    state_thrashing REAL NOT NULL DEFAULT 0.0,
    state_ungrounded REAL NOT NULL DEFAULT 0.0,
    state_idle REAL NOT NULL DEFAULT 1.0,
    state_recovering REAL NOT NULL DEFAULT 0.0,
    last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    risk_score REAL NOT NULL DEFAULT 0.0,
    PRIMARY KEY(workspace_id, agent_id)
);
