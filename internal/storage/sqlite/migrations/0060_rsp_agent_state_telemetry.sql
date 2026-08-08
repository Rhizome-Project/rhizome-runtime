-- 0060_rsp_agent_state_telemetry.sql
-- RSP-1.2 Motif Detectors & Agent State Estimator

CREATE TABLE rsp_agent_state_telemetry (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    cache_pressure_i REAL NOT NULL,
    stale_hit_i REAL NOT NULL,
    thrashing_risk REAL NOT NULL,
    ungrounded_risk REAL NOT NULL,
    measured_at TEXT NOT NULL
);

CREATE INDEX idx_rsp_agent_state_workspace_agent ON rsp_agent_state_telemetry(workspace_id, agent_id);
