-- 0059_rsp_shadow_telemetry.sql
-- RSP-1.2 Shadow Mode: Belief and Anomaly Telemetry
-- These tables are intentionally append-only logs ("firehose sink") used for retrospective Brier scoring and parameter tuning.
-- They DO NOT constitute Ground Truth or RRP bounds.

CREATE TABLE rsp_belief_telemetry (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    prior_b_m REAL NOT NULL,
    posterior_b_m REAL NOT NULL,
    uncertainty_u_m REAL NOT NULL,
    evidence_mass REAL NOT NULL,
    drift_score REAL NOT NULL,
    measured_at TEXT NOT NULL
);

CREATE INDEX idx_rsp_belief_telemetry_workspace ON rsp_belief_telemetry(workspace_id, entity_type, entity_id);

CREATE TABLE rsp_anomaly_telemetry (
    alert_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    cluster_mode TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    mu_hat REAL NOT NULL,
    sigma_hat REAL NOT NULL,
    current_value REAL NOT NULL,
    ewma_value REAL NOT NULL,
    source_diversity_discount REAL NOT NULL,
    alert_type TEXT NOT NULL,
    detected_at TEXT NOT NULL
);

CREATE INDEX idx_rsp_anomaly_telemetry_workspace ON rsp_anomaly_telemetry(workspace_id, alert_type);
