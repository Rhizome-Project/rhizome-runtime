-- 0064_rmp_rsp_appendix.sql
-- RMP-1.2: Semantic Garbage Collector and RSP-1.2: Predictive Layer Core

CREATE TABLE IF NOT EXISTS memory_node_salience (
    memory_id TEXT PRIMARY KEY REFERENCES memory_nodes(memory_id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    a_i REAL NOT NULL DEFAULT 1.0,
    t_i_star TEXT NOT NULL,
    t_i_acc TEXT NOT NULL,
    n_i INTEGER NOT NULL DEFAULT 0,
    q_i REAL NOT NULL DEFAULT 0.0,
    h_i REAL NOT NULL,
    t_hot TEXT NOT NULL,
    t_warm TEXT NOT NULL,
    t_gc TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now'))
);

CREATE INDEX IF NOT EXISTS idx_memory_node_salience_workspace
    ON memory_node_salience(workspace_id, memory_id);

CREATE INDEX IF NOT EXISTS idx_memory_node_salience_gc
    ON memory_node_salience(workspace_id, t_gc);

CREATE TABLE IF NOT EXISTS rsp_forecast_state (
    workspace_id TEXT NOT NULL,
    proto_cluster_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    l_k REAL NOT NULL DEFAULT 0.0,
    b_k REAL NOT NULL DEFAULT 0.0,
    v_k REAL NOT NULL DEFAULT 0.0,
    sigma_k REAL NOT NULL DEFAULT 0.0,
    p_k REAL NOT NULL DEFAULT 0.0,
    alpha_k REAL NOT NULL DEFAULT 0.1,
    beta_k REAL NOT NULL DEFAULT 0.05,
    last_y REAL NOT NULL DEFAULT 0.0,
    last_y_tilde REAL NOT NULL DEFAULT 0.0,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    PRIMARY KEY (workspace_id, proto_cluster_id, agent_id, metric_name)
);

CREATE TABLE IF NOT EXISTS rsp_anomaly_baseline (
    workspace_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    task_class TEXT NOT NULL,
    mode TEXT NOT NULL,
    phase TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    mu_hat REAL NOT NULL DEFAULT 0.0,
    sigma_hat REAL NOT NULL DEFAULT 1.0,
    sample_size INTEGER NOT NULL DEFAULT 0,
    last_healthy_window_at TEXT NOT NULL,
    PRIMARY KEY (workspace_id, agent_id, task_class, mode, phase, metric_name)
);
