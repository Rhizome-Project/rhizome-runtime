CREATE TABLE IF NOT EXISTS workspace_cluster_control_state (
    workspace_id               TEXT NOT NULL REFERENCES workspaces(workspace_id),
    proto_cluster_id           TEXT NOT NULL,
    resolution_kind           TEXT NOT NULL DEFAULT '',
    corridor_profile          TEXT NOT NULL DEFAULT '',
    epoch                     INTEGER NOT NULL DEFAULT 0,
    current_mode              TEXT NOT NULL DEFAULT 'STEADY',
    candidate_mode            TEXT NOT NULL DEFAULT 'STEADY',
    candidate_streak          INTEGER NOT NULL DEFAULT 0,
    dominant_violation_kind   TEXT NOT NULL DEFAULT '',
    dominant_violation_score  REAL NOT NULL DEFAULT 0,
    attention_band            TEXT NOT NULL DEFAULT 'STEADY',
    pressure_score            INTEGER NOT NULL DEFAULT 0,
    confirmed_tension_count   INTEGER NOT NULL DEFAULT 0,
    pending_tension_count     INTEGER NOT NULL DEFAULT 0,
    task_ids_json             TEXT NOT NULL DEFAULT '[]',
    session_ids_json          TEXT NOT NULL DEFAULT '[]',
    doc_keys_json             TEXT NOT NULL DEFAULT '[]',
    artifact_refs_json        TEXT NOT NULL DEFAULT '[]',
    agent_ids_json            TEXT NOT NULL DEFAULT '[]',
    confirmed_tension_ids_json TEXT NOT NULL DEFAULT '[]',
    pending_tension_ids_json  TEXT NOT NULL DEFAULT '[]',
    control_vector_json       TEXT NOT NULL DEFAULT '{}',
    violation_vector_json     TEXT NOT NULL DEFAULT '{}',
    summary                   TEXT NOT NULL DEFAULT '',
    last_basis_at             TEXT NOT NULL DEFAULT '',
    last_tick_event_id        TEXT NOT NULL DEFAULT '',
    last_tick_at              TEXT NOT NULL DEFAULT '',
    last_transition_at        TEXT NOT NULL DEFAULT '',
    created_at                TEXT NOT NULL,
    updated_at                TEXT NOT NULL,
    PRIMARY KEY (workspace_id, proto_cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_workspace_cluster_control_state_workspace_mode
    ON workspace_cluster_control_state(workspace_id, current_mode, updated_at DESC, proto_cluster_id DESC);

CREATE INDEX IF NOT EXISTS idx_workspace_cluster_control_state_workspace_pressure
    ON workspace_cluster_control_state(workspace_id, pressure_score DESC, updated_at DESC, proto_cluster_id DESC);
