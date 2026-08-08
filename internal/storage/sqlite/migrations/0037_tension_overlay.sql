CREATE TABLE IF NOT EXISTS workspace_tensions (
    tension_id          TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    proto_cluster_id    TEXT NOT NULL,
    tension_type        TEXT NOT NULL,
    lifecycle_state     TEXT NOT NULL DEFAULT 'ACTIVE',
    review_status       TEXT NOT NULL DEFAULT 'PENDING',
    title               TEXT NOT NULL DEFAULT '',
    summary             TEXT NOT NULL DEFAULT '',
    anchor_kind         TEXT NOT NULL DEFAULT '',
    anchor_ref          TEXT NOT NULL DEFAULT '',
    task_ids_json       TEXT NOT NULL DEFAULT '[]',
    session_ids_json    TEXT NOT NULL DEFAULT '[]',
    doc_keys_json       TEXT NOT NULL DEFAULT '[]',
    artifact_refs_json  TEXT NOT NULL DEFAULT '[]',
    segment_refs_json   TEXT NOT NULL DEFAULT '[]',
    agent_ids_json      TEXT NOT NULL DEFAULT '[]',
    constraint_refs_json TEXT NOT NULL DEFAULT '[]',
    base_score          INTEGER NOT NULL DEFAULT 0,
    surface_score       INTEGER NOT NULL DEFAULT 0,
    evidence_count      INTEGER NOT NULL DEFAULT 0,
    last_seen_event_id  TEXT NOT NULL DEFAULT '',
    last_seen_at        TEXT NOT NULL DEFAULT '',
    confirmed_by        TEXT NOT NULL DEFAULT '',
    archived_by         TEXT NOT NULL DEFAULT '',
    dismissed_reason    TEXT NOT NULL DEFAULT '',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workspace_tensions_workspace_surface
    ON workspace_tensions(workspace_id, lifecycle_state, review_status, surface_score DESC, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_workspace_tensions_workspace_type
    ON workspace_tensions(workspace_id, tension_type, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_workspace_tensions_workspace_cluster
    ON workspace_tensions(workspace_id, proto_cluster_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS workspace_tension_evidence (
    tension_id      TEXT NOT NULL REFERENCES workspace_tensions(tension_id) ON DELETE CASCADE,
    workspace_id    TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    evidence_kind   TEXT NOT NULL,
    evidence_ref    TEXT NOT NULL,
    event_id        TEXT NOT NULL DEFAULT '',
    weight          INTEGER NOT NULL DEFAULT 1,
    summary         TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL,
    PRIMARY KEY (tension_id, evidence_kind, evidence_ref, event_id)
);

CREATE INDEX IF NOT EXISTS idx_workspace_tension_evidence_workspace_tension
    ON workspace_tension_evidence(workspace_id, tension_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_workspace_tension_evidence_workspace_event
    ON workspace_tension_evidence(workspace_id, event_id, created_at DESC);
