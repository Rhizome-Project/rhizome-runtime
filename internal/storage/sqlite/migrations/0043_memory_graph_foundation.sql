CREATE TABLE IF NOT EXISTS memory_nodes (
    memory_id             TEXT PRIMARY KEY,
    workspace_id          TEXT NOT NULL REFERENCES workspaces(workspace_id),
    memory_type           TEXT NOT NULL,
    compat_type           TEXT NOT NULL DEFAULT '',
    visibility            TEXT NOT NULL,
    memory_layer          TEXT NOT NULL,
    epistemic_status      TEXT NOT NULL,
    lifecycle_state       TEXT NOT NULL,
    origin_kind           TEXT NOT NULL,
    origin_id             TEXT NOT NULL,
    source_kind           TEXT NOT NULL DEFAULT '',
    source_id             TEXT NOT NULL DEFAULT '',
    agent_id              TEXT NOT NULL DEFAULT '',
    session_id            TEXT NOT NULL DEFAULT '',
    task_id               TEXT NOT NULL DEFAULT '',
    title                 TEXT NOT NULL DEFAULT '',
    body                  TEXT NOT NULL DEFAULT '',
    summary               TEXT NOT NULL DEFAULT '',
    claim_subject         TEXT NOT NULL DEFAULT '',
    claim_predicate       TEXT NOT NULL DEFAULT '',
    claim_object          TEXT NOT NULL DEFAULT '',
    claim_qualifiers_json TEXT NOT NULL DEFAULT '{}',
    claim_time_scope_json TEXT NOT NULL DEFAULT '{}',
    claim_modality        TEXT NOT NULL DEFAULT '',
    source_set_json       TEXT NOT NULL DEFAULT '[]',
    provenance_json       TEXT NOT NULL DEFAULT '[]',
    temperature           REAL NOT NULL DEFAULT 0,
    importance            REAL NOT NULL DEFAULT 0,
    confidence            REAL NOT NULL DEFAULT 0,
    activation            REAL NOT NULL DEFAULT 0,
    drift                 REAL NOT NULL DEFAULT 0,
    volatility            REAL NOT NULL DEFAULT 0,
    pin_strength          REAL NOT NULL DEFAULT 0,
    archived_at           TEXT,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_nodes_workspace_origin
    ON memory_nodes(workspace_id, origin_kind, origin_id);
CREATE INDEX IF NOT EXISTS idx_memory_nodes_workspace_updated
    ON memory_nodes(workspace_id, updated_at DESC, memory_id DESC);
CREATE INDEX IF NOT EXISTS idx_memory_nodes_workspace_type
    ON memory_nodes(workspace_id, memory_type, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_memory_nodes_workspace_layer
    ON memory_nodes(workspace_id, memory_layer, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_memory_nodes_workspace_task
    ON memory_nodes(workspace_id, task_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_memory_nodes_workspace_session
    ON memory_nodes(workspace_id, session_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_memory_nodes_workspace_lifecycle
    ON memory_nodes(workspace_id, lifecycle_state, updated_at DESC);

CREATE TABLE IF NOT EXISTS memory_node_refs (
    memory_id      TEXT NOT NULL REFERENCES memory_nodes(memory_id) ON DELETE CASCADE,
    workspace_id   TEXT NOT NULL REFERENCES workspaces(workspace_id),
    ref_kind       TEXT NOT NULL,
    ref_id         TEXT NOT NULL,
    ref_role       TEXT NOT NULL DEFAULT '',
    ref_value      TEXT NOT NULL DEFAULT '',
    weight         REAL NOT NULL DEFAULT 1,
    metadata_json  TEXT NOT NULL DEFAULT '{}',
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (memory_id, ref_kind, ref_id, ref_role)
);

CREATE INDEX IF NOT EXISTS idx_memory_node_refs_workspace_kind_id
    ON memory_node_refs(workspace_id, ref_kind, ref_id, memory_id);

CREATE TABLE IF NOT EXISTS memory_node_versions (
    memory_id      TEXT NOT NULL REFERENCES memory_nodes(memory_id) ON DELETE CASCADE,
    workspace_id   TEXT NOT NULL REFERENCES workspaces(workspace_id),
    ref_kind       TEXT NOT NULL,
    ref_id         TEXT NOT NULL,
    version_token  TEXT NOT NULL DEFAULT '',
    weight         REAL NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (memory_id, ref_kind, ref_id)
);

CREATE INDEX IF NOT EXISTS idx_memory_node_versions_workspace_ref
    ON memory_node_versions(workspace_id, ref_kind, ref_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS memory_edges (
    edge_id         TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL REFERENCES workspaces(workspace_id),
    from_memory_id  TEXT NOT NULL,
    to_memory_id    TEXT NOT NULL,
    edge_type       TEXT NOT NULL,
    source_kind     TEXT NOT NULL DEFAULT 'system',
    source_id       TEXT NOT NULL DEFAULT '',
    weight          REAL NOT NULL DEFAULT 1,
    metadata_json   TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE (workspace_id, from_memory_id, to_memory_id, edge_type, source_kind, source_id)
);

CREATE INDEX IF NOT EXISTS idx_memory_edges_workspace_from
    ON memory_edges(workspace_id, from_memory_id, edge_type, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_memory_edges_workspace_to
    ON memory_edges(workspace_id, to_memory_id, edge_type, updated_at DESC);
