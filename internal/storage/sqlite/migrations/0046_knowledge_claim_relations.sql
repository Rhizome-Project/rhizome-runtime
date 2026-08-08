CREATE TABLE IF NOT EXISTS knowledge_claim_relations (
    relation_id      TEXT PRIMARY KEY,
    workspace_id     TEXT NOT NULL,
    from_claim_id    TEXT NOT NULL REFERENCES knowledge_claims(claim_id) ON DELETE CASCADE,
    to_claim_id      TEXT NOT NULL REFERENCES knowledge_claims(claim_id) ON DELETE CASCADE,
    relation_type    TEXT NOT NULL,
    weight           REAL NOT NULL DEFAULT 1,
    source_kind      TEXT NOT NULL DEFAULT 'knowledge_claim',
    source_id        TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    updated_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    UNIQUE (workspace_id, from_claim_id, to_claim_id, relation_type)
);

CREATE INDEX IF NOT EXISTS idx_claim_relations_from
    ON knowledge_claim_relations(workspace_id, from_claim_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_claim_relations_to
    ON knowledge_claim_relations(workspace_id, to_claim_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_claim_relations_type
    ON knowledge_claim_relations(workspace_id, relation_type, updated_at DESC);
