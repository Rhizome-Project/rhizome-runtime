CREATE TABLE IF NOT EXISTS memory_promotion_queue (
    promotion_id       TEXT PRIMARY KEY,
    workspace_id       TEXT NOT NULL,
    queue_key          TEXT NOT NULL,
    state              TEXT NOT NULL,
    candidate_kind     TEXT NOT NULL,
    candidate_type     TEXT NOT NULL,
    target_memory_id   TEXT NOT NULL,
    candidate_json     TEXT NOT NULL,
    basis_digest       TEXT NOT NULL,
    basis_refs_json    TEXT NOT NULL DEFAULT '[]',
    proposed_by        TEXT NOT NULL,
    resolution_note    TEXT NOT NULL DEFAULT '',
    applied_kind       TEXT NOT NULL DEFAULT '',
    applied_id         TEXT NOT NULL DEFAULT '',
    resolved_at        TEXT,
    resolved_by        TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_promotion_queue_workspace_key
    ON memory_promotion_queue(workspace_id, queue_key);

CREATE INDEX IF NOT EXISTS idx_memory_promotion_queue_workspace_state
    ON memory_promotion_queue(workspace_id, state, updated_at DESC, promotion_id DESC);

CREATE INDEX IF NOT EXISTS idx_memory_promotion_queue_workspace_candidate
    ON memory_promotion_queue(workspace_id, candidate_kind, candidate_type, updated_at DESC);
