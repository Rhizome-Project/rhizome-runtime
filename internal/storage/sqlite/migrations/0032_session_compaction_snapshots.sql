CREATE TABLE IF NOT EXISTS session_compaction_snapshots (
    snapshot_id               TEXT PRIMARY KEY,
    session_id                TEXT NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
    workspace_id              TEXT NOT NULL,
    agent_id                  TEXT NOT NULL,
    trigger_kind              TEXT NOT NULL,
    token_budget              INTEGER NOT NULL DEFAULT 0,
    message_count_before      INTEGER NOT NULL DEFAULT 0,
    message_count_after       INTEGER NOT NULL DEFAULT 0,
    message_tokens_before     INTEGER NOT NULL DEFAULT 0,
    message_tokens_after      INTEGER NOT NULL DEFAULT 0,
    total_input_tokens        INTEGER NOT NULL DEFAULT 0,
    total_output_tokens       INTEGER NOT NULL DEFAULT 0,
    summary_text              TEXT NOT NULL DEFAULT '',
    summary_workspace_memory  TEXT,
    created_at                TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now'))
);

CREATE INDEX IF NOT EXISTS idx_session_compaction_snapshots_workspace_id
    ON session_compaction_snapshots(workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_session_compaction_snapshots_session_id
    ON session_compaction_snapshots(session_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_session_compaction_snapshots_agent_id
    ON session_compaction_snapshots(agent_id, created_at DESC);
